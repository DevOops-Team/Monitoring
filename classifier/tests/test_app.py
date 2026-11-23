"""웹훅 JSON을 넣으면 이벤트가 나오는지 끝에서 끝까지 본다 (Kafka는 가짜 프로듀서)."""
import json
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from classifier.app import create_app, pipeline_from_env
from classifier.event import Context
from classifier.pipeline import Pipeline
from classifier.publish import Publisher, validate
from classifier.rules import Rules

GPU = "GPU-b850f46d-d5ea-c752-ddf3-c4453e44d3f7"


class FakeProducer:
    def __init__(self):
        self.sent, self.fail = [], False

    def produce(self, topic, *, key, value, headers, on_delivery):
        if not self.fail:
            self.sent.append(json.loads(value))
        on_delivery("broker down" if self.fail else None, None)

    def flush(self, timeout):
        return 0


def ts(seconds_ago: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(seconds=seconds_ago)).strftime("%Y-%m-%dT%H:%M:%SZ")


def alert(event_ts: str, **labels) -> dict:
    base = {"alertname": "GpuXid", "pipeline": "failover", "signal": "xid", "source": "dmesg",
            "cluster": "eks-a", "node": "ip-10-0-1-23", "gpu_uuid": GPU, "xid": "79"}
    return {"status": "firing", "labels": {**base, **labels},
            "annotations": {"event_ts": event_ts, "log_line": "NVRM: Xid (PCI:0000:00:1e): 79"}}


@pytest.fixture
def env():
    fake = FakeProducer()
    healthy = Context(az="us-east-2a", collector_healthy=True, job_id="3f9a1c2e", generation=3)
    pipeline = Pipeline(Rules.load(), Publisher(fake, "abc1234"), lambda s: healthy)
    return TestClient(create_app(pipeline)), fake


def post(client, *alerts):
    return client.post("/alerts", json={"version": "4", "status": "firing", "alerts": list(alerts)})


def test_webhook_publishes_one_event(env):
    client, fake = env
    r = post(client, alert(ts(30)))
    assert (r.status_code, r.json()) == (200, {"published": 1})
    payload = fake.sent[0]["payload"]
    validate(payload)
    assert (fake.sent[0]["type"], payload["category"], payload["suggested_action"]) == (
        "fault.detected", "hw", "isolate_migrate")
    assert (payload["job_id"], payload["generation"], payload["az"]) == ("3f9a1c2e", 3, "us-east-2a")


def test_resend_is_duplicate(env):
    client, fake = env
    a = alert(ts(30))
    post(client, a)
    assert post(client, a).json() == {"duplicate": 1}
    assert len(fake.sent) == 1


def test_two_paths_one_event(env):
    """같은 Xid가 두 경로로 오면 한 사건. action이 같으니 두 번째는 중복이다."""
    client, fake = env
    r = post(client, alert(ts(30), source="dcgm"), alert(ts(59)))
    assert r.json() == {"published": 1, "duplicate": 1}


def test_mixed_batch(env):
    client, fake = env
    r = post(client,
             alert(ts(30)),                                   # 발행
             alert(ts(30), xid="45"),                         # 무시
             alert(ts(30), pipeline="other"),                 # 계약 위반
             {**alert(ts(30)), "status": "resolved"},         # 꺼짐
             alert(ts(30), xid="999", gpu_uuid="GPU-aaaa"))   # 표에 없음 → hold
    assert r.json() == {"published": 2, "ignored": 1, "rejected": 1, "resolved": 1}
    assert [e["type"] for e in fake.sent] == ["fault.detected", "fault.held"]


def test_publish_failure_returns_503_and_retry_succeeds(env):
    client, fake = env
    a = alert(ts(30))
    fake.fail = True
    assert post(client, a).status_code == 503
    fake.fail = False
    assert post(client, a).json() == {"published": 1}        # 실패한 것은 기억하지 않아 다시 나간다


def test_bad_body(env):
    client, _ = env
    assert client.post("/alerts", json={"alerts": "x"}).status_code == 400


def test_healthz_and_metrics(env):
    client, _ = env
    post(client, alert(ts(30)), alert(ts(30), xid="999", gpu_uuid="GPU-aaaa"), alert(ts(30), signal="nope"))
    assert client.get("/healthz").json() == {"status": "ok"}
    text = client.get("/metrics").text
    assert 'classifier_alerts_total{outcome="published"} 2.0' in text
    assert 'classifier_events_published_total{action="isolate_migrate",category="hw",type="fault.detected"} 1.0' in text
    assert "classifier_unmatched_total 1.0" in text
    assert "classifier_signals_rejected_total 1.0" in text
    assert "classifier_lifecycle_healthy 0.0" in text       # 읽는 쪽을 붙이지 않은 상태
    assert "classifier_pending_pod_exits 0.0" in text
    assert "classifier_leader 1.0" in text                  # 선출 없이 혼자 도는 상태


def test_default_context_holds_everything(monkeypatch, capsys):
    """MIMIR_URL 없음: 수집 경로를 확인할 수 없으니 hold로만 나간다."""
    for name in ("KAFKA_BOOTSTRAP", "REQUIRE_KAFKA", "MIMIR_URL"):
        monkeypatch.delenv(name, raising=False)
    client = TestClient(create_app())
    assert post(client, alert(ts(30))).json() == {"published": 1}
    out = json.loads(capsys.readouterr().out)
    assert (out["value"]["type"], out["value"]["payload"]["suggested_action"]) == ("fault.held", "hold")


def test_require_kafka(monkeypatch):
    monkeypatch.delenv("KAFKA_BOOTSTRAP", raising=False)
    monkeypatch.setenv("REQUIRE_KAFKA", "true")
    with pytest.raises(RuntimeError):
        pipeline_from_env()


def test_timer_flushes_waiting_pod_exit(env):
    """서버가 떠 있으면 타이머가 기다리던 Pod 종료 신호를 스스로 판정한다."""
    import time
    _, fake = env
    client = TestClient(create_app(Pipeline(Rules.load(), Publisher(fake, "abc1234"),
                                            lambda s: Context(az="us-east-2a", collector_healthy=True))))
    pod = alert(ts(28), signal="pod_exit", pod="job-3f9a1c2e-g0003-w-0-0", reason="Error")
    with client:                                    # with 안에서만 타이머가 돈다
        assert post(client, pod).json() == {"deferred": 1}
        deadline = time.monotonic() + 6
        while not fake.sent and time.monotonic() < deadline:
            time.sleep(0.2)
    assert [e["payload"]["source"] for e in fake.sent] == ["pod"]


def test_site_is_disabled_by_default(monkeypatch, capsys):
    """ENABLED_SIGNALS 기본값은 xid · pod_exit. 라우팅이 틀려 site가 와도 이벤트를 내지 않는다."""
    for name in ("KAFKA_BOOTSTRAP", "REQUIRE_KAFKA", "MIMIR_URL", "ENABLED_SIGNALS"):
        monkeypatch.delenv(name, raising=False)
    client = TestClient(create_app())
    assert post(client, alert(ts(30), signal="site", node="")).json() == {"disabled": 1}
    assert "fault." not in capsys.readouterr().out


def test_enabled_signals_env(monkeypatch, capsys):
    for name in ("KAFKA_BOOTSTRAP", "REQUIRE_KAFKA", "MIMIR_URL"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("ENABLED_SIGNALS", "xid, pod_exit ,site")
    client = TestClient(create_app())
    assert post(client, alert(ts(30), signal="site", node="")).json() == {"published": 1}


class HeldLease:
    """다른 Pod가 잡고 계속 갱신하는 Lease."""
    name = "classifier"

    def __init__(self, address="10.0.0.9:8080"):
        self.annotations = {"devoops/leader-address": address} if address else {}

    def get(self):
        return {"metadata": {"resourceVersion": "1", "annotations": self.annotations},
                "spec": {"holderIdentity": "other", "renewTime": ts(0), "leaseDurationSeconds": 15}}


class FreeLease:
    name, lease = "classifier", None

    def get(self):
        return self.lease

    def create(self, body):
        self.lease = {**body, "metadata": {"resourceVersion": "1"}}

    replace = create


def lease_app(fake, lease_api):
    pipeline = Pipeline(Rules.load(), Publisher(fake, "abc1234"), lambda s: Context(az="a", collector_healthy=True))
    return TestClient(create_app(pipeline, lease_api=lease_api))


def wait_for(condition, seconds=3.0):
    import time
    deadline = time.monotonic() + seconds
    while not condition() and time.monotonic() < deadline:
        time.sleep(0.05)
    assert condition()


def test_standby_forwards_to_leader(env, monkeypatch):
    """대기 쪽은 스스로 처리하지 않고 대장에게 넘겨 대장의 응답을 그대로 돌려준다."""
    from fastapi import Response
    from classifier import app as app_module
    _, fake = env
    seen = []

    def fake_forward(address, body):
        seen.append((address, len(body["alerts"])))
        return Response('{"published": 1}', media_type="application/json")

    monkeypatch.setattr(app_module, "forward", fake_forward)
    with lease_app(fake, HeldLease()) as client:
        wait_for(lambda: post(client, alert(ts(30))).status_code == 200)
        assert post(client, alert(ts(30))).json() == {"published": 1}
        assert client.get("/healthz").status_code == 200
        assert "classifier_leader 0.0" in client.get("/metrics").text
    assert seen[-1] == ("10.0.0.9:8080", 1) and fake.sent == []


def test_standby_without_leader_address_is_503(env):
    _, fake = env
    with lease_app(fake, HeldLease(address=None)) as client:
        assert post(client, alert(ts(30))).status_code == 503
    assert fake.sent == []


def test_forwarded_request_is_not_forwarded_again(env, monkeypatch):
    """넘겨받은 쪽이 대장이 아니면(방금 바뀜) 다시 넘기지 않고 503 → Alertmanager가 다시 보낸다."""
    from classifier import app as app_module
    _, fake = env
    monkeypatch.setattr(app_module, "forward", lambda *a: pytest.fail("다시 넘김"))
    with lease_app(fake, HeldLease()) as client:
        r = client.post("/alerts", json={"alerts": [alert(ts(30))]}, headers={app_module.FORWARDED: "1"})
        assert r.status_code == 503


def test_leader_handles_after_acquiring(env):
    _, fake = env
    api = FreeLease()
    with lease_app(fake, api) as client:
        wait_for(lambda: "classifier_leader 1.0" in client.get("/metrics").text)
        assert post(client, alert(ts(30))).json() == {"published": 1}
        assert api.lease["spec"]["holderIdentity"] != ""
    assert api.lease["spec"]["holderIdentity"] == ""        # 정상 종료 때 Lease를 비운다


def test_forward_relays_leader_response(monkeypatch):
    """forward(): 대장의 상태 코드 · 본문을 그대로, 못 닿으면 503."""
    import threading
    from http.server import BaseHTTPRequestHandler, HTTPServer
    from fastapi import HTTPException
    from classifier.app import FORWARDED, forward

    followed = []

    class Leader(BaseHTTPRequestHandler):
        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            if body["alerts"] == ["redirect"]:
                self.send_response(302)
                self.send_header("Location", f"http://127.0.0.1:{self.server.server_port}/elsewhere")
                self.end_headers()
                return
            code = 503 if body["alerts"] == ["fail"] else 200
            assert self.headers[FORWARDED] == "1" and self.path == "/alerts"
            self.send_response(code)
            self.end_headers()
            self.wfile.write(b'{"published": 1}' if code == 200 else b'{"detail": {"publish_failed": 1}}')

        def do_GET(self):
            followed.append(self.path)
            self.send_response(200)
            self.end_headers()

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Leader)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    address = f"127.0.0.1:{server.server_port}"
    # 주소 검증은 Pod IP와 우리 포트만 통과시킨다. 여기서는 로컬 서버로 보내려고 검증을 건너뛴다
    monkeypatch.setattr("classifier.app.leader_url", lambda a: f"http://{a}/alerts")
    ok, failed = forward(address, {"alerts": ["x"]}), forward(address, {"alerts": ["fail"]})
    assert (ok.status_code, json.loads(ok.body)) == (200, {"published": 1})
    assert (failed.status_code, json.loads(failed.body)) == (503, {"detail": {"publish_failed": 1}})
    redirected = forward(address, {"alerts": ["redirect"]})      # 따라가지 않고 대장의 응답 코드를 그대로 돌려준다
    assert redirected.status_code == 302 and followed == []
    server.shutdown()
    server.server_close()
    with pytest.raises(HTTPException) as e:
        forward(address, {"alerts": ["x"]})
    assert (e.value.status_code, e.value.detail) == (503, "대장에 닿지 못함")     # 주소 · 예외 내용을 싣지 않는다


@pytest.mark.parametrize("address,url", [
    ("10.244.1.9:8080", "http://10.244.1.9:8080/alerts"),
    ("fd00::9:8080", "http://[fd00::9]:8080/alerts"),
    ("10.244.1.9:9093", None),                  # 우리 포트가 아니다
    ("example.com:8080", None),                 # IP가 아니다
    ("10.244.1.9:8080/x?", None),
    ("user@10.244.1.9:8080", None),
    ("10.244.1.9", None),
    ("127.0.0.1:8080", None),                   # Pod IP일 수 없는 주소
    ("169.254.169.254:8080", None),
    ("0.0.0.0:8080", None),
    ("224.0.0.1:8080", None),
    ("", None),
])
def test_leader_url_accepts_only_ip_and_our_port(address, url):
    from classifier.app import leader_url
    assert leader_url(address) == url


def test_forward_refuses_bad_address(monkeypatch):
    """Lease에 엉뚱한 값이 적혀 있으면 어디로도 보내지 않는다."""
    import urllib.request
    from fastapi import HTTPException
    from classifier.app import forward
    monkeypatch.setattr(urllib.request, "urlopen", lambda *a, **k: pytest.fail("요청을 보냄"))
    with pytest.raises(HTTPException) as e:
        forward("169.254.169.254:80", {"alerts": []})
    assert e.value.status_code == 503
