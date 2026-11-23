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
    assert client.get("/readyz").status_code == 200


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

    def get(self):
        return {"metadata": {"resourceVersion": "1"},
                "spec": {"holderIdentity": "other", "renewTime": ts(0), "leaseDurationSeconds": 15}}


class FreeLease:
    name, lease = "classifier", None

    def get(self):
        return self.lease

    def create(self, body):
        self.lease = {**body, "metadata": {"resourceVersion": "1"}}

    replace = create


def test_standby_is_not_ready_and_refuses_alerts(env):
    _, fake = env
    pipeline = Pipeline(Rules.load(), Publisher(fake, "abc1234"), lambda s: Context(az="a", collector_healthy=True))
    with TestClient(create_app(pipeline, lease_api=HeldLease())) as client:
        assert client.get("/healthz").status_code == 200      # 살아 있음 (재시작 대상 아님)
        assert client.get("/readyz").status_code == 503       # Service에서 빠짐
        assert post(client, alert(ts(30))).status_code == 503
        assert "classifier_leader 0.0" in client.get("/metrics").text
    assert fake.sent == []


def test_leader_becomes_ready_after_acquiring(env):
    import time
    _, fake = env
    pipeline = Pipeline(Rules.load(), Publisher(fake, "abc1234"), lambda s: Context(az="a", collector_healthy=True))
    with TestClient(create_app(pipeline, lease_api=FreeLease())) as client:
        deadline = time.monotonic() + 3
        while client.get("/readyz").status_code != 200 and time.monotonic() < deadline:
            time.sleep(0.05)
        assert client.get("/readyz").json() == {"status": "leader"}
        assert post(client, alert(ts(30))).json() == {"published": 1}
        assert "classifier_leader 1.0" in client.get("/metrics").text
