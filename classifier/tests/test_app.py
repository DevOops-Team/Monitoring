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


def test_default_context_holds_everything(monkeypatch, capsys):
    """Mimir 보강 전: 수집 경로를 확인할 수 없으니 hold로만 나간다."""
    monkeypatch.delenv("KAFKA_BOOTSTRAP", raising=False)
    monkeypatch.delenv("REQUIRE_KAFKA", raising=False)
    client = TestClient(create_app())
    assert post(client, alert(ts(30))).json() == {"published": 1}
    out = json.loads(capsys.readouterr().out)
    assert (out["value"]["type"], out["value"]["payload"]["suggested_action"]) == ("fault.held", "hold")


def test_require_kafka(monkeypatch):
    monkeypatch.delenv("KAFKA_BOOTSTRAP", raising=False)
    monkeypatch.setenv("REQUIRE_KAFKA", "true")
    with pytest.raises(RuntimeError):
        pipeline_from_env()
