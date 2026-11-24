import json
from datetime import datetime, timezone

import pytest

from classifier import dedup
from classifier.event import Context, build, dedup_code, dedup_target
from classifier.publish import InvalidEvent, Publisher, StdoutProducer, validate
from classifier.rules import Rules
from classifier.signals import Signal

TS = datetime(2026, 11, 18, 14, 2, 11, tzinfo=timezone.utc)
NOW = datetime(2026, 11, 18, 14, 2, 50, tzinfo=timezone.utc)
GPU = "GPU-b850f46d-d5ea-c752-ddf3-c4453e44d3f7"
CTX = Context(az="us-east-2a", collector_healthy=True, job_id="3f9a1c2e", generation=3)
rules = Rules.load()


def signal(**kw) -> Signal:
    base = dict(signal="xid", source="dmesg", alert="GpuXid", cluster="eks-a", ts=TS, node="ip-10-0-1-23",
                gpu_uuid=GPU, xid=79)
    return Signal(**{**base, **kw})


def make(s: Signal, ctx: Context = CTX) -> dict:
    v = rules.judge(s, ctx.collector_healthy)
    key = dedup.Dedup().decide(dedup_target(s, ctx), dedup_code(s), s.ts, v.action).key
    return build(s, v, ctx, key)


class FakeProducer:
    def __init__(self, error=None, pending=0):
        self.sent, self.error, self.pending = [], error, pending

    def produce(self, topic, *, key, value, headers, on_delivery):
        self.sent.append((topic, key, json.loads(value), dict(headers)))
        on_delivery(self.error, None)

    def flush(self, timeout):
        return self.pending


@pytest.mark.parametrize("s, ctx", [
    (signal(), CTX),
    (signal(source="dcgm", xid=79), Context(az="us-east-2a", collector_healthy=False)),
    (signal(xid=999, gpu_uuid=None), Context(az="us-east-2a", collector_healthy=True, gpu_uuid=GPU)),
    (signal(signal="pod_exit", source="pod", xid=None, gpu_uuid=None, exit_code=137, pod="p"), CTX),
    (signal(signal="spot", source="spot", xid=None, gpu_uuid=None), CTX),
    (signal(signal="site", source="site", xid=None, gpu_uuid=None, node=None),
     Context(az="us-east-2a", collector_healthy=False)),
])
def test_built_events_pass_schema(s, ctx):
    e = make(s, ctx)
    validate(e)
    assert e["traceparent"].split("-")[1] == e["event_id"]


def test_dedup_target_and_code():
    assert make(signal())["dedup_key"] == f"{GPU}|79|{int(TS.timestamp())}"
    assert make(signal(signal="spot", source="spot", xid=None, gpu_uuid=None))["dedup_key"].startswith("ip-10-0-1-23|spot|")
    pod = make(signal(signal="pod_exit", source="pod", xid=None, gpu_uuid=None, exit_code=137, pod="job-a-g0001-w-0-0"))
    assert pod["dedup_key"].startswith("job-a-g0001-w-0-0|pod|")
    site = make(signal(signal="site", source="site", xid=None, gpu_uuid=None, node=None),
                Context(az="us-east-2a", collector_healthy=False))
    assert site["dedup_key"].startswith("eks-a|site|") and site["gpu_uuid"] is None
    job = make(signal(signal="site", source="site", xid=None, gpu_uuid=None, node=None),
               Context(az="unknown", collector_healthy=False, job_id="3f9a1c2e", generation=3))
    assert job["dedup_key"].startswith("eks-a/3f9a1c2e|site|")     # 작업마다 다른 사건
    validate(job)


def test_dcgm_fills_metric():
    raw = make(signal(source="dcgm"))["raw"]
    assert (raw["metric"], raw["value"], raw["log_line"]) == ("DCGM_FI_DEV_XID_ERRORS", 79, None)


def test_publish_envelope():
    fake = FakeProducer()
    e = make(signal())
    env = Publisher(fake, "abc1234").publish(e, dedup.NEW, NOW)
    topic, key, value, headers = fake.sent[0]
    assert (topic, key, headers) == ("gpu.fault", b"ip-10-0-1-23", {"rule_version": b"abc1234"})
    assert value == env and env["payload"] == e
    assert (env["event_id"], env["schema_version"], env["type"], env["source"]) == (
        e["event_id"], 1, "fault.detected", "classifier")
    assert (env["occurred_at"], env["payload"]["ts"]) == ("2026-11-18T14:02:50Z", "2026-11-18T14:02:11Z")
    assert (env["job_id"], env["generation"], env["cluster"], env["node"]) == ("3f9a1c2e", 3, "eks-a", "ip-10-0-1-23")


def test_event_types_and_site_key():
    fake = FakeProducer()
    p = Publisher(fake, "abc1234")
    assert p.publish(make(signal(xid=999)), dedup.NEW, NOW)["type"] == "fault.held"
    assert p.publish(make(signal()), dedup.RECLASSIFIED, NOW)["type"] == "fault.reclassified"
    p.publish(make(signal(signal="site", source="site", xid=None, gpu_uuid=None, node=None),
                   Context(az="us-east-2a", collector_healthy=False)), dedup.NEW, NOW)
    assert fake.sent[-1][1] == b"eks-a"


@pytest.mark.parametrize("change", [
    {"suggested_action": "reboot"},                         # enum 밖
    {"collector_healthy": False},                           # false인데 hold가 아님
    {"ts": "어제"},                                          # 시각 형식
    {"dedup_key": "GPU-x|79|2026-11-18T14:02:11Z"},         # ISO 시각 키 금지
    {"event_id": "0" * 32, "traceparent": "00-" + "0" * 32 + "-097ad754b8e4e6ff-01"},
    {"event_id": "da1ed752967bdc6823e4c3a68c072394"},       # trace-id와 다름
])
def test_invalid_event_is_not_published(change):
    fake = FakeProducer()
    with pytest.raises(InvalidEvent):
        Publisher(fake, "abc1234").publish({**make(signal()), **change}, dedup.NEW, NOW)
    assert fake.sent == []


@pytest.mark.parametrize("fake", [FakeProducer(error="broker down"), FakeProducer(pending=1)])
def test_delivery_failure_raises(fake):
    with pytest.raises(RuntimeError):
        Publisher(fake, "abc1234").publish(make(signal()), dedup.NEW, NOW)


def test_stdout_producer(capsys):
    Publisher(StdoutProducer(), "dev").publish(make(signal()), dedup.NEW, NOW)
    out = json.loads(capsys.readouterr().out)
    assert (out["topic"], out["key"], out["value"]["type"]) == ("gpu.fault", "ip-10-0-1-23", "fault.detected")
