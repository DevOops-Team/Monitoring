"""Pod 종료 신호의 30초 대기와 의도된 종료 거르기 (docs/classifier.md 4절). 시각은 전부 손으로 준다."""
import json
from datetime import datetime, timedelta, timezone

import pytest

from classifier import pipeline as p
from classifier.event import Context
from classifier.intended import Lifecycle
from classifier.publish import Publisher
from classifier.rules import Rules

T0 = datetime(2026, 11, 18, 14, 2, 11, tzinfo=timezone.utc)     # Pod가 끝난 시각
GPU = "GPU-b850f46d-d5ea-c752-ddf3-c4453e44d3f7"
JOB, GEN = "3f9a1c2e", 3


def at(seconds: int) -> datetime:
    return T0 + timedelta(seconds=seconds)


class FakeProducer:
    def __init__(self):
        self.sent, self.fail = [], False

    def produce(self, topic, *, key, value, headers, on_delivery):
        if not self.fail:
            self.sent.append(json.loads(value))
        on_delivery("broker down" if self.fail else None, None)

    def flush(self, timeout):
        return 0


def pod_exit(rank=0, code="137", ts=T0, generation=GEN):
    return {"status": "firing",
            "labels": {"alertname": "TrainerExited", "pipeline": "failover", "signal": "pod_exit", "cluster": "eks-a",
                       "node": f"node-{rank}", "pod": f"job-{JOB}-g{generation:04d}-w-0-{rank}", "exit_code": code},
            "annotations": {"event_ts": ts.strftime("%Y-%m-%dT%H:%M:%SZ")}}


def xid(number="79", rank=0, ts=T0):
    return {"status": "firing",
            "labels": {"alertname": "GpuXid", "pipeline": "failover", "signal": "xid", "source": "dmesg",
                       "cluster": "eks-a", "node": f"node-{rank}", "gpu_uuid": GPU, "xid": number},
            "annotations": {"event_ts": ts.strftime("%Y-%m-%dT%H:%M:%SZ")}}


def lifecycle_event(type_, generation=GEN):
    return {"event_id": f"job-{JOB}-g{generation:04d}-x", "schema_version": 1, "type": type_,
            "occurred_at": "2026-11-18T14:02:00Z", "job_id": JOB, "generation": generation, "payload": {}}


@pytest.fixture
def env():
    fake, life = FakeProducer(), Lifecycle()
    life.healthy = True

    def enrich(s):      # Pod 이름에서 세대를 읽는 가짜 Mimir. Xid는 같은 작업 · 세대의 노드에서 났다고 본다
        generation = int(s.pod.split("-g")[1][:4]) if s.pod else GEN
        return Context(az="us-east-2a", collector_healthy=True, job_id=JOB, generation=generation)

    return p.Pipeline(Rules.load(), Publisher(fake, "abc1234"), enrich, life), fake, life


def test_waits_30s_then_publishes(env):
    pipe, fake, _ = env
    assert pipe.handle(pod_exit(), at(10)) == p.DEFERRED
    assert pipe.handle(pod_exit(), at(12)) == p.DEFERRED and len(pipe.pending) == 1     # 재전송은 한 번만 기다린다
    pipe.flush(at(29))
    assert fake.sent == []
    pipe.flush(at(30))
    payload = fake.sent[0]["payload"]
    assert (payload["source"], payload["category"], payload["suggested_action"], payload["raw"]["exit_code"]) == (
        "pod", "app", "restart_in_place", 137)
    assert pipe.pending == []


def test_late_signal_is_judged_at_once(env):
    pipe, fake, _ = env
    assert pipe.handle(pod_exit(), at(45)) == p.PUBLISHED


@pytest.mark.parametrize("type_", ["job.fenced", "job.cancelled"])
def test_intended_termination_is_dropped(env, type_):
    pipe, fake, life = env
    pipe.handle(pod_exit(), at(10))
    life.apply(lifecycle_event(type_), at(15))      # 기다리는 동안 도착
    pipe.flush(at(30))
    assert fake.sent == [] and pipe.pending == []
    assert pipe.handle(pod_exit(rank=1), at(45)) == p.INTENDED


def test_job_failed_is_not_intended(env):
    """장애로 JobSet이 Failed가 되면 그 세대의 끝 사건은 job.failed뿐이다. 거르면 진짜 장애가 가려진다."""
    pipe, fake, life = env
    life.apply(lifecycle_event("job.failed"), at(5))
    assert pipe.handle(pod_exit(), at(45)) == p.PUBLISHED


def test_other_generation_fenced_does_not_hide_this_one(env):
    pipe, _, life = env
    life.apply(lifecycle_event("job.fenced", generation=GEN - 1), at(5))
    assert pipe.handle(pod_exit(), at(45)) == p.PUBLISHED


def test_older_generation_exit_is_stale(env):
    pipe, fake, life = env
    life.apply(lifecycle_event("job.submitted", generation=GEN + 1), at(5))
    assert pipe.handle(pod_exit(), at(45)) == p.STALE
    assert fake.sent == []


def test_cause_arriving_during_wait_absorbs_pod_exits(env):
    """Xid로 한 랭크가 죽고 다른 랭크가 따라 죽는다. 이벤트는 Xid 하나, Pod 종료는 correlated로."""
    pipe, fake, _ = env
    pipe.handle(pod_exit(rank=1), at(5))
    pipe.handle(pod_exit(rank=2), at(6))
    assert pipe.handle(xid(), at(8)) == p.PUBLISHED
    payload = fake.sent[0]["payload"]
    assert payload["suggested_action"] == "isolate_migrate"
    assert [c["source"] for c in payload["correlated"]] == ["pod", "pod"]
    assert pipe.pending == []
    pipe.flush(at(40))
    assert len(fake.sent) == 1


def test_pod_exit_after_cause_is_absorbed(env):
    pipe, fake, _ = env
    pipe.handle(xid(), at(2))
    assert pipe.handle(pod_exit(rank=1), at(10)) == p.DEFERRED
    pipe.flush(at(31))
    assert pipe.handle(pod_exit(rank=2), at(120)) == p.ABSORBED     # 그 세대는 이미 복구 대상이다
    assert len(fake.sent) == 1


def test_warning_xid_does_not_absorb(env):
    """Xid 63은 기록만 한다(action none). 그 뒤의 Pod 종료는 따로 판정해야 한다."""
    pipe, fake, _ = env
    pipe.handle(pod_exit(rank=1), at(5))
    pipe.handle(xid("63"), at(8))
    assert fake.sent[0]["payload"]["correlated"] == [] and len(pipe.pending) == 1
    pipe.flush(at(30))
    assert [e["payload"]["suggested_action"] for e in fake.sent] == ["none", "restart_in_place"]


def test_lifecycle_not_readable_holds(env):
    pipe, fake, life = env
    life.healthy = False
    assert pipe.handle(pod_exit(), at(45)) == p.PUBLISHED
    assert (fake.sent[0]["type"], fake.sent[0]["payload"]["suggested_action"]) == ("fault.held", "hold")


def test_unresolved_job_holds():
    fake, life = FakeProducer(), Lifecycle()
    life.healthy = True
    pipe = p.Pipeline(Rules.load(), Publisher(fake, "abc1234"),
                      lambda s: Context(az="us-east-2a", collector_healthy=True), life)
    assert pipe.handle(pod_exit(), at(45)) == p.PUBLISHED
    assert fake.sent[0]["payload"]["suggested_action"] == "hold"


def test_failed_publish_after_wait_is_retried(env):
    pipe, fake, _ = env
    pipe.handle(pod_exit(), at(10))
    fake.fail = True
    pipe.flush(at(30))
    assert len(pipe.pending) == 1
    fake.fail = False
    pipe.flush(at(34))
    assert fake.sent == []          # 5초 뒤에 다시
    pipe.flush(at(35))
    assert len(fake.sent) == 1 and pipe.pending == []


def test_restore_after_restart(env):
    """재시작 뒤 Kafka에서 읽은 이벤트를 되돌리면, 다시 온 알람이 새 사건으로 나가지 않는다."""
    pipe, fake, life = env
    pipe.handle(xid(), at(2))
    fresh = p.Pipeline(pipe.rules, pipe.publisher, pipe.enrich, life)
    fresh.restore(fake.sent[0])
    fresh.restore({"payload": {"dedup_key": "깨진 메시지"}})        # 건너뛴다
    forged = json.loads(json.dumps(fake.sent[0]))
    forged["source"] = "someone-else"                               # 분류기가 낸 것이 아니면 기억에 넣지 않는다
    forged["payload"]["dedup_key"] = "node-9|48|1795010531"
    fresh.restore(forged)
    assert fresh.dedup.decide("node-9", "48", T0, "reset_then_migrate").kind == "new"
    assert fresh.handle(xid(), at(60)) == p.DUPLICATE
    assert fresh.handle(pod_exit(rank=1), at(60)) == p.ABSORBED
    assert len(fake.sent) == 1


def test_two_pods_on_one_node_are_two_incidents(env):
    """중복 제거 대상은 노드가 아니라 Pod다. 같은 노드에서 거의 동시에 끝난 다른 작업의 Pod를 삼키지 않는다."""
    pipe, fake, _ = env
    first, second = pod_exit(rank=0), pod_exit(rank=1)
    second["labels"]["node"] = first["labels"]["node"]
    assert [pipe.handle(a, at(45)) for a in (first, second, first)] == [p.PUBLISHED, p.PUBLISHED, p.DUPLICATE]
