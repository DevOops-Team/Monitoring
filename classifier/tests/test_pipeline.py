"""Pod 종료 신호의 30초 대기와 의도된 종료 거르기 (docs/classifier.md 4절). 시각은 전부 손으로 준다."""
import json
from dataclasses import replace
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
                       "node": f"node-{rank}", "pod": f"job-{JOB}-g{generation:04d}-w-0-{rank}", "reason": "Error", "exit_code": code},
            "annotations": {"event_ts": ts.strftime("%Y-%m-%dT%H:%M:%SZ")}}


def job_failed(ts=T0, generation=GEN):
    return {"status": "firing",
            "labels": {"alertname": "FailoverJobFailed", "pipeline": "failover", "signal": "job_failed", "cluster": "eks-a",
                       "job_id": JOB, "generation": str(generation)},
            "annotations": {"event_ts": ts.strftime("%Y-%m-%dT%H:%M:%SZ"), "log_line": "jobset failed"}}


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
        generation = int(s.pod.split("-g")[1][:4]) if s.pod else (s.generation or GEN)
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


def test_pending_is_bounded(env, monkeypatch):
    pipe, fake, _ = env
    monkeypatch.setattr(p, "MAX_PENDING", 2)
    assert [pipe.handle(pod_exit(rank=r), at(10)) for r in range(3)] == [p.DEFERRED, p.DEFERRED, p.PUBLISHED]
    assert len(pipe.pending) == 2 and fake.sent[0]["payload"]["suggested_action"] == "hold"


# 사이트: 클러스터 1건 → 그때 돌던 작업마다 사건 1건 (docs/classifier.md 2절)
OTHER = "7c1d0b2a"


def site(ts=T0):
    return {"status": "firing",
            "labels": {"alertname": "FailoverSiteDown", "pipeline": "failover", "signal": "site", "cluster": "eks-a"},
            "annotations": {"event_ts": ts.strftime("%Y-%m-%dT%H:%M:%SZ")}}


def site_pipe(jobs, fake=None):
    fake, life = fake or FakeProducer(), Lifecycle()
    life.healthy = True
    pipe = p.Pipeline(Rules.load(), Publisher(fake, "abc1234"),
                      lambda s: Context(az="unknown", collector_healthy=False, jobs=jobs), life)
    return pipe, fake, life


def test_site_is_one_event_per_job():
    pipe, fake, _ = site_pipe(((JOB, GEN), (OTHER, 1)))
    assert pipe.handle(site(), at(150)) == p.PUBLISHED
    payloads = [m["payload"] for m in fake.sent]
    assert [(e["job_id"], e["generation"], e["suggested_action"]) for e in payloads] == [
        (JOB, GEN, "fence_evacuate"), (OTHER, 1, "fence_evacuate")]
    epoch = int(T0.timestamp())
    assert [e["dedup_key"] for e in payloads] == [f"eks-a/{JOB}|site|{epoch}", f"eks-a/{OTHER}|site|{epoch}"]
    assert pipe.handle(site(), at(210)) == p.DUPLICATE          # Alertmanager 재전송
    assert len(fake.sent) == 2


def test_site_without_jobs_publishes_nothing():
    pipe, fake, _ = site_pipe(())
    assert pipe.handle(site(), at(150)) == p.NO_JOBS
    assert fake.sent == []


def test_site_lookup_failure_publishes_one_without_job():
    """작업을 모르면 job_id 없이 1건. 컨트롤러가 HOLD해 사람이 보게 된다."""
    pipe, fake, _ = site_pipe(None)
    assert pipe.handle(site(), at(150)) == p.PUBLISHED
    e = fake.sent[0]["payload"]
    assert (len(fake.sent), e["job_id"], e["dedup_key"]) == (1, None, f"eks-a|site|{int(T0.timestamp())}")


def test_site_skips_fenced_cancelled_and_stale_jobs():
    third = "9e8f7a6b"
    pipe, fake, life = site_pipe(((JOB, GEN), (OTHER, 1), (third, 2)))
    life.apply(lifecycle_event("job.fenced"), at(5))                                    # JOB 세대 3 펜싱
    life.apply({**lifecycle_event("job.cancelled", 1), "job_id": OTHER}, at(5))        # OTHER 사용자 중단
    assert pipe.handle(site(), at(150)) == p.PUBLISHED
    assert [m["payload"]["job_id"] for m in fake.sent] == [third]
    life.apply({**lifecycle_event("job.submitted", 3), "job_id": third}, at(160))      # 이미 새 세대가 있다
    pipe2, fake2, _ = site_pipe(((third, 2),))
    pipe2.lifecycle = life
    assert pipe2.handle(site(), at(170)) == p.NO_JOBS


def test_site_partial_publish_failure_is_retried():
    """두 번째 작업만 못 냈다 → 503으로 다시 받고, 첫 번째는 중복으로 걸러져 두 번째만 나간다."""
    class FailSecond(FakeProducer):
        def produce(self, topic, *, key, value, headers, on_delivery):
            self.fail = len(self.sent) == 1 and not self.retried
            super().produce(topic, key=key, value=value, headers=headers, on_delivery=on_delivery)
    fake = FailSecond()
    fake.retried = False
    pipe, _, _ = site_pipe(((JOB, GEN), (OTHER, 1)), fake)
    assert pipe.handle(site(), at(150)) == p.PUBLISH_FAILED
    assert [m["payload"]["job_id"] for m in fake.sent] == [JOB]
    fake.retried = True
    assert pipe.handle(site(), at(165)) == p.PUBLISHED
    assert [m["payload"]["job_id"] for m in fake.sent] == [JOB, OTHER]


def test_site_holds_when_lifecycle_is_unreadable():
    """펜싱 · 중단 기록을 못 읽으면 취소된 작업인지 모른다 → 작업마다 내되 hold (Pod 종료와 같은 기준)."""
    pipe, fake, life = site_pipe(((JOB, GEN), (OTHER, 1)))
    life.healthy = False
    assert pipe.handle(site(), at(150)) == p.PUBLISHED
    assert [(m["type"], m["payload"]["job_id"], m["payload"]["suggested_action"]) for m in fake.sent] == [
        ("fault.held", JOB, "hold"), ("fault.held", OTHER, "hold")]



# GPU 노드 소실: 노드 1건 → 그 노드에서 돌던 작업의 사건 1건 (docs/classifier.md 2절)
def node_lost(node="node-0", ts=T0):
    return {"status": "firing",
            "labels": {"alertname": "FailoverGpuNodeLost", "pipeline": "failover", "signal": "node_lost",
                       "cluster": "eks-a", "node": node},
            "annotations": {"event_ts": ts.strftime("%Y-%m-%dT%H:%M:%SZ")}}


def node_pipe(jobs, collector_healthy=True):
    fake, life = FakeProducer(), Lifecycle()
    life.healthy = True
    pipe = p.Pipeline(Rules.load(), Publisher(fake, "abc1234"),
                      lambda s: Context(az="us-east-2a", collector_healthy=collector_healthy, jobs=jobs), life)
    return pipe, fake, life


def test_node_lost_is_isolate_migrate_for_the_job_on_it():
    pipe, fake, _ = node_pipe(((JOB, GEN),))
    assert pipe.handle(node_lost(), at(90)) == p.PUBLISHED
    e = fake.sent[0]["payload"]
    assert (e["source"], e["category"], e["suggested_action"], e["job_id"], e["generation"], e["node"]) == (
        "node", "infra", "isolate_migrate", JOB, GEN, "node-0")
    assert e["dedup_key"] == f"node-0|node|{int(T0.timestamp())}"
    assert pipe.handle(node_lost(), at(150)) == p.DUPLICATE         # Alertmanager 재전송
    assert pipe.handle(node_lost("node-1"), at(150)) == p.PUBLISHED  # 같은 작업의 다른 노드 = 노드마다 1건
    assert len(fake.sent) == 2


def test_node_lost_without_job_publishes_nothing():
    """예비 노드처럼 학습 작업이 없던 노드. job_id 없이 내면 컨트롤러가 HOLD해 Slack이 울린다."""
    pipe, fake, _ = node_pipe(())
    assert pipe.handle(node_lost(), at(90)) == p.NO_JOBS
    assert fake.sent == []


@pytest.mark.parametrize("jobs", [None, ((JOB, GEN), (OTHER, 1))])
def test_node_lost_unknown_job_publishes_one_without_job(jobs):
    """조회 실패 · 작업이 여럿 → 어느 작업인지 모른다. job_id 없이 1건 → 컨트롤러 HOLD → 사람이 본다."""
    pipe, fake, _ = node_pipe(jobs)
    assert pipe.handle(node_lost(), at(90)) == p.PUBLISHED
    e = fake.sent[0]["payload"]
    assert (len(fake.sent), e["job_id"], e["suggested_action"]) == (1, None, "isolate_migrate")


def test_node_lost_skips_fenced_and_stale_jobs():
    pipe, fake, life = node_pipe(((JOB, GEN),))
    life.apply(lifecycle_event("job.fenced"), at(5))
    assert pipe.handle(node_lost(), at(90)) == p.INTENDED
    pipe, fake, life = node_pipe(((JOB, GEN),))
    life.apply(lifecycle_event("job.submitted", GEN + 1), at(5))
    assert pipe.handle(node_lost(), at(90)) == p.STALE
    assert fake.sent == []


def test_node_lost_holds_when_lifecycle_is_unreadable():
    pipe, fake, life = node_pipe(((JOB, GEN),))
    life.healthy = False
    assert pipe.handle(node_lost(), at(90)) == p.PUBLISHED
    assert (fake.sent[0]["type"], fake.sent[0]["payload"]["suggested_action"]) == ("fault.held", "hold")


def test_node_lost_holds_when_cluster_collectors_are_down():
    pipe, fake, _ = node_pipe(((JOB, GEN),), collector_healthy=False)
    assert pipe.handle(node_lost(), at(90)) == p.PUBLISHED
    assert fake.sent[0]["payload"]["suggested_action"] == "hold"


def test_pod_exit_after_node_lost_is_absorbed():
    """노드가 사라진 뒤 그 작업 Pod의 종료가 알람으로 오면 결과이지 새 사건이 아니다."""
    pipe, fake, _ = node_pipe(((JOB, GEN),))
    pipe.enrich = lambda s: Context(az="us-east-2a", collector_healthy=True, job_id=JOB, generation=GEN,
                                    jobs=((JOB, GEN),) if s.signal == "node_lost" else None)
    assert pipe.handle(node_lost(), at(90)) == p.PUBLISHED
    assert pipe.handle(pod_exit(), at(100)) == p.ABSORBED
    assert len(fake.sent) == 1



# ---------- 작업 실패(job_failed): 죽은 Pod가 지워져 Pod 종료 알람이 못 본 경우 (GPU-220) ----------
def test_job_failed_waits_30s_then_publishes_one_event_without_node(env):
    pipe, fake, _ = env
    assert pipe.handle(job_failed(), at(10)) == p.DEFERRED
    assert pipe.handle(job_failed(), at(25)) == p.DEFERRED and len(pipe.pending) == 1     # Alertmanager 재전송
    pipe.flush(at(30))
    payload = fake.sent[0]["payload"]
    assert (payload["source"], payload["node"], payload["job_id"], payload["generation"], payload["suggested_action"]) == (
        "pod", None, JOB, GEN, "restart_in_place")
    assert payload["dedup_key"].startswith(f"eks-a/{JOB}/g{GEN}|pod|")
    assert pipe.handle(job_failed(), at(90)) == p.DUPLICATE         # 알람은 3분 울린다. 다시 와도 한 건
    assert len(fake.sent) == 1


def test_job_failed_after_pod_exit_event_is_absorbed(env):
    """Pod 종료 알람이 먼저 봤으면 작업 실패는 같은 일이다."""
    pipe, fake, _ = env
    assert pipe.handle(pod_exit(), at(45)) == p.PUBLISHED
    assert pipe.handle(job_failed(), at(50)) == p.ABSORBED
    assert len(fake.sent) == 1


def test_pod_exit_after_job_failed_event_is_absorbed(env):
    pipe, fake, _ = env
    assert pipe.handle(job_failed(), at(45)) == p.PUBLISHED
    assert pipe.handle(pod_exit(rank=1), at(50)) == p.ABSORBED
    assert len(fake.sent) == 1


def test_both_waiting_publish_only_one(env):
    pipe, fake, _ = env
    pipe.handle(pod_exit(), at(5))
    pipe.handle(job_failed(), at(6))
    pipe.flush(at(31))
    assert len(fake.sent) == 1 and pipe.pending == []


@pytest.mark.parametrize("type_", ["job.fenced", "job.cancelled"])
def test_job_failed_of_intended_termination_is_dropped(env, type_):
    pipe, fake, life = env
    life.apply(lifecycle_event(type_), at(5))
    assert pipe.handle(job_failed(), at(45)) == p.INTENDED
    assert fake.sent == []


def test_job_failed_of_older_generation_is_stale(env):
    pipe, fake, life = env
    life.apply(lifecycle_event("job.submitted", generation=GEN + 1), at(5))
    assert pipe.handle(job_failed(), at(45)) == p.STALE


def test_job_failed_after_cause_is_absorbed(env):
    """Xid로 이미 복구 대상이 된 세대의 JobSet 실패는 결과다."""
    pipe, fake, _ = env
    assert pipe.handle(xid(), at(2)) == p.PUBLISHED
    assert pipe.handle(job_failed(), at(45)) == p.ABSORBED


def test_job_failed_holds_when_lifecycle_is_unreadable(env):
    pipe, fake, life = env
    life.healthy = False
    assert pipe.handle(job_failed(), at(45)) == p.PUBLISHED
    assert fake.sent[0]["payload"]["suggested_action"] == "hold"


def test_next_generation_job_failed_is_a_new_event(env):
    pipe, fake, _ = env
    assert pipe.handle(job_failed(), at(45)) == p.PUBLISHED
    assert pipe.handle(job_failed(ts=at(600), generation=GEN + 1), at(660)) == p.PUBLISHED
    assert len(fake.sent) == 2


def test_restore_remembers_job_failed_event(env):
    """대장이 바뀐 뒤에도, 이미 낸 작업 실패 사건의 세대에 뒤늦게 온 Pod 종료를 새 사건으로 내지 않는다."""
    pipe, fake, life = env
    pipe.handle(job_failed(), at(45))
    fresh = p.Pipeline(pipe.rules, pipe.publisher, pipe.enrich, life)
    fresh.restore(fake.sent[0])
    assert fresh.handle(job_failed(), at(60)) == p.DUPLICATE
    assert fresh.handle(pod_exit(rank=1), at(60)) == p.ABSORBED
    assert len(fake.sent) == 1


def test_restore_tells_job_failed_by_dedup_key_not_by_missing_node(env):
    """작업 실패 사건에 노드가 채워져 있어도(GPU-288) 작업 실패로 되돌린다. 구분은 dedup_key 대상의 모양이다."""
    pipe, fake, life = env
    pipe.handle(job_failed(), at(45))
    sent = json.loads(json.dumps(fake.sent[0]))
    sent["payload"]["node"] = "node-1"
    fresh = p.Pipeline(pipe.rules, pipe.publisher, pipe.enrich, life)
    fresh.restore(sent)
    assert fresh.exits[(JOB, GEN)][0] == "job_failed"
    assert fresh.handle(pod_exit(rank=1), at(60)) == p.ABSORBED


def test_job_failed_event_carries_the_node_and_log_line_found_in_logs(env):
    """보강이 찾은 죽은 노드 · 로그 줄이 사건에 실린다(GPU-288). 중복 제거 키는 그대로 작업 · 세대다."""
    pipe, fake, _ = env
    found = pipe.enrich
    pipe.enrich = lambda s: replace(found(s), node="node-1", log_line="failed (exitcode: -9) local_rank: 0")
    assert pipe.handle(job_failed(), at(45)) == p.PUBLISHED
    payload = fake.sent[0]["payload"]
    assert (payload["node"], payload["raw"]["log_line"]) == ("node-1", "failed (exitcode: -9) local_rank: 0")
    assert payload["dedup_key"].startswith(f"eks-a/{JOB}/g{GEN}|pod|")
    assert pipe.handle(pod_exit(rank=1), at(60)) == p.ABSORBED


# ---------- 메모리 부족: 재시도 없이 실패 확정 (GPU-222) ----------
OOM_LINE = "torch.OutOfMemoryError: CUDA out of memory. Tried to allocate 2.00 GiB."


def oom_killed(rank=0):
    alert = pod_exit(rank)
    alert["labels"]["reason"] = "OOMKilled"
    return alert


def test_container_oom_is_published_as_refuse(env):
    pipe, fake, _ = env
    assert pipe.handle(oom_killed(), at(45)) == p.PUBLISHED
    payload = fake.sent[0]["payload"]
    assert (payload["category"], payload["suggested_action"]) == ("app", "refuse")


def test_cuda_oom_from_the_log_is_published_as_refuse_with_the_line(env):
    pipe, fake, _ = env
    found = pipe.enrich
    pipe.enrich = lambda s: replace(found(s), oom=True, log_line=OOM_LINE)
    assert pipe.handle(pod_exit(), at(45)) == p.PUBLISHED
    payload = fake.sent[0]["payload"]
    assert (payload["suggested_action"], payload["raw"]["log_line"]) == ("refuse", OOM_LINE)


def test_job_failed_after_a_refused_pod_exit_is_absorbed(env):
    """refuse도 그 작업 · 세대의 사건이다. 뒤에 온 작업 실패 신호가 restart_in_place로 따로 나가면 안 된다."""
    pipe, fake, _ = env
    assert pipe.handle(oom_killed(), at(45)) == p.PUBLISHED
    assert pipe.handle(job_failed(), at(50)) == p.ABSORBED
    assert len(fake.sent) == 1


def test_oom_without_lifecycle_is_hold(env):
    pipe, fake, life = env
    life.healthy = False
    assert pipe.handle(oom_killed(), at(45)) == p.PUBLISHED
    assert fake.sent[0]["payload"]["suggested_action"] == "hold"


# ── 스트래글러 (3.3절). T0 = 느려지기 시작한 시각, 알람은 10분 뒤부터 온다

def straggler(rank=3, ts=T0, generation=GEN):
    return {"status": "firing",
            "labels": {"alertname": "FailoverStraggler", "pipeline": "failover", "signal": "straggler", "cluster": "eks-a",
                       "node": f"node-{rank}", "job_id": JOB, "generation": str(generation), "rank": str(rank)},
            "annotations": {"event_ts": ts.strftime("%Y-%m-%dT%H:%M:%SZ"), "ratio": "1.60"}}


def progress(step, total_steps=10000, generation=GEN):
    return {**lifecycle_event("job.progress", generation), "payload": {"step": step, "total_steps": total_steps}}


@pytest.fixture
def slow_env(env):
    """step 시간이 1.0 → 1.6초로 느려진 작업. 남은 스텝은 테스트가 job.progress로 넣는다."""
    pipe, fake, life = env
    plain = pipe.enrich
    pipe.enrich = lambda s: replace(plain(s), gpu_uuid=GPU, step_seconds=(1.6, 1.0))
    return pipe, fake, life


def test_straggler_worth_moving_carries_evidence(slow_env):
    pipe, fake, life = slow_env
    life.apply(progress(5200), T0)                      # 남은 4,800 스텝 × 0.6초 = 손해 2,880초
    assert pipe.handle(straggler(), at(720)) == p.PUBLISHED
    e = fake.sent[0]["payload"]
    assert (e["category"], e["suggested_action"], e["source"]) == ("straggler", "evacuate_preemptive", "straggler")
    assert e["straggler_evidence"] == {"duration_seconds": 720, "cause_node": "node-3", "estimated_gain_seconds": 1980}
    assert e["straggler_evidence"]["cause_node"] == e["node"]       # 스키마로 못 막는 조건 (fault-event.md 2절)
    assert (e["raw"]["metric"], e["raw"]["value"]) == ("train_rank_compute_seconds_ratio", 1.6)
    assert e["dedup_key"] == f"{GPU}|straggler|{int(T0.timestamp())}"
    assert (e["job_id"], e["generation"]) == (JOB, GEN)


def test_straggler_not_worth_moving_holds_with_evidence(slow_env):
    pipe, fake, life = slow_env
    life.apply(progress(9000), T0)                      # 남은 1,000 스텝 × 0.6초 = 손해 600초 < 1,800
    assert pipe.handle(straggler(), at(720)) == p.PUBLISHED
    e = fake.sent[0]["payload"]
    assert (e["category"], e["suggested_action"], e["confidence"]) == ("straggler", "hold", 0.75)
    assert e["straggler_evidence"]["estimated_gain_seconds"] == -300


def test_straggler_is_published_once_per_job_generation_node(slow_env):
    pipe, fake, life = slow_env
    life.apply(progress(5200), T0)
    assert pipe.handle(straggler(), at(720)) == p.PUBLISHED
    life.apply(progress(9500), at(780))                 # 그 사이 남은 스텝이 줄어 다시 계산하면 hold가 된다
    assert pipe.handle(straggler(), at(780)) == p.DUPLICATE
    assert pipe.handle(straggler(ts=at(3600)), at(4400)) == p.DUPLICATE     # 풀렸다가 다시 느려져도 같은 세대 · 노드면 1건
    assert len(fake.sent) == 1
    assert pipe.handle(straggler(rank=2), at(780)) == p.PUBLISHED           # 다른 노드는 다른 사건
    assert len(fake.sent) == 2


@pytest.mark.parametrize("steps, with_progress", [((1.6, 1.0), False), (None, True)])
def test_straggler_with_unknown_gain_holds_without_evidence(env, steps, with_progress):
    pipe, fake, life = env
    plain = pipe.enrich
    pipe.enrich = lambda s: replace(plain(s), step_seconds=steps)
    if with_progress:
        life.apply(progress(5200), T0)
    assert pipe.handle(straggler(), at(720)) == p.PUBLISHED
    e = fake.sent[0]["payload"]
    assert (e["category"], e["suggested_action"]) == ("straggler", "hold")
    assert "straggler_evidence" not in e


def test_straggler_holds_when_lifecycle_unreadable(slow_env):
    pipe, fake, life = slow_env
    life.apply(progress(5200), T0)
    life.healthy = False
    assert pipe.handle(straggler(), at(720)) == p.PUBLISHED
    assert fake.sent[0]["payload"]["suggested_action"] == "hold"


def test_straggler_of_fenced_or_old_generation_is_dropped(slow_env):
    pipe, fake, life = slow_env
    life.apply(lifecycle_event("job.fenced"), T0)
    assert pipe.handle(straggler(), at(720)) == p.INTENDED
    life.apply(lifecycle_event("job.submitted", generation=GEN + 1), T0)
    assert pipe.handle(straggler(), at(720)) == p.INTENDED      # 펜싱이 먼저 걸린다
    assert pipe.handle(straggler(generation=GEN - 1), at(720)) == p.STALE
    assert fake.sent == []


def test_straggler_failed_publish_is_retried(slow_env):
    pipe, fake, life = slow_env
    life.apply(progress(5200), T0)
    fake.fail = True
    assert pipe.handle(straggler(), at(720)) == p.PUBLISH_FAILED
    fake.fail = False
    assert pipe.handle(straggler(), at(780)) == p.PUBLISHED


def test_straggler_is_remembered_after_restart(slow_env):
    pipe, fake, life = slow_env
    life.apply(progress(5200), T0)
    pipe.handle(straggler(), at(720))
    fresh = p.Pipeline(Rules.load(), Publisher(FakeProducer(), "abc1234"), pipe.enrich, life)
    fresh.restore(fake.sent[0])
    life.apply(progress(9500), at(760))                 # 다시 계산하면 hold로 뒤집히지만, 낸 것을 기억해 다시 내지 않는다
    assert fresh.handle(straggler(), at(780)) == p.DUPLICATE
    # 옮기기로 한 사건은 원인이다. 그 세대의 Pod 종료는 결과라 따로 내지 않는다
    assert fresh.handle(pod_exit(ts=at(800)), at(840)) == p.ABSORBED
