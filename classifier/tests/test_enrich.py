from datetime import datetime, timedelta, timezone

import pytest

from classifier.enrich import Enricher, _earliest, _q
from classifier.signals import Signal

TS = datetime(2026, 11, 18, 14, 2, 11, tzinfo=timezone.utc)
GPU = "GPU-b850f46d-d5ea-c752-ddf3-c4453e44d3f7"
UP = [{"job": "prometheus-agent", "__value__": 1.0}, {"job": "otel-gateway", "__value__": 1.0},
      {"job": "fluent-bit", "node": "n1", "__value__": 1.0}]
POD = {"namespace": "training", "pod": "job-3f9a1c2e-g0003-w-0-0",
       "label_devoops_job_id": "3f9a1c2e", "label_devoops_generation": "3", "__value__": 1.0}


def signal(**kw) -> Signal:
    base = dict(signal="xid", source="dmesg", alert="GpuXid", cluster="eks-a", ts=TS, node="n1", xid=79)
    return Signal(**{**base, **kw})


class FakeMimir:
    """식에 들어 있는 지표 이름으로 답을 고른다. 받은 (식, 시각)을 기록한다."""
    def __init__(self, **answers):
        self.answers, self.calls = answers, []

    def __call__(self, expr, at=None):
        self.calls.append((expr, at))
        for name, rows in self.answers.items():
            if name in expr:
                if isinstance(rows, Exception):
                    raise rows
                return rows
        return []


def test_all_found():
    mimir = FakeMimir(kube_pod_labels=[POD], DCGM_FI_DEV_GPU_TEMP=[{"UUID": GPU, "__value__": 41.0}],
                      kube_node_labels=[{"label_topology_kubernetes_io_zone": "us-east-2a", "__value__": 1.0}], up=UP)
    ctx = Enricher(mimir)(signal())
    assert (ctx.job_id, ctx.generation, ctx.gpu_uuid, ctx.az, ctx.collector_healthy) == (
        "3f9a1c2e", 3, GPU, "us-east-2a", True)
    at = {expr.split("{")[0].split("(")[-1]: t for expr, t in mimir.calls}
    assert at["kube_pod_labels"] == TS and at["DCGM_FI_DEV_GPU_TEMP"] == TS     # 신호 발생 시각 기준
    assert at["up"] is None                                                     # 수집 경로는 지금 기준


def test_nothing_found_is_unknown():
    ctx = Enricher(FakeMimir())(signal())
    assert (ctx.job_id, ctx.generation, ctx.gpu_uuid, ctx.az, ctx.collector_healthy) == (
        None, None, None, "unknown", False)


def test_query_failure_is_unknown():
    err = TimeoutError("timed out")
    ctx = Enricher(FakeMimir(kube_pod_labels=err, DCGM_FI_DEV_GPU_TEMP=err, kube_node_labels=err, up=err))(signal())
    assert (ctx.job_id, ctx.collector_healthy, ctx.az) == (None, False, "unknown")


def test_collector_needs_all_three_up():
    assert not Enricher(FakeMimir(up=UP[:2]))(signal()).collector_healthy               # fluent-bit 없음
    down = [UP[0], UP[1], {**UP[2], "__value__": 0.0}]
    assert not Enricher(FakeMimir(up=down))(signal()).collector_healthy


def test_two_jobs_on_node_is_unresolved():
    other = {**POD, "pod": "job-aaaa1111-g0001-w-0-0", "label_devoops_job_id": "aaaa1111"}
    ctx = Enricher(FakeMimir(kube_pod_labels=[POD, other]))(signal())
    assert (ctx.job_id, ctx.generation) == (None, None)


def test_same_job_two_generations_takes_latest():
    old = {**POD, "pod": "job-3f9a1c2e-g0002-w-0-0", "label_devoops_generation": "2"}
    assert Enricher(FakeMimir(kube_pod_labels=[old, POD]))(signal()).generation == 3


def test_pod_exit_looks_up_by_pod():
    mimir = FakeMimir(kube_pod_labels=[POD])
    ctx = Enricher(mimir)(signal(signal="pod_exit", source="pod", xid=None, exit_code=137, pod=POD["pod"]))
    assert ctx.job_id == "3f9a1c2e"
    job_expr = next(e for e, _ in mimir.calls if "kube_pod_labels" in e)
    assert f'pod="{POD["pod"]}"' in job_expr and "kube_pod_info" not in job_expr
    assert not any("DCGM" in e for e, _ in mimir.calls)         # Xid 신호가 아니면 GPU를 찾지 않는다


def test_gpu_uuid_in_label_skips_lookup():
    mimir = FakeMimir()
    Enricher(mimir)(signal(gpu_uuid=GPU))
    assert not any("DCGM" in e for e, _ in mimir.calls)


def site():
    return signal(signal="site", source="site", xid=None, node=None)


def test_site_finds_running_jobs_of_the_cluster():
    """끊기기 전 마지막 시각(신호 시각)에 Running이던 학습 Pod를 작업별로 묶고, 세대는 가장 큰 값."""
    rows = [POD, {**POD, "pod": "job-3f9a1c2e-g0002-w-0-0", "label_devoops_generation": "2"},
            {**POD, "pod": "job-7c1d0b2a-g0001-w-0-0", "label_devoops_job_id": "7c1d0b2a", "label_devoops_generation": ""}]
    mimir = FakeMimir(kube_pod_labels=rows, up=UP)
    ctx = Enricher(mimir)(site())
    assert (ctx.jobs, ctx.job_id, ctx.collector_healthy, ctx.az) == (
        (("3f9a1c2e", 3), ("7c1d0b2a", None)), None, False, "unknown")
    (expr, at), = mimir.calls
    assert at == TS and 'cluster="eks-a"' in expr and 'phase="Running"' in expr


def test_site_lookup_failure_is_none():
    ctx = Enricher(FakeMimir(kube_pod_labels=RuntimeError("down")))(site())
    assert ctx.jobs is None


def test_site_no_jobs_only_if_ksm_was_up():
    """작업 0개는 그 시각에 KSM이 긁히고 있었을 때만 「작업 없음」. KSM이 먼저 죽었으면 「모름」(None → HOLD 1건)."""
    up = {"job": "kube-state-metrics", "__value__": 1.0}
    mimir = FakeMimir(**{"kube-state-metrics": [up]})
    assert Enricher(mimir)(site()).jobs == ()
    assert mimir.calls[-1][1] == TS and 'cluster="eks-a"' in mimir.calls[-1][0]
    assert Enricher(FakeMimir(**{"kube-state-metrics": [{**up, "__value__": 0.0}]}))(site()).jobs is None
    assert Enricher(FakeMimir())(site()).jobs is None                                       # 지표 자체가 없음
    assert Enricher(FakeMimir(**{"kube-state-metrics": RuntimeError("down")}))(site()).jobs is None


def test_label_values_are_escaped():
    assert _q('n1"} or up{x="') == 'n1\\"} or up{x=\\"'
    mimir = FakeMimir()
    Enricher(mimir)(signal(node='n1"} or vector(1) #'))
    assert all('node="n1\\"} or vector(1) #"' in e for e, _ in mimir.calls if "node=" in e)


def node_lost():
    return signal(signal="node_lost", source="node", xid=None)


def test_node_lost_finds_the_running_job_on_that_node():
    """신호 시각(마지막 Ready)에 그 노드에서 Running이던 학습 Pod. 그 노드의 Fluent Bit이 꺼진 것은 당연해서 수집 경로에서 뺀다."""
    zone = [{"label_topology_kubernetes_io_zone": "us-east-2a", "__value__": 1.0}]
    mimir = FakeMimir(kube_pod_labels=[POD], kube_node_labels=zone, up=UP[:2])
    ctx = Enricher(mimir)(node_lost())
    assert (ctx.jobs, ctx.job_id, ctx.collector_healthy, ctx.az) == ((("3f9a1c2e", 3),), None, True, "us-east-2a")
    jobs_expr, jobs_at = next((e, t) for e, t in mimir.calls if "kube_pod_labels" in e)
    assert jobs_at == TS and 'phase="Running"' in jobs_expr and 'kube_pod_info{cluster="eks-a", node="n1"}' in jobs_expr
    up_expr = next(e for e, _ in mimir.calls if e.startswith("up{"))
    assert "fluent-bit" not in up_expr
    az_expr, az_at = next((e, t) for e, t in mimir.calls if "kube_node_labels" in e)
    assert az_at == TS and az_expr.startswith("last_over_time(")      # 노드가 이미 지워졌을 수 있다


def test_node_lost_still_needs_cluster_collectors():
    assert not Enricher(FakeMimir(up=UP[:1]))(node_lost()).collector_healthy             # 게이트웨이 없음


def test_node_lost_no_jobs_only_if_ksm_was_up():
    up = {"job": "kube-state-metrics", "__value__": 1.0}
    assert Enricher(FakeMimir(**{"kube-state-metrics": [up]}))(node_lost()).jobs == ()
    assert Enricher(FakeMimir())(node_lost()).jobs is None
    assert Enricher(FakeMimir(kube_pod_labels=RuntimeError("down")))(node_lost()).jobs is None


def test_job_failed_takes_job_from_the_alert_and_checks_cluster_collectors_only():
    """작업 실패 신호는 노드가 없다. 작업 · 세대는 알람 값 그대로, 수집 경로는 클러스터 것만 본다."""
    s = signal(signal="job_failed", source="pod", node=None, xid=None, job_id="3f9a1c2e", generation=3)
    mimir = FakeMimir(up=UP[:2])
    ctx = Enricher(mimir)(s)
    assert (ctx.job_id, ctx.generation, ctx.collector_healthy, ctx.az) == ("3f9a1c2e", 3, True, "unknown")
    assert all("fluent-bit" not in e and "kube_pod_labels" not in e for e, _ in mimir.calls)
    assert Enricher(FakeMimir(up=UP[:1]))(s).collector_healthy is False


# ---------- 작업 실패: 죽은 Pod의 노드를 trainer 로그에서 찾는다 (GPU-288) ----------
FAILED_LINE = "E1125 08:00:07.199000 1 api.py:869] failed (exitcode: -9) local_rank: 0 (pid: 61) of binary: /opt/conda/bin/python3.11"
JOB_FAILED = dict(signal="job_failed", source="pod", node=None, xid=None, job_id="3f9a1c2e", generation=3)


OOM_LINE = "torch.OutOfMemoryError: CUDA out of memory. Tried to allocate 2.00 GiB. GPU 0 has a total capacity of 14.57 GiB"


class FakeLogs:
    """answer = 실패 줄 조회의 답, oom = 메모리 부족 줄 조회의 답."""
    def __init__(self, answer, oom=None):
        self.answer, self.oom, self.calls = answer, oom, []

    def __call__(self, query, start, end):
        self.calls.append((query, start, end))
        answer = self.oom if "CUDA out of memory" in query else self.answer
        if isinstance(answer, Exception):
            raise answer
        return answer


def test_job_failed_fills_node_and_log_line_from_the_trainer_log():
    logs = FakeLogs(({"k8s_node_name": "n7", "k8s_pod_name": "job-3f9a1c2e-g0003-workers-0-1-ck529"}, FAILED_LINE))
    mimir = FakeMimir(up=UP[:2], kube_node_labels=[{"label_topology_kubernetes_io_zone": "us-east-2b", "__value__": 1.0}])
    ctx = Enricher(mimir, logs)(signal(**JOB_FAILED))
    assert (ctx.node, ctx.log_line, ctx.az, ctx.collector_healthy, ctx.oom) == ("n7", FAILED_LINE, "us-east-2b", True, False)
    query, start, end = logs.calls[-1]      # 메모리 부족 줄을 먼저 찾고, 없으면 실패 줄
    assert 'k8s_cluster_name="eks-a"' in query and r'k8s_pod_name=~"job\\-3f9a1c2e\\-g0003\\-.+"' in query
    assert '|= "failed (exitcode"' in query and start < TS < end
    assert any('node="n7"' in e for e, _ in mimir.calls)        # 영역은 찾은 노드로 본다


@pytest.mark.parametrize("answer", [None, TimeoutError("timed out"), ({"k8s_pod_name": "p"}, FAILED_LINE)])
def test_job_failed_without_a_usable_log_line_stays_without_node(answer):
    """줄이 없음 · 조회 실패 · 노드 라벨 없음 → 지금처럼 노드 없이 낸다."""
    ctx = Enricher(FakeMimir(up=UP[:2]), FakeLogs(answer))(signal(**JOB_FAILED))
    assert (ctx.node, ctx.log_line, ctx.az, ctx.job_id, ctx.collector_healthy) == (None, None, "unknown", "3f9a1c2e", True)


def test_job_failed_without_loki_does_not_look():
    assert Enricher(FakeMimir(up=UP[:2]))(signal(**JOB_FAILED)).node is None


def test_job_id_cannot_break_the_log_query():
    logs = FakeLogs(None)
    Enricher(FakeMimir(), logs)(signal(**{**JOB_FAILED, "job_id": 'a.b"c'}))
    assert r'k8s_pod_name=~"job\\-a\\.b\"c\\-g0003\\-.+"}' in logs.calls[0][0]     # 정규식 문자 앞에 \\, 따옴표는 \"


def test_earliest_line_across_pods_wins():
    """전 랭크가 같이 죽으면 Pod마다 줄이 있다. 가장 먼저 찍힌 줄의 Pod를 고른다."""
    body = {"status": "success", "data": {"result": [
        {"stream": {"k8s_node_name": "n2"}, "values": [["1795585474457673425", "late"]]},
        {"stream": {"k8s_node_name": "n1"}, "values": [["1795585474220018244", "first"], ["1795585474900000000", "x"]]},
    ]}}
    assert _earliest(body) == ({"k8s_node_name": "n1"}, "first")
    assert _earliest({"status": "success", "data": {"result": []}}) is None


# ---------- 메모리 부족: trainer 로그에 CUDA out of memory가 있으면 oom (GPU-222) ----------
POD_EXIT = dict(signal="pod_exit", source="pod", xid=None, pod=POD["pod"], reason="Error")


def test_pod_exit_with_cuda_oom_in_any_pod_of_the_job():
    """랭크 1이 메모리 부족으로 죽고 랭크 0은 따라 죽었다. 랭크 0의 사건도 같은 판정이 되게 작업 전체 로그를 본다."""
    logs = FakeLogs(None, oom=({"k8s_node_name": "n7", "k8s_pod_name": "job-3f9a1c2e-g0003-w-0-1"}, OOM_LINE))
    ctx = Enricher(FakeMimir(kube_pod_labels=[POD], up=UP), logs)(signal(**POD_EXIT))
    assert (ctx.oom, ctx.log_line, ctx.job_id, ctx.generation, ctx.node) == (True, OOM_LINE, "3f9a1c2e", 3, None)
    (query, start, end), = logs.calls
    assert r'k8s_pod_name=~"job\\-3f9a1c2e\\-g0003\\-.+"' in query and '|= "CUDA out of memory"' in query and start < TS < end


@pytest.mark.parametrize("oom", [None, TimeoutError("timed out")])
def test_pod_exit_without_oom_line_is_not_oom(oom):
    ctx = Enricher(FakeMimir(kube_pod_labels=[POD], up=UP), FakeLogs(None, oom=oom))(signal(**POD_EXIT))
    assert (ctx.oom, ctx.log_line, ctx.job_id) == (False, None, "3f9a1c2e")


@pytest.mark.parametrize("kw, mimir", [
    ({"reason": "OOMKilled"}, {"kube_pod_labels": [POD]}),      # 종료 사유로 이미 안다
    ({}, {}),                                                   # 어느 작업인지 모른다
    ({"signal": "xid", "source": "dcgm", "xid": 79, "pod": None, "reason": None}, {"kube_pod_labels": [POD]}),
])
def test_log_is_not_searched_when_it_cannot_change_the_verdict(kw, mimir):
    logs = FakeLogs(None, oom=({"k8s_node_name": "n7"}, OOM_LINE))
    ctx = Enricher(FakeMimir(up=UP, **mimir), logs)(signal(**{**POD_EXIT, **kw}))
    assert (ctx.oom, logs.calls) == (False, [])


def test_job_failed_with_cuda_oom_takes_node_and_line_from_the_oom_line():
    logs = FakeLogs(({"k8s_node_name": "n3"}, FAILED_LINE), oom=({"k8s_node_name": "n7"}, OOM_LINE))
    ctx = Enricher(FakeMimir(up=UP[:2]), logs)(signal(**JOB_FAILED))
    assert (ctx.oom, ctx.node, ctx.log_line) == (True, "n7", OOM_LINE)
    assert len(logs.calls) == 1         # 찾았으면 실패 줄은 안 본다


def test_job_failed_oom_line_without_node_label_is_still_oom():
    ctx = Enricher(FakeMimir(up=UP[:2]), FakeLogs(None, oom=({}, OOM_LINE)))(signal(**JOB_FAILED))
    assert (ctx.oom, ctx.node, ctx.log_line) == (True, None, OOM_LINE)


def straggler():
    return signal(signal="straggler", source="straggler", alert="FailoverStraggler", xid=None,
                  job_id="3f9a1c2e", generation=3, ratio=1.6)


class StepMimir(FakeMimir):
    """step 시간만 조회 시각에 따라 답이 다르다: 지금(None) = 느린 값, 과거 = 정상 값."""
    def __init__(self, slow, normal, **answers):
        super().__init__(**answers)
        self.slow, self.normal = slow, normal

    def __call__(self, expr, at=None):
        if "train_step_seconds" in expr:
            self.calls.append((expr, at))
            return self.slow if at is None else self.normal
        return super().__call__(expr, at)


def test_straggler_takes_job_from_the_alert_and_reads_both_step_times():
    mimir = StepMimir([{"__value__": 1.6}], [{"__value__": 1.0}], up=UP,
                      DCGM_FI_DEV_GPU_TEMP=[{"UUID": GPU, "__value__": 41.0}])
    ctx = Enricher(mimir)(straggler())
    assert (ctx.job_id, ctx.generation, ctx.gpu_uuid, ctx.collector_healthy) == ("3f9a1c2e", 3, GPU, True)
    assert ctx.step_seconds == (1.6, 1.0)
    steps = [(expr, at) for expr, at in mimir.calls if "train_step_seconds" in expr]
    assert [at for _, at in steps] == [None, TS - timedelta(minutes=1)]     # 지금, 느려지기 1분 전
    assert all('job_id="3f9a1c2e", generation="3", phase="steady"' in expr for expr, _ in steps)
    assert not any("kube_pod_labels" in expr for expr, _ in mimir.calls)    # 작업을 찾으러 가지 않는다


@pytest.mark.parametrize("slow, normal", [
    ([{"__value__": 1.6}], []),                         # 시작부터 느렸다: 직전 값이 없음
    ([{"__value__": 1.6}], [{"__value__": float("nan")}]),
    ([], [{"__value__": 1.0}]),
    (RuntimeError("timeout"), [{"__value__": 1.0}]),
])
def test_straggler_without_both_step_times_is_none(slow, normal):
    class Mimir(StepMimir):
        def __call__(self, expr, at=None):
            rows = super().__call__(expr, at)
            if isinstance(rows, Exception):
                raise rows
            return rows
    assert Enricher(Mimir(slow, normal, up=UP))(straggler()).step_seconds is None
