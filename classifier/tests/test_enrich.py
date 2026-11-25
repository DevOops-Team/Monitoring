from datetime import datetime, timezone

from classifier.enrich import Enricher, _q
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
