"""신호에 없는 값을 Mimir에서 채운다 (docs/fault-event.md 2절 job_id · collector_healthy).
작업 실패(job_failed)만 Loki도 본다: 죽은 Pod가 지워져 지표에 없으므로 trainer 로그에서 죽은 노드를 찾는다(GPU-288).
Pod 종료 · 작업 실패는 같은 로그에서 GPU 메모리 부족 줄도 찾는다. 있으면 재시도 없이 실패로 확정한다(GPU-222).

EKS API를 부르지 않는다. 각 클러스터의 kube-state-metrics · up 지표가 remote_write로 Mimir에 와 있다.
스트래글러만 step 시간 두 개(지금 · 느려지기 직전)를 더 본다: 옮겨서 얻는 이득 계산에 쓴다(GPU-296).

조회가 실패하면 모르는 값으로 둔다: collector_healthy=false(→ hold), job_id=null(→ 컨트롤러가 HOLD), node=null, oom=false,
step_seconds=None(→ hold).
"""
from __future__ import annotations

import json
import re
import urllib.parse
import urllib.request
from datetime import datetime, timedelta
from typing import Any, Callable

from .event import Context
from .pipeline import log
from .signals import Signal

UNKNOWN_AZ = "unknown"
# 노드의 수집 경로. 셋 다 up == 1이어야 살아 있다고 본다. 에이전트가 죽으면 up 자체가 끊겨 결과가 비므로 false가 된다
COLLECTOR_JOBS = ("prometheus-agent", "otel-gateway", "fluent-bit")
# 커널 로그 경로는 PCI 주소만 안다. g4dn은 노드당 GPU 1장이라 노드로 UUID를 찾는다
# TODO(EKS 테스트): dcgm-exporter 지표의 라벨 이름(node · UUID)을 실제 값으로 확인
GPU_METRIC = "DCGM_FI_DEV_GPU_TEMP"

# torchrun이 자기 학습 프로세스가 죽었을 때 남기는 줄. 랭크마다 Pod가 따로 torchrun을 돌려 죽은 Pod의 로그에만 있다
FAILED_LINE = "failed (exitcode"
# PyTorch가 GPU 메모리를 못 잡았을 때 남기는 줄(torch.OutOfMemoryError: CUDA out of memory. Tried to allocate …).
# TODO(EKS 주입 ⑥): 실제 줄 원문으로 확인. 지금은 PyTorch 버전이 달라도 같은 부분만 본다
OOM_LINE = "CUDA out of memory"
# 로그 줄은 작업 실패(신호 시각)보다 앞선다(11/25 실측 8 ~ 45초). 뒤쪽 여유는 노드 시계 차이
LOG_BEFORE, LOG_AFTER = timedelta(minutes=10), timedelta(minutes=1)
# 스트래글러의 「정상 step 시간」을 재는 창의 끝 = 느려진 시각(알람이 pending이 된 시각)보다 이만큼 앞.
# 랭크 계산 시간은 프로파일된 step에서만 갱신되고 수집 · 룰 평가(각 15초)도 거쳐서, 알람이 본 시각보다 조금 먼저 느려졌다
STEP_NORMAL_BEFORE = timedelta(minutes=1)

Query = Callable[[str, datetime | None], list[dict[str, Any]]]
# (LogQL, 시작, 끝) → 그 구간에서 가장 먼저 찍힌 줄의 (스트림 라벨, 줄). 없으면 None
Logs = Callable[[str, datetime, datetime], "tuple[dict[str, str], str] | None"]


def _q(value: str) -> str:
    """PromQL 문자열 안에 넣을 라벨 값. 따옴표 · 역슬래시가 식을 깨지 못하게 한다."""
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def mimir_query(base_url: str, timeout: float = 3.0) -> Query:
    """instant query. 결과의 metric 라벨 묶음과 값을 돌려준다."""
    url = base_url.rstrip("/") + "/prometheus/api/v1/query"

    def query(expr: str, at: datetime | None = None) -> list[dict[str, Any]]:
        params = {"query": expr}
        if at is not None:
            params["time"] = f"{at.timestamp():.3f}"
        with urllib.request.urlopen(f"{url}?{urllib.parse.urlencode(params)}", timeout=timeout) as r:
            body = json.load(r)
        if body.get("status") != "success":
            raise RuntimeError(f"Mimir: {body.get('error')}")
        return [{**x["metric"], "__value__": float(x["value"][1])} for x in body["data"]["result"]]

    return query


def _earliest(body: dict[str, Any]) -> tuple[dict[str, str], str] | None:
    """Loki query_range 응답에서 가장 이른 줄. 여러 Pod에 줄이 있으면 스트림이 여럿이다."""
    lines = [(int(ts), stream["stream"], line) for stream in body["data"]["result"] for ts, line in stream["values"]]
    if not lines:
        return None
    _, labels, line = min(lines, key=lambda x: x[0])
    return labels, line


def loki_first_line(base_url: str, timeout: float = 3.0) -> Logs:
    url = base_url.rstrip("/") + "/loki/api/v1/query_range"

    def first_line(query: str, start: datetime, end: datetime) -> tuple[dict[str, str], str] | None:
        params = {"query": query, "start": f"{start.timestamp():.0f}", "end": f"{end.timestamp():.0f}",
                  "direction": "forward", "limit": "1"}
        with urllib.request.urlopen(f"{url}?{urllib.parse.urlencode(params)}", timeout=timeout) as r:
            body = json.load(r)
        if body.get("status") != "success":
            raise RuntimeError(f"Loki: {body.get('error')}")
        return _earliest(body)

    return first_line


class Enricher:
    def __init__(self, query: Query, logs: Logs | None = None):
        self.query = query
        self.logs = logs        # 없으면 작업 실패 사건은 노드 없이 나간다

    def __call__(self, s: Signal) -> Context:
        if s.signal == "site":      # 클러스터가 통째로 안 보인다. 끊기기 전 마지막 지표로 그때 돌던 작업만 찾는다
            return Context(az=UNKNOWN_AZ, collector_healthy=False, jobs=self._cluster_jobs(s))
        if s.signal == "node_lost":     # 노드가 사라졌다. 그 노드의 Fluent Bit이 꺼진 건 당연하니 클러스터 수집 경로만 본다
            return Context(az=self._az(s, at=s.ts), collector_healthy=self._collector_healthy(s, node_path=False),
                           jobs=self._cluster_jobs(s, node=s.node))
        if s.signal == "job_failed":    # 작업 · 세대는 알람에 있다. 노드는 로그에서 찾고, 수집 경로는 클러스터 것만 본다
            oom = self._trainer_line(s, s.job_id, s.generation, OOM_LINE)
            node, line = oom or self._trainer_line(s, s.job_id, s.generation, FAILED_LINE) or (None, None)
            if node is None and oom is None:    # 실패 줄은 노드를 찾으려고 보는 것이라 노드 라벨이 없으면 쓰지 않는다
                line = None
            return Context(az=self._az(s, at=s.ts, node=node) if node else UNKNOWN_AZ,
                           collector_healthy=self._collector_healthy(s, node_path=False),
                           job_id=s.job_id, generation=s.generation, node=node, log_line=line, oom=oom is not None)
        if s.signal == "straggler":     # 작업 · 세대 · 노드는 알람에 있다(trainer 지표 라벨)
            return Context(az=self._az(s), collector_healthy=self._collector_healthy(s), job_id=s.job_id,
                           generation=s.generation, gpu_uuid=self._gpu_uuid(s), step_seconds=self._step_seconds(s))
        job_id, generation = self._job(s)
        oom = None
        # 종료 사유가 OOMKilled면 이미 메모리 부족이다(rules.py). 그 밖의 Pod 종료만 로그를 본다
        if s.signal == "pod_exit" and s.reason != "OOMKilled" and job_id is not None and generation is not None:
            oom = self._trainer_line(s, job_id, generation, OOM_LINE)
        return Context(
            az=self._az(s),
            collector_healthy=self._collector_healthy(s),
            job_id=job_id,
            generation=generation,
            gpu_uuid=self._gpu_uuid(s),
            log_line=oom[1] if oom else None,
            oom=oom is not None,
        )

    def nodes_not_ready(self, cluster: str, job_id: str, generation: int) -> tuple[str, ...] | None:
        """그 작업 · 세대의 Pod가 최근 10분 안에 있던 노드 가운데 지금 Ready가 아닌(NotReady · Unknown · 삭제) 노드. 조회 실패는 None.
        종료 신호를 판정할 때 묻는다: 노드가 죽는 중이면 곧 노드 소실 사건이 온다(pipeline.py _pod_exit, GPU-220).
        KSM이 안 긁히면 노드 지표가 한꺼번에 사라져 전부 죽은 것처럼 보이므로, 그때는 빈 답을 낸다(FailoverGpuNodeLost와 같은 조건)."""
        c = _q(cluster)
        expr = (f'group by (node) (last_over_time(kube_pod_info{{cluster="{c}", namespace="training"}}[10m])'
                f' and on (namespace, pod) last_over_time(kube_pod_labels{{cluster="{c}", namespace="training",'
                f' label_devoops_job_id="{_q(job_id)}", label_devoops_generation="{generation}"}}[10m]))'
                f' unless on (node) (kube_node_status_condition{{cluster="{c}", condition="Ready", status="true"}} == 1)'
                f' and on () (max(up{{cluster="{c}", job="kube-state-metrics"}}) == 1)')
        try:
            rows = self.query(expr, None)
        except Exception as e:
            log("enrich.failed", what="nodes_not_ready", reason=str(e), cluster=cluster, job_id=job_id)
            return None
        return tuple(sorted(n for r in rows if (n := r.get("node"))))

    def _try(self, what: str, s: Signal, expr: str, at: datetime | None) -> list[dict[str, Any]] | None:
        try:
            return self.query(expr, at)
        except Exception as e:
            log("enrich.failed", what=what, reason=str(e), cluster=s.cluster, node=s.node)
            return None

    def _trainer_line(self, s: Signal, job_id: str, generation: int, needle: str) -> tuple[str | None, str] | None:
        """그 작업 · 세대의 모든 trainer Pod 로그에서 needle이 든, 가장 먼저 찍힌 줄의 (노드, 줄). 못 찾으면 None.
        Pod 하나가 아니라 작업 전체를 본다: Pod 종료 사건은 랭크마다 나가는데 판정이 랭크마다 갈리면 안 된다."""
        if self.logs is None:
            return None
        pods = _q(re.escape(f"job-{job_id}-g{generation:04d}-")) + ".+"
        query = (f'{{k8s_cluster_name="{_q(s.cluster)}", k8s_namespace_name="training", k8s_container_name="trainer",'
                 f' k8s_pod_name=~"{pods}"}} |= "{needle}"')
        try:
            found = self.logs(query, s.ts - LOG_BEFORE, s.ts + LOG_AFTER)
        except Exception as e:
            log("enrich.failed", what="trainer_line", needle=needle, reason=str(e), cluster=s.cluster, job_id=job_id)
            return None
        if found is None:
            return None
        return found[0].get("k8s_node_name") or None, found[1]

    def _collector_healthy(self, s: Signal, node_path: bool = True) -> bool:
        """분류 시점(지금) 기준. node_path=False면 그 노드의 Fluent Bit은 보지 않는다(노드가 사라진 신호)."""
        c = _q(s.cluster)
        expr = f'up{{cluster="{c}", job=~"prometheus-agent|otel-gateway"}}'
        if node_path:
            expr += f' or up{{cluster="{c}", job="fluent-bit", node="{_q(s.node)}"}}'
        rows = self._try("collector", s, expr, None)
        if rows is None:
            return False
        jobs = COLLECTOR_JOBS if node_path else COLLECTOR_JOBS[:2]
        up = {job: [r["__value__"] for r in rows if r.get("job") == job] for job in jobs}
        return all(values and min(values) == 1 for values in up.values())

    def _job(self, s: Signal) -> tuple[str | None, int | None]:
        """신호 발생 시각에 그 Pod(또는 그 노드의 Pod)에 붙어 있던 작업 라벨. Pod가 이미 사라졌어도 5분 안이면 찾는다."""
        c = _q(s.cluster)
        labels = f'last_over_time(kube_pod_labels{{cluster="{c}", label_devoops_job_id!=""%s}}[5m])'
        if s.pod:
            expr = labels % f', pod="{_q(s.pod)}"'
        else:
            expr = (labels % "") + (f' and on (namespace, pod)'
                                    f' last_over_time(kube_pod_info{{cluster="{c}", node="{_q(s.node)}"}}[5m])')
        rows = self._try("job", s, expr, s.ts)
        jobs = {r["label_devoops_job_id"] for r in rows or []}
        if len(jobs) != 1:          # 없거나, 한 노드에 여러 작업 → 특정할 수 없다
            return None, None
        generations = [int(g) for r in rows if (g := r.get("label_devoops_generation", "")).isdigit()]
        return jobs.pop(), max(generations) if generations else None

    def _cluster_jobs(self, s: Signal, node: str | None = None) -> tuple[tuple[str, int | None], ...] | None:
        """신호 시각(마지막 샘플)에 그 클러스터(node를 주면 그 노드)에서 Running이던 학습 Pod의 (작업, 가장 큰 세대). 끝난 Pod는 뺀다."""
        c = _q(s.cluster)
        on_node = ('' if node is None else
                   f' and on (namespace, pod) last_over_time(kube_pod_info{{cluster="{c}", node="{_q(node)}"}}[5m])')
        rows = self._try("cluster_jobs", s,
                         f'last_over_time(kube_pod_labels{{cluster="{c}", label_devoops_job_id!=""}}[5m])'
                         f' and on (namespace, pod)'
                         f' (last_over_time(kube_pod_status_phase{{cluster="{c}", phase="Running"}}[5m]) == 1)'
                         + on_node, s.ts)
        if rows is None:
            return None
        if not rows and not self._ksm_was_up(s):    # KSM이 먼저 죽어 있었으면 「작업 없음」이 아니라 「모름」
            return None
        generations: dict[str, list[int]] = {}
        for r in rows:
            g = r.get("label_devoops_generation", "")
            generations.setdefault(r["label_devoops_job_id"], []).extend([int(g)] if g.isdigit() else [])
        return tuple((job, max(gens) if gens else None) for job, gens in sorted(generations.items()))

    def _ksm_was_up(self, s: Signal) -> bool:
        """신호 시각(직전 5분)에 그 클러스터 KSM이 긁히고 있었나. 조회 실패 · 지표 없음 · 0이면 False."""
        rows = self._try("ksm_up", s, f'last_over_time(up{{cluster="{_q(s.cluster)}", job="kube-state-metrics"}}[5m])', s.ts)
        return bool(rows) and all(r["__value__"] == 1 for r in rows)

    def _az(self, s: Signal, at: datetime | None = None, node: str | None = None) -> str:
        """at을 주면 그 시각 직전 5분의 마지막 값(노드가 이미 지워졌을 수 있는 신호). node는 신호에 노드가 없을 때."""
        # TODO(EKS overlay): KSM --metric-labels-allowlist에 nodes=[topology.kubernetes.io/zone]을 넣어야 값이 나온다
        expr = f'kube_node_labels{{cluster="{_q(s.cluster)}", node="{_q(node or s.node)}"}}'
        rows = self._try("az", s, expr if at is None else f'last_over_time({expr}[5m])', at)
        zones = {z for r in rows or [] if (z := r.get("label_topology_kubernetes_io_zone"))}
        return zones.pop() if len(zones) == 1 else UNKNOWN_AZ

    def _step_seconds(self, s: Signal) -> tuple[float, float] | None:
        """그 작업 · 세대의 5분 평균 step 시간 (지금 = 느린 값, 느려지기 직전 = 정상 값). FSDP는 랭크가 같이 나아가
        step 시간이 랭크마다 같으므로 작업 전체로 본다. 시작부터 느렸으면 직전 값이 없어 None."""
        sel = f'{{cluster="{_q(s.cluster)}", job_id="{_q(s.job_id)}", generation="{s.generation}", phase="steady"}}'
        expr = f'sum(rate(train_step_seconds_sum{sel}[5m])) / sum(rate(train_step_seconds_count{sel}[5m]))'
        found = []
        for what, at in (("step_slow", None), ("step_normal", s.ts - STEP_NORMAL_BEFORE)):
            rows = self._try(what, s, expr, at)
            value = rows[0]["__value__"] if rows else None
            if value is None or not value > 0 or value == float("inf"):     # NaN(그 창에 step이 없음)도 여기서 걸린다
                return None
            found.append(value)
        return found[0], found[1]

    def _gpu_uuid(self, s: Signal) -> str | None:
        if s.signal not in ("xid", "straggler") or s.gpu_uuid:
            return None
        rows = self._try("gpu_uuid", s,
                         f'last_over_time({GPU_METRIC}{{cluster="{_q(s.cluster)}", node="{_q(s.node)}"}}[5m])', s.ts)
        uuids = {u for r in rows or [] if (u := r.get("UUID"))}
        return uuids.pop() if len(uuids) == 1 else None
