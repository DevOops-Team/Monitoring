"""신호에 없는 값을 Mimir에서 채운다 (docs/fault-event.md 2절 job_id · collector_healthy).

EKS API를 부르지 않는다. 각 클러스터의 kube-state-metrics · up 지표가 remote_write로 Mimir에 와 있다.
조회가 실패하면 모르는 값으로 둔다: collector_healthy=false(→ hold), job_id=null(→ 컨트롤러가 HOLD).
"""
from __future__ import annotations

import json
import urllib.parse
import urllib.request
from datetime import datetime
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

Query = Callable[[str, datetime | None], list[dict[str, Any]]]


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


class Enricher:
    def __init__(self, query: Query):
        self.query = query

    def __call__(self, s: Signal) -> Context:
        if s.signal == "site":      # 클러스터가 통째로 안 보인다. 끊기기 전 마지막 지표로 그때 돌던 작업만 찾는다
            return Context(az=UNKNOWN_AZ, collector_healthy=False, jobs=self._cluster_jobs(s))
        job_id, generation = self._job(s)
        return Context(
            az=self._az(s),
            collector_healthy=self._collector_healthy(s),
            job_id=job_id,
            generation=generation,
            gpu_uuid=self._gpu_uuid(s),
        )

    def _try(self, what: str, s: Signal, expr: str, at: datetime | None) -> list[dict[str, Any]] | None:
        try:
            return self.query(expr, at)
        except Exception as e:
            log("enrich.failed", what=what, reason=str(e), cluster=s.cluster, node=s.node)
            return None

    def _collector_healthy(self, s: Signal) -> bool:
        """분류 시점(지금) 기준."""
        c, n = _q(s.cluster), _q(s.node)
        rows = self._try("collector", s,
                         f'up{{cluster="{c}", job=~"prometheus-agent|otel-gateway"}}'
                         f' or up{{cluster="{c}", job="fluent-bit", node="{n}"}}', None)
        if rows is None:
            return False
        up = {job: [r["__value__"] for r in rows if r.get("job") == job] for job in COLLECTOR_JOBS}
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

    def _cluster_jobs(self, s: Signal) -> tuple[tuple[str, int | None], ...] | None:
        """신호 시각(마지막 샘플)에 그 클러스터에서 Running이던 학습 Pod의 (작업, 가장 큰 세대). 끝난 Pod는 뺀다."""
        c = _q(s.cluster)
        rows = self._try("cluster_jobs", s,
                         f'last_over_time(kube_pod_labels{{cluster="{c}", label_devoops_job_id!=""}}[5m])'
                         f' and on (namespace, pod)'
                         f' (last_over_time(kube_pod_status_phase{{cluster="{c}", phase="Running"}}[5m]) == 1)', s.ts)
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

    def _az(self, s: Signal) -> str:
        # TODO(EKS overlay): KSM --metric-labels-allowlist에 nodes=[topology.kubernetes.io/zone]을 넣어야 값이 나온다
        rows = self._try("az", s, f'kube_node_labels{{cluster="{_q(s.cluster)}", node="{_q(s.node)}"}}', None)
        zones = {z for r in rows or [] if (z := r.get("label_topology_kubernetes_io_zone"))}
        return zones.pop() if len(zones) == 1 else UNKNOWN_AZ

    def _gpu_uuid(self, s: Signal) -> str | None:
        if s.signal != "xid" or s.gpu_uuid:
            return None
        rows = self._try("gpu_uuid", s,
                         f'last_over_time({GPU_METRIC}{{cluster="{_q(s.cluster)}", node="{_q(s.node)}"}}[5m])', s.ts)
        uuids = {u for r in rows or [] if (u := r.get("UUID"))}
        return uuids.pop() if len(uuids) == 1 else None
