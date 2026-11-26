"""신호 + 판정 → v1 장애 이벤트 (docs/fault-event.md 2절). 모든 필드를 채우고 없는 값은 null."""
from __future__ import annotations

import secrets
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from .rules import Verdict
from .signals import Signal

DCGM_XID_METRIC = "DCGM_FI_DEV_XID_ERRORS"


@dataclass(frozen=True)
class Context:
    """신호에 없어 Mimir(작업 실패의 node, log_line · oom은 Loki)에서 채우는 값 (enrich.py)."""
    az: str
    collector_healthy: bool
    job_id: str | None = None
    generation: int | None = None
    gpu_uuid: str | None = None     # 커널 로그 경로는 PCI 주소만 알아서 dcgm-exporter 라벨로 찾은 값
    # site · node_lost 전용: 끊기기 직전까지 그 클러스터(node_lost는 그 노드)에서 Running이던 (작업, 세대). 조회가 실패하면 None
    jobs: tuple[tuple[str, int | None], ...] | None = None
    # job_failed 전용: trainer 로그에서 찾은, 가장 먼저 죽은 Pod의 노드와 그 로그 줄. 못 찾으면 None
    node: str | None = None
    log_line: str | None = None     # Pod 종료 · 작업 실패에서 메모리 부족 줄을 찾았으면 그 줄
    oom: bool = False               # trainer 로그에서 GPU 메모리 부족을 확인했다 → refuse (rules.py)


def iso_z(ts: datetime) -> str:
    return ts.isoformat().replace("+00:00", "Z")


def dedup_target(s: Signal, ctx: Context) -> str:
    """대상 = gpu_uuid, 없으면 node, site는 cluster/작업(작업을 못 찾으면 cluster).
    Pod 종료는 그 Pod(같은 노드의 다른 Pod가 끝난 것은 다른 사건), 작업 실패는 cluster/작업/세대."""
    if s.signal == "site" and ctx.job_id:
        return f"{s.cluster}/{ctx.job_id}"     # 사이트 1건이 작업마다 사건이 된다. 키가 같으면 두 번째 작업부터 중복으로 버려진다
    if s.signal == "job_failed":
        return f"{s.cluster}/{s.job_id}/g{s.generation}"   # 작업 · 세대마다 1건
    return s.pod or s.gpu_uuid or ctx.gpu_uuid or s.node or s.cluster


def dedup_code(s: Signal) -> str:
    """코드 = xid, 없으면 source."""
    return str(s.xid) if s.xid is not None else s.source


def build(s: Signal, v: Verdict, ctx: Context, dedup_key: str,
          correlated: list[dict[str, str]] | None = None) -> dict[str, Any]:
    event_id = secrets.token_hex(16)        # trace-id와 같은 값. 판정마다 새로 만든다(재판정 포함)
    from_dcgm = s.source == "dcgm"
    return {
        "schema_version": "1",
        "event_id": event_id,
        "traceparent": f"00-{event_id}-{secrets.token_hex(8)}-01",
        "ts": iso_z(s.ts),
        "cluster": s.cluster,
        "az": ctx.az,
        "node": s.node or ctx.node,
        "gpu_uuid": None if s.signal == "site" else (s.gpu_uuid or ctx.gpu_uuid),
        "job_id": ctx.job_id,
        "generation": ctx.generation,
        "source": s.source,
        "raw": {
            "xid": s.xid,
            "exit_code": s.exit_code,
            "metric": DCGM_XID_METRIC if from_dcgm else None,
            "value": s.xid if from_dcgm else None,
            "log_line": ctx.log_line or s.log_line,
        },
        "correlated": correlated or [],
        "collector_healthy": ctx.collector_healthy,
        "dedup_key": dedup_key,
        "category": v.category,
        "confidence": v.confidence,
        "suggested_action": v.action,
    }
