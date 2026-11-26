"""Alertmanager 웹훅의 알람 1건 → 내부 신호 (docs/classifier.md 2절 라벨 계약)."""
from __future__ import annotations

import math
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

# signal 라벨 → 이벤트 source. xid만 경로가 둘이라 source 라벨로 받는다
# job_failed = 학습 작업(JobSet)이 실패로 끝남. Pod 종료와 같은 종류의 사건이라 source는 pod, 어느 Pod · 노드인지는 모른다
# straggler = 한 랭크가 10분째 느림. 알람이 작업 · 세대 · 노드를 직접 준다(trainer 지표 라벨)
_SOURCE = {"pod_exit": "pod", "job_failed": "pod", "spot": "spot", "site": "site", "node_lost": "node",
           "straggler": "straggler"}
_NO_NODE = ("site", "job_failed")
_WITH_JOB = ("job_failed", "straggler")     # 알람 라벨에 job_id · generation이 있는 신호
_XID_SOURCES = ("dmesg", "dcgm")
_GPU_UUID = re.compile(r"^GPU-[0-9a-f-]+$")     # v1 스키마와 같은 형식
_POD_NAME = re.compile(r"^[a-z0-9]([-a-z0-9.]{0,251}[a-z0-9])?$")     # 쿠버네티스 이름 규칙
_FUTURE_SKEW = timedelta(minutes=5)             # 노드 시계 오차 여유. 과거 쪽은 막지 않는다(밀린 로그 줄도 같은 사건으로 묶어야 함)


class RejectError(ValueError):
    """라벨 계약 위반. 이벤트를 만들지 않고 카운터만 올린다."""


@dataclass(frozen=True)
class Signal:
    signal: str                 # xid | pod_exit | job_failed | spot | site | node_lost | straggler
    source: str                 # dcgm | dmesg | pod | spot | site | node | straggler
    alert: str                  # alertname
    cluster: str
    ts: datetime                # 원본 신호 발생 시각 (annotation event_ts)
    node: str | None = None     # site · job_failed는 None
    gpu_uuid: str | None = None
    xid: int | None = None
    exit_code: int | None = None   # 알 수 있을 때만. kube-state-metrics는 재시작하지 않는 컨테이너의 종료 코드를 내지 않는다
    reason: str | None = None   # pod_exit: 컨테이너 종료 사유 (Error · OOMKilled …)
    pod: str | None = None
    log_line: str | None = None
    job_id: str | None = None       # job_failed · straggler만: 알람이 작업 · 세대를 직접 준다
    generation: int | None = None
    ratio: float | None = None      # straggler만: 그 랭크 계산 시간 ÷ 랭크 중앙값 (annotation ratio)


def _require(labels: dict[str, str], name: str) -> str:
    v = labels.get(name)
    if not v:
        raise RejectError(f"missing label {name}")
    return v


def _ident(labels: dict[str, str], name: str) -> str:
    """dedup_key(대상|코드|시각)에 들어가는 값. 구분자가 섞이면 키 형식이 깨진다."""
    v = _require(labels, name)
    if "|" in v:
        raise RejectError(f"label {name}: '|' 포함")
    return v


def _pod(labels: dict[str, str]) -> str:
    """Pod 종료의 dedup_key 대상이 된다. 이름 모양이 아니면 받지 않는다."""
    v = _require(labels, "pod")
    if not _POD_NAME.fullmatch(v):
        raise RejectError("label pod: 쿠버네티스 이름 형식이 아님")
    return v


def _int(labels: dict[str, str], name: str) -> int:
    try:
        return int(_require(labels, name))
    except ValueError as e:
        raise RejectError(f"label {name}: 정수가 아님") from e


def _opt_int(labels: dict[str, str], name: str) -> int | None:
    return _int(labels, name) if labels.get(name) else None


def _ratio(annotations: dict[str, str]) -> float:
    try:
        v = float(annotations.get("ratio") or "")
    except ValueError as e:
        raise RejectError("annotation ratio: 숫자가 아님") from e
    if not math.isfinite(v):    # 중앙값이 0이면 룰이 +Inf를 낸다. 사건(JSON)에 실을 수 없는 값이다
        raise RejectError("annotation ratio: 유한한 수가 아님")
    return v


def _event_ts(annotations: dict[str, str], now: datetime) -> datetime:
    v = annotations.get("event_ts")
    if not v:
        raise RejectError("missing annotation event_ts")
    try:
        ts = datetime.fromisoformat(v.replace("Z", "+00:00"))
    except ValueError as e:
        raise RejectError(f"event_ts: 시각 형식 오류 ({v})") from e
    if ts.tzinfo is None:
        raise RejectError("event_ts: 시간대 없음 (UTC 'Z' 필요)")
    ts = ts.astimezone(timezone.utc)
    if ts > now + _FUTURE_SKEW:
        raise RejectError(f"event_ts: 미래 시각 ({v})")
    return ts


def _gpu_uuid(labels: dict[str, str]) -> str | None:
    """형식이 다르면 버린다(이벤트는 node를 대상으로 나간다). 통째로 거부하면 진짜 고장을 놓친다."""
    v = labels.get("gpu_uuid")
    return v if v and _GPU_UUID.fullmatch(v) else None


def parse_alert(alert: dict[str, Any], now: datetime | None = None) -> Signal | None:
    """resolved 알람은 None. 계약에 안 맞으면 RejectError."""
    if alert.get("status") == "resolved":
        return None
    labels = alert.get("labels") or {}
    annotations = alert.get("annotations") or {}
    if labels.get("pipeline") != "failover":
        raise RejectError(f"label pipeline: {labels.get('pipeline')}")
    now = now or datetime.now(timezone.utc)

    signal = _require(labels, "signal")
    if signal == "xid":
        source = _require(labels, "source")
        if source not in _XID_SOURCES:
            raise RejectError(f"label source: {source}")
    elif signal in _SOURCE:
        source = _SOURCE[signal]
    else:
        raise RejectError(f"label signal: {signal}")

    return Signal(
        signal=signal,
        source=source,
        alert=_require(labels, "alertname"),
        cluster=_ident(labels, "cluster"),
        ts=_event_ts(annotations, now),
        node=None if signal in _NO_NODE else _ident(labels, "node"),
        gpu_uuid=_gpu_uuid(labels),
        xid=_int(labels, "xid") if signal == "xid" else None,
        exit_code=_opt_int(labels, "exit_code") if signal == "pod_exit" else None,
        reason=_require(labels, "reason") if signal == "pod_exit" else None,
        pod=_pod(labels) if signal == "pod_exit" else None,
        log_line=annotations.get("log_line") or None,
        job_id=_ident(labels, "job_id") if signal in _WITH_JOB else None,
        generation=_int(labels, "generation") if signal in _WITH_JOB else None,
        ratio=_ratio(annotations) if signal == "straggler" else None,
    )
