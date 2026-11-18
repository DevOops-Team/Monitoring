"""Alertmanager 웹훅의 알람 1건 → 내부 신호 (docs/classifier.md 2절 라벨 계약)."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

# signal 라벨 → 이벤트 source. xid만 경로가 둘이라 source 라벨로 받는다
_SOURCE = {"pod_exit": "pod", "spot": "spot", "site": "site"}
_XID_SOURCES = ("dmesg", "dcgm")


class RejectError(ValueError):
    """라벨 계약 위반. 이벤트를 만들지 않고 카운터만 올린다."""


@dataclass(frozen=True)
class Signal:
    signal: str                 # xid | pod_exit | spot | site
    source: str                 # dcgm | dmesg | pod | spot | site
    alert: str                  # alertname
    cluster: str
    ts: datetime                # 원본 신호 발생 시각 (annotation event_ts)
    node: str | None = None     # site는 None
    gpu_uuid: str | None = None
    xid: int | None = None
    exit_code: int | None = None
    pod: str | None = None
    log_line: str | None = None


def _require(labels: dict[str, str], name: str) -> str:
    v = labels.get(name)
    if not v:
        raise RejectError(f"missing label {name}")
    return v


def _int(labels: dict[str, str], name: str) -> int:
    try:
        return int(_require(labels, name))
    except ValueError as e:
        raise RejectError(f"label {name}: 정수가 아님") from e


def _event_ts(annotations: dict[str, str]) -> datetime:
    v = annotations.get("event_ts")
    if not v:
        raise RejectError("missing annotation event_ts")
    try:
        ts = datetime.fromisoformat(v.replace("Z", "+00:00"))
    except ValueError as e:
        raise RejectError(f"event_ts: 시각 형식 오류 ({v})") from e
    if ts.tzinfo is None:
        raise RejectError("event_ts: 시간대 없음 (UTC 'Z' 필요)")
    return ts


def parse_alert(alert: dict[str, Any]) -> Signal | None:
    """resolved 알람은 None. 계약에 안 맞으면 RejectError."""
    if alert.get("status") == "resolved":
        return None
    labels = alert.get("labels") or {}
    annotations = alert.get("annotations") or {}

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
        cluster=_require(labels, "cluster"),
        ts=_event_ts(annotations),
        node=None if signal == "site" else _require(labels, "node"),
        gpu_uuid=labels.get("gpu_uuid") or None,
        xid=_int(labels, "xid") if signal == "xid" else None,
        exit_code=_int(labels, "exit_code") if signal == "pod_exit" else None,
        pod=_require(labels, "pod") if signal == "pod_exit" else None,
        log_line=annotations.get("log_line") or None,
    )
