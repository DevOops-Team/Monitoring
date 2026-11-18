"""v1 스키마 검증 → 봉투 → Kafka gpu.fault (docs/classifier.md 6절).

봉투는 수집기 형식(Data 레포 collector/collector/envelope.py)과 같고, 복구 컨트롤러는 payload를 읽는다.
검증에 실패한 이벤트는 내지 않는다. 컨트롤러는 형식이 깨진 메시지를 받으면 멈춘다.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Protocol

from jsonschema import Draft202012Validator, FormatChecker

from . import dedup
from .event import iso_z

TOPIC = "gpu.fault"
SOURCE = "classifier"
ENVELOPE_SCHEMA_VERSION = 1     # 봉투는 정수 1, payload 안의 schema_version은 문자열 "1"
SCHEMA_PATH = Path(__file__).resolve().parents[2] / "schemas" / "fault-event.v1.json"

_validator = Draft202012Validator(json.loads(SCHEMA_PATH.read_text(encoding="utf-8")),
                                  format_checker=FormatChecker())


class InvalidEvent(ValueError):
    """v1 스키마 위반 또는 event_id ≠ trace-id. 발행하지 않는다."""


def validate(event: dict[str, Any]) -> None:
    errors = sorted(_validator.iter_errors(event), key=lambda e: list(e.path))
    if errors:
        raise InvalidEvent("; ".join(f"{'/'.join(map(str, e.path)) or '(root)'}: {e.message}" for e in errors))
    # 스키마로는 못 막는 규칙 (docs/fault-event.md 2절)
    if event["traceparent"].split("-")[1] != event["event_id"]:
        raise InvalidEvent("event_id가 traceparent의 trace-id와 다름")


def event_type(kind: str, action: str) -> str:
    if kind == dedup.RECLASSIFIED:
        return "fault.reclassified"
    return "fault.held" if action == "hold" else "fault.detected"


def envelope(event: dict[str, Any], kind: str, now: datetime) -> dict[str, Any]:
    return {
        "event_id": event["event_id"],
        "schema_version": ENVELOPE_SCHEMA_VERSION,
        "type": event_type(kind, event["suggested_action"]),
        "occurred_at": iso_z(now),          # 분류기 판정 시각 (ADR-15). 원본 신호 시각은 payload.ts
        "source": SOURCE,
        "cluster": event["cluster"],
        "node": event["node"],
        "job_id": event["job_id"],
        "generation": event["generation"],
        "payload": event,
    }


class Producer(Protocol):
    def produce(self, topic: str, *, key: bytes, value: bytes, headers: list[tuple[str, bytes]],
                on_delivery: Callable[[Any, Any], None]) -> None: ...
    def flush(self, timeout: float) -> int: ...


class StdoutProducer:
    """KAFKA_BOOTSTRAP 없음 → 표준 출력으로만 (로컬 확인용)."""
    def produce(self, topic, *, key, value, headers, on_delivery):
        print(json.dumps({"topic": topic, "key": key.decode(), "value": json.loads(value)}, ensure_ascii=False),
              file=sys.stdout, flush=True)
        on_delivery(None, None)

    def flush(self, timeout):
        return 0


def kafka_producer(bootstrap: str) -> Producer:
    from confluent_kafka import Producer as KafkaProducer
    return KafkaProducer({
        "bootstrap.servers": bootstrap,
        "client.id": os.environ.get("HOSTNAME", SOURCE),
        "acks": "all",                  # min.insync.replicas=2 → 브로커 1대 장애에도 유실 없음
        "enable.idempotence": True,     # 재시도로 생기는 중복 방지
    })


class Publisher:
    def __init__(self, producer: Producer, rule_version: str):
        self.producer = producer
        self.rule_version = rule_version

    def publish(self, event: dict[str, Any], kind: str, now: datetime) -> dict[str, Any]:
        """검증하고 낸다. 브로커가 받았다고 답할 때까지 기다린다(그 뒤에야 중복 제거에 기록하므로)."""
        validate(event)
        env = envelope(event, kind, now)
        failed: list[Any] = []
        self.producer.produce(
            TOPIC,
            key=(event["node"] or event["cluster"]).encode(),      # site는 node가 없다
            value=json.dumps(env, ensure_ascii=False).encode(),
            headers=[("rule_version", self.rule_version.encode())],
            on_delivery=lambda err, _msg: failed.append(err) if err is not None else None,
        )
        if self.producer.flush(10.0) or failed:
            raise RuntimeError(f"Kafka 전달 실패: {failed[0] if failed else '10초 안에 확인 못 함'}")
        return env
