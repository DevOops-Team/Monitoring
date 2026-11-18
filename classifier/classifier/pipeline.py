"""알람 1건을 끝까지 처리한다: 정규화 → 보강 → 판정 → 중복 제거 → 조립 → 발행 (docs/classifier.md 1절)."""
from __future__ import annotations

import json
import logging
import threading
from datetime import datetime
from typing import Any, Callable

from prometheus_client import CollectorRegistry, Counter

from . import dedup
from .event import Context, build, dedup_code, dedup_target
from .publish import InvalidEvent, Publisher
from .rules import Rules
from .signals import RejectError, Signal, parse_alert

logger = logging.getLogger("classifier")

# 처리 결과. classifier_alerts_total의 outcome 라벨 값
RESOLVED = "resolved"               # 꺼짐 알림
REJECTED = "rejected"               # 라벨 계약 위반
IGNORED = "ignored"                 # 이벤트를 만들지 않는 신호 (Xid 45, 종료 코드 0 · 75)
DUPLICATE = "duplicate"             # 이미 낸 사건
PUBLISHED = "published"
INVALID = "invalid"                 # 스키마 검증 실패
PUBLISH_FAILED = "publish_failed"   # Kafka 전달 실패. 웹훅이 503을 돌려줘 Alertmanager가 다시 보낸다


def log(msg: str, **fields: Any) -> None:
    logger.info(json.dumps({"msg": msg, **fields}, ensure_ascii=False, default=str))


class Pipeline:
    def __init__(self, rules: Rules, publisher: Publisher, enrich: Callable[[Signal], Context]):
        self.rules = rules
        self.publisher = publisher
        self.enrich = enrich
        self.dedup = dedup.Dedup()
        self._lock = threading.Lock()       # 웹훅은 스레드 풀에서 돈다. 중복 제거는 한 번에 하나씩

        self.registry = CollectorRegistry()
        self.alerts = Counter("classifier_alerts", "받은 알람 수 (처리 결과별)", ["outcome"], registry=self.registry)
        self.rejected = Counter("classifier_signals_rejected", "라벨 계약 위반으로 버린 알람", registry=self.registry)
        self.unmatched = Counter("classifier_unmatched", "판정 표에 없어 hold로 낸 신호", registry=self.registry)
        self.invalid = Counter("classifier_events_invalid", "스키마 검증 실패로 내지 않은 이벤트", registry=self.registry)
        self.published = Counter("classifier_events_published", "발행한 이벤트",
                                 ["type", "category", "action"], registry=self.registry)

    def handle(self, alert: dict[str, Any], now: datetime) -> str:
        with self._lock:
            outcome = self._handle(alert, now)
        self.alerts.labels(outcome).inc()
        return outcome

    def _handle(self, alert: dict[str, Any], now: datetime) -> str:
        try:
            s = parse_alert(alert, now)
        except RejectError as e:
            self.rejected.inc()
            log("signal.rejected", reason=str(e), labels=alert.get("labels"))
            return REJECTED
        if s is None:
            return RESOLVED
        if self.rules.ignored(s):
            return IGNORED

        ctx = self.enrich(s)
        v = self.rules.judge(s, ctx.collector_healthy)
        if not v.matched:
            self.unmatched.inc()
            log("signal.unmatched", alert=s.alert, xid=s.xid, node=s.node, log_line=s.log_line)

        self.dedup.prune(now)
        d = self.dedup.decide(dedup_target(s, ctx), dedup_code(s), s.ts, v.action)
        if d.kind == dedup.DUPLICATE:
            return DUPLICATE

        event = build(s, v, ctx, d.key)
        try:
            env = self.publisher.publish(event, d.kind, now)
        except InvalidEvent as e:
            self.invalid.inc()
            log("event.invalid", reason=str(e), event=event)
            return INVALID
        except Exception as e:      # Kafka 전달 실패. 기억하지 않으므로 재전송 때 다시 시도한다
            log("event.publish_failed", reason=str(e), dedup_key=d.key)
            return PUBLISH_FAILED

        self.dedup.record(d.key, v.action)
        self.published.labels(env["type"], v.category, v.action).inc()
        log("event.published", event_id=event["event_id"], type=env["type"], dedup_key=d.key,
            category=v.category, action=v.action, node=s.node, job_id=ctx.job_id)
        return PUBLISHED
