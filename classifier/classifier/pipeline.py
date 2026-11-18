"""알람 1건을 끝까지 처리한다: 정규화 → 보강 → 판정 → 중복 제거 → 조립 → 발행 (docs/classifier.md 1절).

Pod 종료 신호만 길이 다르다. 의도된 종료인지 확인하려고 신호 시각 + 30초까지 기다렸다가 판정한다 (4절).
"""
from __future__ import annotations

import json
import logging
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Callable

from prometheus_client import CollectorRegistry, Counter

from . import dedup
from .event import Context, build, dedup_code, dedup_target, iso_z
from .intended import Lifecycle
from .publish import InvalidEvent, Publisher
from .rules import Rules, Verdict
from .signals import RejectError, Signal, parse_alert

logger = logging.getLogger("classifier")

POD_EXIT_WAIT = timedelta(seconds=30)       # 임시값. E2E에서 job.fenced와 Pod 종료의 실제 간격을 재서 고친다
RETRY_AFTER = timedelta(seconds=5)          # 기다리던 신호의 발행이 실패했을 때 다시 해 볼 간격
NO_RECOVERY = ("hold", "none")              # 복구를 부르지 않는 action. 원인 사건으로 치지 않는다

# 처리 결과. classifier_alerts_total의 outcome 라벨 값
RESOLVED = "resolved"               # 꺼짐 알림
REJECTED = "rejected"               # 라벨 계약 위반
IGNORED = "ignored"                 # 이벤트를 만들지 않는 신호 (Xid 45, 종료 코드 0 · 75)
DUPLICATE = "duplicate"             # 이미 낸 사건
PUBLISHED = "published"
INVALID = "invalid"                 # 스키마 검증 실패
PUBLISH_FAILED = "publish_failed"   # Kafka 전달 실패. 웹훅이 503을 돌려줘 Alertmanager가 다시 보낸다
DEFERRED = "deferred"               # Pod 종료. 30초 뒤 판정
INTENDED = "intended"               # Pod 종료. 펜싱 · 사용자 중단
STALE = "stale_generation"          # Pod 종료. 옛 세대의 뒤늦은 종료
ABSORBED = "absorbed"               # Pod 종료. 같은 작업 · 세대의 원인 사건에 묶임


def log(msg: str, **fields: Any) -> None:
    logger.info(json.dumps({"msg": msg, **fields}, ensure_ascii=False, default=str))


@dataclass
class Pending:
    s: Signal
    ctx: Context
    due: datetime


class Pipeline:
    def __init__(self, rules: Rules, publisher: Publisher, enrich: Callable[[Signal], Context],
                 lifecycle: Lifecycle | None = None):
        self.rules = rules
        self.publisher = publisher
        self.enrich = enrich
        self.lifecycle = lifecycle or Lifecycle()   # 읽는 쪽이 없으면 healthy=False라 Pod 종료는 전부 hold
        self.dedup = dedup.Dedup()
        self.pending: list[Pending] = []
        # (job_id, generation) → 복구를 부르는 원인 사건의 신호 시각. 그 세대의 Pod 종료는 결과이지 새 사건이 아니다
        self.causes: dict[tuple[str, int], datetime] = {}
        self._lock = threading.Lock()       # 웹훅은 스레드 풀, 대기 처리는 타이머 스레드에서 돈다

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

    def flush(self, now: datetime) -> None:
        """기한이 된 Pod 종료 신호를 판정한다. 타이머가 1초마다 부른다."""
        with self._lock:
            due = [p for p in self.pending if p.due <= now]
            for p in due:
                outcome = self._pod_exit(p.s, p.ctx, now)
                if outcome == PUBLISH_FAILED:       # 웹훅 응답은 이미 나갔다. 여기서 다시 해 본다
                    p.due = now + RETRY_AFTER
                    continue
                self.pending.remove(p)
                self.alerts.labels(outcome).inc()
            self.dedup.prune(now)
            self.lifecycle.prune(now)
            for key in [k for k, ts in self.causes.items() if ts < now - timedelta(seconds=dedup.KEEP_SECONDS)]:
                del self.causes[key]

    def restore(self, envelope: dict[str, Any]) -> None:
        """시작할 때 Kafka gpu.fault에서 읽은, 전에 낸 이벤트 1건을 기억에 되돌린다 (7절)."""
        try:
            e = envelope["payload"]
            ts = datetime.fromisoformat(e["ts"].replace("Z", "+00:00"))
            with self._lock:
                self.dedup.record(e["dedup_key"], e["suggested_action"])
                if e["source"] != "pod":
                    self._remember_cause(e["job_id"], e["generation"], e["suggested_action"], ts)
        except (KeyError, TypeError, ValueError) as err:    # 우리가 낸 형식이 아닌 메시지. 건너뛴다
            log("restore.skipped", reason=repr(err))

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
        if s.signal != "pod_exit":
            return self._emit(s, ctx, now)
        if now < s.ts + POD_EXIT_WAIT:
            if not any(p.s == s for p in self.pending):     # Alertmanager 재전송
                self.pending.append(Pending(s, ctx, s.ts + POD_EXIT_WAIT))
            return DEFERRED
        return self._pod_exit(s, ctx, now)

    def _pod_exit(self, s: Signal, ctx: Context, now: datetime) -> str:
        """대기가 끝난 Pod 종료 신호 (4절 표)."""
        if ctx.job_id is None or ctx.generation is None:
            return self._emit(s, ctx, now, hold=True)       # 어느 작업인지 몰라 확인할 수 없다
        if self.lifecycle.intended(ctx.job_id, ctx.generation):
            return INTENDED
        if (self.lifecycle.latest(ctx.job_id) or 0) > ctx.generation:
            return STALE
        if (ctx.job_id, ctx.generation) in self.causes:
            return ABSORBED
        return self._emit(s, ctx, now, hold=not self.lifecycle.healthy)

    def _emit(self, s: Signal, ctx: Context, now: datetime, hold: bool = False) -> str:
        v = self.rules.judge(s, ctx.collector_healthy)
        if hold:
            v = self.rules.hold(v)
        if not v.matched:
            self.unmatched.inc()
            log("signal.unmatched", alert=s.alert, xid=s.xid, node=s.node, log_line=s.log_line)

        d = self.dedup.decide(dedup_target(s, ctx), dedup_code(s), s.ts, v.action)
        if d.kind == dedup.DUPLICATE:
            return DUPLICATE

        absorbed = self._absorbable(s, ctx, v)
        correlated = [{"alert": p.s.alert, "source": p.s.source, "ts": iso_z(p.s.ts)} for p in absorbed]
        event = build(s, v, ctx, d.key, correlated)
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
        if s.signal != "pod_exit":
            self._remember_cause(ctx.job_id, ctx.generation, v.action, s.ts)
        for p in absorbed:
            self.pending.remove(p)
            self.alerts.labels(ABSORBED).inc()
        self.published.labels(env["type"], v.category, v.action).inc()
        log("event.published", event_id=event["event_id"], type=env["type"], dedup_key=d.key,
            category=v.category, action=v.action, node=s.node, job_id=ctx.job_id, correlated=len(correlated))
        return PUBLISHED

    def _absorbable(self, s: Signal, ctx: Context, v: Verdict) -> list[Pending]:
        """이 사건이 원인이면, 기다리고 있던 같은 작업 · 세대의 Pod 종료를 함께 묶는다."""
        if s.signal == "pod_exit" or v.action in NO_RECOVERY or ctx.job_id is None:
            return []
        return [p for p in self.pending if (p.ctx.job_id, p.ctx.generation) == (ctx.job_id, ctx.generation)]

    def _remember_cause(self, job_id: str | None, generation: int | None, action: str, ts: datetime) -> None:
        if job_id is not None and generation is not None and action not in NO_RECOVERY:
            self.causes[(job_id, generation)] = ts
