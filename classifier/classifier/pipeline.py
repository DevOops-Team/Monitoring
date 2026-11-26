"""알람 1건을 끝까지 처리한다: 정규화 → 보강 → 판정 → 중복 제거 → 조립 → 발행 (docs/classifier.md 1절).

Pod 종료 · 작업 실패 신호만 길이 다르다. 의도된 종료인지 확인하려고 신호 시각 + 30초까지 기다렸다가 판정한다 (4절).
작업 실패(job_failed)는 죽은 Pod가 지워져 Pod 종료 알람이 못 본 경우를 잡는다. 같은 작업 · 세대에 둘 다 오면 먼저 낸 쪽만 사건이다.
사이트 신호는 그 클러스터에서 돌던 작업마다 사건 1건을 낸다(컨트롤러가 사건 1건 = 작업 1개로 대피한다).
GPU 노드 소실은 그 노드에서 돌던 작업 1개의 사건을 낸다(작업이 없으면 사건 없음).
스트래글러는 느린 동안 알람이 계속 온다. 작업 · 세대 · 노드마다 사건 1건만 내고, 옮길 만한지(straggler_evidence)를 같이 싣는다.
"""
from __future__ import annotations

import json
import logging
import threading
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from typing import Any, Callable

from prometheus_client import CollectorRegistry, Counter, Gauge

from . import dedup, intended
from .event import Context, build, dedup_code, dedup_target, iso_z
from .intended import Lifecycle
from .publish import SOURCE, InvalidEvent, Publisher, validate
from .rules import EXIT_SIGNALS, Rules, Verdict
from .signals import RejectError, Signal, parse_alert

logger = logging.getLogger("classifier")

POD_EXIT_WAIT = timedelta(seconds=30)       # 임시값. E2E에서 job.fenced와 Pod 종료의 실제 간격을 재서 고친다
MAX_PENDING = 1000                          # 기다리는 신호의 상한. 넘으면 기다리지 않고 hold로 낸다(메모리 보호)
RETRY_AFTER = timedelta(seconds=5)          # 기다리던 신호의 발행이 실패했을 때 다시 해 볼 간격
NO_RECOVERY = ("hold", "none")              # 복구를 부르지 않는 action. 원인 사건으로 치지 않는다

# 처리 결과. classifier_alerts_total의 outcome 라벨 값
RESOLVED = "resolved"               # 꺼짐 알림
REJECTED = "rejected"               # 라벨 계약 위반
IGNORED = "ignored"                 # 이벤트를 만들지 않는 신호 (Xid 45, 종료 코드 0)
DISABLED = "disabled"               # 받지 않기로 한 종류의 신호 (ENABLED_SIGNALS)
DUPLICATE = "duplicate"             # 이미 낸 사건
PUBLISHED = "published"
INVALID = "invalid"                 # 스키마 검증 실패
PUBLISH_FAILED = "publish_failed"   # Kafka 전달 실패. 웹훅이 503을 돌려줘 Alertmanager가 다시 보낸다
DEFERRED = "deferred"               # Pod 종료. 30초 뒤 판정
INTENDED = "intended"               # Pod 종료. 펜싱 · 사용자 중단
STALE = "stale_generation"          # Pod 종료. 옛 세대의 뒤늦은 종료
ABSORBED = "absorbed"               # Pod 종료. 같은 작업 · 세대의 원인 사건에 묶임
NO_JOBS = "no_jobs"                 # 사이트 · 노드 소실. 그 클러스터(노드)에 돌던 작업이 없다(또는 전부 펜싱 · 중단 · 옛 세대)


def log(msg: str, **fields: Any) -> None:
    logger.info(json.dumps({"msg": msg, **fields}, ensure_ascii=False, default=str))


@dataclass
class Pending:
    s: Signal
    ctx: Context
    due: datetime


class Pipeline:
    def __init__(self, rules: Rules, publisher: Publisher, enrich: Callable[[Signal], Context],
                 lifecycle: Lifecycle | None = None, signals: frozenset[str] | None = None):
        self.rules = rules
        self.signals = signals              # 받을 신호 종류. None이면 전부. 라우팅(Alertmanager)이 틀려도 여기서 막는다
        self.publisher = publisher
        self.enrich = enrich
        self.lifecycle = lifecycle or Lifecycle()   # 읽는 쪽이 없으면 healthy=False라 Pod 종료는 전부 hold
        self.dedup = dedup.Dedup()
        self.pending: list[Pending] = []
        # (job_id, generation) → 복구를 부르는 원인 사건의 신호 시각. 그 세대의 Pod 종료는 결과이지 새 사건이 아니다
        self.causes: dict[tuple[str, int], datetime] = {}
        # (job_id, generation) → 그 세대에 이미 낸 종료 사건의 (신호 종류, 시각). 다른 종류의 종료 신호는 같은 일을 두 번 보는 것이다
        self.exits: dict[tuple[str, int], tuple[str, datetime]] = {}
        # (job_id, generation, node) → 이미 낸 스트래글러 사건의 신호 시각. 느린 동안 알람이 1분마다 다시 온다
        self.stragglers: dict[tuple[str, int, str], datetime] = {}
        self._lock = threading.Lock()       # 웹훅은 스레드 풀, 대기 처리는 타이머 스레드에서 돈다

        self.registry = CollectorRegistry()
        self.alerts = Counter("classifier_alerts", "받은 알람 수 (처리 결과별)", ["outcome"], registry=self.registry)
        self.rejected = Counter("classifier_signals_rejected", "라벨 계약 위반으로 버린 알람", registry=self.registry)
        self.unmatched = Counter("classifier_unmatched", "판정 표에 없어 hold로 낸 신호", registry=self.registry)
        self.invalid = Counter("classifier_events_invalid", "스키마 검증 실패로 내지 않은 이벤트", registry=self.registry)
        self.published = Counter("classifier_events_published", "발행한 이벤트",
                                 ["type", "category", "action"], registry=self.registry)
        # 0이면 Pod 종료 신호가 전부 hold로 나간다. 재시작 전까지 0으로 남는 경우도 있어 알람으로 봐야 한다
        Gauge("classifier_lifecycle_healthy", "job.lifecycle을 읽고 있는가 (1 = 읽는 중)",
              registry=self.registry).set_function(lambda: float(self.lifecycle.healthy))
        Gauge("classifier_pending_pod_exits", "판정을 기다리는 Pod 종료 신호",
              registry=self.registry).set_function(lambda: float(len(self.pending)))

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
            for key in [k for k, (_, ts) in self.exits.items() if ts < now - timedelta(seconds=dedup.KEEP_SECONDS)]:
                del self.exits[key]
            for key in [k for k, ts in self.stragglers.items() if ts < now - intended.KEEP]:
                del self.stragglers[key]

    def restore(self, envelope: dict[str, Any]) -> None:
        """시작할 때 Kafka gpu.fault에서 읽은, 전에 낸 이벤트 1건을 기억에 되돌린다 (7절)."""
        try:
            if envelope.get("source") != SOURCE:
                raise ValueError(f"source={envelope.get('source')}")
            e = envelope["payload"]
            validate(e)
            ts = datetime.fromisoformat(e["ts"].replace("Z", "+00:00"))
            with self._lock:
                self.dedup.record(e["dedup_key"], e["suggested_action"])
                if e["source"] == "straggler" and e["job_id"] and e["generation"] is not None and e["node"]:
                    self.stragglers[(e["job_id"], e["generation"], e["node"])] = ts
                if e["source"] != "pod":
                    self._remember_cause(e["job_id"], e["generation"], e["suggested_action"], ts)
                else:       # source=pod 가운데 대상이 「cluster/작업/g세대」인 것이 작업 실패 사건이다(Pod 이름에는 /가 없다)
                    self._remember_exit("job_failed" if "/" in e["dedup_key"].split("|")[0] else "pod_exit",
                                        e["job_id"], e["generation"], e["suggested_action"], ts)
        except (KeyError, TypeError, ValueError) as err:    # 분류기가 낸 v1 이벤트가 아니다. 기억에 넣지 않는다
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
        if self.signals is not None and s.signal not in self.signals:
            log("signal.disabled", signal=s.signal, alert=s.alert, cluster=s.cluster)
            return DISABLED
        if self.rules.ignored(s):
            return IGNORED

        ctx = self.enrich(s)
        if s.signal == "site":
            return self._site(s, ctx, now)
        if s.signal == "node_lost":
            return self._node_lost(s, ctx, now)
        if s.signal == "straggler":
            return self._straggler(s, ctx, now)
        if s.signal not in EXIT_SIGNALS:
            return self._emit(s, ctx, now)
        if now < s.ts + POD_EXIT_WAIT:
            if any(p.s == s for p in self.pending):         # Alertmanager 재전송
                return DEFERRED
            if len(self.pending) >= MAX_PENDING:
                return self._emit(s, ctx, now, hold=True)
            self.pending.append(Pending(s, ctx, s.ts + POD_EXIT_WAIT))
            return DEFERRED
        return self._pod_exit(s, ctx, now)

    def _site(self, s: Signal, ctx: Context, now: datetime) -> str:
        """작업마다 사건 1건. 조회가 실패하면 job_id 없는 1건(컨트롤러가 HOLD → 사람이 본다)."""
        if ctx.jobs is None:
            return self._emit(s, ctx, now)
        jobs = [(job, gen) for job, gen in ctx.jobs
                if gen is None or not (self.lifecycle.intended(job, gen) or (self.lifecycle.latest(job) or 0) > gen)]
        if not jobs:
            log("site.no_jobs", cluster=s.cluster, found=len(ctx.jobs))
            return NO_JOBS
        # job.lifecycle을 못 읽으면 위 거르기가 비어 있다 → 취소된 작업을 되살리지 않게 hold (Pod 종료와 같은 기준)
        hold = not self.lifecycle.healthy
        outcomes = [self._emit(s, replace(ctx, job_id=job, generation=gen, jobs=None), now, hold=hold)
                    for job, gen in jobs]
        # 하나라도 못 냈으면 Alertmanager가 다시 보내게 한다. 이미 낸 작업은 그때 duplicate로 걸러진다
        for outcome in (PUBLISH_FAILED, PUBLISHED):
            if outcome in outcomes:
                return outcome
        return outcomes[0]

    def _node_lost(self, s: Signal, ctx: Context, now: datetime) -> str:
        """그 노드에서 돌던 작업의 사건 1건. 조회가 실패했거나 작업이 여럿이면 job_id 없는 1건(컨트롤러가 HOLD → 사람이 본다).
        같은 작업의 다른 노드도 사라지면 노드마다 1건씩 나가고, 둘째 건부터는 컨트롤러가 이미 복구 중이라 닫는다."""
        if ctx.jobs is None or len(ctx.jobs) > 1:
            return self._emit(s, replace(ctx, jobs=None), now)
        if not ctx.jobs:
            log("node_lost.no_jobs", cluster=s.cluster, node=s.node)
            return NO_JOBS
        (job, gen), = ctx.jobs
        if gen is not None and self.lifecycle.intended(job, gen):
            return INTENDED
        if gen is not None and (self.lifecycle.latest(job) or 0) > gen:
            return STALE
        # job.lifecycle을 못 읽으면 위 거르기가 비어 있다 → 취소된 작업을 되살리지 않게 hold (Pod 종료와 같은 기준)
        return self._emit(s, replace(ctx, job_id=job, generation=gen, jobs=None), now, hold=not self.lifecycle.healthy)

    def _straggler(self, s: Signal, ctx: Context, now: datetime) -> str:
        """작업 · 세대 · 노드마다 1건. 한 번 낸 뒤에는 남은 스텝이 줄어 판정이 달라져도 다시 내지 않는다
        (옮기는 중에 hold로 뒤집히면 안 된다. 이득은 처음 판정할 때가 가장 크다)."""
        if self.lifecycle.intended(s.job_id, s.generation):
            return INTENDED
        if (self.lifecycle.latest(s.job_id) or 0) > s.generation:
            return STALE
        key = (s.job_id, s.generation, s.node)
        if key in self.stragglers:
            return DUPLICATE
        duration = max(0, int((now - s.ts).total_seconds()))
        remaining = self.lifecycle.remaining_steps(s.job_id, s.generation)
        evidence, worth = self.rules.straggler_evidence(s.node, duration, remaining, ctx.step_seconds)
        if evidence is None:
            log("straggler.gain_unknown", job_id=s.job_id, generation=s.generation, node=s.node,
                remaining_steps=remaining, step_seconds=ctx.step_seconds)
        # job.lifecycle을 못 읽으면 위 거르기가 비어 있다 → 취소된 작업을 옮기지 않게 hold (Pod 종료와 같은 기준)
        outcome = self._emit(s, replace(ctx, evidence=evidence, worth=worth), now, hold=not self.lifecycle.healthy)
        if outcome in (PUBLISHED, DUPLICATE):
            self.stragglers[key] = s.ts
        return outcome

    def _pod_exit(self, s: Signal, ctx: Context, now: datetime) -> str:
        """대기가 끝난 Pod 종료 · 작업 실패 신호 (4절 표)."""
        if ctx.job_id is None or ctx.generation is None:
            return self._emit(s, ctx, now, hold=True)       # 어느 작업인지 몰라 확인할 수 없다
        if self.lifecycle.intended(ctx.job_id, ctx.generation):
            return INTENDED
        if (self.lifecycle.latest(ctx.job_id) or 0) > ctx.generation:
            return STALE
        if (ctx.job_id, ctx.generation) in self.causes:
            return ABSORBED
        other = self.exits.get((ctx.job_id, ctx.generation))
        if other is not None and other[0] != s.signal:      # Pod 종료와 작업 실패가 같은 일을 가리킨다. 먼저 낸 쪽만 사건
            return ABSORBED
        return self._emit(s, ctx, now, hold=not self.lifecycle.healthy)

    def _emit(self, s: Signal, ctx: Context, now: datetime, hold: bool = False) -> str:
        v = self.rules.judge(s, ctx.collector_healthy, oom=ctx.oom, worth=ctx.worth)
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
        if s.signal in EXIT_SIGNALS:
            self._remember_exit(s.signal, ctx.job_id, ctx.generation, v.action, s.ts)
        else:
            self._remember_cause(ctx.job_id, ctx.generation, v.action, s.ts)
        for p in absorbed:
            self.pending.remove(p)
            self.alerts.labels(ABSORBED).inc()
        self.published.labels(env["type"], v.category, v.action).inc()
        log("event.published", event_id=event["event_id"], type=env["type"], dedup_key=d.key,
            category=v.category, action=v.action, node=event["node"], job_id=ctx.job_id, correlated=len(correlated))
        return PUBLISHED

    def _absorbable(self, s: Signal, ctx: Context, v: Verdict) -> list[Pending]:
        """이 사건이 원인이면, 기다리고 있던 같은 작업 · 세대의 Pod 종료를 함께 묶는다."""
        if s.signal in EXIT_SIGNALS or v.action in NO_RECOVERY or ctx.job_id is None:
            return []
        return [p for p in self.pending if (p.ctx.job_id, p.ctx.generation) == (ctx.job_id, ctx.generation)]

    def _remember_cause(self, job_id: str | None, generation: int | None, action: str, ts: datetime) -> None:
        if job_id is not None and generation is not None and action not in NO_RECOVERY:
            self.causes[(job_id, generation)] = ts

    def _remember_exit(self, signal: str, job_id: str | None, generation: int | None, action: str, ts: datetime) -> None:
        if job_id is not None and generation is not None and action not in NO_RECOVERY:
            self.exits.setdefault((job_id, generation), (signal, ts))
