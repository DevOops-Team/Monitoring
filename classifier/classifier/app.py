"""HTTP 입구. 실행: uvicorn classifier.app:create_app --factory --host 0.0.0.0 --port 8080

환경 변수
  KAFKA_BOOTSTRAP  브로커 주소. 없으면 표준 출력으로만 낸다(로컬 확인용)
  REQUIRE_KAFKA    true면 KAFKA_BOOTSTRAP 없이 시작하지 않는다. 배포 매니페스트는 항상 true
  MIMIR_URL        예 http://mimir:9009. 없으면 수집 경로를 확인할 수 없어 전부 hold로 낸다
  LOKI_URL         예 http://loki:3100. trainer 로그에서 작업 실패 사건의 죽은 노드와 GPU 메모리 부족 줄을 찾는다.
                   없으면 노드 없이, 메모리 부족은 종료 사유(OOMKilled)로만 판정한다
  RULE_VERSION     rules.yaml의 git SHA 앞 7자 (이미지 빌드 때 넣음). Kafka 헤더로 나간다
  LEASE_NAME       대장 선출에 쓸 Lease 이름. 있으면 Lease를 잡은 Pod만 알람을 처리한다(7절). 없으면 혼자 도는 것으로 본다
  POD_IP           LEASE_NAME과 같이 쓴다. 대기 쪽이 받은 알람을 넘겨줄 대장 주소로 Lease에 적힌다
"""
from __future__ import annotations

import ipaddress
import json
import logging
import os
import threading
import urllib.error
import urllib.request
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from fastapi import Body, FastAPI, HTTPException, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, Gauge, generate_latest

from . import dedup, intended
from . import pipeline as p
from .enrich import Enricher, loki_first_line, mimir_query
from .event import Context
from .kafka_reader import TopicReader, kafka_consumer
from .lease import Elector, LeaseApi
from .publish import Publisher, StdoutProducer, kafka_producer
from .rules import Rules
from .signals import Signal


def _unknown_context(s: Signal) -> Context:
    """MIMIR_URL이 없을 때. 수집 경로를 확인할 수 없으니 전부 hold로 나간다."""
    return Context(az="unknown", collector_healthy=False)


DEFAULT_SIGNALS = "xid,pod_exit"     # site · spot · node_lost · job_failed는 명시해야 받는다 (docs/classifier.md 2절)


def enabled_signals() -> frozenset[str]:
    return frozenset(x.strip() for x in os.environ.get("ENABLED_SIGNALS", DEFAULT_SIGNALS).split(",") if x.strip())


def pipeline_from_env() -> tuple[p.Pipeline, Callable[[], None]]:
    """파이프라인과 「기억 복원」 함수. 복원은 알람을 처리하기 직전에 부른다(혼자 돌면 시작할 때, 아니면 대장이 됐을 때)."""
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP")
    if not bootstrap and os.environ.get("REQUIRE_KAFKA", "").lower() == "true":
        raise RuntimeError("REQUIRE_KAFKA=true인데 KAFKA_BOOTSTRAP이 없다")
    producer = kafka_producer(bootstrap) if bootstrap else StdoutProducer()
    mimir = os.environ.get("MIMIR_URL")
    loki = os.environ.get("LOKI_URL")
    enrich = Enricher(mimir_query(mimir), loki_first_line(loki) if loki else None) if mimir else _unknown_context
    signals = enabled_signals()
    p.log("startup", kafka=bootstrap or "stdout", mimir=mimir or "none", loki=loki or "none", signals=sorted(signals))
    lifecycle = intended.Lifecycle()
    pipeline = p.Pipeline(Rules.load(), Publisher(producer, os.environ.get("RULE_VERSION", "dev")), enrich, lifecycle,
                          signals)
    if not bootstrap:
        return pipeline, lambda: None
    return pipeline, lambda: restore_from_kafka(bootstrap, pipeline, lifecycle)


def restore_from_kafka(bootstrap: str, pipeline: p.Pipeline, lifecycle: intended.Lifecycle) -> None:
    """기억을 Kafka에서 되살린 뒤에 돌아온다. 실패하면 예외로 시작을 멈춘다(기억 없이 받으면 같은 사건을 또 낸다)."""
    faults = TopicReader(kafka_consumer(bootstrap), "gpu.fault",
                         timedelta(seconds=dedup.KEEP_SECONDS), pipeline.restore)
    faults.catch_up()
    faults.close()      # 처리하는 쪽은 대장 하나라 그 뒤로 내가 낸 것은 내가 기억한다. 이어 읽지 않는다

    jobs = TopicReader(kafka_consumer(bootstrap), "job.lifecycle", intended.KEEP,
                       lambda e: lifecycle.apply(e, datetime.now(timezone.utc)),
                       on_health=lambda healthy: setattr(lifecycle, "healthy", healthy))
    jobs.catch_up()
    jobs.follow()


def _die(reason: str) -> None:
    """프로세스를 바로 끝낸다. 쿠버네티스가 다시 띄우면 대기 쪽으로 시작한다."""
    p.log("exit", reason=reason)
    os._exit(1)


PORT = 8080
FORWARDED = "x-classifier-forwarded"    # 대기 쪽이 넘겨준 요청. 받은 쪽이 대장이 아니어도 다시 넘기지 않는다
FORWARD_TIMEOUT = 15.0                  # 대장이 Kafka 전달 확인을 기다리는 시간(10초)보다 길게
MAX_RESPONSE = 64 * 1024                # 대장의 응답은 처리 결과별 개수라 작다. 그보다 큰 것은 읽지 않는다


def leader_url(address: str) -> str | None:
    """Lease에 적힌 값이 「IP:우리 포트」일 때만 주소로 쓴다. 누가 Lease에 다른 값을 적어도 그리로 보내지 않는다."""
    host, _, port = address.rpartition(":")
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        return None
    if port != str(PORT) or ip.is_loopback or ip.is_link_local or ip.is_unspecified or ip.is_multicast:
        return None
    return f"http://[{ip}]:{PORT}/alerts" if ip.version == 6 else f"http://{ip}:{PORT}/alerts"


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """리다이렉트를 따라가지 않는다. 검증한 주소가 아닌 곳으로 요청이 넘어가지 않게."""

    def redirect_request(self, *args, **kwargs):
        return None


_opener = urllib.request.build_opener(_NoRedirect)


def forward(address: str, body: dict[str, Any]) -> Response:
    """받은 웹훅을 대장에게 그대로 보내고 대장의 응답을 돌려준다. 대장에 못 닿으면 503 → Alertmanager가 다시 보낸다."""
    url = leader_url(address)
    if url is None:
        p.log("forward.bad_address", address=address)
        raise HTTPException(status_code=503, detail="대장 주소를 알 수 없다")
    req = urllib.request.Request(url, data=json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json", FORWARDED: "1"})
    try:
        with _opener.open(req, timeout=FORWARD_TIMEOUT) as r:
            return Response(r.read(MAX_RESPONSE), status_code=r.status, media_type="application/json")
    except urllib.error.HTTPError as e:
        return Response(e.read(MAX_RESPONSE), status_code=e.code, media_type="application/json")
    except OSError as e:        # 주소 · 원인은 로그에만 남긴다
        p.log("forward.failed", address=address, reason=repr(e))
        raise HTTPException(status_code=503, detail="대장에 닿지 못함") from e


def create_app(pipeline: p.Pipeline | None = None, lease_api=None) -> FastAPI:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    restore: Callable[[], None] = lambda: None
    if pipeline is None:
        pipeline, restore = pipeline_from_env()

    active = threading.Event()      # 알람을 처리하는 중인가: 대장이고 기억 복원이 끝났다
    Gauge("classifier_leader", "이 Pod가 알람을 처리하는가 (1 = 대장이고 기억 복원이 끝남)",
          registry=pipeline.registry).set_function(lambda: float(active.is_set()))

    def lead() -> None:
        try:
            restore()
        except Exception:           # 기억 없이 받으면 같은 사건을 또 낸다. 내려놓고 다른 쪽이 잡게 한다
            p.logger.exception("restore failed")
            _die("restore failed")
        active.set()

    elector = None
    lease = os.environ.get("LEASE_NAME")
    if lease_api is None and lease:
        lease_api = LeaseApi(lease, os.environ["KUBERNETES_SERVICE_HOST"], os.environ["KUBERNETES_SERVICE_PORT"])
    if lease_api is not None:
        # 갱신하지 못한 대장은 처리를 멈춰야 한다. 기억도 버려야 하므로(그 사이 새 대장이 낸 것을 모른다) 프로세스를 끝낸다
        elector = Elector(lease_api, os.environ.get("HOSTNAME", "local"), f"{os.environ.get('POD_IP', '127.0.0.1')}:{PORT}",
                          lead, lambda: _die("lease lost"))
    else:
        restore()                   # 혼자 돈다. 복원이 끝나야 포트를 연다. 실패하면 예외로 시작을 멈춘다
        active.set()

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        """기다리는 Pod 종료 신호를 1초마다 확인하는 타이머 + 대장 선출."""
        stop = threading.Event()

        def tick() -> None:
            while not stop.wait(1.0):
                try:
                    pipeline.flush(datetime.now(timezone.utc))
                except Exception:
                    p.logger.exception("flush failed")

        threading.Thread(target=tick, name="flush", daemon=True).start()
        lease_thread = threading.Thread(target=elector.run, args=(stop,), name="lease", daemon=True) if elector else None
        if lease_thread:
            lease_thread.start()
        yield
        stop.set()
        if lease_thread:
            lease_thread.join(10.0)     # 대장이면 Lease를 비우고 끝난다

    app = FastAPI(title="classifier", docs_url=None, redoc_url=None, openapi_url=None, lifespan=lifespan)

    # def(동기)라서 스레드 풀에서 돈다. Kafka 전달 확인을 기다려도 다른 요청을 막지 않는다
    @app.post("/alerts")
    def alerts(request: Request, body: dict[str, Any] = Body(...)):
        """Alertmanager 웹훅. 전달 실패가 하나라도 있으면 503 → Alertmanager가 묶음을 다시 보낸다(이미 낸 것은 중복 제거로 걸러짐)."""
        if not active.is_set():     # 대기 쪽이거나 대장이 된 직후 복원 중. Service는 두 Pod에 번갈아 보낸다
            leader = elector.leader_address if elector else None
            if leader is None or FORWARDED in request.headers:
                raise HTTPException(status_code=503, detail="처리할 대장이 없다")
            return forward(leader, body)
        items = body.get("alerts")
        if not isinstance(items, list):
            raise HTTPException(status_code=400, detail="alerts 배열이 없다")
        now = datetime.now(timezone.utc)
        counts: dict[str, int] = {}
        for alert in items:
            outcome = pipeline.handle(alert, now) if isinstance(alert, dict) else p.REJECTED
            counts[outcome] = counts.get(outcome, 0) + 1
        if counts.get(p.PUBLISH_FAILED):
            raise HTTPException(status_code=503, detail=counts)
        return counts

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/metrics")
    def metrics() -> Response:
        return Response(generate_latest(pipeline.registry), media_type=CONTENT_TYPE_LATEST)

    return app
