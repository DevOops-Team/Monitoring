"""HTTP 입구. 실행: uvicorn classifier.app:create_app --factory --host 0.0.0.0 --port 8080

환경 변수
  KAFKA_BOOTSTRAP  브로커 주소. 없으면 표준 출력으로만 낸다(로컬 확인용)
  REQUIRE_KAFKA    true면 KAFKA_BOOTSTRAP 없이 시작하지 않는다. 배포 매니페스트는 항상 true
  MIMIR_URL        예 http://mimir:9009. 없으면 수집 경로를 확인할 수 없어 전부 hold로 낸다
  RULE_VERSION     rules.yaml의 git SHA 앞 7자 (이미지 빌드 때 넣음). Kafka 헤더로 나간다
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

from fastapi import Body, FastAPI, HTTPException, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from . import pipeline as p
from .enrich import Enricher, mimir_query
from .event import Context
from .publish import Publisher, StdoutProducer, kafka_producer
from .rules import Rules
from .signals import Signal


def _unknown_context(s: Signal) -> Context:
    """MIMIR_URL이 없을 때. 수집 경로를 확인할 수 없으니 전부 hold로 나간다."""
    return Context(az="unknown", collector_healthy=False)


def pipeline_from_env() -> p.Pipeline:
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP")
    if not bootstrap and os.environ.get("REQUIRE_KAFKA", "").lower() == "true":
        raise RuntimeError("REQUIRE_KAFKA=true인데 KAFKA_BOOTSTRAP이 없다")
    producer = kafka_producer(bootstrap) if bootstrap else StdoutProducer()
    mimir = os.environ.get("MIMIR_URL")
    enrich = Enricher(mimir_query(mimir)) if mimir else _unknown_context
    p.log("startup", kafka=bootstrap or "stdout", mimir=mimir or "none")
    return p.Pipeline(Rules.load(), Publisher(producer, os.environ.get("RULE_VERSION", "dev")), enrich)


def create_app(pipeline: p.Pipeline | None = None) -> FastAPI:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    pipeline = pipeline or pipeline_from_env()
    app = FastAPI(title="classifier", docs_url=None, redoc_url=None, openapi_url=None)

    # def(동기)라서 스레드 풀에서 돈다. Kafka 전달 확인을 기다려도 다른 요청을 막지 않는다
    @app.post("/alerts")
    def alerts(body: dict[str, Any] = Body(...)) -> dict[str, int]:
        """Alertmanager 웹훅. 전달 실패가 하나라도 있으면 503 → Alertmanager가 묶음을 다시 보낸다(이미 낸 것은 중복 제거로 걸러짐)."""
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
