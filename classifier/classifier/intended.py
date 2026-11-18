"""Kafka job.lifecycle에서 본 것: 의도된 종료와 작업별 최신 세대 (docs/classifier.md 4절).

펜싱 · 사용자 중단으로 지워진 Pod도 종료 코드를 남긴다. 숫자로는 장애와 구분할 수 없어 이 표로 거른다.
job.failed는 의도된 종료가 아니다. 장애로 JobSet이 먼저 Failed가 되면 그 세대의 끝 사건은 job.failed 하나뿐이다.
"""
from __future__ import annotations

import threading
from datetime import datetime, timedelta
from typing import Any

INTENDED_TYPES = ("job.fenced", "job.cancelled")
KEEP = timedelta(hours=24)      # 시작할 때 Kafka에서 복원하는 범위와 같다 (docs/classifier.md 7절)


class Lifecycle:
    def __init__(self) -> None:
        self.healthy = False    # job.lifecycle을 읽고 있는가. 읽는 쪽(kafka reader)이 바꾼다
        self._lock = threading.Lock()
        self._intended: set[tuple[str, int]] = set()
        self._latest: dict[str, int] = {}
        self._seen_at: dict[str, datetime] = {}

    def apply(self, envelope: dict[str, Any], now: datetime) -> None:
        """봉투 1건 반영. job_id · generation이 없는 사건은 볼 것이 없다."""
        job_id, generation, type_ = envelope.get("job_id"), envelope.get("generation"), envelope.get("type")
        if not isinstance(job_id, str) or type(generation) is not int:
            return
        with self._lock:
            self._latest[job_id] = max(generation, self._latest.get(job_id, generation))
            self._seen_at[job_id] = now
            if type_ in INTENDED_TYPES:
                self._intended.add((job_id, generation))

    def intended(self, job_id: str, generation: int) -> bool:
        with self._lock:
            return (job_id, generation) in self._intended

    def latest(self, job_id: str) -> int | None:
        with self._lock:
            return self._latest.get(job_id)

    def prune(self, now: datetime) -> None:
        with self._lock:
            for job_id in [j for j, seen in self._seen_at.items() if seen < now - KEEP]:
                del self._seen_at[job_id], self._latest[job_id]
                self._intended = {k for k in self._intended if k[0] != job_id}
