"""Kafka job.lifecycle에서 본 것: 의도된 종료 · 작업별 최신 세대 · 남은 스텝 (docs/classifier.md 4절).

펜싱 · 사용자 중단으로 지워진 Pod도 종료 코드를 남긴다. 숫자로는 장애와 구분할 수 없어 이 표로 거른다.
job.failed는 의도된 종료가 아니다. 장애로 JobSet이 먼저 Failed가 되면 그 세대의 끝 사건은 job.failed 하나뿐이다.
남은 스텝은 rank 0 사이드카의 job.progress(step · total_steps, 약 30초마다)에서 온다. 스트래글러 이득 계산에 쓴다(3.3절).
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
        self._progress: dict[str, tuple[int, int, int | None]] = {}    # 작업 → 가장 새 (세대, step, total_steps)

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
            if type_ == "job.progress":
                self._apply_progress(job_id, generation, envelope.get("payload"))

    def _apply_progress(self, job_id: str, generation: int, payload: Any) -> None:
        """더 새로운 세대 · step일 때만 덮어쓴다. total_steps가 빠진 사건은 같은 세대의 앞 값을 유지한다."""
        if not isinstance(payload, dict) or type(payload.get("step")) is not int:
            return
        step, total = payload["step"], payload.get("total_steps")
        old = self._progress.get(job_id)
        if old is not None and (generation, step) < old[:2]:
            return
        if type(total) is not int:
            total = old[2] if old is not None and old[0] == generation else None
        self._progress[job_id] = (generation, step, total)

    def remaining_steps(self, job_id: str, generation: int) -> int | None:
        """그 세대의 total_steps − step. 그 세대의 진행률을 아직 못 봤으면 None."""
        with self._lock:
            p = self._progress.get(job_id)
        if p is None or p[0] != generation or p[2] is None:
            return None
        return max(0, p[2] - p[1])

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
                self._progress.pop(job_id, None)
                self._intended = {k for k in self._intended if k[0] != job_id}
