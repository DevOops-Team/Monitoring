"""같은 사건을 한 번만 낸다 (docs/fault-event.md 2.1절).

같은 대상+코드 신호가 이미 본 사건의 기준 시각 ±45초 안이면 같은 사건이고 그 키를 다시 쓴다.
비교는 도착 시각이 아니라 신호 발생 시각(ts)으로 한다.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

WINDOW_SECONDS = 45     # dcgm 수집 5 + 스크레이프 15 + 룰 평가 15 + 여유 10. 주기를 바꾸면 다시 계산
KEEP_SECONDS = 3600     # 시작할 때 Kafka에서 복원하는 범위와 같다 (docs/classifier.md 7절)

NEW = "new"                     # 처음 보는 사건
RECLASSIFIED = "reclassified"   # 같은 사건, action이 바뀜 → 새 event_id로 낸다
DUPLICATE = "duplicate"         # 같은 사건, 같은 action → 내지 않는다


@dataclass(frozen=True)
class Decision:
    key: str
    kind: str


class Dedup:
    def __init__(self) -> None:
        # (대상, 코드) → {기준 시각(epoch 초): 마지막으로 낸 action}
        self._seen: dict[tuple[str, str], dict[int, str]] = {}

    def decide(self, target: str, code: str, ts: datetime, action: str) -> Decision:
        """판단만 한다. 발행에 성공한 뒤 record()로 남긴다."""
        epoch = int(ts.timestamp())
        for ref, last in self._seen.get((target, code), {}).items():
            if abs(epoch - ref) <= WINDOW_SECONDS:
                return Decision(f"{target}|{code}|{ref}", DUPLICATE if last == action else RECLASSIFIED)
        return Decision(f"{target}|{code}|{epoch}", NEW)

    def record(self, key: str, action: str) -> None:
        """낸 이벤트를 기억한다. 재시작 뒤 Kafka에서 읽은 이벤트도 이것으로 넣는다."""
        target, code, ref = key.rsplit("|", 2)
        self._seen.setdefault((target, code), {})[int(ref)] = action

    def prune(self, now: datetime) -> None:
        limit = int(now.timestamp()) - KEEP_SECONDS
        for pair in list(self._seen):
            refs = self._seen[pair]
            for ref in [r for r in refs if r < limit]:
                del refs[ref]
            if not refs:
                del self._seen[pair]
