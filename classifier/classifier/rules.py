"""표 판정 (docs/classifier.md 3절). 신호 1건 → category · suggested_action · confidence."""
from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import yaml

from .signals import Signal

RULES_PATH = Path(__file__).with_name("rules.yaml")
# 「학습이 끝났다」는 결과 신호. 원인 사건(Xid · 노드 소실 · 사이트)에 묶이고(pipeline.py), 메모리 부족 판정은 이 둘에만 있다
EXIT_SIGNALS = ("pod_exit", "job_failed")


@dataclass(frozen=True)
class Verdict:
    category: str
    action: str
    confidence: float
    matched: bool = True        # False = 표에 없어 unmatched()가 정함


def _verdict(row: dict[str, Any], matched: bool = True) -> Verdict:
    return Verdict(row["category"], row["action"], float(row["confidence"]), matched)


class Rules:
    def __init__(self, table: dict[str, Any]):
        self.table = table

    @classmethod
    def load(cls, path: Path = RULES_PATH) -> "Rules":
        return cls(yaml.safe_load(path.read_text(encoding="utf-8")))

    def ignored(self, s: Signal) -> bool:
        ignore = self.table["ignore"]
        return s.xid in ignore["xid"] or s.exit_code in ignore["exit_code"] or s.reason in ignore["reason"]

    def judge(self, s: Signal, collector_healthy: bool, oom: bool = False) -> Verdict | None:
        """None = 이벤트를 만들지 않는다. oom = 신호 밖(trainer 로그)에서 메모리 부족을 확인했다."""
        if self.ignored(s):
            return None
        v = self._match(s, oom) or self.unmatched(s)
        if not collector_healthy and v.category != "site":
            v = self.hold(v)
        return v

    def hold(self, v: Verdict) -> Verdict:
        """확인할 수 없는 상태의 판정. category는 두고 hold로 낮춘다."""
        down = self.table["collector_down"]
        return replace(v, action=down["action"], confidence=float(down["confidence"]))

    def _match(self, s: Signal, oom: bool = False) -> Verdict | None:
        if s.signal == "xid":
            for row in self.table["xid"]:
                if s.xid in row["xids"]:
                    return _verdict(row)
            return None
        if s.signal in EXIT_SIGNALS and (oom or s.reason == "OOMKilled"):
            return _verdict(self.table["oom"])
        return _verdict(self.table["signal"][s.signal])

    def unmatched(self, s: Signal) -> Verdict:
        """표에 없는 신호가 모이는 자리. 모델을 붙일 때 이 함수만 바꾼다 (docs/classifier.md 5절)."""
        return _verdict(self.table["unmatched"], matched=False)
