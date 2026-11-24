"""schemas/examples의 장면을 신호로 넣으면 같은 판정이 나오는지 본다."""
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from classifier.rules import Rules
from classifier.signals import Signal

EXAMPLES = Path(__file__).resolve().parents[2] / "schemas" / "examples"
SIGNAL_OF = {"dmesg": "xid", "dcgm": "xid", "pod": "pod_exit", "spot": "spot", "site": "site", "node": "node_lost"}
TS = datetime(2026, 11, 18, 14, 2, 11, tzinfo=timezone.utc)

rules = Rules.load()


def signal(**kw) -> Signal:
    base = dict(signal="xid", source="dmesg", alert="GpuXid", cluster="eks-a", ts=TS, node="n1")
    return Signal(**{**base, **kw})


# 07 · 09는 스트래글러(이번 범위 밖)
@pytest.mark.parametrize("name", [
    "01-hw-xid79-isolate", "02-hw-xid48-reset", "03-app-xid13-restart", "04-hwwarn-xid63-none",
    "05-infra-spot-evacuate", "06-site-az-fence", "08-hold-collector-down", "10-infra-node-lost-isolate",
])
def test_examples(name):
    ex = json.loads((EXAMPLES / f"{name}.json").read_text(encoding="utf-8"))
    s = signal(signal=SIGNAL_OF[ex["source"]], source=ex["source"], node=ex["node"],
               xid=ex["raw"]["xid"], exit_code=ex["raw"]["exit_code"])
    v = rules.judge(s, ex["collector_healthy"])
    assert (v.category, v.action, v.confidence) == (ex["category"], ex["suggested_action"], ex["confidence"])


def test_xid45_ignored():
    assert rules.judge(signal(xid=45), True) is None


@pytest.mark.parametrize("code", [0, 75])
def test_exit_code_ignored(code):
    assert rules.judge(signal(signal="pod_exit", source="pod", exit_code=code, pod="p"), True) is None


def test_completed_ignored():
    assert rules.judge(signal(signal="pod_exit", source="pod", reason="Completed", pod="p"), True) is None


@pytest.mark.parametrize("kw", [{"reason": "Error"}, {"reason": "OOMKilled"}, {"reason": "Error", "exit_code": 137}])
def test_pod_exit(kw):
    v = rules.judge(signal(signal="pod_exit", source="pod", pod="p", **kw), True)
    assert (v.category, v.action) == ("app", "restart_in_place")


def test_unknown_xid_holds():
    v = rules.judge(signal(xid=999), True)
    assert (v.category, v.action, v.matched) == ("hw", "hold", False)


def test_collector_down_keeps_category():
    v = rules.judge(signal(xid=13), False)
    assert (v.category, v.action, v.confidence) == ("app", "hold", 0.5)
