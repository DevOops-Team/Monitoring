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


@pytest.mark.parametrize("xid", [45, 174])
def test_loop_xids_ignored(xid):
    # 컨트롤러 조치(Pod 종료)가 만드는 Xid라 사건을 내면 루프가 된다
    assert rules.judge(signal(xid=xid), True) is None


def test_exit_code_0_ignored():
    assert rules.judge(signal(signal="pod_exit", source="pod", exit_code=0, pod="p"), True) is None


def test_exit_code_75_is_failure():
    # ADR-43: 75(옛 STOP)는 더 이상 뜻이 없다. 펜싱 종료는 job.lifecycle로 거르고, 75는 일반 Pod 종료로 본다
    v = rules.judge(signal(signal="pod_exit", source="pod", reason="Error", exit_code=75, pod="p"), True)
    assert (v.category, v.action) == ("app", "restart_in_place")


def test_completed_ignored():
    assert rules.judge(signal(signal="pod_exit", source="pod", reason="Completed", pod="p"), True) is None


@pytest.mark.parametrize("kw", [{"reason": "Error"}, {"reason": "OOMKilled"}, {"reason": "Error", "exit_code": 137}])
def test_pod_exit(kw):
    v = rules.judge(signal(signal="pod_exit", source="pod", pod="p", **kw), True)
    assert (v.category, v.action) == ("app", "restart_in_place")


def test_job_failed_is_judged_like_pod_exit():
    v = rules.judge(signal(signal="job_failed", source="pod", node=None, job_id="3f9a1c2e", generation=3), True)
    assert (v.category, v.action, v.confidence, v.matched) == ("app", "restart_in_place", 0.6, True)


@pytest.mark.parametrize("xid, expected", [
    (64, ("hw", "reset_then_migrate", 0.85)),       # NVIDIA 카탈로그 즉시 조치 RESET_GPU
    (94, ("hw", "restart_in_place", 0.8)),          # RESTART_APP
    (95, ("hw", "reset_then_migrate", 0.85)),       # RESET_GPU
    (119, ("driver", "reset_then_migrate", 0.5)),   # RESET_GPU, 원인 후보가 넓어 confidence 낮음
    (32, ("app", "restart_in_place", 0.7)),         # RESTART_APP, 카탈로그 Parsing Policy가 31과 함께 User-App
    (8, ("driver", "restart_in_place", 0.6)),       # RESTART_APP
    (183, ("driver", "restart_in_place", 0.6)),     # RESTART_APP
    (140, ("hw", "reset_then_migrate", 0.85)),      # RESET_GPU
    (175, ("hw", "reset_then_migrate", 0.85)),      # RESET_GPU
    (62, ("hw", "reset_then_migrate", 0.7)),        # RESET_GPU
    (158, ("hw", "reset_then_migrate", 0.7)),       # RESET_GPU
    (120, ("driver", "reset_then_migrate", 0.5)),   # RESET_GPU, 119의 짝
    (184, ("driver", "reset_then_migrate", 0.5)),   # RESET_GPU
    (38, ("driver", "none", 0.9)),                  # IGNORE → 기록만
    (154, ("driver", "none", 0.9)),                 # 복구 힌트. 조치는 같이 난 Xid가 정한다
    (171, ("hw", "none", 0.9)),                     # 48 보조 정보
])
def test_xid_catalog_rows(xid, expected):
    v = rules.judge(signal(xid=xid), True)
    assert (v.category, v.action, v.confidence) == expected and v.matched


def test_unknown_xid_holds():
    v = rules.judge(signal(xid=999), True)
    assert (v.category, v.action, v.matched) == ("hw", "hold", False)


def test_collector_down_keeps_category():
    v = rules.judge(signal(xid=13), False)
    assert (v.category, v.action, v.confidence) == ("app", "hold", 0.5)
