from datetime import datetime, timedelta, timezone

from classifier.dedup import DUPLICATE, NEW, RECLASSIFIED, Dedup

GPU = "GPU-b850f46d-d5ea-c752-ddf3-c4453e44d3f7"
DCGM_TS = datetime(2026, 11, 18, 14, 2, 40, tzinfo=timezone.utc)    # 예시 08: DCGM 경로가 먼저 도착
DMESG_TS = datetime(2026, 11, 18, 14, 2, 11, tzinfo=timezone.utc)   # 예시 01: 커널 로그가 뒤늦게 도착


def test_two_paths_share_key_and_reclassify():
    d = Dedup()
    first = d.decide(GPU, "79", DCGM_TS, "hold")
    assert (first.kind, first.key) == (NEW, f"{GPU}|79|{int(DCGM_TS.timestamp())}")
    d.record(first.key, "hold")

    second = d.decide(GPU, "79", DMESG_TS, "isolate_migrate")       # 29초 차이 < 45초
    assert (second.kind, second.key) == (RECLASSIFIED, first.key)
    d.record(second.key, "isolate_migrate")

    assert d.decide(GPU, "79", DMESG_TS, "isolate_migrate").kind == DUPLICATE   # Alertmanager 재전송


def test_outside_window_is_new():
    d = Dedup()
    d.record(d.decide(GPU, "79", DCGM_TS, "isolate_migrate").key, "isolate_migrate")
    assert d.decide(GPU, "79", DCGM_TS + timedelta(seconds=45), "isolate_migrate").kind == DUPLICATE
    assert d.decide(GPU, "79", DCGM_TS + timedelta(seconds=46), "isolate_migrate").kind == NEW


def test_other_target_or_code_is_new():
    d = Dedup()
    d.record(d.decide(GPU, "79", DCGM_TS, "isolate_migrate").key, "isolate_migrate")
    assert d.decide(GPU, "48", DCGM_TS, "reset_then_migrate").kind == NEW
    assert d.decide("GPU-other", "79", DCGM_TS, "isolate_migrate").kind == NEW


def test_undecided_is_not_remembered():
    d = Dedup()
    d.decide(GPU, "79", DCGM_TS, "hold")                 # 발행 실패 → record 안 함
    assert d.decide(GPU, "79", DCGM_TS, "hold").kind == NEW


def test_prune():
    d = Dedup()
    d.record(f"{GPU}|79|{int(DCGM_TS.timestamp())}", "hold")
    d.prune(DCGM_TS + timedelta(minutes=59))
    assert d.decide(GPU, "79", DCGM_TS, "hold").kind == DUPLICATE
    d.prune(DCGM_TS + timedelta(minutes=61))
    assert d.decide(GPU, "79", DCGM_TS, "hold").kind == NEW
