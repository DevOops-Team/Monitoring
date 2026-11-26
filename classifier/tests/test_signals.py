from datetime import datetime, timedelta, timezone

import pytest

from classifier.signals import RejectError, parse_alert


NOW = datetime(2026, 11, 18, 14, 3, 0, tzinfo=timezone.utc)


def parse(a):
    return parse_alert(a, now=NOW)


def alert(labels=None, annotations=None, status="firing"):
    base = {"alertname": "GpuXid", "pipeline": "failover", "signal": "xid", "source": "dmesg",
            "cluster": "eks-a", "node": "ip-10-0-1-23", "xid": "79"}
    return {"status": status, "labels": {**base, **(labels or {})},
            "annotations": {"event_ts": "2026-11-18T14:02:11Z", **(annotations or {})}}


def test_xid():
    s = parse(alert(annotations={"log_line": "NVRM: Xid (PCI:0000:00:1e): 79"}))
    assert (s.signal, s.source, s.xid, s.node, s.gpu_uuid) == ("xid", "dmesg", 79, "ip-10-0-1-23", None)
    assert s.ts == datetime(2026, 11, 18, 14, 2, 11, tzinfo=timezone.utc)
    assert s.log_line.startswith("NVRM")


def test_pod_exit():
    s = parse(alert({"signal": "pod_exit", "pod": "job-3f9a1c2e-g0003-w-0-0", "reason": "OOMKilled", "exit_code": "137"}))
    assert (s.source, s.reason, s.exit_code, s.pod, s.xid) == ("pod", "OOMKilled", 137, "job-3f9a1c2e-g0003-w-0-0", None)


def test_pod_exit_without_exit_code():
    """kube-state-metrics 룰은 종료 사유만 안다."""
    s = parse(alert({"signal": "pod_exit", "pod": "job-3f9a1c2e-g0003-w-0-0", "reason": "Error"}))
    assert (s.reason, s.exit_code) == ("Error", None)


def test_site_has_no_node():
    s = parse(alert({"signal": "site", "node": ""}))
    assert (s.source, s.node) == ("site", None)


def test_resolved_is_skipped():
    assert parse(alert(status="resolved")) is None


@pytest.mark.parametrize("labels, annotations", [
    ({"pipeline": ""}, None),
    ({"pipeline": "other"}, None),
    ({"signal": "unknown"}, None),
    ({"signal": ""}, None),
    ({"source": "pod"}, None),              # xid인데 경로가 dmesg · dcgm이 아님
    ({"xid": "abc"}, None),
    ({"node": ""}, None),
    ({"node": "n1|x"}, None),               # dedup_key 구분자
    ({"cluster": "a|b"}, None),
    ({"signal": "pod_exit"}, None),         # pod · reason 없음
    ({"signal": "pod_exit", "pod": "job-a-g0001-w-0-0"}, None),             # reason 없음
    ({"signal": "pod_exit", "pod": "job-a-g0001-w-0-0", "reason": "Error", "exit_code": "abc"}, None),
    ({"signal": "pod_exit", "reason": "Error", "pod": "Job_A|x"}, None),    # 쿠버네티스 이름 형식이 아님
    (None, {"event_ts": ""}),
    (None, {"event_ts": "2026-11-18T14:02:11"}),   # 시간대 없음
    (None, {"event_ts": "2026-11-18T14:09:00Z"}),  # 5분 넘게 미래
])
def test_reject(labels, annotations):
    with pytest.raises(RejectError):
        parse(alert(labels, annotations))


def test_bad_gpu_uuid_is_dropped():
    assert parse(alert({"gpu_uuid": "0000:00:1e.0"})).gpu_uuid is None
    assert parse(alert({"gpu_uuid": "GPU-b850f46d-d5ea"})).gpu_uuid == "GPU-b850f46d-d5ea"


def test_ts_normalized_to_utc_and_old_ts_allowed():
    s = parse(alert(annotations={"event_ts": "2026-11-18T20:02:11+09:00"}))
    assert s.ts == datetime(2026, 11, 18, 11, 2, 11, tzinfo=timezone.utc)
    assert s.ts.utcoffset() == timedelta(0)


def test_node_lost():
    s = parse(alert({"signal": "node_lost", "node": "ip-10-0-1-23"}))
    assert (s.signal, s.source, s.node) == ("node_lost", "node", "ip-10-0-1-23")


def test_node_lost_needs_node():
    with pytest.raises(RejectError):
        parse(alert({"signal": "node_lost", "node": ""}))


def job_failed(**labels):
    return alert({"signal": "job_failed", "node": "", "job_id": "3f9a1c2e", "generation": "3", **labels})


def test_job_failed_carries_job_and_generation_without_node():
    """관리 클러스터 JobSet이 실패로 끝남. 작업 · 세대는 알람이 주고 어느 Pod · 노드인지는 없다."""
    s = parse(job_failed())
    assert (s.signal, s.source, s.node, s.pod, s.job_id, s.generation) == ("job_failed", "pod", None, None, "3f9a1c2e", 3)


@pytest.mark.parametrize("labels", [{"job_id": ""}, {"generation": ""}, {"generation": "x"}, {"job_id": "a|b"}])
def test_job_failed_needs_job_and_generation(labels):
    with pytest.raises(RejectError):
        parse(job_failed(**labels))


def straggler(labels=None, annotations=None):
    return alert({"alertname": "FailoverStraggler", "signal": "straggler", "job_id": "3f9a1c2e", "generation": "3",
                  "rank": "3", **(labels or {})}, {"ratio": "1.60", **(annotations or {})})


def test_straggler_carries_job_node_and_ratio():
    s = parse(straggler())
    assert (s.signal, s.source, s.node, s.job_id, s.generation, s.ratio) == (
        "straggler", "straggler", "ip-10-0-1-23", "3f9a1c2e", 3, 1.6)
    assert s.xid is None and s.pod is None


@pytest.mark.parametrize("labels, annotations", [
    ({"node": ""}, None), ({"job_id": ""}, None), ({"generation": ""}, None), ({"generation": "x"}, None),
    (None, {"ratio": ""}), (None, {"ratio": "fast"}),
])
def test_straggler_needs_node_job_generation_and_ratio(labels, annotations):
    with pytest.raises(RejectError):
        parse(straggler(labels, annotations))
