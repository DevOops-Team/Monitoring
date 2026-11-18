from datetime import datetime, timezone

import pytest

from classifier.signals import RejectError, parse_alert


def alert(labels=None, annotations=None, status="firing"):
    base = {"alertname": "GpuXid", "pipeline": "failover", "signal": "xid", "source": "dmesg",
            "cluster": "eks-a", "node": "ip-10-0-1-23", "xid": "79"}
    return {"status": status, "labels": {**base, **(labels or {})},
            "annotations": {"event_ts": "2026-11-18T14:02:11Z", **(annotations or {})}}


def test_xid():
    s = parse_alert(alert(annotations={"log_line": "NVRM: Xid (PCI:0000:00:1e): 79"}))
    assert (s.signal, s.source, s.xid, s.node, s.gpu_uuid) == ("xid", "dmesg", 79, "ip-10-0-1-23", None)
    assert s.ts == datetime(2026, 11, 18, 14, 2, 11, tzinfo=timezone.utc)
    assert s.log_line.startswith("NVRM")


def test_pod_exit():
    s = parse_alert(alert({"signal": "pod_exit", "pod": "job-3f9a1c2e-g0003-w-0-0", "exit_code": "137"}))
    assert (s.source, s.exit_code, s.pod, s.xid) == ("pod", 137, "job-3f9a1c2e-g0003-w-0-0", None)


def test_site_has_no_node():
    s = parse_alert(alert({"signal": "site", "node": ""}))
    assert (s.source, s.node) == ("site", None)


def test_resolved_is_skipped():
    assert parse_alert(alert(status="resolved")) is None


@pytest.mark.parametrize("labels, annotations", [
    ({"signal": "unknown"}, None),
    ({"signal": ""}, None),
    ({"source": "pod"}, None),              # xid인데 경로가 dmesg · dcgm이 아님
    ({"xid": "abc"}, None),
    ({"node": ""}, None),
    ({"signal": "pod_exit"}, None),         # pod · exit_code 없음
    (None, {"event_ts": ""}),
    (None, {"event_ts": "2026-11-18T14:02:11"}),   # 시간대 없음
])
def test_reject(labels, annotations):
    with pytest.raises(RejectError):
        parse_alert(alert(labels, annotations))
