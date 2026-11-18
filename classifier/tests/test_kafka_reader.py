"""따라잡기의 시작 위치와 끝 판단. 브로커는 가짜로 흉내 낸다(실제 Kafka는 kind 검증에서)."""
import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from classifier.kafka_reader import TopicReader

NOW = datetime(2026, 11, 18, 14, 0, 0, tzinfo=timezone.utc)


class Msg:
    def __init__(self, partition, offset, value):
        self._p, self._o, self._v = partition, offset, value

    def partition(self): return self._p
    def offset(self): return self._o
    def value(self): return self._v
    def error(self): return None


class FakeConsumer:
    """log[partition] = [(시각, 값), ...]. offset은 목록의 자리."""
    def __init__(self, log):
        self.log, self.queue, self.asked_ms = log, [], None

    def list_topics(self, topic, timeout):
        return SimpleNamespace(topics={topic: SimpleNamespace(error=None, partitions={p: None for p in self.log})})

    def offsets_for_times(self, tps, timeout):
        self.asked_ms = tps[0].offset
        for tp in tps:
            after = [i for i, (ts, _) in enumerate(self.log[tp.partition]) if ts.timestamp() * 1000 >= tp.offset]
            tp.offset = after[0] if after else -1
        return tps

    def get_watermark_offsets(self, tp, timeout, cached):
        return 0, len(self.log[tp.partition])

    def assign(self, tps):
        for tp in tps:
            self.queue += [Msg(tp.partition, i, v) for i, (_, v) in enumerate(self.log[tp.partition]) if i >= tp.offset]

    def poll(self, timeout):
        return self.queue.pop(0) if self.queue else None


def event(n):
    return json.dumps({"n": n}).encode()


def test_reads_only_since_and_all_partitions():
    log = {0: [(NOW - timedelta(hours=3), event(1)), (NOW - timedelta(minutes=30), event(2))],
           1: [(NOW - timedelta(minutes=10), event(3))],
           2: []}
    seen, health = [], []
    reader = TopicReader(FakeConsumer(log), "gpu.fault", timedelta(hours=1), seen.append, on_health=health.append)
    assert reader.catch_up(NOW) == 2
    assert sorted(e["n"] for e in seen) == [2, 3]
    assert reader.consumer.asked_ms == int((NOW - timedelta(hours=1)).timestamp() * 1000)
    assert (reader.healthy, health) == (True, [True])


def test_nothing_recent_returns_at_once():
    reader = TopicReader(FakeConsumer({0: [(NOW - timedelta(hours=5), event(1))]}), "gpu.fault",
                         timedelta(hours=1), lambda e: pytest.fail("옛 메시지를 읽음"))
    assert reader.catch_up(NOW) == 0


def test_broken_messages_are_skipped():
    log = {0: [(NOW, b"not json"), (NOW, b"[1, 2]"), (NOW, None), (NOW, event(7))]}
    seen = []
    assert TopicReader(FakeConsumer(log), "job.lifecycle", timedelta(hours=1), seen.append).catch_up(NOW) == 1
    assert seen == [{"n": 7}]


def test_times_out_when_end_is_never_reached():
    consumer = FakeConsumer({0: [(NOW, event(1))]})
    consumer.assign = lambda tps: None      # 메시지가 오지 않는 브로커
    with pytest.raises(TimeoutError):
        TopicReader(consumer, "gpu.fault", timedelta(hours=1), lambda e: None).catch_up(NOW, timeout=0.1)


def test_health_needs_every_partition_and_progress():
    log = {0: [(NOW, event(1))], 1: []}
    consumer = FakeConsumer(log)
    reader = TopicReader(consumer, "job.lifecycle", timedelta(hours=1), lambda e: None)
    reader.catch_up(NOW)
    reader._check()
    assert reader.healthy                       # 다 읽었다

    log[1].append((NOW, event(2)))              # 새 메시지가 왔는데 읽지 않는다
    reader._check()
    assert reader.healthy                       # 첫 확인: 아직 읽는 중일 수 있다
    reader._check()
    assert not reader.healthy                   # 두 번째 확인에도 그대로면 멈춘 것
    reader._position[1] = 1                     # 읽기가 나아가면 돌아온다
    reader._check()
    assert reader.healthy

    def broken(tp, timeout, cached):
        if tp.partition == 1:
            raise RuntimeError("unknown partition")
        return 0, 1
    consumer.get_watermark_offsets = broken     # 한 파티션만 물을 수 없어도 unhealthy
    reader._check()
    assert not reader.healthy


def test_failed_handle_stays_unhealthy_until_restart():
    reader = TopicReader(FakeConsumer({0: []}), "job.lifecycle", timedelta(hours=1), lambda e: None)
    reader.catch_up(NOW)
    reader._degraded = "ValueError()"           # _run()에서 handle()이 실패했을 때의 상태
    reader._set_healthy(False, reader._degraded)
    reader._check()
    assert not reader.healthy
