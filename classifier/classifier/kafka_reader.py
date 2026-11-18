"""Kafka 토픽을 시각 기준으로 읽는다 (docs/classifier.md 7절).

분류기의 기억(중복 제거 창 · 의도된 종료 표)은 메모리에만 있어서, 시작할 때 최근 구간을 다시 읽어 되살린다.
오프셋을 커밋하지 않는다. 매번 "지금부터 N시간 전"에서 읽으므로 수집기(data-collector)의 위치와 무관하다.
"""
from __future__ import annotations

import json
import os
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from .pipeline import log

HEALTH_CHECK_SECONDS = 5.0


def kafka_consumer(bootstrap: str):
    from confluent_kafka import Consumer
    return Consumer({
        "bootstrap.servers": bootstrap,
        # librdkafka가 group.id를 요구할 뿐이다. assign()으로 읽고 커밋하지 않아 그룹 오프셋은 남지 않는다
        "group.id": f"classifier-{os.environ.get('HOSTNAME', 'local')}",
        "enable.auto.commit": False,
    })


class TopicReader:
    def __init__(self, consumer, topic: str, since: timedelta, handle: Callable[[dict[str, Any]], None],
                 on_health: Callable[[bool], None] | None = None):
        self.consumer = consumer
        self.topic = topic
        self.since = since
        self.handle = handle
        self.on_health = on_health      # 읽을 수 있는 상태가 바뀔 때 부른다
        self.healthy = False
        self._partitions: list[Any] = []
        self._position: dict[int, int] = {}         # 파티션 → 다음에 읽을 오프셋
        self._checked_position: dict[int, int] = {}
        self._was_behind = False
        self._degraded: str | None = None           # 메시지를 반영하지 못한 적이 있다. 재시작 전까지 healthy로 돌아가지 않는다
        self._stop = threading.Event()

    def catch_up(self, now: datetime | None = None, timeout: float = 30.0) -> int:
        """since 전부터 지금 끝까지 읽고 돌아온다. 이것이 끝나야 웹훅을 받기 시작한다."""
        from confluent_kafka import TopicPartition

        now = now or datetime.now(timezone.utc)
        meta = self.consumer.list_topics(self.topic, timeout=10.0).topics[self.topic]
        if meta.error is not None:
            raise RuntimeError(f"{self.topic}: {meta.error}")
        start_ms = int((now - self.since).timestamp() * 1000)
        found = self.consumer.offsets_for_times(
            [TopicPartition(self.topic, p, start_ms) for p in meta.partitions], timeout=10.0)

        ends: dict[int, int] = {}
        for tp in found:
            _, ends[tp.partition] = self.consumer.get_watermark_offsets(tp, timeout=10.0, cached=False)
            if tp.offset < 0:               # 그 시각 뒤로 온 메시지가 없다 → 끝에서 시작
                tp.offset = ends[tp.partition]
        self._partitions = found
        self.consumer.assign(found)

        position = self._position = {tp.partition: tp.offset for tp in found}
        count, deadline = 0, time.monotonic() + timeout
        while any(position[p] < ends[p] for p in ends):
            if time.monotonic() > deadline:
                raise TimeoutError(f"{self.topic}: {timeout}초 안에 따라잡지 못함")
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise RuntimeError(f"{self.topic}: {msg.error()}")
            position[msg.partition()] = msg.offset() + 1
            count += self._deliver(msg)
        log("kafka.caught_up", topic=self.topic, messages=count, since=str(self.since))
        self._set_healthy(True)
        return count

    def close(self) -> None:
        """따라잡기만 하고 이어 읽지 않을 때."""
        self.consumer.close()

    def follow(self) -> None:
        """catch_up 뒤에 이어서 계속 읽는다 (데몬 스레드)."""
        threading.Thread(target=self._run, name=f"read-{self.topic}", daemon=True).start()

    def stop(self) -> None:
        self._stop.set()

    def _run(self) -> None:
        next_check = time.monotonic() + HEALTH_CHECK_SECONDS
        while not self._stop.is_set():
            try:
                msg = self.consumer.poll(1.0)
                if msg is not None:
                    if msg.error():
                        self._set_healthy(False, str(msg.error()))
                    else:
                        self._position[msg.partition()] = msg.offset() + 1
                        self._deliver(msg)
                if time.monotonic() >= next_check:
                    next_check = time.monotonic() + HEALTH_CHECK_SECONDS
                    self._check()
            except Exception as e:      # handle()이 실패했다. 그 메시지는 반영되지 않았고 다시 읽지도 않는다
                self._degraded = repr(e)
                self._set_healthy(False, self._degraded)
        self.consumer.close()

    def _check(self) -> None:
        """브로커에 실제로 물어본다. poll()이 조용한 것만으로는 끊긴 것인지 메시지가 없는 것인지 알 수 없다.

        healthy = 모든 파티션에 물을 수 있고, 밀린 메시지가 두 번 연속 그대로 남아 있지 않다.
        """
        if self._degraded:
            return
        try:
            behind = False
            for tp in self._partitions:
                _, high = self.consumer.get_watermark_offsets(tp, timeout=3.0, cached=False)
                behind = behind or high > self._position[tp.partition]
        except Exception as e:
            self._set_healthy(False, repr(e))
            return
        # 지난 확인 때도 밀려 있었고 그 뒤로 한 건도 못 읽었으면 멈춘 것. 방금 온 메시지 때문에 한 번 밀린 것은 봐준다
        stuck = behind and self._was_behind and self._position == self._checked_position
        self._was_behind, self._checked_position = behind, dict(self._position)
        self._set_healthy(not stuck, "밀린 메시지가 있는데 읽기가 멈춤" if stuck else None)

    def _set_healthy(self, healthy: bool, reason: str | None = None) -> None:
        if healthy == self.healthy:
            return
        self.healthy = healthy
        log("kafka.health_changed", topic=self.topic, healthy=healthy, reason=reason)
        if self.on_health:
            self.on_health(healthy)

    def _deliver(self, msg) -> int:
        try:
            value = json.loads(msg.value())
        except (TypeError, ValueError):
            return 0
        if not isinstance(value, dict):
            return 0
        self.handle(value)
        return 1
