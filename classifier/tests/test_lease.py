"""대장 선출: Lease를 잡는 쪽은 하나, 갱신 못 하면 내려놓는다 (가짜 API · 가짜 시계)."""
import threading

import pytest

from classifier import lease
from classifier.lease import Conflict, Elector


class FakeApi:
    """resourceVersion이 다르면 409를 주는 Lease 하나."""
    name = "classifier"

    def __init__(self):
        self.lease, self.version, self.down = None, 0, False

    def get(self):
        if self.down:
            raise OSError("api down")
        return self.lease and {**self.lease, "spec": dict(self.lease["spec"])}

    def create(self, body):
        if self.lease is not None:
            raise Conflict()
        self._store(body)

    def replace(self, body):
        if self.down:
            raise OSError("api down")
        if body["metadata"].get("resourceVersion") != str(self.version):
            raise Conflict()
        self._store(body)

    def _store(self, body):
        self.version += 1
        self.lease = {"metadata": {**body["metadata"], "resourceVersion": str(self.version)}, "spec": dict(body["spec"])}


class Clock:
    def __init__(self):
        self.t = 100.0

    def __call__(self):
        return self.t


class Node:
    """선출에 참여하는 Pod 하나. on_lead는 다른 스레드에서 불리므로 Event로 기다린다."""
    def __init__(self, api, clock, name):
        self.led, self.lost = threading.Event(), []
        self.elector = Elector(api, name, f"{name}:8080", self.led.set, lambda: self.lost.append(True), monotonic=clock)

    def tick(self):
        self.elector.tick()
        return self.elector.leading


@pytest.fixture
def env():
    api, clock = FakeApi(), Clock()
    return api, clock, Node(api, clock, "a"), Node(api, clock, "b")


def test_first_one_creates_and_leads(env):
    api, _, a, b = env
    assert a.tick() and a.led.wait(1)
    assert not b.tick()
    assert (api.lease["spec"]["holderIdentity"], api.lease["spec"]["leaseTransitions"]) == ("a", 0)


def test_standby_waits_full_lease_after_last_renewal(env):
    """대장이 갱신하는 동안은 못 가져온다. 갱신이 멈추고 LEASE_SECONDS가 지나야 가져온다."""
    api, clock, a, b = env
    a.tick(), b.tick()
    for _ in range(10):                 # 20초 동안 대장이 계속 갱신
        clock.t += 2
        assert a.tick() and not b.tick()
    clock.t += lease.LEASE_SECONDS - 1  # 대장이 죽음. 대기 쪽이 마지막으로 본 내용이 아직 만료 전
    assert not b.tick()
    clock.t += 1
    assert b.tick() and b.led.wait(1)
    assert (api.lease["spec"]["holderIdentity"], api.lease["spec"]["leaseTransitions"]) == ("b", 1)


def test_leader_steps_down_when_it_cannot_renew(env):
    api, clock, a, _ = env
    a.tick()
    api.down = True
    clock.t += lease.RENEW_DEADLINE
    assert a.tick() and not a.lost      # 기한까지는 대장으로 남아 다시 해 본다
    clock.t += 1
    assert not a.tick() and a.lost == [True]


def test_old_leader_loses_after_takeover(env):
    """멈췄다 돌아온 옛 대장: Lease가 남의 것이 됐으니 갱신하지 못하고 내려놓는다."""
    api, clock, a, b = env
    a.tick(), b.tick()
    clock.t += lease.LEASE_SECONDS
    assert b.tick()
    assert not a.tick() and a.lost == [True]
    assert api.lease["spec"]["holderIdentity"] == "b"


def test_conflict_means_not_acquired(env):
    """둘이 같은 내용을 읽고 동시에 쓰면 늦은 쪽은 409로 진다."""
    api, clock, a, b = env
    a.tick(), b.tick()
    clock.t += lease.LEASE_SECONDS
    stale = api.get()
    assert b.tick()
    with pytest.raises(Conflict):
        api.replace(a.elector._body(stale))


def test_restarted_leader_keeps_its_lease(env):
    """같은 이름으로 다시 뜨면(컨테이너 재시작) 기다리지 않고 이어 잡는다."""
    api, clock, a, _ = env
    a.tick()
    again = Node(api, clock, "a")
    assert again.tick() and again.led.wait(1)
    assert api.lease["spec"]["leaseTransitions"] == 0


def test_standby_learns_leader_address(env):
    _, clock, a, b = env
    a.tick(), b.tick()
    assert (a.elector.leader_address, b.elector.leader_address) == (None, "a:8080")
    clock.t += lease.LEASE_SECONDS
    b.tick(), a.tick()                  # 넘겨받으면 주소도 바뀐다
    assert (a.elector.leader_address, b.elector.leader_address) == ("b:8080", None)


def test_leader_releases_on_shutdown(env):
    """정상 종료하는 대장은 Lease를 비운다 → 대기 쪽이 만료를 기다리지 않고 바로 잡는다."""
    api, _, a, b = env
    stop = threading.Event()
    stop.set()
    a.elector.run(stop)                 # 한 번 잡고, 멈추라는 신호에 바로 비우고 끝난다
    assert api.lease["spec"]["holderIdentity"] == ""
    assert b.tick() and b.led.wait(1)


def test_standby_does_not_release(env):
    api, _, a, b = env
    a.tick()
    stop = threading.Event()
    stop.set()
    b.elector.run(stop)
    assert api.lease["spec"]["holderIdentity"] == "a"
