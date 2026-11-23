"""대장 선출 (docs/classifier.md 7절). 쿠버네티스 Lease를 잡은 Pod 하나만 알람을 처리한다.

규칙은 client-go의 leader election과 같다.
- 대장은 RETRY_SECONDS마다 Lease의 renewTime을 고쳐 쓴다. RENEW_DEADLINE 동안 한 번도 못 쓰면 대장을 내려놓는다
- 대기 쪽은 Lease 내용이 LEASE_SECONDS 동안 그대로면 주인이 죽은 것으로 보고 가져온다
- RENEW_DEADLINE < LEASE_SECONDS라서 옛 대장이 먼저 멈추고 그 뒤에 새 대장이 생긴다
- 만료는 Lease에 적힌 시각이 아니라 「내가 그 내용을 처음 본 뒤 흐른 시간」으로 잰다(노드 시계가 달라도 됨)

Lease는 매니페스트로 만들지 않는다. Argo CD가 Lease를 관리 대상에서 빼므로 여기서 직접 만든다(Role에 create 필요).
"""
from __future__ import annotations

import json
import ssl
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Any, Callable

from .pipeline import log

LEASE_SECONDS = 15
RENEW_DEADLINE = 10.0
RETRY_SECONDS = 2.0
HTTP_TIMEOUT = 3.0      # 조회 + 쓰기 두 번을 해도 RENEW_DEADLINE 안에 한 번은 다시 해 볼 수 있게

SERVICE_ACCOUNT = "/var/run/secrets/kubernetes.io/serviceaccount"


class Conflict(Exception):
    """다른 쪽이 먼저 썼다. 이번 시도는 진 것이다."""


class LeaseApi:
    """Pod 안에서 ServiceAccount 토큰으로 쿠버네티스 API의 Lease 하나를 읽고 쓴다."""

    def __init__(self, name: str, host: str, port: str):
        with open(f"{SERVICE_ACCOUNT}/namespace") as f:
            self.namespace = f.read().strip()
        self.name = name
        self._base = (f"https://{host}:{port}/apis/coordination.k8s.io/v1"
                      f"/namespaces/{self.namespace}/leases")
        self._ssl = ssl.create_default_context(cafile=f"{SERVICE_ACCOUNT}/ca.crt")

    def get(self) -> dict[str, Any] | None:
        try:
            return self._call("GET", f"{self._base}/{self.name}")
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            raise

    def create(self, lease: dict[str, Any]) -> None:
        self._write("POST", self._base, lease)

    def replace(self, lease: dict[str, Any]) -> None:
        self._write("PUT", f"{self._base}/{self.name}", lease)

    def _write(self, method: str, url: str, lease: dict[str, Any]) -> None:
        try:
            self._call(method, url, lease)
        except urllib.error.HTTPError as e:
            if e.code == 409:
                raise Conflict() from e
            raise

    def _call(self, method: str, url: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
        with open(f"{SERVICE_ACCOUNT}/token") as f:     # 토큰은 주기적으로 바뀐다. 매번 다시 읽는다
            token = f.read().strip()
        req = urllib.request.Request(url, method=method, data=json.dumps(body).encode() if body else None,
                                     headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT, context=self._ssl) as r:
            return json.load(r)


class Elector:
    def __init__(self, api, identity: str, on_lead: Callable[[], None], on_lost: Callable[[], None],
                 monotonic: Callable[[], float] = time.monotonic):
        self.api = api
        self.identity = identity
        self.on_lead = on_lead      # 대장이 됐을 때 한 번. 오래 걸려도 갱신이 멈추지 않게 따로 띄운 스레드에서 부른다
        self.on_lost = on_lost      # 대장인데 RENEW_DEADLINE 동안 갱신하지 못했을 때
        self.monotonic = monotonic
        self.leading = False
        self._renewed_at = 0.0
        self._seen: tuple[Any, Any] | None = None       # 남이 잡은 Lease에서 마지막으로 본 (주인, renewTime)
        self._seen_at = 0.0

    def run(self, stop: threading.Event) -> None:
        self.tick()
        while not stop.wait(RETRY_SECONDS):
            self.tick()

    def tick(self) -> None:
        """한 번 시도하고 그 결과로 대장 여부를 바꾼다."""
        try:
            held = self._try()
        except Conflict:
            held = False
        except Exception as e:      # API 서버에 못 닿음 · 권한 없음. 대장이면 기한까지 다시 해 본다
            log("lease.error", reason=repr(e))
            held = False
        now = self.monotonic()
        if held:
            self._renewed_at = now
            if not self.leading:
                self.leading = True
                log("lease.acquired", identity=self.identity)
                threading.Thread(target=self.on_lead, name="on-lead", daemon=True).start()
        elif self.leading and now - self._renewed_at > RENEW_DEADLINE:
            self.leading = False
            log("lease.lost", identity=self.identity)
            self.on_lost()

    def _try(self) -> bool:
        """Lease를 잡거나 갱신했으면 True."""
        lease = self.api.get()
        if lease is None:
            self.api.create(self._body(None))
            return True
        spec = lease.get("spec") or {}
        holder = spec.get("holderIdentity")
        if holder != self.identity:
            seen = (holder, spec.get("renewTime"))
            if seen != self._seen:
                self._seen, self._seen_at = seen, self.monotonic()
            if holder and self.monotonic() - self._seen_at < spec.get("leaseDurationSeconds", LEASE_SECONDS):
                return False
        self.api.replace(self._body(lease))
        return True

    def _body(self, current: dict[str, Any] | None) -> dict[str, Any]:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        old = (current or {}).get("spec") or {}
        mine = old.get("holderIdentity") == self.identity
        meta = {"name": self.api.name}
        if current:     # 읽은 뒤 남이 고쳤으면 409로 지게 한다
            meta["resourceVersion"] = current["metadata"]["resourceVersion"]
        return {
            "apiVersion": "coordination.k8s.io/v1", "kind": "Lease", "metadata": meta,
            "spec": {
                "holderIdentity": self.identity,
                "leaseDurationSeconds": LEASE_SECONDS,
                "renewTime": now,
                "acquireTime": old.get("acquireTime", now) if mine else now,
                "leaseTransitions": old.get("leaseTransitions", 0) + (0 if mine or not current else 1),
            },
        }
