"""Network shim: ALL node-to-node traffic must go through this module.

It lets experiments inject faults without Docker/iptables:
  - partitions: messages to/from blocked peers are dropped (raise PeerUnreachable)
  - delay: every message sleeps uniform(delay_min_ms, delay_max_ms) before sending

Controlled at runtime via the /admin/* endpoints (see docs/API.md).

Every outgoing internal request carries the header  X-From-Node: <node_id>
so the receiving node can also drop messages from peers it has blocked
(server.py checks `shim.is_blocked(sender)` on every /internal/* request).

Requests to the node itself get the same random delay as remote ones and are
then served in-process by `local_handler` (no HTTP round trip), so that W=1 /
R=1 results are not dominated by "the coordinator's own copy always wins".
"""

from __future__ import annotations

import asyncio
import random
from typing import Any, Awaitable, Callable

import aiohttp

FROM_HEADER = "X-From-Node"

# (method, path, json_body) -> parsed JSON reply. Raise PeerUnreachable on failure.
LocalHandler = Callable[[str, str, Any], Awaitable[Any]]


class PeerUnreachable(Exception):
    """Raised when a peer is partitioned away or the request fails/times out."""


class NetworkShim:
    def __init__(self, self_id: str, peer_urls: dict[str, str],
                 delay_min_ms: int = 0, delay_max_ms: int = 0,
                 local_handler: LocalHandler | None = None):
        self.self_id = self_id
        self.peer_urls = peer_urls          # node_id -> base url
        self.blocked: set[str] = set()
        self.delay_min_ms = 0
        self.delay_max_ms = 0
        self.set_delay(delay_min_ms, delay_max_ms)
        self.local_handler = local_handler
        self._session: aiohttp.ClientSession | None = None

    # ---- admin controls --------------------------------------------------
    def set_partition(self, blocked: list[str]) -> None:
        self.blocked = set(blocked) - {self.self_id}

    def heal(self) -> None:
        self.blocked.clear()

    def set_delay(self, min_ms: int, max_ms: int) -> None:
        if not 0 <= min_ms <= max_ms:
            raise ValueError("need 0 <= min_ms <= max_ms")
        self.delay_min_ms, self.delay_max_ms = min_ms, max_ms

    def is_blocked(self, peer_id: str) -> bool:
        return peer_id in self.blocked

    # ---- messaging --------------------------------------------------------
    def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=0))
        return self._session

    async def _delay(self) -> None:
        if self.delay_max_ms > 0:
            await asyncio.sleep(random.uniform(self.delay_min_ms, self.delay_max_ms) / 1000.0)

    async def send(self, peer_id: str, method: str, path: str,
                   json_body: Any = None, timeout_s: float = 1.0) -> Any:
        """Send one internal request to a peer and return its parsed JSON reply.

        Raises PeerUnreachable if the peer is blocked, or on any connection
        error, timeout or non-2xx reply.
        """
        if self.is_blocked(peer_id):
            raise PeerUnreachable(f"{peer_id} is partitioned from {self.self_id}")
        await self._delay()
        # A partition may have been installed while we were "on the wire".
        if self.is_blocked(peer_id):
            raise PeerUnreachable(f"{peer_id} is partitioned from {self.self_id}")

        if peer_id == self.self_id and self.local_handler is not None:
            return await self.local_handler(method, path, json_body)

        base = self.peer_urls.get(peer_id)
        if base is None:
            raise PeerUnreachable(f"unknown peer {peer_id!r}")
        try:
            async with self._get_session().request(
                method, base + path, json=json_body,
                headers={FROM_HEADER: self.self_id},
                timeout=aiohttp.ClientTimeout(total=timeout_s),
            ) as resp:
                if resp.status // 100 != 2:
                    raise PeerUnreachable(f"{peer_id} replied HTTP {resp.status}")
                return await resp.json()
        except PeerUnreachable:
            raise
        except (aiohttp.ClientError, asyncio.TimeoutError, OSError, ValueError) as exc:
            raise PeerUnreachable(f"{peer_id}: {type(exc).__name__}: {exc}") from exc

    async def close(self) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None
