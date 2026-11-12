"""Network shim: ALL node-to-node traffic must go through this module.  [Owner: Person B]

It lets experiments inject faults without Docker/iptables:
  - partitions: messages to/from blocked peers are dropped (raise PeerUnreachable)
  - delay: every message sleeps uniform(delay_min_ms, delay_max_ms) before sending

Controlled at runtime via the /admin/* endpoints (see docs/API.md).

Every outgoing internal request must carry the header  X-From-Node: <node_id>
so the receiving node can also drop messages from peers it has blocked
(server.py checks `shim.is_blocked(sender)` on every /internal/* request).
"""

from __future__ import annotations

import asyncio
import random
from typing import Any

import aiohttp


class PeerUnreachable(Exception):
    """Raised when a peer is partitioned away or the request fails/times out."""


class NetworkShim:
    def __init__(self, self_id: str, peer_urls: dict[str, str],
                 delay_min_ms: int = 0, delay_max_ms: int = 0):
        self.self_id = self_id
        self.peer_urls = peer_urls          # node_id -> base url
        self.blocked: set[str] = set()
        self.delay_min_ms = delay_min_ms
        self.delay_max_ms = delay_max_ms
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
    async def send(self, peer_id: str, method: str, path: str,
                   json_body: Any = None, timeout_s: float = 1.0) -> Any:
        """Send one internal request to a peer and return its parsed JSON reply.

        TODO (Person B):
          1. if peer is blocked -> raise PeerUnreachable
          2. await asyncio.sleep(random delay)
          3. issue the HTTP request with header X-From-Node, honouring timeout_s
          4. on any connection error / timeout / non-2xx -> raise PeerUnreachable
          Requests to self_id MUST get the same random delay (they may skip
          HTTP and call the local store directly after sleeping). See
          docs/API.md: otherwise W=1 / R=1 results are skewed.
        """
        raise NotImplementedError

    async def close(self) -> None:
        if self._session is not None:
            await self._session.close()
