"""Client library.  [Owner: Person C]

Exposes R and W as client settings, carries the causal context between GET and
PUT, and offers a sibling resolver for explicit reconciliation.

Typical read-modify-write:

    async with KVClient("http://127.0.0.1:8001", client_id="c1", r=2, w=2) as kv:
        res = await kv.get("cart")              # res.versions, res.context
        merged = set().union(*(v["value"] for v in res.versions)) | {"milk"}
        await kv.put("cart", sorted(merged), context=res.context)

Every call should append a row to an OpLog (see experiments/oplog.py) so all
experiments are driven from the same CSV format.

MUST follow the client rules in docs/API.md:
  1. one client_id per sequential actor (one per concurrent asyncio task)
  2. client_id unique per process run (add a uuid suffix)
  3. keep self._own_clock[key]: merge in every clock seen (GET context and the
     clock of the version in the PUT reply), and on PUT send
     merge(caller_context, self._own_clock[key]). Without this, a client's
     blind writes after its first one are silently dropped.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable


@dataclass
class GetResult:
    versions: list[dict]           # [{"value", "clock", "timestamp_ms", "coordinator"}, ...]
    context: dict = field(default_factory=dict)

    @property
    def has_conflict(self) -> bool:
        return len(self.versions) > 1


class QuorumError(Exception):
    """Raised when the coordinator answers 503 quorum_not_met."""


class KVClient:
    def __init__(self, node_url: str, client_id: str, r: int = 2, w: int = 2,
                 timeout_s: float = 2.0):
        self.node_url = node_url.rstrip("/")
        self.client_id = client_id
        self.r = r
        self.w = w
        self.timeout_s = timeout_s
        self._own_clock: dict[str, dict] = {}   # key -> clock dict (client rule 3)

    async def __aenter__(self) -> "KVClient":
        # TODO (Person C): open aiohttp.ClientSession
        return self

    async def __aexit__(self, *exc) -> None:
        # TODO (Person C): close session
        pass

    async def get(self, key: str, r: int | None = None) -> GetResult:
        """GET /kv/{key}?r=..  TODO (Person C)."""
        raise NotImplementedError

    async def put(self, key: str, value: Any, context: dict | None = None,
                  w: int | None = None) -> dict:
        """PUT /kv/{key}?w=..  body {"value", "context", "client_id"}. TODO (Person C)."""
        raise NotImplementedError

    async def reconcile(self, key: str, merge_fn: Callable[[list[Any]], Any]) -> GetResult:
        """Read, and if there are siblings, write back merge_fn(values) with the
        merged context so the conflict is resolved. TODO (Person C)."""
        raise NotImplementedError
