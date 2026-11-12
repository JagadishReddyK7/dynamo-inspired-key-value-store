"""Quorum coordination for client GET/PUT requests.  [Owner: Person B, read repair: Person A]

Any node can coordinate any request. Algorithm:

PUT(key, value, context, client_id, W):
  1. new_clock = VectorClock.from_dict(context).increment(client_id)
  2. version = Version(value, new_clock, timestamp_ms=now_ms(), coordinator=self_id)
  3. send PUT /internal/replica/{key} {"versions": [version]} to ALL N replicas
     in ring.preference_list(key, N) *in parallel*
  4. return success as soon as W acks arrive; fail (QuorumNotMet) if the
     timeout elapses or too many replicas fail to ever reach W.
  5. IMPORTANT: do NOT cancel the remaining in-flight sends - replication to the
     other replicas continues in the background. This async gap is what makes
     stale reads possible when R + W <= N.

GET(key, R):
  1. send GET /internal/replica/{key} to all N replicas in parallel
  2. once R replies arrive: versions = reconcile(all returned versions)
  3. context = context_of(versions)    # merge of ALL sibling clocks, before policy
  4. answer = policies.resolve(versions, conflict_policy)
  5. read repair (background): send `versions` to every replica whose reply
     was missing some of them (and, if you like, to late repliers too).
  6. fail with QuorumNotMet if fewer than R replies before timeout.
"""

from __future__ import annotations

import time

from .network_shim import NetworkShim
from .ring import ConsistentHashRing
from .storage import LocalStore, Version


class QuorumNotMet(Exception):
    def __init__(self, required: int, received: int):
        super().__init__(f"quorum not met: required {required}, received {received}")
        self.required = required
        self.received = received


def now_ms() -> int:
    return int(time.time() * 1000)


class Coordinator:
    def __init__(self, self_id: str, ring: ConsistentHashRing, shim: NetworkShim,
                 store: LocalStore, n: int, timeout_ms: int, conflict_policy: str):
        self.self_id = self_id
        self.ring = ring
        self.shim = shim
        self.store = store
        self.n = n
        self.timeout_ms = timeout_ms
        self.conflict_policy = conflict_policy

    async def put(self, key: str, value, context: dict, client_id: str, w: int) -> Version:
        """Returns the Version that was written. Raises QuorumNotMet. TODO (Person B)."""
        raise NotImplementedError

    async def get(self, key: str, r: int) -> tuple[list[Version], dict]:
        """Returns (versions_after_policy, context_dict). Raises QuorumNotMet. TODO (Person B)."""
        raise NotImplementedError

    async def read_repair(self, key: str, versions: list[Version],
                          replies: dict[str, list[Version]]) -> None:
        """Push `versions` to replicas that are missing any of them. TODO (Person A)."""
        raise NotImplementedError
