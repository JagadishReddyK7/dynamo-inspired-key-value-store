"""Quorum coordination for client GET/PUT requests.

Any node can coordinate any request. Algorithm:

PUT(key, value, context, client_id, W):
  1. new_clock = VectorClock.from_dict(context).increment(client_id)
  2. version = Version(value, new_clock, timestamp_ms=now_ms(), coordinator=self_id)
  3. send PUT /internal/replica/{key} {"versions": [version]} to ALL N replicas
     in ring.preference_list(key, N) *in parallel*
  4. return success as soon as W acks arrive; fail (QuorumNotMet) if the
     timeout elapses or too many replicas fail to ever reach W.
  5. IMPORTANT: the remaining in-flight sends are NOT cancelled - replication to
     the other replicas continues in the background. This async gap is what
     makes stale reads possible when R + W <= N.

GET(key, R):
  1. send GET /internal/replica/{key} to all N replicas in parallel
  2. once R replies arrive: versions = reconcile(all returned versions)
  3. context = context_of(versions)    # merge of ALL sibling clocks, before policy
  4. answer = policies.resolve(versions, conflict_policy)
  5. read repair (background): wait (bounded by the timeout) for the late
     replies too, then push the reconciled versions to every replica whose
     reply was missing some of them.
  6. fail with QuorumNotMet if fewer than R replies before timeout.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any, Awaitable
from urllib.parse import quote

from . import policies
from .network_shim import NetworkShim, PeerUnreachable
from .ring import ConsistentHashRing
from .storage import LocalStore, Version, context_of, reconcile
from .vclock import VectorClock


class QuorumNotMet(Exception):
    def __init__(self, required: int, received: int):
        super().__init__(f"quorum not met: required {required}, received {received}")
        self.required = required
        self.received = received


def now_ms() -> int:
    return int(time.time() * 1000)


def replica_path(key: str) -> str:
    return "/internal/replica/" + quote(key, safe="")


class Coordinator:
    def __init__(self, self_id: str, ring: ConsistentHashRing, shim: NetworkShim,
                 store: LocalStore, n: int, timeout_ms: int, conflict_policy: str):
        if conflict_policy not in policies.POLICIES:
            raise ValueError(f"unknown conflict policy {conflict_policy!r}")
        self.self_id = self_id
        self.ring = ring
        self.shim = shim
        self.store = store
        self.n = n
        self.timeout_ms = timeout_ms
        self.conflict_policy = conflict_policy
        self._background: set[asyncio.Task] = set()

    # ---- helpers ------------------------------------------------------------
    @property
    def timeout_s(self) -> float:
        return self.timeout_ms / 1000.0

    def check_quorum_arg(self, name: str, value: int) -> None:
        if not 1 <= value <= self.n:
            raise ValueError(f"{name} must be between 1 and {self.n}, got {value}")

    def _spawn(self, coro: Awaitable[Any]) -> asyncio.Task:
        """Start a background task, keep a reference and swallow its exception."""
        task = asyncio.ensure_future(coro)
        self._background.add(task)
        task.add_done_callback(self._task_done)
        return task

    def _task_done(self, task: asyncio.Task) -> None:
        self._background.discard(task)
        if not task.cancelled():
            task.exception()  # mark as retrieved: failures are expected here

    def _fan_out(self, replicas: list[str], method: str, path: str,
                 body: Any = None) -> dict[str, asyncio.Task]:
        return {
            node: self._spawn(self.shim.send(node, method, path, body, timeout_s=self.timeout_s))
            for node in replicas
        }

    async def _await_quorum(self, tasks: dict[str, asyncio.Task], needed: int,
                            deadline: float) -> dict[str, Any]:
        """Wait until `needed` tasks succeeded, the deadline passed, or the
        quorum became impossible. Never cancels the remaining tasks."""
        loop = asyncio.get_running_loop()
        node_of = {t: node for node, t in tasks.items()}
        pending = set(tasks.values())
        results: dict[str, Any] = {}
        failed = 0
        while pending and len(results) < needed:
            remaining = deadline - loop.time()
            if remaining <= 0:
                break
            done, pending = await asyncio.wait(pending, timeout=remaining,
                                               return_when=asyncio.FIRST_COMPLETED)
            for t in done:
                if t.cancelled() or t.exception() is not None:
                    failed += 1
                else:
                    results[node_of[t]] = t.result()
            if len(tasks) - failed < needed:
                break  # cannot reach the quorum any more: fail fast
        return results

    # ---- client operations ----------------------------------------------------
    async def put(self, key: str, value, context: dict, client_id: str,
                  w: int) -> tuple[Version, int]:
        """Returns (the Version that was written, number of acks). Raises QuorumNotMet."""
        self.check_quorum_arg("w", w)
        clock = VectorClock.from_dict(context).increment(client_id)
        version = Version(value, clock, timestamp_ms=now_ms(), coordinator=self.self_id)
        replicas = self.ring.preference_list(key, self.n)
        deadline = asyncio.get_running_loop().time() + self.timeout_s
        tasks = self._fan_out(replicas, "PUT", replica_path(key),
                              {"versions": [version.to_dict()]})
        acks = await self._await_quorum(tasks, w, deadline)
        if len(acks) < w:
            raise QuorumNotMet(w, len(acks))
        return version, len(acks)

    async def get(self, key: str, r: int) -> tuple[list[Version], dict]:
        """Returns (versions_after_policy, context_dict). Raises QuorumNotMet."""
        self.check_quorum_arg("r", r)
        replicas = self.ring.preference_list(key, self.n)
        deadline = asyncio.get_running_loop().time() + self.timeout_s
        tasks = self._fan_out(replicas, "GET", replica_path(key))
        raw = await self._await_quorum(tasks, r, deadline)
        if len(raw) < r:
            raise QuorumNotMet(r, len(raw))

        replies = {node: _parse_versions(body) for node, body in raw.items()}
        versions = reconcile(v for vs in replies.values() for v in vs)
        context = context_of(versions).to_dict()
        answer = policies.resolve(versions, self.conflict_policy)
        self._spawn(self._repair_after_all_replies(key, tasks, deadline))
        return answer, context

    # ---- read repair ------------------------------------------------------------
    async def _repair_after_all_replies(self, key: str, tasks: dict[str, asyncio.Task],
                                        deadline: float) -> None:
        """Collect every reply (including late ones), then repair stale replicas."""
        remaining = max(0.0, deadline - asyncio.get_running_loop().time())
        await asyncio.wait(list(tasks.values()), timeout=remaining)
        replies: dict[str, list[Version]] = {}
        for node, t in tasks.items():
            if t.done() and not t.cancelled() and t.exception() is None:
                replies[node] = _parse_versions(t.result())
        if not replies:
            return
        versions = reconcile(v for vs in replies.values() for v in vs)
        await self.read_repair(key, versions, replies)

    async def read_repair(self, key: str, versions: list[Version],
                          replies: dict[str, list[Version]]) -> None:
        """Push `versions` to every replica whose reply is missing any of them.

        A version is "missing" at a replica when none of that replica's versions
        descends from it. Only the missing versions are sent; the replica's
        put_many drops whatever it already supersedes.
        """
        sends = []
        for node, held in replies.items():
            missing = [v for v in versions
                       if not any(h.clock.descends(v.clock) for h in held)]
            if missing:
                body = {"versions": [v.to_dict() for v in missing]}
                sends.append(self.shim.send(node, "PUT", replica_path(key), body,
                                            timeout_s=self.timeout_s))
        if sends:
            results = await asyncio.gather(*sends, return_exceptions=True)
            for res in results:
                if isinstance(res, BaseException) and not isinstance(res, PeerUnreachable):
                    raise res

    async def close(self) -> None:
        tasks = list(self._background)
        for t in tasks:
            t.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)


def _parse_versions(body: Any) -> list[Version]:
    return [Version.from_dict(v) for v in (body or {}).get("versions", [])]
