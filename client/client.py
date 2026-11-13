"""Client library.

Exposes R and W as client settings, carries the causal context between GET and
PUT, and offers a sibling resolver for explicit reconciliation.

Typical read-modify-write:

    async with KVClient("http://127.0.0.1:8001", r=2, w=2) as kv:
        res = await kv.get("cart")              # res.versions, res.context
        merged = set().union(*(v["value"] for v in res.versions)) | {"milk"}
        await kv.put("cart", sorted(merged), context=res.context)

`node_url` may also be a list of URLs: each request then goes to a randomly
chosen node (used by the stale-read experiment).

Every call appends a row to an optional OpLog (see experiments/oplog.py) so all
experiments are driven from the same CSV format.

Follows the client rules in docs/API.md:
  1. one client_id per sequential actor: writes from one KVClient are
     serialised with a lock, so a client_id never has two writes in flight
  2. client_id unique per process run: if none is given, make_client_id()
     generates one with a uuid suffix
  3. self._own_clock[key] merges every clock seen (GET context and the clock of
     the version in the PUT reply); on PUT we send
     merge(caller_context, self._own_clock[key]).
  4. read-your-writes: self._last_written[key] is the version of this client's
     last successful PUT. If a GET misses it (its context does not descend that
     clock - possible when R + W <= N), the version is added to the result, so a
     read-modify-write never drops the client's own previous write (rule 3
     makes the next write supersede it).
"""

from __future__ import annotations

import asyncio
import json
import random
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable
from urllib.parse import quote

import aiohttp

from kvstore.policies import lww_winner
from kvstore.storage import Version, reconcile
from kvstore.vclock import VectorClock


def make_client_id(prefix: str = "c") -> str:
    """A client id that is unique per process run (client rule 2)."""
    return f"{prefix}-{uuid.uuid4().hex[:6]}"


@dataclass
class GetResult:
    versions: list[dict]           # [{"value", "clock", "timestamp_ms", "coordinator"}, ...]
    context: dict = field(default_factory=dict)
    coordinator: str = ""
    own_write_added: bool = False  # rule 4: the read missed this client's last write

    @property
    def has_conflict(self) -> bool:
        return len(self.versions) > 1

    @property
    def values(self) -> list[Any]:
        return [v["value"] for v in self.versions]


class KVError(Exception):
    """Base class for client errors."""

    code = "error"


class QuorumError(KVError):
    """Raised when the coordinator answers 503 quorum_not_met."""

    code = "quorum_not_met"

    def __init__(self, required: int | None = None, received: int | None = None):
        super().__init__(f"quorum not met: required {required}, received {received}")
        self.required = required
        self.received = received


class BadRequest(KVError):
    code = "bad_request"


class NodeUnavailable(KVError):
    """The contacted node did not answer (connection error or timeout)."""

    code = "node_unavailable"


def _now_ms() -> float:
    return time.time() * 1000.0


class KVClient:
    def __init__(self, node_url: str | list[str], client_id: str | None = None,
                 r: int = 2, w: int = 2, timeout_s: float = 2.0,
                 oplog=None, experiment: str = "", read_your_writes: bool = True):
        urls = [node_url] if isinstance(node_url, str) else list(node_url)
        if not urls:
            raise ValueError("need at least one node url")
        self.node_urls = [u.rstrip("/") for u in urls]
        self.client_id = client_id or make_client_id()
        self.r = r
        self.w = w
        self.timeout_s = timeout_s
        self.oplog = oplog
        self.experiment = experiment
        self.read_your_writes = read_your_writes  # False only to measure rule 4's effect
        self._own_clock: dict[str, dict] = {}   # key -> clock dict (client rule 3)
        self._last_written: dict[str, dict] = {}  # key -> Version dict (client rule 4)
        self._write_lock = asyncio.Lock()        # client rule 1
        self._session: aiohttp.ClientSession | None = None
        self._owns_session = False

    @property
    def node_url(self) -> str:
        return self.node_urls[0]

    async def __aenter__(self) -> "KVClient":
        await self.open()
        return self

    async def __aexit__(self, *exc) -> None:
        await self.close()

    async def open(self, session: aiohttp.ClientSession | None = None) -> None:
        """Open a session, or share an existing one (it is then not closed by us)."""
        if session is not None:
            self._session, self._owns_session = session, False
        elif self._session is None or self._session.closed:
            self._session, self._owns_session = aiohttp.ClientSession(), True

    async def close(self) -> None:
        if self._session is not None and self._owns_session:
            await self._session.close()
        self._session = None

    # ---- clock tracking (rule 3) ------------------------------------------------
    def own_clock(self, key: str) -> dict:
        return dict(self._own_clock.get(key, {}))

    def _observe(self, key: str, clock: dict | None) -> None:
        merged = VectorClock.from_dict(self._own_clock.get(key)).merge(VectorClock.from_dict(clock))
        self._own_clock[key] = merged.to_dict()

    # ---- read-your-writes (rule 4) -------------------------------------------------
    def _read_your_writes(self, key: str, result: GetResult, policy: str) -> GetResult:
        mine = self._last_written.get(key)
        if mine is None or not self.read_your_writes:
            return result
        mine_v = Version.from_dict(mine)
        if VectorClock.from_dict(result.context).descends(mine_v.clock):
            return result
        seen = [Version.from_dict(v) for v in result.versions]
        if policy == "lww":
            versions = [lww_winner(seen + [mine_v])]
        else:
            versions = reconcile(seen + [mine_v])
        context = VectorClock.from_dict(result.context).merge(mine_v.clock).to_dict()
        return GetResult(versions=[v.to_dict() for v in versions], context=context,
                         coordinator=result.coordinator, own_write_added=True)

    # ---- HTTP -------------------------------------------------------------------
    def _pick_url(self) -> str:
        return self.node_urls[0] if len(self.node_urls) == 1 else random.choice(self.node_urls)

    async def _request(self, method: str, url: str, params: dict,
                       body: Any = None) -> dict:
        if self._session is None:
            await self.open()
        try:
            async with self._session.request(
                method, url, params=params, json=body,
                timeout=aiohttp.ClientTimeout(total=self.timeout_s),
            ) as resp:
                try:
                    data = await resp.json(content_type=None)
                except (ValueError, aiohttp.ContentTypeError):
                    data = {}
                if resp.status == 200:
                    return data
                if resp.status == 503 and data.get("error") == "quorum_not_met":
                    raise QuorumError(data.get("required"), data.get("received"))
                if resp.status == 400:
                    raise BadRequest(data.get("detail", "bad request"))
                raise KVError(f"HTTP {resp.status}: {data}")
        except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as exc:
            raise NodeUnavailable(f"{url}: {type(exc).__name__}: {exc}") from exc

    def _log(self, **fields) -> None:
        if self.oplog is None:
            return
        from experiments.oplog import OpRecord   # imported lazily: optional dependency
        self.oplog.record(OpRecord(client_id=self.client_id,
                                   experiment=self.experiment, **fields))

    # ---- public API ---------------------------------------------------------------
    async def get(self, key: str, r: int | None = None) -> GetResult:
        """GET /kv/{key}?r=..  Raises QuorumError / NodeUnavailable / BadRequest."""
        r = r or self.r
        base = self._pick_url()
        start = _now_ms()
        try:
            data = await self._request("GET", f"{base}/kv/{quote(key, safe='')}", {"r": r})
        except KVError as exc:
            self._log(op="get", key=key, node=base, r=r, start_ms=start,
                      end_ms=_now_ms(), ok=0, error=exc.code)
            raise
        end = _now_ms()
        result = GetResult(versions=data.get("versions", []),
                           context=data.get("context", {}) or {},
                           coordinator=data.get("coordinator", ""))
        result = self._read_your_writes(key, result, data.get("policy", "siblings"))
        self._observe(key, result.context)
        self._log(op="get", key=key, node=result.coordinator or base, r=r,
                  start_ms=start, end_ms=end, ok=1,
                  value=json.dumps(result.values, sort_keys=True),
                  num_versions=len(result.versions))
        return result

    async def put(self, key: str, value: Any, context: dict | None = None,
                  w: int | None = None) -> dict:
        """PUT /kv/{key}?w=..  body {"value", "context", "client_id"}.

        Returns the reply {"key", "version", "acks"}. Raises QuorumError /
        NodeUnavailable / BadRequest.
        """
        w = w or self.w
        async with self._write_lock:
            sent_ctx = (VectorClock.from_dict(context)
                        .merge(VectorClock.from_dict(self._own_clock.get(key)))).to_dict()
            base = self._pick_url()
            body = {"value": value, "context": sent_ctx, "client_id": self.client_id}
            start = _now_ms()
            try:
                data = await self._request("PUT", f"{base}/kv/{quote(key, safe='')}",
                                           {"w": w}, body)
            except KVError as exc:
                self._log(op="put", key=key, node=base, w=w, start_ms=start,
                          end_ms=_now_ms(), ok=0, error=exc.code,
                          value=json.dumps(value, sort_keys=True))
                raise
            end = _now_ms()
            version = data.get("version", {})
            self._observe(key, version.get("clock"))
            if version:
                self._last_written[key] = version
            self._log(op="put", key=key, node=version.get("coordinator") or base, w=w,
                      start_ms=start, end_ms=end, ok=1,
                      value=json.dumps(value, sort_keys=True))
            return data

    async def reconcile(self, key: str, merge_fn: Callable[[list[Any]], Any],
                        r: int | None = None, w: int | None = None) -> GetResult:
        """Read, and if there are siblings, write back merge_fn(values) with the
        merged context so the conflict is resolved. Returns the resulting state
        (the single reconciled version, or the unchanged read)."""
        res = await self.get(key, r=r)
        if not res.has_conflict:
            return res
        reply = await self.put(key, merge_fn(res.values), context=res.context, w=w)
        version = reply["version"]
        return GetResult(versions=[version], context=version["clock"],
                         coordinator=version.get("coordinator", ""))
