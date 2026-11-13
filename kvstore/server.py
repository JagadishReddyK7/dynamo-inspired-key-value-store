"""HTTP server for one node.

Run one node:   python -m kvstore.server --node-id n1 [--config config/cluster.json]
Run a cluster:  python scripts/run_cluster.py

Endpoints are specified in docs/API.md - keep this file in sync with it.
"""

from __future__ import annotations

import argparse
from typing import Any
from urllib.parse import unquote

from aiohttp import web

from .config import DEFAULT_CONFIG, ClusterConfig, NodeInfo, load_config
from .coordinator import Coordinator, QuorumNotMet
from .network_shim import FROM_HEADER, NetworkShim, PeerUnreachable
from .ring import ConsistentHashRing
from .storage import LocalStore, Version
from .timer_resolution import use_1ms_timer

CFG_KEY = web.AppKey("cfg", ClusterConfig)
NODE_KEY = web.AppKey("node", NodeInfo)
STORE_KEY = web.AppKey("store", LocalStore)
RING_KEY = web.AppKey("ring", ConsistentHashRing)
SHIM_KEY = web.AppKey("shim", NetworkShim)
COORD_KEY = web.AppKey("coordinator", Coordinator)

REPLICA_PREFIX = "/internal/replica/"


def _bad_request(detail: str) -> web.Response:
    return web.json_response({"error": "bad_request", "detail": detail}, status=400)


def _quorum_failed(exc: QuorumNotMet) -> web.Response:
    return web.json_response(
        {"error": "quorum_not_met", "required": exc.required, "received": exc.received},
        status=503)


def _quorum_param(request: web.Request, name: str, default: int, n: int) -> int:
    raw = request.query.get(name)
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
    except ValueError:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from None
    if not 1 <= value <= n:
        raise ValueError(f"{name} must be between 1 and {n}, got {value}")
    return value


def build_app(node_id: str, config_path: str) -> web.Application:
    cfg = load_config(config_path)
    me = cfg.node(node_id)
    app = web.Application()
    app[CFG_KEY] = cfg
    app[NODE_KEY] = me

    store = LocalStore()
    ring = ConsistentHashRing(cfg.node_ids, cfg.vnodes)
    shim = NetworkShim(me.id, {n.id: n.url for n in cfg.nodes},
                       cfg.delay_min_ms, cfg.delay_max_ms)
    coordinator = Coordinator(me.id, ring, shim, store, cfg.n,
                              cfg.request_timeout_ms, cfg.conflict_policy)
    app[STORE_KEY] = store
    app[RING_KEY] = ring
    app[SHIM_KEY] = shim
    app[COORD_KEY] = coordinator

    # ---- replica operations (shared by HTTP routes and the shim's local path)
    def replica_get(key: str) -> dict:
        return {"versions": [v.to_dict() for v in store.get(key)]}

    def replica_put(key: str, body: Any) -> dict:
        versions = [Version.from_dict(v) for v in (body or {}).get("versions", [])]
        return {"ok": True, "changed": store.put_many(key, versions)}

    async def local_handler(method: str, path: str, body: Any) -> Any:
        """Serves the coordinator's requests to itself without HTTP."""
        if not path.startswith(REPLICA_PREFIX):
            raise PeerUnreachable(f"no local route for {path}")
        key = unquote(path[len(REPLICA_PREFIX):])
        if method == "GET":
            return replica_get(key)
        if method == "PUT":
            return replica_put(key, body)
        raise PeerUnreachable(f"no local route for {method} {path}")

    shim.local_handler = local_handler

    # ---- health ---------------------------------------------------------------
    async def health(request: web.Request) -> web.Response:
        return web.json_response({"node": me.id, "ok": True})

    # ---- client API -------------------------------------------------------------
    async def kv_get(request: web.Request) -> web.Response:
        key = request.match_info["key"]
        try:
            r = _quorum_param(request, "r", cfg.default_r, cfg.n)
        except ValueError as exc:
            return _bad_request(str(exc))
        try:
            versions, context = await coordinator.get(key, r)
        except QuorumNotMet as exc:
            return _quorum_failed(exc)
        return web.json_response({
            "key": key,
            "versions": [v.to_dict() for v in versions],
            "context": context,
            "coordinator": me.id,
            "policy": coordinator.conflict_policy,
        })

    async def kv_put(request: web.Request) -> web.Response:
        key = request.match_info["key"]
        try:
            w = _quorum_param(request, "w", cfg.default_w, cfg.n)
            body = await request.json()
        except ValueError as exc:
            return _bad_request(str(exc))
        if not isinstance(body, dict) or "value" not in body:
            return _bad_request("body must be a JSON object with a 'value' field")
        client_id = body.get("client_id")
        if not isinstance(client_id, str) or not client_id:
            return _bad_request("'client_id' must be a non-empty string")
        context = body.get("context") or {}
        if not isinstance(context, dict):
            return _bad_request("'context' must be a JSON object (vector clock)")
        try:
            version, acks = await coordinator.put(key, body["value"], context, client_id, w)
        except QuorumNotMet as exc:
            return _quorum_failed(exc)
        except ValueError as exc:  # e.g. malformed clock counters
            return _bad_request(str(exc))
        return web.json_response({"key": key, "version": version.to_dict(), "acks": acks})

    # ---- internal replica API -----------------------------------------------------
    def _partitioned(request: web.Request) -> web.Response | None:
        sender = request.headers.get(FROM_HEADER)
        if sender and shim.is_blocked(sender):
            return web.json_response({"error": "partitioned"}, status=503)
        return None

    async def internal_get(request: web.Request) -> web.Response:
        if (resp := _partitioned(request)) is not None:
            return resp
        return web.json_response(replica_get(request.match_info["key"]))

    async def internal_put(request: web.Request) -> web.Response:
        if (resp := _partitioned(request)) is not None:
            return resp
        try:
            body = await request.json()
            result = replica_put(request.match_info["key"], body)
        except (ValueError, KeyError, TypeError) as exc:
            return _bad_request(str(exc))
        return web.json_response(result)

    # ---- admin / fault injection ----------------------------------------------------
    async def admin_partition(request: web.Request) -> web.Response:
        try:
            body = await request.json()
            blocked = body["blocked"]
            if not isinstance(blocked, list) or not all(isinstance(b, str) for b in blocked):
                raise ValueError("'blocked' must be a list of node ids")
        except (ValueError, KeyError, TypeError) as exc:
            return _bad_request(str(exc))
        shim.set_partition(blocked)
        return web.json_response({"node": me.id, "blocked": sorted(shim.blocked)})

    async def admin_heal(request: web.Request) -> web.Response:
        shim.heal()
        return web.json_response({"node": me.id, "blocked": []})

    async def admin_delay(request: web.Request) -> web.Response:
        try:
            body = await request.json()
            shim.set_delay(int(body["min_ms"]), int(body["max_ms"]))
        except (ValueError, KeyError, TypeError) as exc:
            return _bad_request(str(exc))
        return web.json_response({"node": me.id, "delay": _delay_dict()})

    def _delay_dict() -> dict:
        return {"min_ms": shim.delay_min_ms, "max_ms": shim.delay_max_ms}

    async def admin_state(request: web.Request) -> web.Response:
        return web.json_response({
            "node": me.id,
            "blocked": sorted(shim.blocked),
            "delay": _delay_dict(),
            "conflict_policy": coordinator.conflict_policy,
            "store": store.dump(),
        })

    async def cleanup(app: web.Application) -> None:
        await coordinator.close()
        await shim.close()

    app.on_cleanup.append(cleanup)

    app.router.add_get("/health", health)
    app.router.add_get("/kv/{key}", kv_get)
    app.router.add_put("/kv/{key}", kv_put)
    app.router.add_get("/internal/replica/{key}", internal_get)
    app.router.add_put("/internal/replica/{key}", internal_put)
    app.router.add_post("/admin/partition", admin_partition)
    app.router.add_post("/admin/heal", admin_heal)
    app.router.add_post("/admin/delay", admin_delay)
    app.router.add_get("/admin/state", admin_state)
    return app


def main() -> None:
    parser = argparse.ArgumentParser(description="Run one key-value store node")
    parser.add_argument("--node-id", required=True)
    parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    args = parser.parse_args()
    use_1ms_timer()   # accurate injected delays on Windows (see timer_resolution.py)
    app = build_app(args.node_id, args.config)
    node = app[NODE_KEY]
    web.run_app(app, host=node.host, port=node.port, print=None)


if __name__ == "__main__":
    main()
