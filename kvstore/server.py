"""HTTP server for one node.  [Owner: Person B]

Run one node:   python -m kvstore.server --node-id n1 [--config config/cluster.json]
Run a cluster:  python scripts/run_cluster.py

Endpoints are specified in docs/API.md - keep this file in sync with it.
"""

from __future__ import annotations

import argparse

from aiohttp import web

from .config import DEFAULT_CONFIG, ClusterConfig, NodeInfo, load_config

CFG_KEY = web.AppKey("cfg", ClusterConfig)
NODE_KEY = web.AppKey("node", NodeInfo)


def build_app(node_id: str, config_path: str) -> web.Application:
    cfg = load_config(config_path)
    me = cfg.node(node_id)
    app = web.Application()
    app[CFG_KEY] = cfg
    app[NODE_KEY] = me

    # TODO (Person B): construct LocalStore, ConsistentHashRing, NetworkShim, Coordinator
    # and store them on `app` (define more web.AppKey constants like the ones above);
    # close the shim's session in app.on_cleanup.

    async def not_implemented(request: web.Request) -> web.Response:
        return web.json_response({"error": "not_implemented"}, status=501)

    async def health(request: web.Request) -> web.Response:
        return web.json_response({"node": me.id, "ok": True})

    app.router.add_get("/health", health)
    # Client API
    app.router.add_get("/kv/{key}", not_implemented)
    app.router.add_put("/kv/{key}", not_implemented)
    # Internal replica API (reject if X-From-Node sender is blocked by the shim)
    app.router.add_get("/internal/replica/{key}", not_implemented)
    app.router.add_put("/internal/replica/{key}", not_implemented)
    # Admin / fault injection
    app.router.add_post("/admin/partition", not_implemented)
    app.router.add_post("/admin/heal", not_implemented)
    app.router.add_post("/admin/delay", not_implemented)
    app.router.add_get("/admin/state", not_implemented)
    return app


def main() -> None:
    parser = argparse.ArgumentParser(description="Run one key-value store node")
    parser.add_argument("--node-id", required=True)
    parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    args = parser.parse_args()
    app = build_app(args.node_id, args.config)
    node = app[NODE_KEY]
    web.run_app(app, host=node.host, port=node.port, print=None)


if __name__ == "__main__":
    main()
