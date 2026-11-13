"""Shared fixtures: an in-process 5-node cluster on free localhost ports."""

from __future__ import annotations

import json
import socket
from contextlib import asynccontextmanager

import aiohttp
import pytest
from aiohttp import web

from kvstore.server import COORD_KEY, RING_KEY, SHIM_KEY, STORE_KEY, build_app


def free_ports(count: int) -> list[int]:
    socks, ports = [], []
    for _ in range(count):
        s = socket.socket()
        s.bind(("127.0.0.1", 0))
        socks.append(s)
        ports.append(s.getsockname()[1])
    for s in socks:
        s.close()
    return ports


def write_config(path, ports, policy="siblings", delay=(0, 0), timeout_ms=1000,
                 default_r=2, default_w=2):
    cfg = {
        "nodes": [{"id": f"n{i + 1}", "host": "127.0.0.1", "port": p} for i, p in enumerate(ports)],
        "replication": {"n": 3, "vnodes": 64},
        "quorum": {"default_r": default_r, "default_w": default_w,
                   "request_timeout_ms": timeout_ms},
        "conflict_policy": policy,
        "network": {"delay_min_ms": delay[0], "delay_max_ms": delay[1]},
    }
    path.write_text(json.dumps(cfg), encoding="utf-8")
    return path


class Cluster:
    def __init__(self, apps: dict[str, web.Application], urls: dict[str, str]):
        self.apps = apps
        self.urls = urls
        self.session: aiohttp.ClientSession | None = None

    def store(self, node):
        return self.apps[node][STORE_KEY]

    def shim(self, node):
        return self.apps[node][SHIM_KEY]

    def coordinator(self, node):
        return self.apps[node][COORD_KEY]

    def replicas(self, key):
        return next(iter(self.apps.values()))[RING_KEY].preference_list(key, 3)

    def non_replica(self, key):
        reps = self.replicas(key)
        return next(n for n in self.apps if n not in reps)

    def partition(self, side_a, side_b):
        """Symmetric cut, like POST /admin/partition on every node."""
        for n in side_a:
            self.shim(n).set_partition(list(side_b))
        for n in side_b:
            self.shim(n).set_partition(list(side_a))

    def heal(self):
        for n in self.apps:
            self.shim(n).heal()

    def values_at(self, node, key):
        return sorted(json.dumps(v.value) for v in self.store(node).get(key))


@asynccontextmanager
async def run_cluster(tmp_path, **cfg_kwargs):
    ports = free_ports(5)
    cfg_path = write_config(tmp_path / "cluster.json", ports, **cfg_kwargs)
    runners, apps, urls = [], {}, {}
    try:
        for i, port in enumerate(ports):
            node_id = f"n{i + 1}"
            app = build_app(node_id, str(cfg_path))
            runner = web.AppRunner(app)
            await runner.setup()
            await web.TCPSite(runner, "127.0.0.1", port).start()
            runners.append(runner)
            apps[node_id] = app
            urls[node_id] = f"http://127.0.0.1:{port}"
        cluster = Cluster(apps, urls)
        async with aiohttp.ClientSession() as session:
            cluster.session = session
            yield cluster
    finally:
        for runner in runners:
            await runner.cleanup()


@pytest.fixture
async def cluster(tmp_path):
    async with run_cluster(tmp_path) as c:
        yield c


@pytest.fixture
async def lww_cluster(tmp_path):
    async with run_cluster(tmp_path, policy="lww") as c:
        yield c
