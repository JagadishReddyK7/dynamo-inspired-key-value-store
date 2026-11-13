"""Helpers shared by the experiment scripts: start/stop a real cluster (one OS
process per node), and drive the /admin fault-injection API."""

from __future__ import annotations

import argparse
import asyncio
import atexit
import json
import os
import subprocess
import sys
import tempfile
import time
from contextlib import asynccontextmanager
from pathlib import Path

import aiohttp

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from kvstore.config import DEFAULT_CONFIG, ClusterConfig, load_config  # noqa: E402
from kvstore.ring import ConsistentHashRing  # noqa: E402
from kvstore.timer_resolution import use_1ms_timer  # noqa: E402

RESULTS = ROOT / "results"
use_1ms_timer()   # client-side sleeps and timings in every experiment process


def base_parser(description: str) -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=description)
    p.add_argument("--config", default=str(DEFAULT_CONFIG))
    p.add_argument("--no-spawn", action="store_true",
                   help="use an already running cluster (scripts/run_cluster.py) "
                        "instead of starting one")
    p.add_argument("--out", default=str(RESULTS), help="directory for CSV results")
    return p


def config_variant(base: str | Path, **overrides) -> Path:
    """Write a copy of the config with overrides, e.g. conflict_policy='lww'
    or delay=(5, 50). Returns the path of the temporary file."""
    raw = json.loads(Path(base).read_text(encoding="utf-8"))
    if "conflict_policy" in overrides:
        raw["conflict_policy"] = overrides["conflict_policy"]
    if "delay" in overrides:
        lo, hi = overrides["delay"]
        raw["network"] = {"delay_min_ms": lo, "delay_max_ms": hi}
    if "timeout_ms" in overrides:
        raw["quorum"]["request_timeout_ms"] = overrides["timeout_ms"]
    fd, path = tempfile.mkstemp(prefix="cluster-", suffix=".json")
    with open(fd, "w", encoding="utf-8") as fh:
        json.dump(raw, fh)
    atexit.register(_remove_quietly, path)   # nodes only read it at start-up
    return Path(path)


def _remove_quietly(path: str) -> None:
    try:
        os.remove(path)
    except OSError:
        pass


class Cluster:
    """Handle on a running cluster: URLs, ring and admin helpers."""

    def __init__(self, cfg: ClusterConfig, session: aiohttp.ClientSession):
        self.cfg = cfg
        self.session = session
        self.urls = {n.id: n.url for n in cfg.nodes}
        self.ring = ConsistentHashRing(cfg.node_ids, cfg.vnodes)

    def replicas(self, key: str) -> list[str]:
        return self.ring.preference_list(key, self.cfg.n)

    async def _post(self, node: str, path: str, body=None) -> dict:
        async with self.session.post(self.urls[node] + path, json=body) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def partition(self, *groups) -> None:
        """Split into groups; every node blocks all nodes outside its group.
        Nodes not listed form one extra group of their own."""
        groups = [set(g) for g in groups]
        rest = set(self.urls) - set().union(*groups)
        if rest:
            groups.append(rest)
        await asyncio.gather(*(
            self._post(node, "/admin/partition",
                       {"blocked": sorted(set(self.urls) - group)})
            for group in groups for node in group))

    async def heal(self) -> None:
        await asyncio.gather(*(self._post(n, "/admin/heal") for n in self.urls))

    async def set_delay(self, min_ms: int, max_ms: int) -> None:
        await asyncio.gather(*(self._post(n, "/admin/delay",
                                          {"min_ms": min_ms, "max_ms": max_ms})
                               for n in self.urls))

    async def state(self, node: str) -> dict:
        async with self.session.get(self.urls[node] + "/admin/state") as resp:
            resp.raise_for_status()
            return await resp.json()

    async def wait_healthy(self, timeout_s: float = 20.0) -> None:
        deadline = time.monotonic() + timeout_s
        pending = set(self.urls)
        while pending:
            for node in list(pending):
                try:
                    async with self.session.get(self.urls[node] + "/health",
                                                timeout=aiohttp.ClientTimeout(total=1)) as r:
                        if r.status == 200:
                            pending.discard(node)
                except (aiohttp.ClientError, asyncio.TimeoutError, OSError):
                    pass
            if pending:
                if time.monotonic() > deadline:
                    raise RuntimeError(f"nodes not healthy: {sorted(pending)}")
                await asyncio.sleep(0.2)


def start_nodes(config_path: str | Path) -> list[subprocess.Popen]:
    cfg = load_config(config_path)
    return [subprocess.Popen([sys.executable, "-m", "kvstore.server", "--node-id", n.id,
                              "--config", str(config_path)], cwd=ROOT)
            for n in cfg.nodes]


def stop_nodes(procs: list[subprocess.Popen]) -> None:
    for p in procs:
        if p.poll() is None:
            p.terminate()
    for p in procs:
        try:
            p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            p.kill()


@asynccontextmanager
async def cluster(config_path: str | Path, spawn: bool = True):
    """Start (or attach to) a cluster, heal it and yield a Cluster handle."""
    cfg = load_config(config_path)
    procs = start_nodes(config_path) if spawn else []
    try:
        async with aiohttp.ClientSession() as session:
            c = Cluster(cfg, session)
            await c.wait_healthy()
            await c.heal()
            await c.set_delay(cfg.delay_min_ms, cfg.delay_max_ms)
            yield c
    finally:
        stop_nodes(procs)


def percentile_table(df, group_col: str, value_col: str = "latency_ms"):
    """p50/p95/p99/mean per group as a DataFrame."""
    g = df.groupby(group_col)[value_col]
    return g.quantile([0.5, 0.95, 0.99]).unstack().rename(
        columns={0.5: "p50", 0.95: "p95", 0.99: "p99"}).assign(mean=g.mean(), count=g.size())
