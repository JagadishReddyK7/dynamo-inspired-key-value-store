"""Versions returned under concurrent updates.

Several writers update the SAME key at the same time, each with its own
client_id, doing read-modify-write cycles (read with R, add its item to the
set, write back with the read's context, using W). Readers sample the key and
record how many versions (siblings) each read returned.

With the "siblings" policy, concurrent read-modify-writes create siblings;
with "lww" every read returns at most one version, but updates are lost.
At the end a client reconciles the siblings (set union) and we count how many
of the writers' items survived: with siblings + union no acknowledged item is
lost, with lww some are.

Usage:  python experiments/concurrent_writes.py [--writers 4] [--rounds 15] [--policy both]
Output: results/concurrent_writes.csv, results/concurrent_writes_summary.csv
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd  # noqa: E402

from client.client import KVClient, KVError, make_client_id  # noqa: E402
from experiments.common import base_parser, cluster, config_variant  # noqa: E402
from experiments.oplog import OpLog  # noqa: E402

SETTINGS = [(1, 1), (2, 2), (3, 3)]


def union(values):
    merged = set()
    for v in values:
        merged.update(v or [])
    return sorted(merged)


async def writer(urls, key, r, w, rounds, log, exp, acked: set):
    async with KVClient(urls, make_client_id("cw"), r=r, w=w,
                        oplog=log, experiment=exp) as kv:
        for i in range(rounds):
            item = f"{kv.client_id}:{i}"
            try:
                res = await kv.get(key)
                await kv.put(key, union(res.values + [[item]]), context=res.context)
                acked.add(item)
            except KVError:
                pass


async def reader(urls, key, r, log, exp, done: asyncio.Event):
    async with KVClient(urls, make_client_id("cr"), r=r, oplog=log, experiment=exp) as kv:
        while not done.is_set():
            try:
                await kv.get(key)
            except KVError:
                pass
            await asyncio.sleep(0.005)


async def run_policy(args, policy: str, log: OpLog) -> list[dict]:
    cfg_path = config_variant(args.config, conflict_policy=policy, delay=tuple(args.delay))
    rows = []
    async with cluster(cfg_path, spawn=not args.no_spawn) as c:
        state = await c.state(next(iter(c.urls)))
        if state.get("conflict_policy") != policy:
            print(f"  skipping policy {policy}: running cluster uses "
                  f"{state.get('conflict_policy')}", flush=True)
            return rows
        await c.set_delay(*args.delay)
        urls = list(c.urls.values())
        for r, w in SETTINGS:
            key = f"cw-{policy}-r{r}w{w}"
            exp = f"concurrent_{policy}_r{r}w{w}"
            acked: set[str] = set()
            done = asyncio.Event()

            async def writers():
                await asyncio.gather(*(writer(urls, key, r, w, args.rounds, log, exp, acked)
                                       for _ in range(args.writers)))
                done.set()

            await asyncio.gather(writers(), *(reader(urls, key, r, log, exp, done)
                                              for _ in range(args.readers)))
            async with KVClient(urls, r=3, w=3) as fixer:
                before = await fixer.get(key)
                final = await fixer.reconcile(key, union)
            survived = set(union(final.values))
            rows.append({"policy": policy, "r": r, "w": w,
                         "acked_items": len(acked),
                         "siblings_before_reconcile": len(before.versions),
                         "items_lost": len(acked - survived)})
            print(f"  {policy} R={r} W={w}: {rows[-1]}", flush=True)
    return rows


async def run(args) -> list[dict]:
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    policies = ["siblings", "lww"] if args.policy == "both" else [args.policy]
    rows = []
    with OpLog(out / "concurrent_writes.csv") as log:
        for policy in policies:
            rows += await run_policy(args, policy, log)
    return rows


def main() -> None:
    p = base_parser(__doc__.splitlines()[0])
    p.add_argument("--writers", type=int, default=4)
    p.add_argument("--readers", type=int, default=2)
    p.add_argument("--rounds", type=int, default=15)
    p.add_argument("--policy", choices=["siblings", "lww", "both"], default="both")
    p.add_argument("--delay", type=int, nargs=2, default=[5, 50], metavar=("MIN", "MAX"))
    args = p.parse_args()
    rows = asyncio.run(run(args))
    summary = pd.DataFrame(rows)
    summary.to_csv(Path(args.out) / "concurrent_writes_summary.csv", index=False)
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
