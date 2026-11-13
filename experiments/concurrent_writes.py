"""Versions returned under concurrent updates.

Several writers update the SAME key at the same time, each with its own
client_id, doing read-modify-write cycles (read with R, add its item to the
set, write back with the read's context, using W). Readers sample the key and
record how many versions (siblings) each read returned.

With the "siblings" policy, concurrent read-modify-writes create siblings;
with "lww" every read returns at most one version, but updates are lost.
At the end a client reconciles the siblings (set union) and we count how many
of the writers' acknowledged items survived.

--no-ryw turns off client rule 4 (read-your-writes) to measure its effect:
without it, a read-modify-write built on a read that missed the writer's own
previous write (possible when R + W <= N) drops that write, even under
"siblings".

Usage:  python experiments/concurrent_writes.py [--writers 4] [--rounds 15]
                                                [--trials 5] [--policy both] [--no-ryw]
Output: results/<name>.csv, results/<name>_summary.csv
        (name = concurrent_writes, or concurrent_writes_noryw with --no-ryw)
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


async def writer(urls, key, r, w, rounds, log, exp, acked: set, stats: dict, ryw: bool):
    async with KVClient(urls, make_client_id("cw"), r=r, w=w, oplog=log,
                        experiment=exp, read_your_writes=ryw) as kv:
        for i in range(rounds):
            item = f"{kv.client_id}:{i}"
            try:
                res = await kv.get(key)
                stats["own_write_restored"] += res.own_write_added
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
        for trial in range(1, args.trials + 1):
            for r, w in SETTINGS:
                key = f"{args.name}-{policy}-r{r}w{w}-t{trial}"
                exp = f"concurrent_{policy}_r{r}w{w}_t{trial}"
                acked: set[str] = set()
                stats = {"own_write_restored": 0}
                done = asyncio.Event()

                async def writers():
                    await asyncio.gather(*(writer(urls, key, r, w, args.rounds, log, exp,
                                                  acked, stats, not args.no_ryw)
                                           for _ in range(args.writers)))
                    done.set()

                await asyncio.gather(writers(), *(reader(urls, key, r, log, exp, done)
                                                  for _ in range(args.readers)))
                async with KVClient(urls, r=3, w=3) as fixer:
                    before = await fixer.get(key)
                    final = await fixer.reconcile(key, union)
                survived = set(union(final.values))
                rows.append({"policy": policy, "r": r, "w": w, "trial": trial,
                             "acked_items": len(acked),
                             "siblings_before_reconcile": len(before.versions),
                             "own_write_restored": stats["own_write_restored"],
                             "items_lost": len(acked - survived)})
            print(f"  {policy}: trial {trial}/{args.trials} done", flush=True)
    return rows


async def run(args) -> list[dict]:
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    policies = ["siblings", "lww"] if args.policy == "both" else [args.policy]
    rows = []
    with OpLog(out / f"{args.name}.csv") as log:
        for policy in policies:
            rows += await run_policy(args, policy, log)
    return rows


def latency_table(csv_path: Path) -> pd.DataFrame:
    """p50 latency of successful gets/puts and mean versions per read,
    per (policy, r, w), i.e. latency under concurrent updates."""
    df = pd.read_csv(csv_path, keep_default_na=False)
    df = df[df["ok"] == 1].copy()
    df[["policy", "r", "w"]] = df["experiment"].str.extract(r"concurrent_(\w+?)_r(\d)w(\d)")
    df[["r", "w"]] = df[["r", "w"]].astype(int)
    df["latency_ms"] = df["end_ms"].astype(float) - df["start_ms"].astype(float)
    keys = ["policy", "r", "w"]
    out = df.pivot_table(index=keys, columns="op", values="latency_ms", aggfunc="median")
    out.columns = [f"{op}_p50_ms" for op in out.columns]
    gets = df[df["op"] == "get"].copy()
    gets["num_versions"] = gets["num_versions"].astype(int)
    out["mean_versions_per_read"] = gets.groupby(keys)["num_versions"].mean()
    return out.round(2).reset_index()


def summarise(rows: list[dict], csv_path: Path) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    out = (df.groupby(["policy", "r", "w"], sort=False)
           .agg(trials=("trial", "nunique"), acked_items=("acked_items", "sum"),
                items_lost=("items_lost", "sum"),
                own_write_restored=("own_write_restored", "sum"),
                mean_siblings_before_reconcile=("siblings_before_reconcile", "mean"))
           .reset_index())
    out["lost_pct"] = (100 * out["items_lost"] / out["acked_items"]).round(2)
    return out.merge(latency_table(csv_path), on=["policy", "r", "w"], how="left")


def main() -> None:
    p = base_parser(__doc__.splitlines()[0])
    p.add_argument("--writers", type=int, default=4)
    p.add_argument("--readers", type=int, default=2)
    p.add_argument("--rounds", type=int, default=15)
    p.add_argument("--trials", type=int, default=5)
    p.add_argument("--policy", choices=["siblings", "lww", "both"], default="both")
    p.add_argument("--delay", type=int, nargs=2, default=[5, 50], metavar=("MIN", "MAX"))
    p.add_argument("--no-ryw", action="store_true",
                   help="disable client read-your-writes (client rule 4) for comparison")
    p.add_argument("--name", default=None, help="output file prefix")
    args = p.parse_args()
    args.name = args.name or ("concurrent_writes_noryw" if args.no_ryw else "concurrent_writes")
    rows = asyncio.run(run(args))
    if not rows:
        return
    summary = summarise(rows, Path(args.out) / f"{args.name}.csv")
    summary.to_csv(Path(args.out) / f"{args.name}_summary.csv", index=False)
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
