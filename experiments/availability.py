"""Availability vs (R, W) under unreachable replicas.

Scenarios: 0, 1, 2 replicas of the key unreachable. The k replicas are cut off
with a symmetric /admin/partition; the client talks to a coordinator that is
NOT a replica of the key and sits on the majority side.
For each (R, W): success rate of gets and puts.

Expected (strict quorum, N=3): an operation needing k acks fails once more than
3 - k replicas are unreachable. E.g. W=3 fails with a single replica down.

Usage:  python experiments/availability.py [--ops 20] [--delay 5 50]
Output: results/availability.csv (op log), results/availability_summary.csv
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

SETTINGS = [(1, 1), (1, 2), (2, 1), (2, 2), (1, 3), (3, 1), (3, 3)]
DOWN = [0, 1, 2]
N = 3


def tag(down: int, r: int, w: int) -> str:
    return f"avail_down{down}_r{r}w{w}"


async def worker(url, key, r, w, ops, log, exp):
    async with KVClient(url, make_client_id("avail"), r=r, w=w,
                        oplog=log, experiment=exp) as kv:
        for i in range(ops):
            for op in (kv.get(key), kv.put(key, i)):
                try:
                    await op
                except KVError:
                    pass


async def run(args) -> Path:
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    csv_path = out / "availability.csv"
    cfg_path = config_variant(args.config, delay=tuple(args.delay))
    async with cluster(cfg_path, spawn=not args.no_spawn) as c:
        await c.set_delay(*args.delay)
        with OpLog(csv_path) as log:
            for down in DOWN:
                for r, w in SETTINGS:
                    key = f"avail-d{down}-r{r}w{w}"
                    replicas = c.replicas(key)
                    coord = next(n for n in c.urls if n not in replicas)
                    async with KVClient(c.urls[coord], w=3) as seed:
                        await seed.put(key, -1)                  # all 3 replicas start equal
                    unreachable = replicas[:down]
                    if unreachable:
                        await c.partition(set(c.urls) - set(unreachable), unreachable)
                    await asyncio.gather(*(worker(c.urls[coord], key, r, w, args.ops // args.workers,
                                                  log, tag(down, r, w))
                                           for _ in range(args.workers)))
                    await c.heal()
                print(f"  done: {down} replica(s) unreachable", flush=True)
    return csv_path


def summarise(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path, keep_default_na=False)
    parts = df["experiment"].str.extract(r"avail_down(\d)_r(\d)w(\d)").astype(int)
    df[["down", "r", "w"]] = parts
    table = (df.groupby(["down", "r", "w", "op"])["ok"].agg(["mean", "size"])
             .rename(columns={"mean": "success_rate", "size": "ops"}).reset_index())
    need = table["r"].where(table["op"] == "get", table["w"])
    table["expected_ok"] = (N - table["down"]) >= need
    return table.sort_values(["op", "down", "r", "w"]).round(3)


def main() -> None:
    p = base_parser(__doc__.splitlines()[0])
    p.add_argument("--ops", type=int, default=20, help="gets and puts per (setting, scenario)")
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--delay", type=int, nargs=2, default=[5, 50], metavar=("MIN", "MAX"))
    args = p.parse_args()
    csv_path = asyncio.run(run(args))
    summary = summarise(csv_path)
    summary.to_csv(Path(args.out) / "availability_summary.csv", index=False)
    print(summary.to_string(index=False))
    mismatch = summary[(summary["success_rate"] > 0.5) != summary["expected_ok"]]
    if len(mismatch):
        print("\nWARNING: unexpected availability for:\n" + mismatch.to_string(index=False))


if __name__ == "__main__":
    main()
