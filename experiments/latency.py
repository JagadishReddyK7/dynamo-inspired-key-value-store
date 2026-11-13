"""Latency vs (R, W).

For each (R, W) in SETTINGS: run M puts and then M gets over a set of keys,
with injected network delay; every operation is recorded to the OpLog.
Reports p50 / p95 / p99 per setting and operation.

Expected: latency grows with R / W because the coordinator waits for the
R-th / W-th fastest replica; tail latency (p99) grows fastest.

Usage:  python experiments/latency.py [--ops 200] [--delay 5 50] [--no-spawn]
Output: results/latency.csv (op log), results/latency_summary.csv
"""

from __future__ import annotations

import asyncio
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd  # noqa: E402

from client.client import KVClient, KVError, make_client_id  # noqa: E402
from experiments.common import base_parser, cluster, config_variant, percentile_table  # noqa: E402
from experiments.oplog import OpLog  # noqa: E402

SETTINGS = [(1, 1), (1, 2), (2, 1), (2, 2), (1, 3), (3, 1), (3, 3)]


def tag(r: int, w: int) -> str:
    return f"latency_r{r}w{w}"


async def worker(urls, r, w, ops, keys, log, phase):
    async with KVClient(urls, make_client_id("lat"), r=r, w=w,
                        oplog=log, experiment=tag(r, w)) as kv:
        for i in range(ops):
            key = random.choice(keys)
            try:
                if phase == "put":
                    await kv.put(key, {"i": i, "client": kv.client_id})
                else:
                    await kv.get(key)
            except KVError:
                pass   # failures are recorded in the log with ok=0


async def run(args) -> Path:
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    csv_path = out / "latency.csv"
    cfg_path = config_variant(args.config, delay=tuple(args.delay))
    async with cluster(cfg_path, spawn=not args.no_spawn) as c:
        await c.set_delay(*args.delay)
        urls = list(c.urls.values())
        with OpLog(csv_path) as log:
            for r, w in SETTINGS:
                keys = [f"lat-r{r}w{w}-{i}" for i in range(args.keys)]
                per_worker = max(1, args.ops // args.workers)
                for phase in ("put", "get"):
                    await asyncio.gather(*(worker(urls, r, w, per_worker, keys, log, phase)
                                           for _ in range(args.workers)))
                print(f"  done R={r} W={w}", flush=True)
    return csv_path


def summarise(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    df = df[df["ok"] == 1].copy()
    df["latency_ms"] = df["end_ms"] - df["start_ms"]
    df["setting"] = df["experiment"].str.replace("latency_", "", regex=False)
    df["group"] = df["setting"] + "/" + df["op"]
    table = percentile_table(df, "group").reset_index()
    table[["setting", "op"]] = table["group"].str.split("/", expand=True)
    table[["r", "w"]] = table["setting"].str.extract(r"r(\d)w(\d)").astype(int)
    cols = ["r", "w", "op", "count", "p50", "p95", "p99", "mean"]
    return table[cols].sort_values(["op", "r", "w"]).round(2)


def main() -> None:
    p = base_parser(__doc__.splitlines()[0])
    p.add_argument("--ops", type=int, default=200, help="puts and gets per setting")
    p.add_argument("--workers", type=int, default=4, help="concurrent clients")
    p.add_argument("--keys", type=int, default=20)
    p.add_argument("--delay", type=int, nargs=2, default=[5, 50], metavar=("MIN", "MAX"))
    args = p.parse_args()
    csv_path = asyncio.run(run(args))
    summary = summarise(csv_path)
    summary.to_csv(Path(args.out) / "latency_summary.csv", index=False)
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
