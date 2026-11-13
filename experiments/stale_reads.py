"""Stale-read frequency vs (R, W).

Workload (per (R, W) setting):
  - 1 writer writes increasing integers 1, 2, 3, ... to one key with W
  - K readers read the key concurrently with R
  - injected delay (e.g. 5-50 ms) so replication lag exists
  - each request goes to a RANDOM node (not always the same coordinator)
  - every concurrent reader task and the writer have their own client_id

DEFINITION (put this in the report verbatim):
  A read is STALE if it does not return the value of the latest write that was
  ACKNOWLEDGED (put.end_ms) BEFORE the read was SENT (get.start_ms).
  Writes still in flight when the read starts may or may not be visible -
  returning either is NOT stale.

Since the writer's values increase, a read is stale exactly when the largest
value it returned is smaller than the latest acknowledged value.

Expected: ~0% stale when R + W > N (=3); non-zero when R + W <= N, rising with
delay variance.

Usage:  python experiments/stale_reads.py [--writes 100] [--readers 4] [--delay 5 50]
Output: results/stale_reads.csv (op log), results/stale_reads_summary.csv
"""

from __future__ import annotations

import asyncio
import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd  # noqa: E402

from client.client import KVClient, KVError, make_client_id  # noqa: E402
from experiments.common import base_parser, cluster, config_variant  # noqa: E402
from experiments.oplog import OpLog  # noqa: E402

SETTINGS = [(1, 1), (1, 2), (2, 1), (2, 2), (1, 3), (3, 1), (3, 3)]
N = 3


def tag(r: int, w: int) -> str:
    return f"stale_r{r}w{w}"


async def writer(urls, key, r, w, writes, log, done: asyncio.Event):
    try:
        async with KVClient(urls, make_client_id("writer"), r=r, w=w,
                            oplog=log, experiment=tag(r, w)) as kv:
            for value in range(1, writes + 1):
                try:
                    await kv.put(key, value)
                except KVError:
                    pass
    finally:
        done.set()


async def reader(urls, key, r, w, log, done: asyncio.Event):
    async with KVClient(urls, make_client_id("reader"), r=r, w=w,
                        oplog=log, experiment=tag(r, w)) as kv:
        while not done.is_set():
            try:
                await kv.get(key)
            except KVError:
                pass


async def run(args) -> Path:
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    csv_path = out / "stale_reads.csv"
    cfg_path = config_variant(args.config, delay=tuple(args.delay))
    async with cluster(cfg_path, spawn=not args.no_spawn) as c:
        await c.set_delay(*args.delay)
        urls = list(c.urls.values())
        with OpLog(csv_path) as log:
            for r, w in SETTINGS:
                key = f"stale-r{r}w{w}"
                done = asyncio.Event()
                await asyncio.gather(
                    writer(urls, key, r, w, args.writes, log, done),
                    *(reader(urls, key, r, w, log, done) for _ in range(args.readers)))
                print(f"  done R={r} W={w}", flush=True)
    return csv_path


def _max_value(values_json: str):
    values = [v for v in json.loads(values_json) if isinstance(v, (int, float))]
    return max(values) if values else 0


def analyse(csv_path: str | Path) -> dict[tuple[int, int], float]:
    """Return {(r, w): stale_fraction} using the definition above."""
    return {k: row["stale_fraction"] for k, row in _analyse_df(csv_path).iterrows()}


def _analyse_df(csv_path) -> pd.DataFrame:
    df = pd.read_csv(csv_path, keep_default_na=False)
    rows = {}
    for exp, group in df.groupby("experiment"):
        m = re.fullmatch(r"stale_r(\d)w(\d)", str(exp))
        if not m:
            continue
        r, w = int(m.group(1)), int(m.group(2))
        puts = group[(group["op"] == "put") & (group["ok"] == 1)].sort_values("end_ms")
        gets = group[(group["op"] == "get") & (group["ok"] == 1)]
        ack_times = puts["end_ms"].to_numpy()
        ack_values = puts["value"].astype(str).map(json.loads).cummax().to_numpy()
        checked = stale = 0
        for start, values in zip(gets["start_ms"], gets["value"].astype(str)):
            idx = ack_times.searchsorted(start, side="left") - 1  # last ack before start
            if idx < 0:
                continue        # no write acknowledged yet: cannot be stale
            checked += 1
            if _max_value(values) < ack_values[idx]:
                stale += 1
        rows[(r, w)] = {"r": r, "w": w, "strict_quorum": r + w > N,
                        "reads_checked": checked, "stale_reads": stale,
                        "stale_fraction": stale / checked if checked else 0.0}
    out = pd.DataFrame.from_dict(rows, orient="index")
    out.index = pd.MultiIndex.from_tuples(out.index, names=["R", "W"])
    return out.sort_index()


def main() -> None:
    p = base_parser(__doc__.splitlines()[0])
    p.add_argument("--writes", type=int, default=100)
    p.add_argument("--readers", type=int, default=4)
    p.add_argument("--delay", type=int, nargs=2, default=[5, 50], metavar=("MIN", "MAX"))
    p.add_argument("--analyse-only", action="store_true",
                   help="skip the run and analyse an existing results/stale_reads.csv")
    args = p.parse_args()
    csv_path = Path(args.out) / "stale_reads.csv"
    if not args.analyse_only:
        csv_path = asyncio.run(run(args))
    summary = _analyse_df(csv_path)
    summary.to_csv(Path(args.out) / "stale_reads_summary.csv", index=False)
    print(summary.assign(stale_pct=(summary["stale_fraction"] * 100).round(2))
          .drop(columns="stale_fraction").to_string(index=False))


if __name__ == "__main__":
    main()
