"""Stale-read frequency vs (R, W).

Workload (per (R, W) setting and trial):
  - 1 writer writes increasing integers 1, 2, 3, ... to one key with W
  - K readers read the key concurrently with R
  - injected delay (e.g. 5-50 ms) so replication lag exists
  - each request goes to a RANDOM node (not always the same coordinator)
  - every concurrent reader task and the writer have their own client_id
The whole sweep over SETTINGS is repeated --trials times (trial is the outer
loop, so slow drift of the machine affects all settings alike); the summary
reports the pooled stale fraction and the mean / std across trials.

DEFINITION (put this in the report verbatim):
  A read is STALE if it does not return the value of the latest write that was
  ACKNOWLEDGED (put.end_ms) BEFORE the read was SENT (get.start_ms).
  Writes still in flight when the read starts may or may not be visible -
  returning either is NOT stale.

Since the writer's values increase, a read is stale exactly when the largest
value it returned is smaller than the latest acknowledged value.

Expected: ~0% stale when R + W > N (=3); non-zero when R + W <= N, rising with
delay variance.

Usage:  python experiments/stale_reads.py [--trials 5] [--writes 100] [--readers 4]
                                          [--delay 5 50] [--name stale_reads]
Output: results/<name>.csv (op log), results/<name>_summary.csv
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
TAG_RE = re.compile(r"stale_r(\d)w(\d)(?:_t(\d+))?")


def tag(r: int, w: int, trial: int) -> str:
    return f"stale_r{r}w{w}_t{trial}"


async def writer(urls, key, w, writes, log, exp, done: asyncio.Event):
    try:
        async with KVClient(urls, make_client_id("writer"), w=w,
                            oplog=log, experiment=exp) as kv:
            for value in range(1, writes + 1):
                try:
                    await kv.put(key, value)
                except KVError:
                    pass
    finally:
        done.set()


async def reader(urls, key, r, log, exp, done: asyncio.Event):
    async with KVClient(urls, make_client_id("reader"), r=r,
                        oplog=log, experiment=exp) as kv:
        while not done.is_set():
            try:
                await kv.get(key)
            except KVError:
                pass


async def run(args) -> Path:
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    csv_path = out / f"{args.name}.csv"
    cfg_path = config_variant(args.config, delay=tuple(args.delay))
    async with cluster(cfg_path, spawn=not args.no_spawn) as c:
        await c.set_delay(*args.delay)
        urls = list(c.urls.values())
        with OpLog(csv_path) as log:
            for trial in range(1, args.trials + 1):
                for r, w in SETTINGS:
                    key = f"{args.name}-r{r}w{w}-t{trial}"
                    exp = tag(r, w, trial)
                    done = asyncio.Event()
                    await asyncio.gather(
                        writer(urls, key, w, args.writes, log, exp, done),
                        *(reader(urls, key, r, log, exp, done) for _ in range(args.readers)))
                print(f"  trial {trial}/{args.trials} done", flush=True)
    return csv_path


def _max_value(values_json: str):
    values = [v for v in json.loads(values_json) if isinstance(v, (int, float))]
    return max(values) if values else 0


def per_trial(csv_path: str | Path) -> pd.DataFrame:
    """One row per (r, w, trial): reads checked, stale reads, stale fraction."""
    df = pd.read_csv(csv_path, keep_default_na=False)
    rows = []
    for exp, group in df.groupby("experiment"):
        m = TAG_RE.fullmatch(str(exp))
        if not m:
            continue
        r, w, trial = int(m.group(1)), int(m.group(2)), int(m.group(3) or 1)
        puts = group[(group["op"] == "put") & (group["ok"] == 1)].sort_values("end_ms")
        gets = group[(group["op"] == "get") & (group["ok"] == 1)]
        ack_times = puts["end_ms"].astype(float).to_numpy()
        ack_values = puts["value"].astype(str).map(json.loads).cummax().to_numpy()
        checked = stale = 0
        for start, values in zip(gets["start_ms"].astype(float), gets["value"].astype(str)):
            idx = ack_times.searchsorted(start, side="left") - 1  # last ack before start
            if idx < 0:
                continue        # no write acknowledged yet: cannot be stale
            checked += 1
            if _max_value(values) < ack_values[idx]:
                stale += 1
        rows.append({"r": r, "w": w, "trial": trial, "reads_checked": checked,
                     "stale_reads": stale,
                     "stale_fraction": stale / checked if checked else 0.0})
    return pd.DataFrame(rows)


def summary(csv_path: str | Path) -> pd.DataFrame:
    """One row per (r, w): pooled stale % plus mean / std / max across trials."""
    t = per_trial(csv_path)
    g = t.groupby(["r", "w"])
    out = g.agg(trials=("trial", "nunique"), reads_checked=("reads_checked", "sum"),
                stale_reads=("stale_reads", "sum")).reset_index()
    out["strict_quorum"] = out["r"] + out["w"] > N
    out["stale_pct"] = 100 * out["stale_reads"] / out["reads_checked"].clip(lower=1)
    pct = g["stale_fraction"]
    out["mean_pct"] = (100 * pct.mean()).to_numpy()
    out["std_pct"] = (100 * pct.std(ddof=1)).fillna(0).to_numpy()
    out["max_pct"] = (100 * pct.max()).to_numpy()
    cols = ["r", "w", "strict_quorum", "trials", "reads_checked", "stale_reads",
            "stale_pct", "mean_pct", "std_pct", "max_pct"]
    return out[cols].sort_values(["r", "w"]).round(3).reset_index(drop=True)


def analyse(csv_path: str | Path) -> dict[tuple[int, int], float]:
    """Return {(r, w): pooled stale_fraction}."""
    s = summary(csv_path)
    return {(r, w): pct / 100 for r, w, pct in zip(s["r"], s["w"], s["stale_pct"])}


def main() -> None:
    p = base_parser(__doc__.splitlines()[0])
    p.add_argument("--trials", type=int, default=5)
    p.add_argument("--writes", type=int, default=100, help="writes per setting per trial")
    p.add_argument("--readers", type=int, default=4)
    p.add_argument("--delay", type=int, nargs=2, default=[5, 50], metavar=("MIN", "MAX"))
    p.add_argument("--name", default="stale_reads", help="output file prefix")
    p.add_argument("--analyse-only", action="store_true",
                   help="skip the run and analyse an existing results/<name>.csv")
    args = p.parse_args()
    csv_path = Path(args.out) / f"{args.name}.csv"
    if not args.analyse_only:
        csv_path = asyncio.run(run(args))
    s = summary(csv_path)
    s.to_csv(Path(args.out) / f"{args.name}_summary.csv", index=False)
    print(s.to_string(index=False))


if __name__ == "__main__":
    main()
