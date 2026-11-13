"""Run every experiment in sequence and regenerate the figures.

Usage:  python scripts/run_experiments.py [--quick]

Each experiment starts its own 5-node cluster on the ports in
config/cluster.json, so nothing else may be using ports 8001-8005.
All outputs (CSVs, *_summary.csv, fig*.png and a console log per step) go to
results/. --quick uses much smaller workloads (a smoke test, not for the report).
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "results"

FULL = [
    ("partition_test", ["experiments/partition_test.py"]),
    ("latency", ["experiments/latency.py"]),
    ("stale_reads", ["experiments/stale_reads.py", "--trials", "5"]),
    ("stale_reads_wide", ["experiments/stale_reads.py", "--name", "stale_reads_wide",
                          "--delay", "5", "200", "--trials", "3", "--writes", "60"]),
    ("availability", ["experiments/availability.py"]),
    ("concurrent_writes", ["experiments/concurrent_writes.py"]),
    ("concurrent_writes_noryw", ["experiments/concurrent_writes.py", "--no-ryw",
                                 "--policy", "siblings"]),
    ("plot", ["experiments/plot.py"]),
]

QUICK = [
    ("partition_test", ["experiments/partition_test.py"]),
    ("latency", ["experiments/latency.py", "--ops", "40"]),
    ("stale_reads", ["experiments/stale_reads.py", "--trials", "2", "--writes", "20"]),
    ("availability", ["experiments/availability.py", "--ops", "8"]),
    ("concurrent_writes", ["experiments/concurrent_writes.py", "--rounds", "4", "--trials", "1"]),
    ("plot", ["experiments/plot.py"]),
]


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--quick", action="store_true")
    args = p.parse_args()
    RESULTS.mkdir(exist_ok=True)
    failed = []
    for name, cmd in (QUICK if args.quick else FULL):
        print(f"=== {name} ===", flush=True)
        start = time.monotonic()
        with (RESULTS / f"log_{name}.txt").open("w", encoding="utf-8") as log:
            proc = subprocess.run([sys.executable, *cmd], cwd=ROOT, stdout=subprocess.PIPE,
                                  stderr=subprocess.STDOUT, text=True, encoding="utf-8")
            log.write(proc.stdout)
        print(proc.stdout.rstrip())
        print(f"--- {name}: exit {proc.returncode} in {time.monotonic() - start:.0f}s\n",
              flush=True)
        if proc.returncode != 0:
            failed.append(name)
    print("ALL OK" if not failed else f"FAILED: {', '.join(failed)}")
    raise SystemExit(1 if failed else 0)


if __name__ == "__main__":
    main()
