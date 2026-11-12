"""Start every node in the cluster config as a separate process. Ctrl+C stops all.

Usage:  python scripts/run_cluster.py [--config config/cluster.json]
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from kvstore.config import DEFAULT_CONFIG, load_config  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    args = parser.parse_args()
    cfg = load_config(args.config)

    procs = []
    for node in cfg.nodes:
        cmd = [sys.executable, "-m", "kvstore.server", "--node-id", node.id, "--config", args.config]
        procs.append(subprocess.Popen(cmd, cwd=ROOT))
        print(f"started {node.id} on {node.url} (pid {procs[-1].pid})")

    try:
        for p in procs:
            p.wait()
    except KeyboardInterrupt:
        print("stopping cluster...")
    finally:
        for p in procs:
            if p.poll() is None:
                p.terminate()
        for p in procs:
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()


if __name__ == "__main__":
    main()
