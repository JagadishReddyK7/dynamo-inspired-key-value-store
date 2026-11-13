"""Plots for the report, generated from the OpLog CSVs in results/.

Figures (each skipped if its input CSV is missing):
  1. latency p50/p99 per (R, W) - grouped bar chart     <- latency.csv
  2. stale-read % per (R, W), coloured by R + W > N      <- stale_reads.csv
  3. availability (success rate) per (R, W), 0/1/2 down  <- availability.csv
  4. versions returned per read under concurrent writes  <- concurrent_writes.csv

Usage:  python experiments/plot.py [--out results]
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import matplotlib  # noqa: E402

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from experiments.common import RESULTS, base_parser  # noqa: E402
from experiments.latency import summarise as latency_summary  # noqa: E402
from experiments.stale_reads import _analyse_df as stale_summary  # noqa: E402
from experiments.availability import summarise as availability_summary  # noqa: E402

N = 3
BLUE, ORANGE, GREEN, GREY = "#3b6fb6", "#e08a2c", "#4a9a5b", "#9a9a9a"


def _label(r, w) -> str:
    return f"R={r}\nW={w}"


def plot_latency(src: Path, dst: Path) -> None:
    t = latency_summary(src)
    fig, axes = plt.subplots(1, 2, figsize=(11, 4), sharey=True)
    for ax, op in zip(axes, ("get", "put")):
        d = t[t["op"] == op].sort_values(["r", "w"])
        x = np.arange(len(d))
        ax.bar(x - 0.2, d["p50"], 0.4, label="p50", color=BLUE)
        ax.bar(x + 0.2, d["p99"], 0.4, label="p99", color=ORANGE)
        ax.set_xticks(x, [_label(r, w) for r, w in zip(d["r"], d["w"])])
        ax.set_title(f"{op.upper()} latency")
        ax.grid(axis="y", alpha=0.3)
    axes[0].set_ylabel("latency (ms)")
    axes[0].legend()
    fig.tight_layout()
    fig.savefig(dst, dpi=150)
    plt.close(fig)


def plot_stale(src: Path, dst: Path) -> None:
    t = stale_summary(src).reset_index(drop=True)
    fig, ax = plt.subplots(figsize=(7, 4))
    colours = [GREEN if s else ORANGE for s in t["strict_quorum"]]
    pct = t["stale_fraction"] * 100
    ax.bar(range(len(t)), pct, color=colours)
    for i, v in enumerate(pct):
        ax.text(i, v, f"{v:.1f}%", ha="center", va="bottom", fontsize=8)
    ax.set_xticks(range(len(t)), [_label(r, w) for r, w in zip(t["r"], t["w"])])
    ax.set_ylabel("stale reads (%)")
    ax.set_title("Stale-read frequency (N = 3)")
    ax.legend(handles=[plt.Rectangle((0, 0), 1, 1, color=GREEN),
                       plt.Rectangle((0, 0), 1, 1, color=ORANGE)],
              labels=["R + W > N", "R + W ≤ N"])
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    fig.savefig(dst, dpi=150)
    plt.close(fig)


def plot_availability(src: Path, dst: Path) -> None:
    t = availability_summary(src)
    fig, axes = plt.subplots(1, 2, figsize=(11, 4), sharey=True)
    for ax, op in zip(axes, ("get", "put")):
        d = t[t["op"] == op]
        settings = sorted({(r, w) for r, w in zip(d["r"], d["w"])})
        x = np.arange(len(settings))
        for i, (down, colour) in enumerate(zip((0, 1, 2), (GREEN, BLUE, ORANGE))):
            rate = [d[(d["down"] == down) & (d["r"] == r) & (d["w"] == w)]["success_rate"]
                    .mean() * 100 for r, w in settings]
            ax.bar(x + (i - 1) * 0.27, rate, 0.27, color=colour,
                   label=f"{down} replica(s) unreachable")
        ax.set_xticks(x, [_label(r, w) for r, w in settings])
        ax.set_title(f"{op.upper()} availability")
        ax.grid(axis="y", alpha=0.3)
    axes[0].set_ylabel("success rate (%)")
    axes[0].set_ylim(0, 105)
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="lower center", ncol=3, fontsize=9, frameon=False)
    fig.tight_layout(rect=(0, 0.08, 1, 1))
    fig.savefig(dst, dpi=150)
    plt.close(fig)


def plot_versions(src: Path, dst: Path) -> None:
    df = pd.read_csv(src, keep_default_na=False)
    gets = df[(df["op"] == "get") & (df["ok"] == 1)].copy()
    gets["num_versions"] = gets["num_versions"].astype(int)
    gets["policy"] = gets["experiment"].str.extract(r"concurrent_(\w+?)_r")[0]
    gets = gets[gets["num_versions"] > 0]
    fig, ax = plt.subplots(figsize=(7, 4))
    top = max(1, int(gets["num_versions"].max())) if len(gets) else 1
    bins = np.arange(1, top + 2)
    for policy, colour in (("siblings", BLUE), ("lww", ORANGE)):
        sub = gets[gets["policy"] == policy]["num_versions"]
        if len(sub):
            counts = np.array([(sub == b).sum() for b in bins[:-1]]) / len(sub) * 100
            offset = -0.2 if policy == "siblings" else 0.2
            ax.bar(bins[:-1] + offset, counts, 0.4, color=colour, label=policy)
    ax.set_xticks(bins[:-1])
    ax.set_xlabel("versions returned by one read")
    ax.set_ylabel("share of reads (%)")
    ax.set_title("Versions returned under concurrent updates")
    ax.legend(title="policy")
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    fig.savefig(dst, dpi=150)
    plt.close(fig)


FIGURES = [
    ("latency.csv", "fig1_latency.png", plot_latency),
    ("stale_reads.csv", "fig2_stale_reads.png", plot_stale),
    ("availability.csv", "fig3_availability.png", plot_availability),
    ("concurrent_writes.csv", "fig4_versions.png", plot_versions),
]


def main() -> None:
    p = base_parser(__doc__.splitlines()[0])
    args = p.parse_args()
    out = Path(args.out or RESULTS)
    for src_name, dst_name, fn in FIGURES:
        src = out / src_name
        if not src.exists():
            print(f"skip {dst_name}: {src} not found")
            continue
        fn(src, out / dst_name)
        print(f"wrote {out / dst_name}")


if __name__ == "__main__":
    main()
