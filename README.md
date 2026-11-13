# Dynamo-Inspired Key-Value Store with Configurable Consistency

A replicated key-value store with client-configurable read and write quorums (R, W), N = 3 replicas per key, vector clocks for detecting concurrent writes, and an explicit conflict-resolution policy. Experiments compare latency, availability, stale-read frequency and returned versions under concurrent updates and network partitions.

The full contract between components is in [docs/API.md](docs/API.md). **Read it first.**

**Teammates:** your task checklist, the setup steps and the Git workflow are in [docs/TASKS.md](docs/TASKS.md).

## Setup (Windows, PowerShell)

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
pytest                         # unit tests
python scripts\run_cluster.py  # start 5 nodes on ports 8001-8005 (Ctrl+C to stop)
```

## Layout and ownership

| Path | What it is | Owner | Status |
|---|---|---|---|
| `kvstore/vclock.py` | Vector clocks (compare, merge, increment) | A | ✅ done + tested |
| `kvstore/storage.py` | Per-node sibling store, `reconcile`, `context_of` | A | ✅ done + tested |
| `kvstore/policies.py` | `siblings` / `lww` resolution | A | ✅ done + tested |
| `kvstore/ring.py` | Consistent hashing, preference lists | B | ✅ done + tested |
| `kvstore/config.py`, `config/cluster.json` | Static cluster config | B | ✅ done |
| `scripts/run_cluster.py` | Launches all nodes as processes | B | ✅ done |
| `kvstore/network_shim.py` | Partition and delay injection for all node-to-node traffic | B | ✅ done + tested |
| `kvstore/coordinator.py` | Quorum GET/PUT, background replication; read repair (A) | B / A | ✅ done + tested |
| `kvstore/server.py` | aiohttp routes (client, internal, admin) | B | ✅ done + tested |
| `client/client.py` | Client library with R/W settings, context, reconcile | C | ✅ done + tested |
| `experiments/oplog.py` | Shared CSV op-log format | C | ✅ done |
| `experiments/common.py` | Start/stop a real cluster, admin helpers | C | ✅ done |
| `experiments/{latency,stale_reads,availability,concurrent_writes}.py` | Experiments | C | ✅ done |
| `experiments/partition_test.py` | Partition, update both sides, heal, verify the rule | A | ✅ done (PASS/FAIL) |
| `experiments/plot.py` | Report figures | C | ✅ done |

## Running the experiments

Each script starts its own 5-node cluster (pass `--no-spawn` to use one already started with `scripts\run_cluster.py`) and writes CSVs to `results/`.

```powershell
python experiments\partition_test.py      # PASS/FAIL for both policies (siblings, lww)
python experiments\latency.py             # p50/p95/p99 per (R, W)
python experiments\stale_reads.py         # stale-read % per (R, W)
python experiments\availability.py        # success rate with 0/1/2 replicas unreachable
python experiments\concurrent_writes.py   # versions returned + lost updates, siblings vs lww
python experiments\plot.py                # results\fig1..fig5 *.png
```

Or run everything (about 15 minutes, logs to `results\log_*.txt`):

```powershell
python scripts\run_experiments.py         # --quick for a 1-minute smoke test
```

Each module's docstring describes its algorithm.

## Schedule (15 days)

| Days | Milestone |
|---|---|
| 1–2 | Review and freeze `docs/API.md`, set up the environment |
| 3–5 | Server routes and shim (B), read repair (A), client (C) |
| 6–8 | Coordinator quorum logic (B), partition test (A), op-log in client and latency script (C) |
| **9** | **Integration: end-to-end on 5 nodes (hard deadline)** |
| 10–12 | Experiments and plots. Optional: sloppy quorum / anti-entropy, only if day 9 was met |
| 13–14 | Report |
| 15 | Buffer and demo rehearsal |
