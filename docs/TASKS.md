# Team Tasks

Who does what, in what order, and how we work together. Tick the boxes as you go (edit this file in your own pull requests).

**Before writing any code:** read [API.md](API.md) and the docstrings in your own files. Each stub's docstring describes the algorithm to implement.

---

## Getting started (everyone)

```powershell
git clone https://github.com/<owner>/dynamo-inspired-key-value-store.git
cd dynamo-inspired-key-value-store
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
pytest                      # should show 28 passed
```

If `Activate.ps1` is blocked, run this once: `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`

---

## Shared milestones

- [ ] **Day 2:** all three review and **freeze** `docs/API.md`. After that, any change to it needs agreement from everyone.
- [ ] **Day 9:** **integration, a hard deadline.** 5 nodes running, client get/put working end to end. If it slips, drop all optional work.
- [ ] **Days 13–14:** report written together.
- [ ] **Day 15:** buffer and demo rehearsal.

---

## Person B: cluster and networking

**Start first: A and C are blocked until the server routes exist.**

- [ ] **Days 3–4:** `kvstore/network_shim.py`: implement `send()`
  - blocked peer → raise `PeerUnreachable`
  - random delay `uniform(delay_min_ms, delay_max_ms)`, **including requests to yourself**
  - HTTP call with header `X-From-Node`, honouring the timeout
  - any connection error, timeout or non-2xx reply → `PeerUnreachable`
- [ ] **Days 3–4:** `kvstore/server.py`
  - build `LocalStore`, `ConsistentHashRing`, `NetworkShim` and `Coordinator`, and store them with `web.AppKey`
  - `/internal/replica/{key}` GET and PUT (reply 503 if the sender is blocked)
  - `/admin/partition`, `/admin/heal`, `/admin/delay`, `/admin/state`
  - 👉 **merge this early**, even before the coordinator is done
- [ ] **Days 5–8:** `kvstore/coordinator.py`: `put()` and `get()`
  - send to all N replicas in parallel; return after W or R replies
  - **do not cancel** the remaining sends (background replication)
  - `QuorumNotMet` on timeout
  - then wire up the `/kv/{key}` GET and PUT routes (503 on `QuorumNotMet`, 400 on bad R or W)
- [ ] Add tests: an in-process test with 3 apps using `aiohttp.test_utils`, or a test against a running cluster
- [ ] **Days 10–12, optional, only if day 9 was met:** sloppy quorum + hinted handoff, or anti-entropy
- [ ] Report: architecture section (ring, coordinator, shim)

## Person A: data model and correctness

- [x] `vclock.py`, `storage.py`, `policies.py`, with tests (done in the scaffold)
- [ ] **Days 3–5:** `read_repair()` in `kvstore/coordinator.py`
  - push the reconciled versions to every replica whose reply was missing any of them
  - runs in the background and must not delay the GET reply
  - B also edits this file, so coordinate with B: separate branches, merge often
- [ ] **Days 6–8:** `experiments/partition_test.py`: automated PASS/FAIL (steps are in its docstring)
  - both policies: `siblings` → 2 concurrent versions and then a reconciled one; `lww` → the expected winner
  - minority side: W=2 and R=2 **fail**; majority side: W=2 **succeeds**
  - after read repair, all 3 replicas hold the same versions (check with `/admin/state`)
- [ ] Review B's coordinator pull requests for clock and context handling
- [ ] Report: versioning section (why client-ID clocks, why client rule 3, results of the partition test)

## Person C: client and experiments

- [ ] **Days 3–5:** `client/client.py`: `get`, `put`, `reconcile`
  - follow **all three client rules** in `docs/API.md`, especially **per-key own-clock tracking**
  - until B's routes exist, test against a small fake aiohttp server that returns the JSON shapes from API.md
- [ ] **Days 6–8:** log every client call to `experiments/oplog.py`, then write `experiments/latency.py` (p50/p95/p99 per R/W)
- [ ] **Days 10–12:** `experiments/stale_reads.py`: random coordinator per request, one `client_id` per task
- [ ] **Days 10–12:** `experiments/availability.py`: success rate with 0, 1 and 2 replicas unreachable
- [ ] **Days 10–12:** `experiments/plot.py`: the 4 figures listed in its docstring, saved to `results/`
- [ ] Report: experiments and results section, with figures

---

## Git workflow

```powershell
git checkout main; git pull
git checkout -b b/network-shim        # <your letter>/<task>
# ...work...
pytest                                # must pass before you push
git add .; git commit -m "Implement NetworkShim.send"
git push -u origin b/network-shim     # then open a pull request on GitHub
```

- One branch per task. Never push directly to `main`.
- Each pull request is reviewed by one other person before merging. A and B review each other's changes to `coordinator.py`.
- Pull `main` daily so merge conflicts stay small.
- A change to `docs/API.md` must be discussed with everyone first.
