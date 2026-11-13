# API Specification (frozen on day 2; changes need agreement from all three of us)

All bodies are JSON. A **Version** object is:

```json
{"value": <any JSON>, "clock": {"c1": 2, "c2": 1}, "timestamp_ms": 1731400000000, "coordinator": "n2"}
```

A **context** is a vector clock as a JSON object (`{"c1": 2, "c2": 1}`). Clients treat it as opaque: take it from a GET and send it back unchanged on the next PUT.

---

## 1. Client API (send to any node; that node coordinates the request)

### `GET /kv/{key}?r=<1..N>`

`r` is optional and defaults to `quorum.default_r` from the config.

**200** (a missing key returns `versions: []` and `context: {}`):
```json
{"key": "cart", "versions": [<Version>, ...], "context": {"c1": 2, "c2": 1}, "coordinator": "n2", "policy": "siblings"}
```
- More than one entry in `versions` means **siblings** (concurrent versions). The client must reconcile them.
- Under the `lww` policy, `versions` always has at most one entry.
- `context` is the merge of **all** sibling clocks, computed before the policy is applied.
- `policy` is the cluster's conflict policy; the client needs it to apply read-your-writes (client rule 4).

**503**: `{"error": "quorum_not_met", "required": 2, "received": 1}`. The coordinator fails as soon as the quorum becomes impossible, so `received` counts the replies that had arrived by then.
**400**: `{"error": "bad_request", "detail": "..."}` (for example `r` out of range)

### `PUT /kv/{key}?w=<1..N>`

Request body:
```json
{"value": <any JSON>, "context": {"c1": 2}, "client_id": "c7"}
```
- `context` may be `{}` for a blind write.
- The coordinator computes the new clock as `context` with `client_id` incremented.

### Client rules (required for correctness; enforced by `client/client.py`)

1. **One `client_id` = one sequential actor.** A `client_id` never has two writes in flight at once. Every concurrent asyncio task in an experiment gets its **own** `client_id`.
2. **`client_id` is unique per process run**, e.g. `f"c{n}-{uuid4().hex[:6]}"`. Clocks are in memory only, so a restarted client that reused an old ID would restart its counter at 1.
3. **The client tracks its own clock per key.** It merges the clock of every version it receives (from GET contexts and PUT responses) into `own_clock[key]`. On every PUT it sends `merge(context_given_by_caller, own_clock[key])`.

   *Why:* without rule 3, two blind writes from `c1` both get the clock `{c1:1}`. The replica treats the second as a duplicate of the first and **silently drops it** (see `test_blind_writes_without_client_tracking_are_lost` in `tests/test_storage.py`). Merging in the client's own clock makes each write by the same client supersede its previous one, which is correct because that client's writes are sequential.
4. **Read-your-writes.** The client keeps the version returned by its last PUT per key. If a GET's context does not descend that version's clock (the read missed the client's own write, possible when R + W <= N), the client adds that version to the result: as a sibling under `siblings` (dropping the versions it supersedes), or as the LWW candidate under `lww`.

   *Why:* rule 3 makes the client's next write supersede its previous one. If that next write is a read-modify-write built on a read that missed the previous write, the previous write's content is silently lost. Measured before this rule: 1 of 60 acknowledged items lost with R = W = 1 in `experiments/concurrent_writes.py`.

**200**: `{"key": "cart", "version": <Version>, "acks": 2}`
**503 / 400**: same formats as for GET.

---

## 2. Internal replica API (node to node, always sent through `NetworkShim`)

Every request carries the header `X-From-Node: <node_id>`. If the receiver has that sender blocked, it replies **503** `{"error": "partitioned"}`.

| Method | Path | Body | Reply |
|---|---|---|---|
| `GET` | `/internal/replica/{key}` | none | `{"versions": [<Version>...]}` (local siblings) |
| `PUT` | `/internal/replica/{key}` | `{"versions": [<Version>...]}` | `{"ok": true, "changed": bool}` |

Both replication writes and read repair use `PUT /internal/replica`. The replica applies each version with `LocalStore.put_many`.

---

## 3. Admin / fault-injection API (used by experiment scripts)

| Method | Path | Body | Effect |
|---|---|---|---|
| `POST` | `/admin/partition` | `{"blocked": ["n4", "n5"]}` | This node drops all traffic to and from the listed nodes |
| `POST` | `/admin/heal` | none | Clears this node's blocked set |
| `POST` | `/admin/delay` | `{"min_ms": 5, "max_ms": 50}` | Each outgoing internal message sleeps for a uniform random time in this range |
| `GET` | `/admin/state` | none | `{"node": "n1", "blocked": [...], "delay": {...}, "conflict_policy": "siblings", "store": {key: [Version...]}}` |
| `GET` | `/health` | none | `{"node": "n1", "ok": true}` |

**Delay applies to the coordinator's own replica too.** When the coordinator is itself one of the key's replicas, its local read or write goes through the shim with the same random delay. Otherwise the local copy always answers first, and results for W=1 and R=1 would mostly measure "which node did the client contact".

**Partition convention:** to split the cluster into sides A and B, send `/admin/partition` to **every** node, with `blocked` set to the nodes on the other side. This keeps the cut symmetric. Clients pick which side they are on by choosing which node they contact.

---

## 4. Decisions recorded

| Topic | Decision |
|---|---|
| Replicas per key | N = 3, the first 3 distinct nodes clockwise on the consistent-hash ring (64 virtual nodes per node) |
| Membership | Static, from `config/cluster.json` (no gossip) |
| Quorum | Strict: only the N replicas in the preference list count. Sloppy quorum + hinted handoff is an optional extension. |
| Replication | The coordinator sends to all N replicas in parallel and replies after W acks. The other sends keep running in the background. |
| Vector clock actor | **Client ID** (avoids lost updates that node-ID clocks allow) |
| Conflict policy | Config `conflict_policy`: `siblings` (default, client reconciles) or `lww` (`max(timestamp_ms, coordinator)`) |
| Convergence | Read repair on every GET (after waiting, within the timeout, for late replies too). Anti-entropy is optional. |
| Failed writes | A PUT that misses its quorum may still be applied on the replicas it reached (as in Dynamo). |
| Stale read | A read that misses the latest write *acknowledged before the read was sent* |
| Clock tracking | The client merges every clock it sees into a per-key `own_clock` and sends it on each PUT (client rule 3) |
| Out of scope | Deletes and tombstones, persistence to disk, dynamic membership. State these in the report as deliberate limits. |
| LWW timestamps | Wall-clock time from the coordinator. All nodes share one clock on localhost; on real machines clock skew would make LWW pick the "wrong" winner. Mention this in the report. |
