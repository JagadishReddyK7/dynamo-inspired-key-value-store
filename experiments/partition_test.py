"""Partition -> concurrent updates -> heal -> verify the declared resolution rule.
Automated PASS/FAIL check (exit code 1 if any check fails).

Scenario for key K with replicas [a, b, c] (from the ring) and a 2|1 split:
  1. write K = v0 (W=3) so every replica starts from the same version
  2. partition: side1 = {a, b, + the other nodes}, side2 = {c}
     (POST /admin/partition on EVERY node so the cut is symmetric)
  3. on side2 (client2 -> node c):
       - R=2 reads and W=2 writes FAIL -> the minority side is unavailable
         for quorum operations
       - read K (R=1), write K = v2 with that context, W=1
     on side1 (client1 -> node a):
       - read K (R=2) succeeds, write K = v1 with that context, W=2 succeeds
         -> the majority side stays available
     (client1 and client2 have different client_ids)
  4. heal (POST /admin/heal on every node)
  5. read K with R=3 and check the declared rule:
       policy "siblings": exactly 2 versions {v1, v2}, clocks CONCURRENT,
                          then client reconciles -> next read returns 1 merged version
       policy "lww":      exactly 1 version, equal to the higher (timestamp_ms, coordinator)
  6. after read repair, /admin/state on a, b, c must all hold the same version set

Note on step 3: a write that fails its quorum may still be applied on the
replicas it did reach (here: on c itself), exactly as in Dynamo. Client2's
later v2 write is made with a context that covers it, so it is superseded.

Usage:  python experiments/partition_test.py [--policy both|siblings|lww] [--no-spawn]
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from client.client import KVClient, QuorumError, make_client_id  # noqa: E402
from experiments.common import base_parser, cluster, config_variant  # noqa: E402
from kvstore.vclock import VectorClock  # noqa: E402


class Checks:
    def __init__(self, title: str):
        self.title = title
        self.failed = 0

    def check(self, ok: bool, what: str, detail: str = "") -> bool:
        print(f"  [{'PASS' if ok else 'FAIL'}] {what}" + (f"  ({detail})" if detail and not ok else ""))
        self.failed += not ok
        return ok


async def expect_quorum_error(coro) -> bool:
    try:
        await coro
    except QuorumError:
        return True
    return False


def version_set(versions) -> set:
    """Comparable representation of a list of Version dicts."""
    return {(repr(v["value"]), tuple(sorted(v["clock"].items()))) for v in versions}


async def replica_sets(c, key, replicas):
    states = await asyncio.gather(*(c.state(n) for n in replicas))
    return [version_set(s["store"].get(key, [])) for s in states]


async def converged(c, key, replicas, timeout_s=3.0) -> bool:
    loop = asyncio.get_running_loop()
    end = loop.time() + timeout_s
    while True:
        sets = await replica_sets(c, key, replicas)
        if all(s == sets[0] for s in sets) and sets[0]:
            return True
        if loop.time() > end:
            return False
        await asyncio.sleep(0.1)


async def scenario(args, policy: str) -> int:
    cfg_path = config_variant(args.config, conflict_policy=policy)
    t = Checks(policy)
    print(f"\n=== policy: {policy} ===")
    async with cluster(cfg_path, spawn=not args.no_spawn) as c:
        state = await c.state(next(iter(c.urls)))
        if state.get("conflict_policy") != policy:
            print(f"  SKIP: the running cluster uses {state.get('conflict_policy')!r}")
            return 0
        key = f"partition-{policy}-{make_client_id('k')}"
        a, b, cc = c.replicas(key)
        side1 = [n for n in c.urls if n != cc]
        print(f"  key {key!r}: replicas a={a} b={b} c={cc}; side1={side1} side2=[{cc}]")

        # 1. common starting point
        async with KVClient(c.urls[a], make_client_id("c0"), w=3) as c0:
            await c0.put(key, "v0")

        # 2. symmetric partition
        await c.partition(side1, [cc])

        async with KVClient(c.urls[a], make_client_id("c1")) as client1, \
                KVClient(c.urls[cc], make_client_id("c2")) as client2:
            # 3a. minority side: quorum operations fail
            t.check(await expect_quorum_error(client2.get(key, r=2)), "side2: R=2 read fails")
            t.check(await expect_quorum_error(client2.put(key, "rejected", w=2)),
                    "side2: W=2 write fails")
            # 3b. minority side: R=1 / W=1 still work
            res2 = await client2.get(key, r=1)
            v2 = (await client2.put(key, "v2", context=res2.context, w=1))["version"]
            t.check(True, "side2: R=1 read + W=1 write of v2 succeed")
            # 3c. majority side stays available with quorums
            res1 = await client1.get(key, r=2)
            t.check(res1.values == ["v0"], "side1: R=2 read succeeds and sees v0",
                    f"got {res1.values}")
            v1 = (await client1.put(key, "v1", context=res1.context, w=2))["version"]
            t.check(True, "side1: W=2 write of v1 succeeds")

        # 4. heal
        await c.heal()

        # 5. verify the declared rule
        async with KVClient(c.urls[a], make_client_id("c3"), r=3, w=3) as client3:
            res = await client3.get(key)
            if policy == "siblings":
                t.check(sorted(res.values) == ["v1", "v2"],
                        "R=3 read returns exactly 2 siblings {v1, v2}", f"got {res.values}")
                if len(res.versions) == 2:
                    k1, k2 = (VectorClock.from_dict(v["clock"]) for v in res.versions)
                    t.check(k1.concurrent_with(k2), "sibling clocks are concurrent",
                            f"{k1} vs {k2}")
            else:
                winner = max((v1, v2), key=lambda v: (v["timestamp_ms"], v["coordinator"]))
                t.check(res.values == [winner["value"]],
                        f"R=3 read returns only the LWW winner {winner['value']!r}",
                        f"got {res.values}")
            expected_ctx = VectorClock.from_dict(v1["clock"]).merge(
                VectorClock.from_dict(v2["clock"])).to_dict()
            t.check(res.context == expected_ctx, "read context covers both writes",
                    f"{res.context} != {expected_ctx}")

            # 6. read repair converges the replicas
            t.check(await converged(c, key, [a, b, cc]),
                    "after read repair a, b, c hold the same version set",
                    str(await replica_sets(c, key, [a, b, cc])))

            if policy == "siblings":
                merged = await client3.reconcile(key, lambda vals: "+".join(sorted(vals)))
                after = await client3.get(key)
                t.check(merged.values == ["v1+v2"] and after.values == ["v1+v2"],
                        "client reconciliation leaves 1 merged version", f"got {after.values}")
                t.check(await converged(c, key, [a, b, cc]),
                        "all replicas hold only the merged version")
            else:
                # the next write, made with the read context, supersedes both siblings
                await client3.put(key, "v3", context=res.context)
                after = await client3.get(key)
                t.check(after.values == ["v3"] and await converged(c, key, [a, b, cc]),
                        "a write with the read context supersedes both versions everywhere",
                        f"got {after.values}")

    print(f"  -> {policy}: {'PASS' if t.failed == 0 else f'FAIL ({t.failed} checks)'}")
    return t.failed


async def run(args) -> int:
    policies = ["siblings", "lww"] if args.policy == "both" else [args.policy]
    failed = 0
    for policy in policies:
        failed += await scenario(args, policy)
    return failed


def main() -> None:
    p = base_parser(__doc__.splitlines()[0])
    p.add_argument("--policy", choices=["siblings", "lww", "both"], default="both")
    args = p.parse_args()
    failed = asyncio.run(run(args))
    print("\nOVERALL:", "PASS" if failed == 0 else f"FAIL ({failed} failed checks)")
    raise SystemExit(1 if failed else 0)


if __name__ == "__main__":
    main()
