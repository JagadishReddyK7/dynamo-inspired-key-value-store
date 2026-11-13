"""Read repair: a GET pushes reconciled versions to replicas that miss them."""

from __future__ import annotations

from kvstore.storage import Version
from kvstore.vclock import VectorClock
from test_cluster import eventually, get, put


async def test_partition_then_heal_read_repair_converges(cluster):
    key = "k"
    a, b, c = cluster.replicas(key)
    side1 = [n for n in cluster.urls if n != c]
    _, base = await put(cluster, a, key, "v0", w=3, client_id="c0")
    ctx = base["version"]["clock"]
    cluster.partition(side1, [c])
    assert (await put(cluster, a, key, "v1", w=2, context=ctx, client_id="c1"))[0] == 200
    assert (await put(cluster, c, key, "v2", w=1, context=ctx, client_id="c2"))[0] == 200
    assert (await put(cluster, c, key, "nope", w=2, context=ctx, client_id="c3"))[0] == 503
    cluster.heal()
    status, res = await get(cluster, a, key, r=3)
    assert status == 200
    # The failed W=2 write was still applied on c's own replica (Dynamo semantics:
    # a failed write is "maybe applied"), and it is concurrent with v1 and v2.
    assert sorted(v["value"] for v in res["versions"]) == ["nope", "v1", "v2"]
    # every replica ends with the same sibling set thanks to read repair
    assert await eventually(lambda: cluster.values_at(a, key) == cluster.values_at(b, key)
                            == cluster.values_at(c, key))
    assert '"v1"' in cluster.values_at(c, key) and '"v2"' in cluster.values_at(a, key)


async def test_read_with_r1_still_repairs_late_replicas(cluster):
    key = "k"
    a, b, c = cluster.replicas(key)
    # Plant a newer version only on replica a.
    cluster.store(a).put(key, Version("new", VectorClock({"c1": 2}), 1, a))
    cluster.store(b).put(key, Version("old", VectorClock({"c1": 1}), 0, a))
    status, _ = await get(cluster, cluster.non_replica(key), key, r=1)
    assert status == 200
    assert await eventually(lambda: all(cluster.values_at(n, key) == ['"new"'] for n in (a, b, c)))


async def test_read_repair_sends_only_to_replicas_missing_versions(cluster):
    key = "k"
    a, b, c = cluster.replicas(key)
    coord = cluster.coordinator(a)
    v1 = Version("v1", VectorClock({"c1": 1}), 1, a)
    v2 = Version("v2", VectorClock({"c2": 1}), 2, a)
    sent = []
    original = coord.shim.send

    async def spy(node, method, path, body=None, timeout_s=1.0):
        sent.append((node, [v["value"] for v in body["versions"]]))
        return await original(node, method, path, body, timeout_s)

    coord.shim.send = spy
    await coord.read_repair(key, [v1, v2], {a: [v1, v2], b: [v1], c: []})
    assert sorted(sent) == [(b, ["v2"]), (c, ["v1", "v2"])]
    assert cluster.values_at(c, key) == ['"v1"', '"v2"']
