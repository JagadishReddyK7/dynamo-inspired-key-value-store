"""End-to-end tests of the server, network shim and quorum coordinator,
using raw HTTP against an in-process 5-node cluster (see conftest.py)."""

from __future__ import annotations

import asyncio

from kvstore.network_shim import NetworkShim, PeerUnreachable


async def put(c, node, key, value, w=2, context=None, client_id="t1"):
    async with c.session.put(f"{c.urls[node]}/kv/{key}", params={"w": w},
                             json={"value": value, "context": context or {},
                                   "client_id": client_id}) as resp:
        return resp.status, await resp.json()


async def get(c, node, key, r=2):
    async with c.session.get(f"{c.urls[node]}/kv/{key}", params={"r": r}) as resp:
        return resp.status, await resp.json()


async def eventually(predicate, timeout=2.0):
    loop = asyncio.get_running_loop()
    end = loop.time() + timeout
    while loop.time() < end:
        if predicate():
            return True
        await asyncio.sleep(0.02)
    return predicate()


async def test_missing_key_returns_empty(cluster):
    status, body = await get(cluster, "n1", "nothing")
    assert status == 200
    assert body["versions"] == [] and body["context"] == {} and body["coordinator"] == "n1"


async def test_put_then_get_from_any_node(cluster):
    status, body = await put(cluster, "n1", "k", {"a": 1}, w=3)
    assert status == 200 and body["acks"] == 3
    assert body["version"]["clock"] == {"t1": 1}
    for node in cluster.urls:
        status, res = await get(cluster, node, "k", r=2)
        assert status == 200
        assert [v["value"] for v in res["versions"]] == [{"a": 1}]
        assert res["context"] == {"t1": 1}


async def test_only_preference_list_nodes_store_the_key(cluster):
    await put(cluster, "n1", "k", "v", w=3)
    reps = set(cluster.replicas("k"))
    for node in cluster.urls:
        assert bool(cluster.store(node).get("k")) == (node in reps)


async def test_background_replication_reaches_all_replicas(cluster):
    await put(cluster, cluster.non_replica("k"), "k", "v", w=1)
    assert await eventually(lambda: all(cluster.store(n).get("k") for n in cluster.replicas("k")))


async def test_bad_quorum_arguments_rejected(cluster):
    assert (await get(cluster, "n1", "k", r=0))[0] == 400
    assert (await get(cluster, "n1", "k", r=4))[0] == 400
    assert (await put(cluster, "n1", "k", "v", w=9))[0] == 400
    async with cluster.session.put(f"{cluster.urls['n1']}/kv/k", json={"value": 1}) as resp:
        assert resp.status == 400        # missing client_id


async def test_quorum_fails_when_replicas_partitioned(cluster):
    key = "k"
    a, b, c = cluster.replicas(key)
    coord = cluster.non_replica(key)
    await put(cluster, coord, key, "v0", w=3)
    cluster.partition([coord], [b, c])   # coordinator reaches only replica a
    assert (await put(cluster, coord, key, "v1", w=1))[0] == 200
    status, body = await put(cluster, coord, key, "v2", w=2)
    assert status == 503
    # Fails fast once the quorum is impossible, possibly before a's ack arrived.
    assert body["error"] == "quorum_not_met" and body["required"] == 2
    assert body["received"] in (0, 1)
    assert (await get(cluster, coord, key, r=1))[0] == 200
    assert (await get(cluster, coord, key, r=2))[0] == 503


async def test_internal_api_rejects_blocked_sender(cluster):
    cluster.shim("n2").set_partition(["n1"])
    async with cluster.session.get(f"{cluster.urls['n2']}/internal/replica/k",
                                   headers={"X-From-Node": "n1"}) as resp:
        assert resp.status == 503
        assert await resp.json() == {"error": "partitioned"}
    async with cluster.session.get(f"{cluster.urls['n2']}/internal/replica/k",
                                   headers={"X-From-Node": "n3"}) as resp:
        assert resp.status == 200


async def test_concurrent_writes_become_siblings_and_reconcile(cluster):
    key = "cart"
    _, base = await put(cluster, "n1", key, ["x"], w=3, client_id="c0")
    ctx = base["version"]["clock"]
    await put(cluster, "n1", key, ["x", "a"], w=3, context=ctx, client_id="c1")
    await put(cluster, "n2", key, ["x", "b"], w=3, context=ctx, client_id="c2")
    status, res = await get(cluster, "n3", key, r=3)
    assert status == 200
    assert sorted(map(tuple, (v["value"] for v in res["versions"]))) == [("x", "a"), ("x", "b")]
    assert res["context"] == {"c0": 1, "c1": 1, "c2": 1}
    await put(cluster, "n3", key, ["x", "a", "b"], w=3, context=res["context"], client_id="c3")
    _, res = await get(cluster, "n4", key, r=3)
    assert [v["value"] for v in res["versions"]] == [["x", "a", "b"]]


async def test_lww_returns_single_latest_version(lww_cluster):
    c = lww_cluster
    key = "k"
    _, base = await put(c, "n1", key, "v0", w=3, client_id="c0")
    ctx = base["version"]["clock"]
    _, first = await put(c, "n1", key, "first", w=3, context=ctx, client_id="c1")
    await asyncio.sleep(0.01)
    _, second = await put(c, "n2", key, "second", w=3, context=ctx, client_id="c2")
    _, res = await get(c, "n3", key, r=3)
    assert [v["value"] for v in res["versions"]] == ["second"]
    # context still covers BOTH concurrent clocks, so the next write supersedes both
    assert res["context"] == {"c0": 1, "c1": 1, "c2": 1}


async def test_admin_endpoints(cluster):
    url = cluster.urls["n1"]
    async with cluster.session.post(f"{url}/admin/partition", json={"blocked": ["n4", "n1"]}) as r:
        assert r.status == 200 and (await r.json())["blocked"] == ["n4"]
    async with cluster.session.post(f"{url}/admin/delay", json={"min_ms": 1, "max_ms": 3}) as r:
        assert r.status == 200
    async with cluster.session.post(f"{url}/admin/delay", json={"min_ms": 5, "max_ms": 1}) as r:
        assert r.status == 400
    await put(cluster, "n1", "k", "v", w=3)
    async with cluster.session.get(f"{url}/admin/state") as r:
        state = await r.json()
    assert state["node"] == "n1" and state["blocked"] == ["n4"]
    assert state["delay"] == {"min_ms": 1, "max_ms": 3}
    assert state["conflict_policy"] == "siblings"
    async with cluster.session.post(f"{url}/admin/heal") as r:
        assert (await r.json())["blocked"] == []


async def test_shim_blocks_and_delays_requests_to_self():
    calls = []

    async def local(method, path, body):
        calls.append((method, path))
        return {"ok": True}

    shim = NetworkShim("n1", {"n1": "http://unused"}, 30, 30, local_handler=local)
    loop = asyncio.get_running_loop()
    start = loop.time()
    assert await shim.send("n1", "GET", "/internal/replica/k") == {"ok": True}
    assert loop.time() - start >= 0.025          # self-requests get the delay too
    assert calls == [("GET", "/internal/replica/k")]
    shim.set_partition(["n2"])
    try:
        await shim.send("n2", "GET", "/x")
        raise AssertionError("expected PeerUnreachable")
    except PeerUnreachable:
        pass
    await shim.close()
