"""Client library against the in-process cluster, including the three client rules."""

from __future__ import annotations

import asyncio
import csv

import pytest

from client.client import KVClient, NodeUnavailable, QuorumError, make_client_id
from experiments.oplog import OpLog


async def test_client_ids_are_unique_per_run():
    assert make_client_id("c") != make_client_id("c")
    assert KVClient("http://x").client_id != KVClient("http://x").client_id


async def test_blind_writes_from_one_client_are_not_lost(cluster):
    """Client rule 3: the second blind write must supersede the first."""
    async with KVClient(cluster.urls["n1"], r=3, w=3) as kv:
        await kv.put("k", "first")
        reply = await kv.put("k", "second")
        assert reply["version"]["clock"] == {kv.client_id: 2}
        res = await kv.get("k")
        assert res.values == ["second"] and not res.has_conflict


async def test_read_modify_write_and_reconcile(cluster):
    async with KVClient(cluster.urls["n1"], r=3, w=3) as alice, \
            KVClient(cluster.urls["n2"], r=3, w=3) as bob, \
            KVClient(cluster.urls["n3"], r=3, w=3) as carol:
        await alice.put("cart", ["bread"])
        a_ctx = (await alice.get("cart")).context
        b_ctx = (await bob.get("cart")).context
        await alice.put("cart", ["bread", "milk"], context=a_ctx)
        await bob.put("cart", ["bread", "eggs"], context=b_ctx)

        res = await carol.get("cart")
        assert res.has_conflict
        assert sorted(map(tuple, res.values)) == [("bread", "eggs"), ("bread", "milk")]

        merged = await carol.reconcile("cart", lambda vals: sorted(set().union(*vals)))
        assert merged.values == [["bread", "eggs", "milk"]]
        assert (await alice.get("cart")).values == [["bread", "eggs", "milk"]]


async def test_quorum_error_and_unavailable_node(cluster):
    key = "k"
    coord = cluster.non_replica(key)
    cluster.partition([coord], cluster.replicas(key)[1:])
    async with KVClient(cluster.urls[coord]) as kv:
        await kv.put(key, "ok", w=1)
        with pytest.raises(QuorumError) as err:
            await kv.put(key, "fail", w=3)
        assert err.value.required == 3 and err.value.received in (0, 1)
        with pytest.raises(QuorumError):
            await kv.get(key, r=2)
    async with KVClient("http://127.0.0.1:9", timeout_s=0.5) as kv:
        with pytest.raises(NodeUnavailable):
            await kv.get(key)


async def test_writes_from_one_client_are_serialised(cluster):
    """Client rule 1: concurrent put() calls on one client never overlap."""
    async with KVClient(cluster.urls["n1"], w=3) as kv:
        await asyncio.gather(*(kv.put("k", i) for i in range(5)))
        res = await kv.get("k", r=3)
        assert len(res.versions) == 1 and res.context == {kv.client_id: 5}


async def test_operations_are_logged(cluster, tmp_path):
    path = tmp_path / "ops.csv"
    with OpLog(path) as log:
        async with KVClient(list(cluster.urls.values()), r=2, w=2,
                            oplog=log, experiment="unit") as kv:
            await kv.put("k", {"n": 1})
            await kv.get("k")
            await kv.get("k", r=3)
    rows = list(csv.DictReader(path.open(encoding="utf-8")))
    assert [r["op"] for r in rows] == ["put", "get", "get"]
    assert all(r["ok"] == "1" and r["experiment"] == "unit" for r in rows)
    assert rows[0]["w"] == "2" and rows[0]["r"] == ""
    assert rows[2]["r"] == "3" and rows[2]["num_versions"] == "1"
    assert rows[1]["value"] == '[{"n": 1}]'
    assert float(rows[0]["end_ms"]) >= float(rows[0]["start_ms"])
