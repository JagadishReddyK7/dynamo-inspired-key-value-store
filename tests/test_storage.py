from kvstore.policies import resolve
from kvstore.storage import LocalStore, Version, context_of, reconcile
from kvstore.vclock import VectorClock


def ver(value, ts=0, node="n1", **clock):
    return Version(value, VectorClock(clock), timestamp_ms=ts, coordinator=node)


def test_newer_version_replaces_older():
    s = LocalStore()
    assert s.put("k", ver("a", c1=1))
    assert s.put("k", ver("b", c1=2))
    assert [v.value for v in s.get("k")] == ["b"]


def test_older_or_duplicate_version_is_ignored():
    s = LocalStore()
    s.put("k", ver("b", c1=2))
    assert not s.put("k", ver("a", c1=1))
    assert not s.put("k", ver("b", c1=2))
    assert [v.value for v in s.get("k")] == ["b"]


def test_concurrent_versions_kept_as_siblings():
    s = LocalStore()
    s.put("k", ver("left", c0=1, c1=1))
    s.put("k", ver("right", c0=1, c2=1))
    assert sorted(v.value for v in s.get("k")) == ["left", "right"]


def test_reconciling_write_collapses_siblings():
    s = LocalStore()
    a, b = ver("left", c0=1, c1=1), ver("right", c0=1, c2=1)
    s.put_many("k", [a, b])
    ctx = context_of(s.get("k"))
    s.put("k", Version("merged", ctx.increment("c3")))
    assert [v.value for v in s.get("k")] == ["merged"]


def test_blind_writes_without_client_tracking_are_lost():
    # Documents WHY the client must track its own clock (docs/API.md, client rule 3).
    s = LocalStore()
    s.put("k", Version("first", VectorClock().increment("c1")))
    s.put("k", Version("second", VectorClock().increment("c1")))   # same clock {c1:1}
    assert [v.value for v in s.get("k")] == ["first"]               # second write lost


def test_blind_writes_with_client_tracking_succeed():
    s = LocalStore()
    own = VectorClock()
    for value in ["first", "second", "third"]:
        clock = own.increment("c1")          # coordinator: merge(ctx, own).increment(c1)
        s.put("k", Version(value, clock))
        own = own.merge(clock)               # client: remember the clock from the PUT reply
    assert [v.value for v in s.get("k")] == ["third"]


def test_reconcile_replica_replies():
    # Replies from 3 replicas: one stale, two with concurrent siblings.
    replies = [
        [ver("v0", c0=1)],
        [ver("v1", c0=1, c1=1)],
        [ver("v1", c0=1, c1=1), ver("v2", c0=1, c2=1)],
    ]
    result = reconcile(v for reply in replies for v in reply)
    assert sorted(v.value for v in result) == ["v1", "v2"]
    assert context_of(result).to_dict() == {"c0": 1, "c1": 1, "c2": 1}


def test_version_dict_roundtrip():
    v = ver({"items": [1, 2]}, ts=123, node="n4", c1=3)
    assert Version.from_dict(v.to_dict()) == v


def test_policy_siblings_returns_all():
    vs = [ver("a", ts=1, c1=1), ver("b", ts=2, c2=1)]
    assert resolve(vs, "siblings") == vs


def test_policy_lww_picks_latest_then_node_tiebreak():
    a, b = ver("a", ts=5, node="n1", c1=1), ver("b", ts=9, node="n2", c2=1)
    assert resolve([a, b], "lww") == [b]
    c, d = ver("c", ts=7, node="n1", c1=1), ver("d", ts=7, node="n3", c2=1)
    assert resolve([c, d], "lww") == [d]
