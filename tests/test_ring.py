from collections import Counter

import pytest

from kvstore.ring import ConsistentHashRing

NODES = ["n1", "n2", "n3", "n4", "n5"]


def test_preference_list_has_n_distinct_nodes():
    ring = ConsistentHashRing(NODES)
    for i in range(500):
        prefs = ring.preference_list(f"key-{i}", 3)
        assert len(prefs) == 3
        assert len(set(prefs)) == 3
        assert set(prefs) <= set(NODES)


def test_deterministic_across_instances():
    a, b = ConsistentHashRing(NODES), ConsistentHashRing(list(NODES))
    for i in range(100):
        assert a.preference_list(f"k{i}", 3) == b.preference_list(f"k{i}", 3)


def test_n_larger_than_cluster_returns_all_nodes():
    ring = ConsistentHashRing(["n1", "n2"])
    assert sorted(ring.preference_list("x", 3)) == ["n1", "n2"]


def test_longer_list_extends_shorter_one():
    # Fallback nodes for sloppy quorum come right after the N replicas.
    ring = ConsistentHashRing(NODES)
    for i in range(100):
        assert ring.preference_list(f"k{i}", 5)[:3] == ring.preference_list(f"k{i}", 3)


def test_load_is_reasonably_balanced():
    ring = ConsistentHashRing(NODES, vnodes=64)
    primaries = Counter(ring.preference_list(f"key-{i}", 1)[0] for i in range(10_000))
    expected = 10_000 / len(NODES)
    for node in NODES:
        assert 0.6 * expected < primaries[node] < 1.4 * expected, primaries


def test_invalid_construction():
    with pytest.raises(ValueError):
        ConsistentHashRing([])
    with pytest.raises(ValueError):
        ConsistentHashRing(["n1", "n1"])
