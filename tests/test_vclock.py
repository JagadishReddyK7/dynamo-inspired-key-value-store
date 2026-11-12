import pytest

from kvstore.vclock import Ordering, VectorClock, merge_all


def vc(**counters):
    return VectorClock(counters)


def test_empty_clocks_are_equal():
    assert VectorClock().compare(VectorClock()) is Ordering.EQUAL


def test_increment_is_immutable_and_creates_successor():
    a = VectorClock()
    b = a.increment("c1")
    assert a.to_dict() == {}
    assert b.to_dict() == {"c1": 1}
    assert b.compare(a) is Ordering.AFTER
    assert a.compare(b) is Ordering.BEFORE


def test_zero_entries_are_ignored():
    assert vc(c1=1, c2=0) == vc(c1=1)


def test_negative_counter_rejected():
    with pytest.raises(ValueError):
        VectorClock({"c1": -1})


def test_concurrent_writes_from_same_context():
    base = vc(c1=1)
    left = base.increment("c1")   # {c1:2}
    right = base.increment("c2")  # {c1:1, c2:1}
    assert left.compare(right) is Ordering.CONCURRENT
    assert left.concurrent_with(right)


def test_merge_is_pointwise_max_and_dominates_both():
    a, b = vc(c1=3, c2=1), vc(c1=1, c2=4, c3=2)
    m = a.merge(b)
    assert m.to_dict() == {"c1": 3, "c2": 4, "c3": 2}
    assert m.descends(a) and m.descends(b)
    assert m.dominates(a) and m.dominates(b)


def test_merge_is_commutative_associative_idempotent():
    a, b, c = vc(x=1, y=2), vc(y=3), vc(x=5, z=1)
    assert a.merge(b) == b.merge(a)
    assert a.merge(b).merge(c) == a.merge(b.merge(c))
    assert a.merge(a) == a


def test_reconciliation_write_supersedes_siblings():
    # Partition scenario: two sides write from the same base, then a client
    # reads both siblings and writes back with the merged context.
    base = vc(c0=1)
    s1, s2 = base.increment("c1"), base.increment("c2")
    resolved = merge_all([s1, s2]).increment("c3")
    assert resolved.dominates(s1) and resolved.dominates(s2)


def test_descends_and_dominates_on_equal():
    a = vc(c1=2)
    assert a.descends(vc(c1=2))
    assert not a.dominates(vc(c1=2))


def test_serialisation_roundtrip_and_hash():
    a = vc(c1=2, c2=7)
    assert VectorClock.from_dict(a.to_dict()) == a
    assert VectorClock.from_dict(None) == VectorClock()
    assert len({a, vc(c2=7, c1=2)}) == 1
