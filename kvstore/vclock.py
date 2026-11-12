"""Vector clocks for tracking causality between versions of a key.

Design decision: the *actor* in the clock is the CLIENT ID, not the coordinator
node ID. With node-ID actors (as in the original Dynamo paper), two clients
writing concurrently through the same coordinator get clocks like {A:2} and
{A:3}; the second appears to supersede the first and an update is silently
lost. Client-ID actors avoid this, at the cost of clocks growing with the
number of clients (fine at our scale).

Contract: each client_id is a *sequential* actor - one client_id must not issue
two writes concurrently.

VectorClock is immutable: every operation returns a new clock.
"""

from __future__ import annotations

from enum import Enum
from typing import Mapping


class Ordering(Enum):
    BEFORE = "before"          # self happened-before other   (self < other)
    AFTER = "after"            # other happened-before self   (self > other)
    EQUAL = "equal"            # identical clocks
    CONCURRENT = "concurrent"  # neither descends from the other


class VectorClock:
    __slots__ = ("_counters",)

    def __init__(self, counters: Mapping[str, int] | None = None):
        clean: dict[str, int] = {}
        for actor, count in (counters or {}).items():
            if not isinstance(count, int) or count < 0:
                raise ValueError(f"invalid counter for {actor!r}: {count!r}")
            if count > 0:  # zero entries are equivalent to absent entries
                clean[actor] = count
        self._counters = clean

    # ---- construction / serialisation ---------------------------------
    @classmethod
    def from_dict(cls, data: Mapping[str, int] | None) -> "VectorClock":
        return cls(data or {})

    def to_dict(self) -> dict[str, int]:
        return dict(self._counters)

    # ---- core operations ----------------------------------------------
    def get(self, actor: str) -> int:
        return self._counters.get(actor, 0)

    def increment(self, actor: str) -> "VectorClock":
        counters = dict(self._counters)
        counters[actor] = counters.get(actor, 0) + 1
        return VectorClock(counters)

    def merge(self, other: "VectorClock") -> "VectorClock":
        """Pointwise maximum (least upper bound) of the two clocks."""
        counters = dict(self._counters)
        for actor, count in other._counters.items():
            if count > counters.get(actor, 0):
                counters[actor] = count
        return VectorClock(counters)

    def descends(self, other: "VectorClock") -> bool:
        """True if self >= other pointwise (self has seen everything other has)."""
        return all(self.get(a) >= c for a, c in other._counters.items())

    def dominates(self, other: "VectorClock") -> bool:
        """True if self > other strictly (descends and not equal)."""
        return self.descends(other) and self != other

    def compare(self, other: "VectorClock") -> Ordering:
        self_ge = self.descends(other)
        other_ge = other.descends(self)
        if self_ge and other_ge:
            return Ordering.EQUAL
        if self_ge:
            return Ordering.AFTER
        if other_ge:
            return Ordering.BEFORE
        return Ordering.CONCURRENT

    def concurrent_with(self, other: "VectorClock") -> bool:
        return self.compare(other) is Ordering.CONCURRENT

    # ---- dunder ---------------------------------------------------------
    def __eq__(self, other: object) -> bool:
        return isinstance(other, VectorClock) and self._counters == other._counters

    def __hash__(self) -> int:
        return hash(frozenset(self._counters.items()))

    def __repr__(self) -> str:
        inner = ", ".join(f"{a}:{c}" for a, c in sorted(self._counters.items()))
        return f"VC{{{inner}}}"


def merge_all(clocks) -> VectorClock:
    """Merge any number of clocks; used to build the read *context*."""
    result = VectorClock()
    for clock in clocks:
        result = result.merge(clock)
    return result
