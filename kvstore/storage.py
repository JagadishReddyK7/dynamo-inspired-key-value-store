"""Per-node local storage holding the set of concurrent versions (siblings) per key.

Invariant: for every key, the stored versions are pairwise CONCURRENT - no
stored version descends from another.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable

from .vclock import VectorClock, merge_all


@dataclass(frozen=True)
class Version:
    value: Any
    clock: VectorClock
    timestamp_ms: int = 0          # wall-clock time set by coordinator (used by LWW)
    coordinator: str = ""          # node id that coordinated the write (LWW tiebreak)

    def to_dict(self) -> dict:
        return {
            "value": self.value,
            "clock": self.clock.to_dict(),
            "timestamp_ms": self.timestamp_ms,
            "coordinator": self.coordinator,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Version":
        return cls(
            value=data["value"],
            clock=VectorClock.from_dict(data.get("clock")),
            timestamp_ms=int(data.get("timestamp_ms", 0)),
            coordinator=str(data.get("coordinator", "")),
        )


def apply_version(existing: list[Version], incoming: Version) -> tuple[list[Version], bool]:
    """Merge one incoming version into a sibling set.

    Returns (new_sibling_list, changed).
    - If some stored version already descends (>=) the incoming one, it is obsolete: ignore.
    - Otherwise drop every stored version the incoming one descends, and add it.
    """
    for v in existing:
        if v.clock.descends(incoming.clock):
            return existing, False
    kept = [v for v in existing if not incoming.clock.descends(v.clock)]
    kept.append(incoming)
    return kept, True


def reconcile(versions: Iterable[Version]) -> list[Version]:
    """Reduce any collection of versions (e.g. replies from R replicas) to the
    set of maximal, pairwise-concurrent versions. Duplicates are removed."""
    result: list[Version] = []
    for v in versions:
        result, _ = apply_version(result, v)
    return result


def context_of(versions: Iterable[Version]) -> VectorClock:
    """The causal context returned to clients on GET = merge of all sibling clocks."""
    return merge_all(v.clock for v in versions)


@dataclass
class LocalStore:
    _data: dict[str, list[Version]] = field(default_factory=dict)

    def get(self, key: str) -> list[Version]:
        return list(self._data.get(key, []))

    def put(self, key: str, version: Version) -> bool:
        """Apply a version; returns True if the stored state changed."""
        updated, changed = apply_version(self._data.get(key, []), version)
        if changed:
            self._data[key] = updated
        return changed

    def put_many(self, key: str, versions: Iterable[Version]) -> bool:
        """Apply several versions (used by replication and read repair)."""
        changed = False
        for v in versions:
            changed |= self.put(key, v)
        return changed

    def keys(self) -> list[str]:
        return list(self._data.keys())

    def dump(self) -> dict[str, list[dict]]:
        return {k: [v.to_dict() for v in vs] for k, vs in self._data.items()}
