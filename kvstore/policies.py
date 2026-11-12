"""Declared conflict-resolution policies.

The policy is a cluster-wide setting (config: "conflict_policy") applied by the
coordinator to the reconciled sibling set before answering a GET.

- "siblings": return all concurrent versions; the CLIENT must reconcile them
              and write back with the returned context. (Default.)
- "lww":      last-writer-wins - pick the version with the highest
              (timestamp_ms, coordinator) pair. Deterministic, but silently
              discards concurrent updates (the partition experiment shows this).
"""

from __future__ import annotations

from .storage import Version

POLICIES = ("siblings", "lww")


def lww_winner(versions: list[Version]) -> Version:
    return max(versions, key=lambda v: (v.timestamp_ms, v.coordinator))


def resolve(versions: list[Version], policy: str) -> list[Version]:
    if policy not in POLICIES:
        raise ValueError(f"unknown conflict policy {policy!r}; expected one of {POLICIES}")
    if policy == "siblings" or len(versions) <= 1:
        return list(versions)
    return [lww_winner(versions)]
