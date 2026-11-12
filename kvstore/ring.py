"""Consistent hashing ring with virtual nodes.

Each physical node owns `vnodes` points on a 128-bit ring (MD5). A key's
preference list is obtained by walking clockwise from hash(key) and collecting
distinct physical nodes. The first N entries are the key's replicas; entries
beyond N are fallback nodes (useful for an optional sloppy quorum).
"""

from __future__ import annotations

import bisect
import hashlib


def _hash(data: str) -> int:
    return int.from_bytes(hashlib.md5(data.encode("utf-8")).digest(), "big")


class ConsistentHashRing:
    def __init__(self, node_ids: list[str], vnodes: int = 64):
        if not node_ids:
            raise ValueError("ring needs at least one node")
        if len(set(node_ids)) != len(node_ids):
            raise ValueError("duplicate node ids")
        if vnodes < 1:
            raise ValueError("vnodes must be >= 1")
        self.node_ids = list(node_ids)
        self.vnodes = vnodes
        points = []
        for node in node_ids:
            for i in range(vnodes):
                points.append((_hash(f"{node}#vn{i}"), node))
        points.sort()
        self._hashes = [h for h, _ in points]
        self._owners = [n for _, n in points]

    def preference_list(self, key: str, n: int) -> list[str]:
        """First `n` distinct physical nodes clockwise from hash(key).

        If n exceeds the cluster size, all nodes are returned.
        """
        n = min(n, len(self.node_ids))
        start = bisect.bisect_right(self._hashes, _hash(key))
        result: list[str] = []
        seen: set[str] = set()
        total = len(self._hashes)
        for offset in range(total):
            node = self._owners[(start + offset) % total]
            if node not in seen:
                seen.add(node)
                result.append(node)
                if len(result) == n:
                    break
        return result

    def replicas(self, key: str, n: int = 3) -> list[str]:
        return self.preference_list(key, n)
