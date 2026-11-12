"""Cluster configuration loader (static membership - no gossip)."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

DEFAULT_CONFIG = Path(__file__).resolve().parent.parent / "config" / "cluster.json"


@dataclass(frozen=True)
class NodeInfo:
    id: str
    host: str
    port: int

    @property
    def url(self) -> str:
        return f"http://{self.host}:{self.port}"


@dataclass(frozen=True)
class ClusterConfig:
    nodes: list[NodeInfo]
    n: int
    vnodes: int
    default_r: int
    default_w: int
    request_timeout_ms: int
    conflict_policy: str
    delay_min_ms: int
    delay_max_ms: int

    def node(self, node_id: str) -> NodeInfo:
        for n in self.nodes:
            if n.id == node_id:
                return n
        raise KeyError(node_id)

    @property
    def node_ids(self) -> list[str]:
        return [n.id for n in self.nodes]


def load_config(path: str | Path = DEFAULT_CONFIG) -> ClusterConfig:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    cfg = ClusterConfig(
        nodes=[NodeInfo(**n) for n in raw["nodes"]],
        n=raw["replication"]["n"],
        vnodes=raw["replication"]["vnodes"],
        default_r=raw["quorum"]["default_r"],
        default_w=raw["quorum"]["default_w"],
        request_timeout_ms=raw["quorum"]["request_timeout_ms"],
        conflict_policy=raw["conflict_policy"],
        delay_min_ms=raw["network"]["delay_min_ms"],
        delay_max_ms=raw["network"]["delay_max_ms"],
    )
    if not 1 <= cfg.n <= len(cfg.nodes):
        raise ValueError("replication.n must be between 1 and the number of nodes")
    for name, v in (("default_r", cfg.default_r), ("default_w", cfg.default_w)):
        if not 1 <= v <= cfg.n:
            raise ValueError(f"{name} must be between 1 and n")
    return cfg
