"""Shared operation log: one CSV row per client operation.  [Owner: Person C]

Every experiment writes this format, and plot.py / the stale-read analysis read
it, so all results come from one consistent source.

Columns:
  op            "get" | "put"
  key
  client_id
  node          coordinator node the client contacted
  r, w          quorum settings used (the unused one is blank)
  start_ms      wall-clock time the client sent the request
  end_ms        wall-clock time the client received the reply
  ok            1 if the operation succeeded, 0 if it failed (quorum/timeout)
  error         error code if ok == 0
  value         value written (put) or JSON list of values returned (get)
  num_versions  number of versions returned (get) - >1 means siblings
  experiment    free-text tag, e.g. "latency_r2w2" or "partition_phase1"
"""

from __future__ import annotations

import csv
import json
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

FIELDS = ["op", "key", "client_id", "node", "r", "w", "start_ms", "end_ms",
          "ok", "error", "value", "num_versions", "experiment"]


def now_ms() -> float:
    return time.time() * 1000.0


@dataclass
class OpRecord:
    op: str
    key: str
    client_id: str
    node: str
    start_ms: float
    end_ms: float
    ok: int
    r: int | str = ""
    w: int | str = ""
    error: str = ""
    value: str = ""
    num_versions: int | str = ""
    experiment: str = ""

    @property
    def latency_ms(self) -> float:
        return self.end_ms - self.start_ms


class OpLog:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = self.path.open("w", newline="", encoding="utf-8")
        self._writer = csv.DictWriter(self._fh, fieldnames=FIELDS)
        self._writer.writeheader()

    def record(self, rec: OpRecord) -> None:
        row = asdict(rec)
        self._writer.writerow({k: row[k] for k in FIELDS})

    def close(self) -> None:
        self._fh.close()

    def __enter__(self) -> "OpLog":
        return self

    def __exit__(self, *exc) -> None:
        self.close()


def encode_value(value: Any) -> str:
    return json.dumps(value, sort_keys=True)
