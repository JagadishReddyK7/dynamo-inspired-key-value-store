"""Latency vs (R, W).  [Owner: Person C]

For each (R, W) in SETTINGS: run M puts and M gets (mixed keys) with injected
delay; record to OpLog; report p50 / p95 / p99 per setting.

Expected: latency grows with R / W because the coordinator waits for the
R-th / W-th fastest replica; tail latency (p99) grows fastest.

TODO (Person C).
"""

SETTINGS = [(1, 1), (1, 2), (2, 1), (2, 2), (1, 3), (3, 1), (3, 3)]

if __name__ == "__main__":
    raise SystemExit("not implemented yet")
