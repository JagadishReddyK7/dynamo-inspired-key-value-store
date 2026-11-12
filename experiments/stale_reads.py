"""Stale-read frequency vs (R, W).  [Owner: Person C]

Workload (per (R, W) setting):
  - 1 writer writes increasing integers 1, 2, 3, ... to one key with W
  - K readers read the key concurrently with R
  - injected delay (e.g. 5-50 ms) so replication lag exists
  - each request goes to a RANDOM node (not always the same coordinator)
  - every concurrent reader task and the writer have their own client_id

DEFINITION (put this in the report verbatim):
  A read is STALE if it does not return the value of the latest write that was
  ACKNOWLEDGED (put.end_ms) BEFORE the read was SENT (get.start_ms).
  Writes still in flight when the read starts may or may not be visible -
  returning either is NOT stale.

Expected: ~0% stale when R + W > N (=3); non-zero when R + W <= N, rising with
delay variance.

TODO (Person C): implement run() using client.KVClient + OpLog, then
analyse() from the CSV using the definition above.
"""

SETTINGS = [(1, 1), (1, 2), (2, 1), (2, 2), (1, 3), (3, 1), (3, 3)]


def analyse(csv_path: str) -> dict[tuple[int, int], float]:
    """Return {(r, w): stale_fraction}. TODO (Person C)."""
    raise NotImplementedError


if __name__ == "__main__":
    raise SystemExit("not implemented yet")
