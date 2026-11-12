"""Availability vs (R, W) under node failures / partitions.  [Owner: Person C]

Scenarios: 0, 1, 2 replicas of the key unreachable (via /admin/partition on the
coordinator's side, or by killing node processes).
For each (R, W): success rate of gets and puts.

Expected (strict quorum, N=3): an operation needing k acks fails once more than
3 - k replicas are unreachable. E.g. W=3 fails with a single replica down.

TODO (Person C).
"""

if __name__ == "__main__":
    raise SystemExit("not implemented yet")
