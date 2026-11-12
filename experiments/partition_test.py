"""Partition -> concurrent updates -> heal -> verify the declared resolution rule.
[Owner: Person A]  Must be an automated PASS/FAIL check.

Scenario for key K with replicas [a, b, c] (from the ring) and a 2|1 split:
  1. write K = v0 (W=3) so every replica starts from the same version
  2. partition: side1 = {a, b, + any other nodes}, side2 = {c}
     (POST /admin/partition on EVERY node so the cut is symmetric)
  3. client1 -> node on side1: read K (R=1), write K = v1 with that context, W=1
     client2 -> node c:         read K (R=1), write K = v2 with that context, W=1
     (client1 and client2 MUST have different client_ids)
     (on side2 also assert that R=2 reads and W=2 writes FAIL -> shows unavailability;
      on side1 W=2 succeeds -> the majority side stays available)
  4. heal (POST /admin/heal on every node)
  5. read K with R=3 and check the declared rule:
       policy "siblings": exactly 2 versions {v1, v2}, clocks CONCURRENT,
                          then client reconciles -> next read returns 1 merged version
       policy "lww":      exactly 1 version, equal to the higher (timestamp_ms, coordinator)
  6. after read repair, /admin/state on a, b, c must all hold the same version set

TODO (Person A).
"""

if __name__ == "__main__":
    raise SystemExit("not implemented yet")
