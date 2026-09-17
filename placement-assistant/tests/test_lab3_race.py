"""Lab 3 — two runs booking the same slot; one wins cleanly.

TODO: write these tests yourself. Delete the skip line when you start.

1. test_two_workers_two_runs_one_slot
   Two students who are both eligible for TCS (22IT017 and 22CS045) each get a thread and a queued run.
   Each run applies to drive 2 and books slot 3 (build the model turns with PositionalMock).
   Use two RunStore and two PlacementDb connections on the same files (the db_files fixture), and one Worker
   per connection. Assert: both runs SUCCEED (losing a race is not a crash), exactly one book_interview_slot
   result is "booked", the other is the "slot_taken" error, and slot 3 holds exactly one student.

2. test_truly_concurrent_claims_have_one_winner
   Both students apply to drive 2. Read slot 3's version once. Start 8 threads, each with its OWN
   PlacementDb connection, held at a threading.Barrier, then all call claim_slot(3, student, version).
   Assert exactly one True, seven False, and the version went up by exactly one.
"""
import pytest

pytest.skip("lab 3: write these tests", allow_module_level=True)
