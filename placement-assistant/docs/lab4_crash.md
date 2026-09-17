# Lab 4 — crash drill

## Before idempotency
If you ran the drill before finishing Part 2 (or with `call_tool` reverted to call tools directly), paste the last lines here.

## After
Paste the output of three runs of `python -m scripts.crash_drill`.

## Explain
1. At which moment was worker-A killed, and what had and hadn't been written?
2. How did worker-B know where to resume?
3. Which line of code stopped the second notification?
