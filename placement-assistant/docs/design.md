# Sprint — design notes

Answer each in a short paragraph. Point to the code.

1. Why is the unit of retry the tool call, and not the whole request?
2. The idempotency key is stored in placement.db, not agent.db. Why there, and why in the same transaction as the side effect?
3. What happens if the lease is shorter than one model call? What would you change?
4. Why do both databases open transactions with BEGIN IMMEDIATE instead of plain BEGIN?
5. Name one thing in this system that is still not exactly-once, and what it would take to fix it.
