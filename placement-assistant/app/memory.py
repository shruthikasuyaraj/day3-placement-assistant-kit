"""agent.db: conversations, runs and the job queue."""
import json
import time
import uuid
from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from app.placement_db import connect

SCHEMA = Path(__file__).resolve().parent.parent / "schema" / "agent.sql"
NOW_SQL = "strftime('%Y-%m-%dT%H:%M:%fZ', 'now')"
TERMINAL = ("succeeded", "failed", "cancelled", "dead")


@dataclass(frozen=True)
class Claimed:
    run_id: str
    thread_id: str
    attempts: int


class RunStore:
    def __init__(self, path: str = ":memory:", clock: Callable[[], float] = time.time):
        self.conn = connect(path)
        self.clock = clock

    # ================================================================== given (Day 2 answers)

    def migrate(self) -> None:
        self.conn.executescript(SCHEMA.read_text())

    @contextmanager
    def transaction(self):
        """BEGIN IMMEDIATE: take the write lock first, so two workers queue instead of deadlocking."""
        if self.conn.in_transaction:
            yield self.conn
            return
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            yield self.conn
        except BaseException:
            self.conn.execute("ROLLBACK")
            raise
        self.conn.execute("COMMIT")

    def create_thread(self, student_id: str) -> str:
        thread_id = str(uuid.uuid4())
        self.conn.execute("INSERT INTO thread (id, student_id) VALUES (?, ?)", (thread_id, student_id))
        return thread_id

    def get_thread(self, thread_id: str) -> dict | None:
        r = self.conn.execute("SELECT * FROM thread WHERE id = ?", (thread_id,)).fetchone()
        return dict(r) if r else None

    def append_message(self, thread_id: str, role: str, text: str) -> int:
        cur = self.conn.execute(
            "INSERT INTO message (thread_id, seq, role, text)"
            " VALUES (?, (SELECT COALESCE(MAX(seq), 0) + 1 FROM message WHERE thread_id = ?), ?, ?)",
            (thread_id, thread_id, role, text))
        return self.conn.execute("SELECT seq FROM message WHERE id = ?", (cur.lastrowid,)).fetchone()["seq"]

    def load_history(self, thread_id: str) -> list[dict]:
        rows = self.conn.execute(
            "SELECT seq, role, text FROM message WHERE thread_id = ? ORDER BY seq",
            (thread_id,)).fetchall()
        return [dict(r) for r in rows]

    def record_model_step(self, run_id: str, seq: int, tokens_in: int, tokens_out: int,
                          text: str | None, tool_calls: list[dict]) -> int:
        with self.transaction() as c:
            step_id = c.execute(
                "INSERT INTO run_step (run_id, seq, kind, tokens_in, tokens_out, text, tool_calls)"
                " VALUES (?, ?, 'model', ?, ?, ?, ?)",
                (run_id, seq, tokens_in, tokens_out, text, json.dumps(tool_calls))).lastrowid
            c.execute(
                "UPDATE run SET tokens_in = tokens_in + ?, tokens_out = tokens_out + ? WHERE id = ?",
                (tokens_in, tokens_out, run_id))
            return step_id

    def record_tool_call(self, run_id: str, seq: int, name: str, args: dict, result: dict,
                         ok: bool, latency_ms: int, idempotency_key: str | None = None) -> int:
        with self.transaction() as c:
            step_id = c.execute(
                "INSERT INTO run_step (run_id, seq, kind) VALUES (?, ?, 'tool')",
                (run_id, seq)).lastrowid
            c.execute(
                "INSERT INTO tool_call (run_step_id, tool_name, args, result, ok, latency_ms, idempotency_key)"
                " VALUES (?, ?, ?, ?, ?, ?, ?)",
                (step_id, name, json.dumps(args, default=str), json.dumps(result, default=str),
                 int(ok), latency_ms, idempotency_key))
            return step_id

    def load_steps(self, run_id: str) -> list[dict]:
        """Every recorded step of a run, in order, with JSON decoded. Used to resume after a crash."""
        rows = self.conn.execute(
            """SELECT s.seq, s.kind, s.text, s.tool_calls, t.tool_name, t.args, t.result, t.ok
                 FROM run_step s LEFT JOIN tool_call t ON t.run_step_id = s.id
                WHERE s.run_id = ? ORDER BY s.seq""", (run_id,)).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            for k in ("tool_calls", "args", "result"):
                d[k] = json.loads(d[k]) if d[k] is not None else None
            out.append(d)
        return out

    def get_run(self, run_id: str) -> dict | None:
        r = self.conn.execute("SELECT * FROM run WHERE id = ?", (run_id,)).fetchone()
        return {**dict(r), "steps": self.load_steps(run_id)} if r else None

    # ================================================================== Part 1: the queue

    def enqueue(self, thread_id: str, text: str, model: str, max_attempts: int = 3) -> str:
        run_id = str(uuid.uuid4())
        now = self.clock()

        with self.transaction() as c:
            self.append_message(thread_id, "user", text)
            c.execute(
                "INSERT INTO run (id, thread_id, status, model, max_attempts, available_at)"
                " VALUES (?, ?, 'queued', ?, ?, ?)",
                (run_id, thread_id, model, max_attempts, now)
            )

        return run_id

    def claim_next(self, worker_id: str, lease_seconds: float) -> Claimed | None:
        """Atomically claim the oldest queued run."""
        now = self.clock()

        with self.transaction() as c:
            row = c.execute(
                "SELECT id, thread_id, attempts "
                "FROM run "
                "WHERE status = 'queued' AND available_at <= ? "
                "ORDER BY available_at, created_at "
                "LIMIT 1",
                (now,)
            ).fetchone()

            if row is None:
                return None

            attempts = row["attempts"] + 1

            c.execute(
                f"UPDATE run SET "
                "status = 'running', "
                "lease_owner = ?, "
                "lease_until = ?, "
                "attempts = ?, "
                f"started_at = COALESCE(started_at, {NOW_SQL}) "
                "WHERE id = ?",
                (worker_id, now + lease_seconds, attempts, row["id"])
            )

            return Claimed(row["id"], row["thread_id"], attempts)

    def heartbeat(self, run_id: str, worker_id: str, lease_seconds: float) -> bool:
        """Extend a lease only when this worker still owns the running run."""
        now = self.clock()

        cur = self.conn.execute(
            "UPDATE run "
            "SET lease_until = ? "
            "WHERE id = ? AND status = 'running' AND lease_owner = ?",
            (now + lease_seconds, run_id, worker_id)
        )

        return cur.rowcount == 1

    def reap_expired(self) -> list[str]:
        """Recover runs whose worker lease has expired."""
        now = self.clock()

        with self.transaction() as c:
            rows = c.execute(
                "SELECT id, attempts, max_attempts "
                "FROM run "
                "WHERE status = 'running' AND lease_until < ?",
                (now,)
            ).fetchall()

            touched = []

            for row in rows:
                touched.append(row["id"])

                if row["attempts"] < row["max_attempts"]:
                    c.execute(
                        "UPDATE run SET "
                        "status = 'queued', "
                        "error_code = 'lease_expired', "
                        "lease_owner = NULL, "
                        "lease_until = NULL, "
                        "available_at = ? "
                        "WHERE id = ?",
                        (now, row["id"])
                    )
                else:
                    c.execute(
                        f"UPDATE run SET "
                        "status = 'dead', "
                        "error_code = 'lease_expired', "
                        "lease_owner = NULL, "
                        "lease_until = NULL, "
                        f"finished_at = {NOW_SQL} "
                        "WHERE id = ?",
                        (row["id"],)
                    )

            return touched

    def complete(self, run_id: str, worker_id: str, reply: str) -> bool:
        """Save the model's reply and mark the run succeeded, together, only if this worker still owns it."""
        with self.transaction() as c:
            row = c.execute(
                "SELECT thread_id FROM run WHERE id = ? AND status = 'running' AND lease_owner = ?",
                (run_id, worker_id)).fetchone()
            if row is None:
                return False
            self.append_message(row["thread_id"], "model", reply)
            c.execute(
                f"UPDATE run SET status = 'succeeded', lease_owner = NULL, lease_until = NULL,"
                f" error_code = NULL, finished_at = {NOW_SQL} WHERE id = ?",
                (run_id,))
            return True

    # ================================================================== Lab 1: cancel

    def request_cancel(self, run_id: str) -> str | None:
        """Cancel queued work immediately or request cancellation of running work."""
        with self.transaction() as c:
            row = c.execute(
                "SELECT status FROM run WHERE id = ?",
                (run_id,)
            ).fetchone()

            if row is None:
                return None

            status = row["status"]

            if status == "queued":
                c.execute(
                    f"UPDATE run SET status = 'cancelled', finished_at = {NOW_SQL} WHERE id = ?",
                    (run_id,)
                )
                return "cancelled"

            if status == "running":
                c.execute(
                    "UPDATE run SET cancel_requested = 1 WHERE id = ?",
                    (run_id,)
                )
                return "running"

            return status

    def cancel_requested(self, run_id: str) -> bool:
        row = self.conn.execute(
            "SELECT cancel_requested FROM run WHERE id = ?",
            (run_id,)
        ).fetchone()

        return bool(row["cancel_requested"]) if row else False

    def mark_cancelled(self, run_id: str, worker_id: str) -> bool:
        """Mark a running run cancelled when owned by this worker."""
        with self.transaction() as c:
            cur = c.execute(
                f"UPDATE run SET "
                "status = 'cancelled', "
                "lease_owner = NULL, "
                "lease_until = NULL, "
                f"finished_at = {NOW_SQL} "
                "WHERE id = ? AND status = 'running' AND lease_owner = ?",
                (run_id, worker_id)
            )

            return cur.rowcount == 1

    # ================================================================== Lab 2: retry and dead-letter

    def fail_attempt(self, run_id: str, worker_id: str, error_code: str, retryable: bool,
                     backoff_seconds: float = 2.0) -> str | None:
        """Record a failed attempt and either retry or finish the run."""
        with self.transaction() as c:
            row = c.execute(
                "SELECT attempts, max_attempts "
                "FROM run "
                "WHERE id = ? AND status = 'running' AND lease_owner = ?",
                (run_id, worker_id)
            ).fetchone()

            if row is None:
                return None

            attempts = row["attempts"]
            max_attempts = row["max_attempts"]

            if retryable and attempts < max_attempts:
                delay = backoff_seconds * (2 ** (attempts - 1))

                c.execute(
                    "UPDATE run SET "
                    "status = 'queued', "
                    "error_code = ?, "
                    "lease_owner = NULL, "
                    "lease_until = NULL, "
                    "available_at = ? "
                    "WHERE id = ?",
                    (error_code, self.clock() + delay, run_id)
                )

                return "queued"

            status = "dead" if retryable else "failed"

            c.execute(
                f"UPDATE run SET "
                "status = ?, "
                "error_code = ?, "
                "lease_owner = NULL, "
                "lease_until = NULL, "
                f"finished_at = {NOW_SQL} "
                "WHERE id = ?",
                (status, error_code, run_id)
            )

            return status