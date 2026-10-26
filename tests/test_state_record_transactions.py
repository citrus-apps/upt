"""S0c-A: composable History writes with independent rollback protection.

Only synthetic temporary SQLite state is used; external retention operations
are mocked.
"""
import _upt_isolation  # noqa: F401 - must stay first; see scripts/run_tests.py
import json
import queue
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from docker_update_manager import retention
from docker_update_manager.sqlite_outcome import (
    AMBIGUOUS, NOT_COMMITTED, OwnedTransaction, TransactionOwnershipError, WriteOutcomeError,
)
from docker_update_manager.state import State
from sqlite_faults import history_insert, sqlite_fault

IMAGE_A = "sha256:" + "a" * 64
IMAGE_B = "sha256:" + "b" * 64
CREATED = "2000-01-01 00:00:00"
FIELDS = dict(stack="example", service="app", action="update", old_image_id=IMAGE_A, result="SUCCESS")
TABLES = ("history", "rollback_refs", "protected_images")


class _ConnectionProbe:
    """Count real transaction boundaries without replacing the owner's trace."""

    def __init__(self, real):
        self.real = real
        self.statements = []
        self.commits = 0
        self.rollbacks = 0

    def __getattr__(self, name):
        return getattr(self.real, name)

    def execute(self, sql, params=()):
        self.statements.append(sql)
        return self.real.execute(sql, params)

    def commit(self):
        self.commits += 1
        self.real.commit()

    def rollback(self):
        self.rollbacks += 1
        self.real.rollback()


class RecordTransactionTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.state = State(self.root / "state")
        self.state.ensure_compatible()
        self.observer = State(self.state.root)
        self.observer.ensure_compatible()

    def rows(self, state=None, tables=TABLES):
        with (state or self.observer).read() as c:
            return {table: [tuple(row) for row in c.execute(f"SELECT * FROM {table} ORDER BY 1")]
                    for table in tables}

    def test_legacy_record_preserves_return_serialization_and_one_transaction(self):
        probe = _ConnectionProbe(self.state._new_connection())
        with patch.object(self.state, "_new_connection", return_value=probe) as opened:
            history_id = self.state.record(**FIELDS, details={"items": ["synthetic"]})
        opened.assert_called_once_with()
        self.assertIs(type(history_id), int)
        self.assertEqual((probe.commits, probe.rollbacks), (1, 0))
        self.assertEqual(probe.statements.count("BEGIN IMMEDIATE"), 1)
        with self.observer.read() as c:
            row = c.execute("SELECT id,details FROM history").fetchone()
        self.assertEqual(row["id"], history_id)
        self.assertEqual(json.loads(row["details"]), {"items": ["synthetic"]})

    def test_legacy_serialization_error_still_precedes_connection_open(self):
        with patch.object(self.state, "connect") as connect:
            with self.assertRaises(TypeError):
                self.state.record(**FIELDS, details={"invalid": object()})
        connect.assert_not_called()

    def test_borrowed_records_have_one_outer_commit_and_provisional_ids(self):
        probe = _ConnectionProbe(self.state._new_connection())
        with patch.object(self.state, "_new_connection", return_value=probe) as opened:
            with self.state.write_transaction() as tx:
                with patch.object(self.state, "connect", side_effect=AssertionError("nested connect")), \
                     patch.object(self.state, "read", side_effect=AssertionError("nested read")), \
                     patch.object(self.state, "_ensure_schema", side_effect=AssertionError("schema open")):
                    first = self.state.record(transaction=tx, **FIELDS)
                    second = self.state.record(transaction=tx, **{**FIELDS, "service": "worker"})
                self.assertNotEqual(first, second)
                self.assertEqual(self.rows(), {table: [] for table in TABLES})
                self.assertEqual(tx.execute("SELECT count(*) FROM history").fetchone()[0], 2)
                self.assertEqual(probe.commits, 0)
            opened.assert_called_once_with(timeout=None)
        self.assertEqual((probe.commits, probe.rollbacks), (1, 0))
        self.assertEqual(probe.statements.count("BEGIN IMMEDIATE"), 1)
        self.assertFalse(any(sql.startswith("SAVEPOINT") for sql in probe.statements))
        rows = self.rows()
        self.assertEqual([row[0] for row in rows["history"]], [first, second])
        self.assertEqual([row[:2] for row in rows["rollback_refs"]], [(first, IMAGE_A), (second, IMAGE_A)])
        self.assertEqual(rows["protected_images"][0][:3], (IMAGE_A, "rollback", first))

    def test_outer_failure_discards_all_inserts_and_preserves_existing_protection(self):
        with self.state.connect() as c:
            c.execute("INSERT INTO protected_images VALUES(?,?,NULL,?)", (IMAGE_A, "manual", CREATED))
        before = self.rows(tables=(*TABLES, "disk_snapshots"))
        cause = ValueError("synthetic outer failure")
        with self.assertRaises(WriteOutcomeError) as caught:
            with self.state.write_transaction() as tx:
                provisional_id = self.state.record(transaction=tx, **FIELDS)
                self.state.record(transaction=tx, **{**FIELDS, "old_image_id": IMAGE_B})
                tx.execute("INSERT INTO disk_snapshots(kind,metrics) VALUES(?,?)", ("synthetic", "{}"))
                raise cause
        self.assertEqual((caught.exception.outcome, caught.exception.phase), (NOT_COMMITTED, "body"))
        self.assertIs(caught.exception.cause, cause)
        self.assertGreater(provisional_id, 0)
        self.assertEqual(self.rows(tables=(*TABLES, "disk_snapshots")), before)

    def test_record_faults_preserve_raw_and_owned_commit_classification(self):
        for borrowed in (False, True):
            for mode in ("before_commit", "after_commit"):
                with self.subTest(borrowed=borrowed, mode=mode):
                    state = State(self.root / f"fault-{borrowed}-{mode}")
                    state.ensure_compatible()
                    expected = WriteOutcomeError if borrowed else sqlite3.OperationalError
                    with sqlite_fault(state, history_insert(result="SUCCESS"), mode=mode) as fault:
                        with self.assertRaises(expected) as caught:
                            if borrowed:
                                with state.write_transaction() as tx:
                                    state.record(transaction=tx, **FIELDS)
                            else:
                                state.record(**FIELDS)
                    if borrowed:
                        self.assertEqual(caught.exception.outcome, AMBIGUOUS if mode == "after_commit" else NOT_COMMITTED)
                        self.assertIsInstance(caught.exception.cause, sqlite3.OperationalError)
                    self.assertEqual(fault.connection_timeouts, [None])
                    rows = self.rows(state)
                    self.assertEqual([len(rows[t]) for t in TABLES], [1, 1, 1] if mode == "after_commit" else [0, 0, 0])

    def test_foreign_state_handles_are_rejected_even_for_the_same_database(self):
        other = State(self.root / "other")
        other.ensure_compatible()
        for target in (self.observer, other):
            with self.subTest(same_database=target is self.observer):
                with self.state.write_transaction() as tx:
                    with patch.object(target, "_record_in_transaction") as helper, \
                         patch.object(target, "_new_connection") as opened:
                        with self.assertRaises(TransactionOwnershipError):
                            target.record(transaction=tx, **FIELDS)
                    helper.assert_not_called()
                    opened.assert_not_called()
        self.assertEqual(self.rows(), {t: [] for t in TABLES})
        self.assertEqual(self.rows(other), {t: [] for t in TABLES})

    def test_raw_connections_cursors_results_and_unissued_handles_are_rejected(self):
        with self.state.read() as raw:
            cursor = raw.execute("SELECT 1")
            unissued = OwnedTransaction(raw, _owner=object())
            try:
                result = unissued.execute("SELECT 1")
                for handle in (raw, cursor, result, unissued, object(), False):
                    with self.subTest(kind=type(handle).__name__):
                        with patch.object(self.state, "_record_in_transaction") as helper:
                            with self.assertRaises(TransactionOwnershipError):
                                self.state.record(transaction=handle, **FIELDS)
                        helper.assert_not_called()
            finally:
                unissued._release()
        self.assertEqual(self.rows(), {t: [] for t in TABLES})

    def test_expired_handle_is_rejected_during_a_later_transaction(self):
        with self.state.write_transaction() as old:
            self.state.record(transaction=old, **FIELDS)
        with self.state.write_transaction() as current:
            with patch.object(self.state, "_record_in_transaction") as helper:
                with self.assertRaises(TransactionOwnershipError):
                    self.state.record(transaction=old, **FIELDS)
            helper.assert_not_called()
            self.state.record(transaction=current, **{**FIELDS, "service": "worker"})
        self.assertEqual(len(self.rows()["history"]), 2)

    def test_cross_thread_handle_is_rejected_before_history_sql(self):
        errors = queue.Queue()

        def use_handle(tx):
            try:
                self.state.record(transaction=tx, **FIELDS)
            except Exception as exc:
                errors.put(exc)

        with self.state.write_transaction() as tx:
            with patch.object(self.state, "_record_in_transaction") as helper:
                worker = threading.Thread(target=use_handle, args=(tx,), name="upt-record-ownership")
                worker.start()
                worker.join(2)
                self.assertFalse(worker.is_alive())
                self.assertIsInstance(errors.get(timeout=1), TransactionOwnershipError)
            helper.assert_not_called()
            self.state.record(transaction=tx, **FIELDS)
        self.assertEqual(len(self.rows()["history"]), 1)

    def test_namespace_and_borrowed_transaction_are_rejected_before_sql(self):
        with self.state.write_transaction() as tx:
            for owner in ("namespace", ""):
                with self.subTest(owner=owner), patch.object(self.state, "_record_in_transaction") as helper:
                    with self.assertRaises(ValueError):
                        self.state.record(transaction=tx, namespace_operation_id=owner, **FIELDS)
                helper.assert_not_called()
        self.assertEqual(self.rows(), {t: [] for t in TABLES})

    def test_lost_or_replaced_transaction_cannot_publish_history(self):
        for replaced in (False, True):
            with self.subTest(replaced=replaced):
                with self.assertRaises(WriteOutcomeError) as caught:
                    with self.state.write_transaction() as tx:
                        tx._connection.rollback()  # Synthetic automatic transaction loss/bypass.
                        if replaced:
                            tx._connection.execute("BEGIN IMMEDIATE")
                        with patch.object(self.state, "_record_in_transaction") as helper:
                            try:
                                self.state.record(transaction=tx, **FIELDS)
                            finally:
                                helper.assert_not_called()
                self.assertEqual(caught.exception.outcome, AMBIGUOUS)
                self.assertIsInstance(caught.exception.cause, TransactionOwnershipError)
        self.assertEqual(self.rows(), {t: [] for t in TABLES})

    def test_manual_null_protection_is_preserved_and_forced_cleanup_cannot_remove_it(self):
        with self.state.connect() as c:
            c.execute("INSERT INTO protected_images VALUES(?,?,NULL,?)", (IMAGE_A, "manual", CREATED))
        before = self.rows()["protected_images"]
        history_id = self.state.record(**FIELDS)
        with self.state.connect() as c:
            c.execute("UPDATE rollback_refs SET created=? WHERE history_id=?", (CREATED, history_id))
        cfg = SimpleNamespace(data={"retention": {}}, log_dir=self.root / "logs")
        for forced in (False, True):
            with self.subTest(forced=forced), \
                 patch.object(retention, "_docker_image_accounting", return_value={}), \
                 patch.object(retention, "_container_image_ids", return_value=set()), \
                 patch.object(retention, "run", side_effect=AssertionError("unexpected external operation")):
                result = retention.cleanup(cfg, self.state, all_protected=forced)
            self.assertEqual(result["images_removed"], 0)
            self.assertTrue(any(item["reason"] == "SKIPPED - UNRESOLVED PROTECTION" for item in result["skipped"]))
            self.assertEqual(self.rows()["protected_images"], before)
        self.assertEqual(self.rows()["rollback_refs"][0][:2], (history_id, IMAGE_A))

    def test_other_history_protection_owner_and_expired_ref_are_not_reassigned(self):
        first = self.state.record(**FIELDS)
        with self.state.connect() as c:
            c.execute("UPDATE protected_images SET reason=?,created=?", ("original owner", CREATED))
            c.execute("UPDATE rollback_refs SET created=?,expired=1,purge_status='EXPIRED'", (CREATED,))
        before = self.rows()
        with self.state.write_transaction() as tx:
            second = self.state.record(transaction=tx, **{**FIELDS, "service": "worker"})
            third = self.state.record(transaction=tx, **{**FIELDS, "service": "api"})
        rows = self.rows()
        self.assertEqual(rows["protected_images"], before["protected_images"])
        self.assertEqual(rows["rollback_refs"][0], before["rollback_refs"][0])
        self.assertEqual([row[:2] for row in rows["rollback_refs"]], [(first, IMAGE_A), (second, IMAGE_A), (third, IMAGE_A)])
        self.assertTrue(all(row[3] == 0 for row in rows["rollback_refs"][1:]))
        self.assertEqual(retention._removal_blocker(self.state, IMAGE_A, check_active=True), "SKIPPED - SHARED ACTIVE ROLLBACK")

    def test_existing_ref_primary_key_conflict_rolls_back_instead_of_replacing(self):
        with self.state.connect() as c:
            c.execute("INSERT INTO rollback_refs(history_id,image_id) VALUES(?,?)", (1, IMAGE_B))
            c.execute("INSERT INTO protected_images VALUES(?,?,NULL,?)", (IMAGE_A, "manual", CREATED))
        before = self.rows()
        with self.assertRaises(sqlite3.IntegrityError):
            self.state.record(**FIELDS)
        self.assertEqual(self.rows(), before)
        with self.assertRaises(WriteOutcomeError) as caught:
            with self.state.write_transaction() as tx:
                self.state.record(transaction=tx, **FIELDS)
        self.assertEqual(caught.exception.outcome, NOT_COMMITTED)
        self.assertIsInstance(caught.exception.cause, sqlite3.IntegrityError)
        self.assertEqual(self.rows(), before)

    def test_late_protection_write_fault_rolls_back_history_and_ref(self):
        def protection_insert(sql, row):
            return sql.startswith("INSERT INTO protected_images")

        with sqlite_fault(self.state, protection_insert):
            with self.assertRaises(WriteOutcomeError) as caught:
                with self.state.write_transaction() as tx:
                    self.state.record(transaction=tx, **FIELDS)
        self.assertEqual(caught.exception.outcome, NOT_COMMITTED)
        self.assertEqual(self.rows(), {t: [] for t in TABLES})

    def test_unrelated_protection_constraint_failure_is_not_ignored(self):
        with self.state.connect() as c:
            c.execute("INSERT INTO protected_images VALUES(?,?,NULL,?)", (IMAGE_B, "rollback", CREATED))
            c.execute("CREATE UNIQUE INDEX synthetic_reason ON protected_images(reason)")
        before = self.rows()
        with self.assertRaises(sqlite3.IntegrityError):
            self.state.record(**FIELDS)
        self.assertEqual(self.rows(), before)

    def test_missing_or_mismatched_protection_postconditions_roll_back(self):
        faults = (
            "CREATE TRIGGER synthetic_protection BEFORE INSERT ON protected_images "
            "BEGIN SELECT RAISE(IGNORE); END",
            "CREATE TRIGGER synthetic_protection AFTER INSERT ON protected_images "
            "BEGIN UPDATE protected_images SET history_id=NULL; END",
            "CREATE TRIGGER synthetic_protection AFTER INSERT ON protected_images "
            "BEGIN UPDATE rollback_refs SET image_id='synthetic-mismatch'; END",
        )
        for fault in faults:
            with self.subTest(fault=fault):
                with self.state.connect() as c:
                    c.execute(fault)
                try:
                    with self.assertRaises(WriteOutcomeError) as caught:
                        with self.state.write_transaction() as tx:
                            self.state.record(transaction=tx, **FIELDS)
                    self.assertEqual(caught.exception.outcome, NOT_COMMITTED)
                    self.assertIsInstance(caught.exception.cause, ValueError)
                    self.assertEqual(self.rows(), {t: [] for t in TABLES})
                finally:
                    with self.state.connect() as c:
                        c.execute("DROP TRIGGER synthetic_protection")

    def test_record_keeps_blocked_compose_failure_and_rollback_ref_rules(self):
        cases = (("update", "BLOCKED", IMAGE_A, False),
                 ("compose_reconcile", "SUCCESS", IMAGE_A, False),
                 ("compose_bootstrap", "SUCCESS", IMAGE_A, False),
                 ("update", "SUCCESS", None, False),
                 ("update", "FAILED", IMAGE_A, True),
                 ("rollback", "ROLLED BACK", IMAGE_A, True),
                 ("update", "SUCCESS", IMAGE_A, True))
        for action, result, image, eligible in cases:
            with self.subTest(action=action, result=result, image=image):
                history_id = self.state.record(**{**FIELDS, "action": action, "result": result, "old_image_id": image})
                with self.state.read() as c:
                    ref = c.execute("SELECT image_id FROM rollback_refs WHERE history_id=?", (history_id,)).fetchone()
                self.assertEqual(tuple(ref) if ref else None, (image,) if eligible else None)


if __name__ == "__main__":
    unittest.main()
