"""Dormant owner/evidence lifecycle; synthetic SQLite only."""
import _upt_isolation  # noqa: F401
import ast
import json
import sqlite3
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from docker_update_manager import completion_fence as f, completion_lifecycle as life
from docker_update_manager.state import State
from docker_update_manager.sqlite_outcome import WriteOutcomeError, TransactionOwnershipError, OwnedResult
from sqlite_faults import sqlite_fault, select_from
from test_g2_completion_fences import insert_fence, insert_claim, compose_service

NOW = "2026-10-27T00:00:00+00:00"
LATER = "2026-10-27T00:01:00+00:00"
IMAGE = "sha256:" + "a" * 64
OTHER = "sha256:" + "b" * 64
CLAIMS = (("backup", "backups/example"), ("image", IMAGE))


class LifecycleTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.state = State(Path(temp.name))
        self.state.ensure_compatible()
        self.owner = life.prepare_owner(operation_id="1" * 32, stack="example", compose_service="app",
            action="update", working_dir="/example", compose_files=("/example/compose.yml",),
            claims=CLAIMS, created_at=NOW)

    def write(self, method, *args, **kw):
        with self.state.write_transaction() as tx:
            result = method(self.state, tx, *args, **kw)
        return result

    def read(self, owner=None):
        with self.state.read() as c:
            return f.exact_fence(c, (owner or self.owner).operation_id)

    def arm(self):
        return self.write(life.arm, self.owner)

    def resolve(self, owner):
        return self.write(life.resolve_active, owner, proof="succeeded", history_id=1, request_digest="f" * 64, updated_at=LATER)

    def enriched(self, owner=None):
        owner = owner or self.owner
        data = json.loads(owner.evidence)
        data["revision"] += 1
        data["claims"].append(["image", OTHER])
        data["facts"]["target_image_id"] = OTHER
        return json.dumps(data)

    def test_arm_enrich_resolve_and_receipt(self):
        self.assertEqual(self.arm(), self.owner)
        enriched = self.write(life.enrich, self.owner, evidence=self.enriched(), updated_at=LATER)
        self.assertEqual(enriched.claims, CLAIMS + (("image", OTHER),))
        resolved = self.resolve(enriched)
        self.assertEqual(self.read(), resolved)
        receipt = json.loads(resolved.evidence)["receipt"]
        self.assertEqual(receipt["prior_evidence_digest"], f.evidence_digest(enriched.evidence))
        self.assertEqual(receipt["request_digest"], "f" * 64)
        with self.state.read() as c:
            self.assertEqual(f.unresolved_fences(c), ())
            self.assertEqual(c.execute("SELECT count(*) FROM completion_fence_claims").fetchone()[0], 3)
            self.assertTrue(f.schema_matches(c))
            self.assertEqual(c.execute("SELECT version FROM schema_version").fetchone()[0], 10)

    def test_manual_is_retained_and_cannot_automatically_resolve_or_enrich(self):
        self.arm()
        manual = self.write(life.mark_manual, self.owner, updated_at=LATER)
        self.assertEqual(self.read(), manual)
        for method, kw in ((life.resolve_active, dict(proof="no_effect", history_id=1, request_digest="f" * 64)),
                           (life.mark_manual, {}), (life.enrich, dict(evidence=self.enriched(manual)))):
            with self.subTest(method=method.__name__), self.assertRaises(WriteOutcomeError):
                self.write(method, manual, updated_at=LATER, **kw)
            self.assertEqual(self.read(), manual)
        with self.state.read() as c:
            self.assertEqual(f.unresolved_claims(c)["image"], frozenset({IMAGE}))

    def test_every_snapshot_field_must_match(self):
        self.arm()
        changes = dict(operation_id="2" * 32, logical_key="0" * 64, stack="other", compose_service="other",
            action="rollback", lifecycle="manual", working_dir="/other", compose_files=("/other/a.yml",),
            created_at=LATER, updated_at=LATER, evidence=self.enriched(), claims=())
        for field, value in changes.items():
            for method, kw in ((life.mark_manual, {}), (life.enrich, dict(evidence=self.enriched())),
                               (life.resolve_active, dict(proof="succeeded", history_id=1, request_digest="f" * 64))):
                with self.subTest(field=field, method=method.__name__), self.assertRaises(WriteOutcomeError) as caught:
                    self.write(method, replace(self.owner, **{field: value}), updated_at=LATER, **kw)
                self.assertEqual(caught.exception.outcome, "not_committed")
                self.assertEqual(self.read(), self.owner)

    def test_duplicate_arming_and_logical_collision_preserve_owner(self):
        self.arm()
        for owner in (self.owner, replace(self.owner, operation_id="2" * 32)):
            with self.assertRaises(WriteOutcomeError):
                self.write(life.arm, owner)
        self.assertEqual(self.read(), self.owner)

    def test_enrichment_is_additive_revisioned_and_cannot_replace_facts(self):
        self.arm()
        enriched = self.write(life.enrich, self.owner, evidence=self.enriched(), updated_at=LATER)
        for mutate in (lambda d: d.update(revision=1), lambda d: d.update(version=3),
                       lambda d: d.update(claims=[]), lambda d: d.update(working_dir="/other"),
                       lambda d: d["facts"].update(target_image_id=IMAGE), lambda d: d.update(facts={})):
            data = json.loads(enriched.evidence)
            data["revision"] += 1
            mutate(data)
            with self.assertRaises(WriteOutcomeError):
                self.write(life.enrich, enriched, evidence=json.dumps(data), updated_at=LATER)
            self.assertEqual(self.read(), enriched)

    def test_v1_active_manual_resolved_and_explicit_upgrade(self):
        service = compose_service(Path("/example"))
        for op, lifecycle in (("3" * 32, "active"), ("4" * 32, "manual"), ("5" * 32, "resolved")):
            service.compose_service = lifecycle
            insert_fence(self.state, op, service, lifecycle=lifecycle)
            if lifecycle != "resolved":
                insert_claim(self.state, op, "image", IMAGE)
            with self.state.read() as c:
                owner = f.exact_fence(c, op)
            self.assertEqual(owner.lifecycle, lifecycle)
            if lifecycle == "active":
                with self.assertRaises(WriteOutcomeError):
                    self.resolve(owner)
                data = json.loads(owner.evidence)
                data.update(version=2, revision=1, claims=[["image", IMAGE]], facts={}, receipt=None)
                upgraded = self.write(life.enrich, owner, evidence=json.dumps(data), updated_at=LATER)
                self.resolve(upgraded)

    def test_shared_image_backup_and_legacy_owners_survive_resolution(self):
        self.arm()
        other = life.prepare_owner(operation_id="2" * 32, stack="example", compose_service="worker",
            action="rollback", working_dir="/example", compose_files=("/example/compose.yml",),
            claims=CLAIMS, created_at=NOW)
        self.write(life.arm, other)
        manual = self.write(life.mark_manual, other, updated_at=LATER)
        with self.state.connect() as c:
            c.execute("INSERT INTO protected_images(image_id,reason,history_id,created) VALUES(?,?,NULL,?)",
                      (IMAGE, "manual", NOW))
        self.resolve(self.owner)
        self.assertEqual(self.read(other), manual)
        from docker_update_manager import retention
        self.assertEqual(retention._image_claim_blocker(self.state, IMAGE),
                         "SKIPPED - UNRESOLVED RECOVERY CLAIM")
        self.assertIn("backups/example", retention._claimed_backups(self.state))
        with self.state.read() as c:
            self.assertEqual(f.unresolved_claims(c), {"image": frozenset({IMAGE}), "backup": frozenset({"backups/example"})})
            self.assertEqual(tuple(c.execute("SELECT * FROM protected_images").fetchone()), (IMAGE, "manual", None, NOW))
            self.assertEqual(c.execute("SELECT count(*) FROM completion_fence_claims").fetchone()[0], 4)

    def test_strict_evidence_versions_shapes_duplicate_keys_and_sizes(self):
        valid = json.loads(self.owner.evidence)
        bad = ["{}", "[]", '{"version":1,"version":2}', "x" * 65537]
        for field, value in (("version", True), ("version", 3), ("revision", True), ("revision", 0),
                ("claims", [["image", "short"]]), ("claims", [["image", IMAGE], ["image", IMAGE]]),
                ("facts", {"secret": "x"}), ("facts", {"target_image_id": "short"}),
                ("facts", {"backup_sha256": {"backups/unclaimed": "f" * 64}}),
                ("receipt", {}), ("compose_files", ["relative"]), ("working_dir", "/example/../other")):
            bad.append(json.dumps(dict(valid, **{field: value})))
        for evidence in bad:
            with self.subTest(evidence=evidence[:80]), self.assertRaises(f.CompletionFenceError):
                f._evidence(evidence)
        with self.assertRaises(WriteOutcomeError):
            self.write(life.arm, replace(self.owner, evidence=self.owner.evidence.replace('"receipt":null', '"receipt":{}')))

    def test_claim_mismatch_and_malformed_resolved_evidence_refuse(self):
        self.arm()
        with self.state.connect() as c:
            c.execute("INSERT INTO completion_fence_claims VALUES(?,?,?)", (self.owner.operation_id, "image", OTHER))
        with self.assertRaises(f.CompletionFenceError):
            self.read()
        with self.assertRaises(f.CompletionFenceError):
            self.state.completion_claims()
        with self.state.connect() as c:
            c.execute("UPDATE completion_fences SET lifecycle='resolved',evidence='{}'")
        with self.assertRaises(f.CompletionFenceError):
            self.read()

    def test_read_errors_are_not_absence(self):
        self.arm()
        with sqlite_fault(self.state, select_from("completion_fences"), mode="read_failure"):
            with self.assertRaises(sqlite3.OperationalError):
                self.read()
        self.assertEqual(self.read(), self.owner)

    def test_transaction_failure_at_each_arm_statement_rolls_back(self):
        for prefix in ("INSERT INTO completion_fences", "INSERT INTO completion_fence_claims"):
            with sqlite_fault(self.state, lambda sql, row: sql.startswith(prefix)):
                with self.assertRaises(WriteOutcomeError) as caught:
                    self.arm()
            self.assertEqual(caught.exception.outcome, "not_committed")
            self.assertIsInstance(caught.exception.cause, sqlite3.OperationalError)
            self.assertIsNone(self.read())
            with self.state.read() as c:
                self.assertEqual(c.execute("SELECT count(*) FROM completion_fence_claims").fetchone()[0], 0)

    def test_enrichment_and_resolution_failures_roll_back(self):
        self.arm()
        for method, kw in ((life.enrich, dict(evidence=self.enriched())),
                           (life.resolve_active, dict(proof="safe_recovery", history_id=1, request_digest="f" * 64)),
                           (life.mark_manual, {})):
            with sqlite_fault(self.state, lambda sql, row: sql.startswith("UPDATE completion_fences")):
                with self.assertRaises(WriteOutcomeError):
                    self.write(method, self.owner, updated_at=LATER, **kw)
            self.assertEqual(self.read(), self.owner)

    def test_rowcount_zero_or_two_rolls_back_real_update(self):
        self.arm()
        for count in (0, 2):
            with patch.object(OwnedResult, "rowcount", property(lambda self: count)):
                with self.assertRaises(WriteOutcomeError) as caught:
                    self.resolve(self.owner)
            self.assertIsInstance(caught.exception.cause, f.CompletionFenceError)
            self.assertEqual(self.read(), self.owner)

    def test_invalid_resolution_proof_never_releases(self):
        self.arm()
        for proof, digest in (("failed", "f" * 64), (None, "f" * 64), ("succeeded", "short")):
            with self.assertRaises(WriteOutcomeError):
                self.write(life.resolve_active, self.owner, proof=proof, history_id=1, request_digest=digest, updated_at=LATER)
            self.assertEqual(self.read(), self.owner)

    def test_after_commit_ambiguity_is_not_success_and_durable_snapshot_is_readable(self):
        for method in ("arm", "enrich", "manual"):
            if method != "arm" and self.read() is None:
                self.arm()
            expected = self.read()
            match = lambda sql, row: sql.startswith("INSERT INTO completion_fences" if method == "arm" else "UPDATE completion_fences")
            with sqlite_fault(self.state, match, mode="after_commit"):
                with self.assertRaises(WriteOutcomeError) as caught:
                    if method == "arm":
                        self.arm()
                    elif method == "enrich":
                        self.write(life.enrich, expected, evidence=self.enriched(expected), updated_at=LATER)
                    elif method == "manual":
                        self.write(life.mark_manual, expected, updated_at=LATER)
            self.assertEqual(caught.exception.outcome, "ambiguous")
            self.assertEqual(caught.exception.phase, "commit")
            self.assertIsInstance(caught.exception.cause, sqlite3.OperationalError)
            self.assertIsNotNone(self.read())
        # Dedicated active owner for ambiguous resolution.
        other = replace(self.owner, operation_id="9" * 32, compose_service="worker")
        from docker_update_manager.compose_baseline import logical_key
        other = replace(other, logical_key=logical_key(other.stack, other.compose_service, other.working_dir, other.compose_files))
        self.write(life.arm, other)
        with sqlite_fault(self.state, lambda sql, row: sql.startswith("UPDATE completion_fences"), mode="after_commit"):
            with self.assertRaises(WriteOutcomeError) as caught:
                self.resolve(other)
        self.assertEqual(caught.exception.outcome, "ambiguous")
        self.assertEqual(self.read(other).lifecycle, "resolved")

    def test_foreign_raw_and_expired_transactions_refuse(self):
        other = State(self.state.root)
        for method, kw in ((life.arm, {}), (life.mark_manual, dict(updated_at=LATER)),
                (life.enrich, dict(updated_at=LATER, evidence=self.enriched())),
                (life.resolve_active, dict(updated_at=LATER, proof="succeeded", history_id=1, request_digest="f" * 64))):
            with self.subTest(method=method.__name__):
                with other.write_transaction() as tx:
                    with self.assertRaises(TransactionOwnershipError):
                        method(self.state, tx, self.owner, **kw)
                with self.assertRaises(TransactionOwnershipError):
                    method(other, tx, self.owner, **kw)
                with self.state.connect() as raw:
                    with self.assertRaises(TransactionOwnershipError):
                        method(self.state, raw, self.owner, **kw)

    def test_no_live_module_imports_dormant_writers(self):
        source = Path(life.__file__).parent
        for path in source.rglob("*.py"):
            if path.name == "completion_lifecycle.py":
                continue
            tree = ast.parse(path.read_text())
            for node in ast.walk(tree):
                if isinstance(node, (ast.Import, ast.ImportFrom)):
                    names = [alias.name for alias in node.names] + [getattr(node, "module", "") or ""]
                    self.assertFalse(any("completion_lifecycle" in name for name in names), path.name)

    def test_cross_thread_loan_refuses_before_sql(self):
        import threading
        errors = []
        with self.state.write_transaction() as tx:
            def work():
                try:
                    life.arm(self.state, tx, self.owner)
                except Exception as exc:
                    errors.append(exc)
            thread = threading.Thread(target=work)
            thread.start()
            thread.join(2)
            self.assertFalse(thread.is_alive())
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], TransactionOwnershipError)
        self.assertIsNone(self.read())

    def test_failed_final_verification_discards_claims_and_transition(self):
        self.arm()
        original = f.exact_fence
        calls = []
        def fail_final(*args):
            calls.append(1)
            if len(calls) == 2:
                raise sqlite3.OperationalError("synthetic read failure")
            return original(*args)
        with patch.object(f, "exact_fence", side_effect=fail_final):
            with self.assertRaises(WriteOutcomeError) as caught:
                self.write(life.enrich, self.owner, evidence=self.enriched(), updated_at=LATER)
        self.assertEqual(caught.exception.outcome, "not_committed")
        self.assertEqual(self.read(), self.owner)

    def test_commit_error_before_durability_is_still_ambiguous(self):
        with sqlite_fault(self.state, lambda sql, row: sql.startswith("INSERT INTO completion_fences"),
                          mode="commit_before_durability"):
            with self.assertRaises(WriteOutcomeError) as caught:
                self.arm()
        self.assertEqual(caught.exception.outcome, "ambiguous")
        self.assertEqual(caught.exception.phase, "commit")
        self.assertIsNone(self.read())

    def test_ambiguous_commit_with_unreadable_snapshot_never_rearms(self):
        with sqlite_fault(self.state, lambda sql, row: sql.startswith("INSERT INTO completion_fences"),
                          mode="after_commit"):
            with self.assertRaises(WriteOutcomeError) as caught:
                self.arm()
        with sqlite_fault(self.state, select_from("completion_fences"), mode="read_failure"):
            with self.assertRaises(sqlite3.OperationalError):
                self.read()
        self.assertEqual(caught.exception.outcome, "ambiguous")
        self.assertEqual(self.read(), self.owner)
        with self.state.read() as c:
            self.assertEqual(c.execute("SELECT count(*) FROM completion_fences").fetchone()[0], 1)

    def test_transaction_loss_stays_ambiguous(self):
        with self.assertRaises(WriteOutcomeError) as caught:
            with self.state.write_transaction() as tx:
                tx._connection.rollback()  # Synthetic automatic transaction loss.
                life.arm(self.state, tx, self.owner)
        self.assertEqual(caught.exception.outcome, "ambiguous")
        self.assertIsNone(self.read())

    def test_resolved_owner_is_final_for_every_primitive(self):
        self.arm()
        resolved = self.resolve(self.owner)
        for method, kw in ((life.mark_manual, {}), (life.enrich, dict(evidence=self.enriched(resolved))),
                (life.resolve_active, dict(proof="no_effect", history_id=1, request_digest="f" * 64))):
            with self.assertRaises(WriteOutcomeError):
                self.write(method, resolved, updated_at=LATER, **kw)
            self.assertEqual(self.read(), resolved)

    def test_namespace_owner_prevents_dual_arming(self):
        with self.state.connect() as c:
            c.execute("INSERT INTO compose_bootstrap_fences VALUES(?,?,?,?,?,?)",
                      (self.owner.logical_key, "8" * 32, "{}", "active", NOW, NOW))
        with self.assertRaises(WriteOutcomeError):
            self.arm()
        self.assertIsNone(self.read())
        with self.state.read() as c:
            self.assertEqual(c.execute("SELECT lifecycle FROM compose_bootstrap_fences").fetchone()[0], "active")

    def test_arming_rejects_service_identity_drift(self):
        self.arm()
        drifted = life.prepare_owner(operation_id="2" * 32, stack="example", compose_service="app",
            action="update", working_dir="/moved", compose_files=("/moved/compose.yml",),
            claims=CLAIMS, created_at=NOW)
        with self.assertRaises(WriteOutcomeError):
            self.write(life.arm, drifted)
        self.assertEqual(self.read(), self.owner)
        self.assertIsNone(self.read(drifted))
