# G2-S0c-A — Transaction composition and non-destructive History protection

**Implementation:** [PR #22](https://github.com/rdna897/upt/pull/22), following the approved [S0c-A architecture proposal](g2-s0c-a-transaction-publication-proposal.md) in [PR #21](https://github.com/rdna897/upt/pull/21). This record describes the implemented foundation, not activation of G2 completion handling.

**Scope and freeze:** Core State transaction composition, standalone compatibility, legacy image-protection ownership and synthetic regressions only. No schema change, G2 completion-fence writer, runtime handler integration, web terminal change, state migration, Docker mutation or production deployment is authorised. The production deployment freeze remains in force.

## Implemented transaction boundary

- `State.record(*, transaction=None, namespace_operation_id=None, **fields)` preserves the existing standalone call pattern: serialize the supported details before opening `State.connect()`, enter one legacy write transaction, return an integer History ID and propagate original exception types.
- The SQL body is shared in `State._record_in_transaction(...)`. With `transaction=tx`, `record()` executes only through the existing `State.write_transaction()` owned handle: no additional connection, `BEGIN`, savepoint, `read()` or commit. The outer context is the only commit/rollback authority; IDs returned inside it are **provisional** until a successful context exit.
- Borrowed calls require an exact `OwnedTransaction` issued by the same `State` instance. Its private issuer token, issuing thread, open scope, transaction state and transaction-control trace checks reject foreign, raw, expired, cross-thread and ended/replaced loans before the History SQL. Existing `WriteOutcomeError` classification is preserved: failed body writes whose original transaction can be rolled back are `not_committed`; commit-acknowledgement failure remains conservatively `ambiguous`.
- Borrowing is refused when `namespace_operation_id` is supplied. Standalone namespace publication retains its existing validation, History, operation marker, legacy bootstrap-fence resolution and completion-event SQL order in one `State.connect()` transaction. The namespace-specific protocol is **not** migrated to G2.

## Legacy image protection and rollback references

For each History row satisfying the existing eligibility predicate (nonempty `old_image_id`, result not `BLOCKED`, action neither `compose_reconcile` nor `compose_bootstrap`), publication now:

1. Adds a separate `rollback_refs(history_id, image_id)` with a plain `INSERT`; an integrity conflict fails rather than overwriting another ref.
2. Inserts the legacy `protected_images` row only when its `image_id` is absent, using `ON CONFLICT(image_id) DO NOTHING`.
3. Checks that its rollback reference and the resulting protection image agree inside the same transaction, and that a newly inserted protection row belongs to this History entry. A failed check rolls back the attempted transaction.

An existing `protected_images` row is **not reassigned**, even when its `history_id` is `NULL` or belongs to an older History entry. Its reason and timestamp remain unchanged. Multiple History rollback references can protect the same image independently; unresolved G2 owners' image/backup claims continue to have their own authority. No G2 tables are written by this change.

**Intentional retention consequence:** a pre-existing manual or namespace `NULL history_id` protection remains unresolved after a new History record, including after namespace success. It may protect an image indefinitely, even when new rollback refs expire or cleanup is forced. This is the approved fail-closed safety cost, not an automatic migration or reason to remove the legacy owner. The single-row `protected_images` schema cannot encode multiple legacy owners; `rollback_refs` and G2 claims retain their separate roles.

## Verification

The new `tests/test_state_record_transactions.py` exercises standalone compatibility, one outer commit for borrowed calls, provisional IDs, atomic rollback of History/refs/protection, original versus classified failure behaviour, pre-/post-commit injection, foreign/raw/expired/cross-thread handles, automatic transaction loss or bypass, insertion conflicts, preservation of `NULL` and older owners, cleanup refusal, and the pre-existing rollback-ref eligibility rules.

Related regressions cover successful namespace publication retaining its original `NULL history_id` owner while adding a rollback ref, unchanged namespace legacy-fence completion, G2 shared-claim independence and retention writer/contention behaviour under concurrent publication. Existing characterisation tests for the still-unfixed S1/S2/S3 completion paths remain in place.

Implementation-head validation is recorded in PR #22: a focused isolated run of 166 tests and full local Python 3.13 discovery of 1,451 tests (56 skipped), plus exact-head GitHub Actions covering Python 3.10, Python 3.13, Chromium, runner-smoke, plan and `merge-gate`. Documentation-only changes to the PR require their own final-head CI verification before merge.

## Remaining gates

This is **S0c-A only**. S0c-B must separately implement strict owner/evidence readers, dormant fence lifecycle primitives, full-bundle idempotency, four-state publication classification (`committed`, `not_committed`, `ambiguous`, `contradictory`), complete-key lookup, bounded reconciliation/deadline checks, and evidence/retention compatibility. S0c-C must separately implement audited operator-only reconciliation and safe release.

No G2 completion writer may be connected to a live update/rollback/startup path before its matching S1/S2/S3 corrected handler, web compatibility/restart requirements where applicable, and audited operator recovery are available and approved. A merged foundation is not deployment approval.
