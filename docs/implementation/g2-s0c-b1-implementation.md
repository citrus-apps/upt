# G2-S0c-B1 — Fence evidence and lifecycle

Implements the B1 split in the [S0c-A proposal](g2-s0c-a-transaction-publication-proposal.md).
Schema 10, its exact DDL and core identity stay unchanged. No migration or live
writer activation is included. Production deployment remains frozen.

`completion_fence.exact_fence()` reads active, manual and resolved owners with
validated identity, timestamps, evidence and exact owner claims. Missing owners
return `None`; failed reads propagate. Admission and retention accept v1 and v2.
V1 rows are preserved, including resolved rows without receipts.

V2 retains `working_dir` and ordered `compose_files`. Its exact extra fields are:

- `revision`: positive bounded integer; enrichment and resolution advance it once.
- `claims`: sorted unique `[kind, value]` pairs matching the stored owner claims.
- `facts`: optional `running_image_id`, `target_image_id`, `source_sha256` and
  `backup_sha256` entries. Image IDs are full SHA-256 identities. Digest maps use
  absolute source paths or claimed managed backup names and lowercase SHA-256.
- `receipt`: null while unresolved; resolved evidence binds `completion_id`,
  positive `history_id`, `request_digest`, `prior_evidence_digest` and proof class
  (`succeeded`, `no_effect` or `safe_recovery`). Digests have 64 lowercase hex
  characters. The prior digest hashes the exact pre-resolution UTF-8 evidence.

Evidence stays within 64 KiB. Unknown fields/versions, duplicate JSON keys,
invalid claims and receipt/lifecycle mismatches refuse. Evidence is private core
data; it contains no source contents or environment values and is not exposed
through HTTP or SSE. Old schema-10 binaries reject unresolved v2: equal schema
numbers do not establish evidence compatibility.

`completion_lifecycle` supplies pure owner preparation and dormant SQL primitives
for arming, additive active enrichment, active→manual and conditional
active→resolved. Each writer requires the issuing State's live owned transaction
and exact expected snapshot, including immutable identity, evidence bytes,
timestamps and claims. Arming refuses namespace owners. Enrichment preserves all
claims and facts; v1 upgrade is explicit. Resolution requires v2, leaves claim
rows stored and cannot release manual owners. Every update requires exactly one
row and verifies the resulting snapshot. Independent G2 and legacy protections
are never reassigned or removed.

These are internal storage steps, not release commands. Trusted future callers
must hold `update.lock`, collect independent proof outside SQLite, and propagate
all errors out of the transaction. Returned snapshots are provisional until
normal context exit. Commit failures remain ambiguous even when a later read
finds absence. No retry, effect or release follows from an unverified result.

`tests/test_completion_lifecycle.py` covers exact-owner mismatches, v1/v2,
invalid evidence, additive claims, shared image/backup ownership, lifecycle
refusals, namespace isolation, transaction ownership/loss, statement and final
verification failures, defensive row counts and commit failures before and after
durability. Existing isolation, namespace/Compose and retention suites remain.
An explicit writer allowlist and import-boundary test keep every live module
from calling these writers.

B2 remains separate: atomic History/ref/protection publication, complete-bundle
verification, canonical request digests, deadline enforcement, read-back/retries
and retention dependencies. B1 receipts alone certify no History or workload
outcome. C must add audited operator recovery before any matching S1–S3 activation.
No automatic pruning or production evidence rewrite is added.
