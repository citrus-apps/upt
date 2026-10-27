"""Dormant G2-S0c-B1 SQL primitives; no live callers.

Use only inside the issuing State.write_transaction(), under update.lock.
Prepare external observations before entry. Let every error escape the context:
returned snapshots are provisional until normal commit. Ambiguity never grants
permission for effects, retry or release. B2 supplies publication/read-back;
C supplies audited operator release. Neither is implemented here.
"""
import json
from dataclasses import replace

from . import completion_fence as f
from .sqlite_outcome import OwnedTransaction, TransactionOwnershipError


def _transaction(state, tx):
    if type(tx) is not OwnedTransaction:
        raise TransactionOwnershipError("fence writes require an owned transaction")
    tx._assert_owner(state._transaction_owner)


def _row(owner):
    return (owner.operation_id, owner.logical_key, owner.stack, owner.compose_service,
            owner.action, owner.lifecycle, owner.evidence, owner.created_at, owner.updated_at)


def _validate(owner):
    if type(owner) is not f.Fence or f._fence(_row(owner)) != replace(owner, claims=()):
        raise f._invalid("owner snapshot")
    if (type(owner.claims) is not tuple or any(type(x) is not tuple or len(x) != 2
            or not f.valid_claim(*x) for x in owner.claims)
            or owner.claims != tuple(sorted(set(owner.claims)))):
        raise f._invalid("claims snapshot")
    data = f._evidence(owner.evidence)
    if data["version"] == 2 and owner.claims != tuple(map(tuple, data["claims"])):
        raise f._invalid("claim evidence mismatch")
    return data


def _expect(tx, expected):
    data = _validate(expected)
    if f.exact_fence(tx, expected.operation_id) != expected:
        raise f._invalid("owner changed")
    return data


def _encode(data):
    text = json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    f._evidence(text)
    return text


def prepare_owner(*, operation_id, stack, compose_service, action, working_dir,
                  compose_files, claims, created_at, facts=None):
    """Pure preparation of a new v2 active snapshot; no filesystem observation."""
    from .compose_baseline import logical_key
    claims = tuple(sorted(set(claims)))
    evidence = _encode(dict(version=2, working_dir=working_dir, compose_files=list(compose_files),
                            revision=1, claims=[list(x) for x in claims], facts={} if facts is None else facts, receipt=None))
    owner = f.Fence(operation_id, logical_key(stack, compose_service, working_dir, compose_files),
                    stack, compose_service, action, "active", working_dir, tuple(compose_files),
                    evidence, created_at, created_at, claims)
    _validate(owner)
    return owner


def arm(state, tx, owner):
    """Insert a new exact owner and claims. Duplicate arming is an error."""
    _transaction(state, tx)
    data = _validate(owner)
    if owner.lifecycle != "active" or data["version"] != 2 or data["revision"] != 1:
        raise f._invalid("initial lifecycle")
    if f.exact_fence(tx, owner.operation_id) is not None:
        raise f._invalid("owner exists")
    if f.service_refusal(f.unresolved_fences(tx), owner):
        raise f._invalid("conflicting service owner")
    if tx.execute("SELECT 1 FROM compose_bootstrap_fences WHERE operation_id=? "
                  "OR (logical_key=? AND lifecycle IS NOT 'resolved') LIMIT 1",
                  (owner.operation_id, owner.logical_key)).fetchone() is not None:
        raise f._invalid("namespace owner")
    tx.execute("INSERT INTO completion_fences (" + f._COLUMNS + ") VALUES(?,?,?,?,?,?,?,?,?)", _row(owner))
    for kind, value in owner.claims:
        tx.execute("INSERT INTO completion_fence_claims(operation_id,kind,value) VALUES(?,?,?)",
                   (owner.operation_id, kind, value))
    if f.exact_fence(tx, owner.operation_id) != owner:
        raise f._invalid("arm verification")
    return owner


def _change(tx, expected, replacement):
    _validate(replacement)
    result = tx.execute(
        "UPDATE completion_fences SET lifecycle=?,evidence=?,updated_at=? "
        "WHERE operation_id=? AND logical_key=? AND stack=? AND compose_service=? "
        "AND action=? AND lifecycle=? AND evidence=? AND created_at=? AND updated_at=?",
        (replacement.lifecycle, replacement.evidence, replacement.updated_at,
         expected.operation_id, expected.logical_key, expected.stack, expected.compose_service,
         expected.action, expected.lifecycle, expected.evidence, expected.created_at, expected.updated_at))
    if result.rowcount != 1:
        raise f._invalid("transition count")
    if f.exact_fence(tx, expected.operation_id) != replacement:
        raise f._invalid("transition verification")
    return replacement


def enrich(state, tx, expected, *, evidence, updated_at):
    """Add claims/facts under an active owner; v1 may explicitly upgrade to v2."""
    _transaction(state, tx)
    old = _expect(tx, expected)
    new = f._evidence(evidence)
    if (expected.lifecycle != "active" or new["version"] != 2 or new["receipt"] is not None
            or new["revision"] != old.get("revision", 0) + 1
            or any(new[k] != old[k] for k in ("working_dir", "compose_files"))):
        raise f._invalid("enrichment transition")
    claims = tuple(map(tuple, new["claims"]))
    if not set(expected.claims) <= set(claims):
        raise f._invalid("claim loss")
    for key, value in old.get("facts", {}).items():
        incoming = new["facts"].get(key)
        if isinstance(value, dict):
            if not isinstance(incoming, dict) or any(incoming.get(k) != v for k, v in value.items()):
                raise f._invalid("fact replacement")
        elif incoming != value:
            raise f._invalid("fact replacement")
    for kind, value in sorted(set(claims) - set(expected.claims)):
        tx.execute("INSERT INTO completion_fence_claims(operation_id,kind,value) VALUES(?,?,?)",
                   (expected.operation_id, kind, value))
    return _change(tx, expected, replace(expected, evidence=evidence, claims=claims, updated_at=updated_at))


def mark_manual(state, tx, expected, *, updated_at):
    """Only active→manual. No claim, evidence or independent owner is consumed."""
    _transaction(state, tx)
    _expect(tx, expected)
    if expected.lifecycle != "active":
        raise f._invalid("manual transition")
    return _change(tx, expected, replace(expected, lifecycle="manual", updated_at=updated_at))


def resolve_active(state, tx, expected, *, proof, history_id, request_digest, updated_at):
    """Conditional active→resolved storage step for a future atomic publisher.

    The trusted caller must independently establish proof and bind its frozen
    request digest. This function does not certify History or workload facts.
    Never use it as a standalone unlock. Manual release belongs to C.
    """
    _transaction(state, tx)
    data = _expect(tx, expected)
    if expected.lifecycle != "active" or data["version"] != 2:
        raise f._invalid("resolution transition")
    data["revision"] += 1
    data["receipt"] = dict(proof=proof, completion_id=expected.operation_id, history_id=history_id,
                           request_digest=request_digest,
                           prior_evidence_digest=f.evidence_digest(expected.evidence))
    return _change(tx, expected, replace(expected, lifecycle="resolved", evidence=_encode(data), updated_at=updated_at))
