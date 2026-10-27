"""G2 schema-10 recovery fences and claims: strict readers and inert lifecycle storage.

Policy: docs/implementation/g2-s0-recovery-fence-design.md (Option B, D3/D3b/
D3c/D10). S0c-B1 adds dormant writers in completion_lifecycle.py. Admission and
deletion paths only read these tables; no live workflow invokes the writers.

Every reader validates strictly. A malformed, unknown or unreadable record
raises CompletionFenceError (or the underlying sqlite3.Error); callers must
treat that as a refusal, never as an empty protection set.
"""
from __future__ import annotations

import json
import hashlib
import os
import re
import sqlite3
from dataclasses import dataclass

UNRESOLVED = ("active", "manual")
ACTIONS = ("update", "pinned_update", "rollback", "web_rollback")
CLAIM_KINDS = ("image", "backup")
# Backup claims name one direct child of a managed recovery root.
BACKUP_CLAIM_ROOTS = ("backups", "conversion-file-backups", "conversion-snapshots", "restore-bundles")
EVIDENCE_VERSION = 2
MAX_EVIDENCE_BYTES = 64 * 1024
MAX_COMPOSE_FILES = 32
MAX_PATH = 4096
MAX_NAME = 255

_OPERATION_ID = re.compile(r"[0-9a-f]{32}")
_LOGICAL_KEY = re.compile(r"[0-9a-f]{64}")
_IMAGE_ID = re.compile(r"sha256:[0-9a-f]{64}")
_BACKUP_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,254}")

# Statements are executed one at a time: trigger bodies contain semicolons.
# The exact stored SQL is part of the schema-10 compatibility identity, so any
# edit here is a schema change that needs its own migration.
COMPLETION_SCHEMA = (
    "CREATE TABLE completion_fences("
    "operation_id TEXT NOT NULL PRIMARY KEY CHECK(typeof(operation_id)='text' AND length(operation_id)=32 "
    "AND operation_id NOT GLOB '*[^0-9a-f]*'), "
    "logical_key TEXT NOT NULL CHECK(typeof(logical_key)='text' AND length(logical_key)=64 "
    "AND logical_key NOT GLOB '*[^0-9a-f]*'), "
    "stack TEXT NOT NULL CHECK(typeof(stack)='text' AND length(stack) BETWEEN 1 AND 255), "
    "compose_service TEXT NOT NULL CHECK(typeof(compose_service)='text' AND length(compose_service) BETWEEN 1 AND 255), "
    "action TEXT NOT NULL CHECK(action IN ('update','pinned_update','rollback','web_rollback')), "
    "lifecycle TEXT NOT NULL CHECK(lifecycle IN ('active','manual','resolved')), "
    "evidence TEXT NOT NULL CHECK(typeof(evidence)='text' AND length(evidence) BETWEEN 2 AND 65536), "
    "created_at TEXT NOT NULL CHECK(typeof(created_at)='text' AND length(created_at) BETWEEN 1 AND 64), "
    "updated_at TEXT NOT NULL CHECK(typeof(updated_at)='text' AND length(updated_at) BETWEEN 1 AND 64))",
    # At most one unresolved G2 fence per logical service.
    "CREATE UNIQUE INDEX idx_completion_fences_unresolved ON completion_fences(logical_key) "
    "WHERE lifecycle IN ('active','manual')",
    "CREATE INDEX idx_completion_fences_service ON completion_fences(stack,compose_service) "
    "WHERE lifecycle IN ('active','manual')",
    "CREATE TABLE completion_fence_claims("
    "operation_id TEXT NOT NULL REFERENCES completion_fences(operation_id) ON UPDATE RESTRICT ON DELETE RESTRICT, "
    "kind TEXT NOT NULL CHECK(kind IN ('image','backup')), "
    "value TEXT NOT NULL CHECK(typeof(value)='text' AND ("
    "(kind='image' AND length(value)=71 AND substr(value,1,7)='sha256:' AND substr(value,8) NOT GLOB '*[^0-9a-f]*') "
    "OR (kind='backup' AND length(value) BETWEEN 3 AND 512 AND instr(value,'..')=0 AND value NOT GLOB '*/*/*' "
    "AND (value GLOB 'backups/?*' OR value GLOB 'conversion-file-backups/?*' "
    "OR value GLOB 'conversion-snapshots/?*' OR value GLOB 'restore-bundles/?*')))), "
    "PRIMARY KEY(operation_id,kind,value)) WITHOUT ROWID",
    # Many independent owners may claim the same image or backup.
    "CREATE INDEX idx_completion_fence_claims_value ON completion_fence_claims(kind,value)",
    "CREATE TRIGGER completion_fences_identity_immutable BEFORE UPDATE OF "
    "operation_id,logical_key,stack,compose_service,action,created_at ON completion_fences "
    "BEGIN SELECT RAISE(ABORT,'completion fence identity is immutable'); END",
    "CREATE TRIGGER completion_fences_resolved_final BEFORE UPDATE OF lifecycle ON completion_fences "
    "WHEN OLD.lifecycle='resolved' AND NEW.lifecycle<>'resolved' "
    "BEGIN SELECT RAISE(ABORT,'a resolved completion fence cannot be reopened'); END",
    "CREATE TRIGGER completion_fences_unresolved_retained BEFORE DELETE ON completion_fences "
    "WHEN OLD.lifecycle IN ('active','manual') "
    "BEGIN SELECT RAISE(ABORT,'an unresolved completion fence cannot be deleted'); END",
    "CREATE TRIGGER completion_fence_claims_immutable BEFORE UPDATE ON completion_fence_claims "
    "BEGIN SELECT RAISE(ABORT,'completion claims are immutable'); END",
    "CREATE TRIGGER completion_fence_claims_unresolved_owner BEFORE INSERT ON completion_fence_claims "
    "WHEN COALESCE((SELECT lifecycle FROM completion_fences WHERE operation_id=NEW.operation_id),'') "
    "NOT IN ('active','manual') "
    "BEGIN SELECT RAISE(ABORT,'claims require an unresolved owner'); END",
    "CREATE TRIGGER completion_fence_claims_retained BEFORE DELETE ON completion_fence_claims "
    "WHEN COALESCE((SELECT lifecycle FROM completion_fences WHERE operation_id=OLD.operation_id),'resolved') "
    "<>'resolved' "
    "BEGIN SELECT RAISE(ABORT,'claims of an unresolved completion fence cannot be deleted'); END",
)
COMPLETION_TABLES = ("completion_fences", "completion_fence_claims")
_SCHEMA_QUERY = ("SELECT type,name,tbl_name,sql FROM sqlite_master "
                 "WHERE tbl_name IN ('completion_fences','completion_fence_claims') "
                 "OR name LIKE '%completion%' ORDER BY type,name")
_expected_schema = None


class CompletionFenceError(ValueError):
    """G2 recovery evidence is unresolved, unproven or cannot be verified."""


@dataclass(frozen=True)
class Fence:
    operation_id: str
    logical_key: str
    stack: str
    compose_service: str
    action: str
    lifecycle: str
    working_dir: str
    compose_files: tuple
    evidence: str
    created_at: str
    updated_at: str
    claims: tuple = ()


def create_schema(c):
    for statement in COMPLETION_SCHEMA:
        c.execute(statement)


def expected_schema():
    """The exact sqlite_master rows this binary creates for schema 10."""
    global _expected_schema
    if _expected_schema is None:
        with sqlite3.connect(":memory:") as memory:
            create_schema(memory)
            _expected_schema = tuple(tuple(row) for row in memory.execute(_SCHEMA_QUERY))
        memory.close()
    return _expected_schema


def schema_matches(c):
    return tuple(tuple(row) for row in c.execute(_SCHEMA_QUERY)) == expected_schema()


def _invalid(detail):
    # Fixed text only: never project evidence, paths or identities.
    return CompletionFenceError(f"completion_evidence_invalid: G2 recovery evidence is malformed ({detail}); "
                                "refusing until it is diagnosed")


def _name(value):
    return isinstance(value, str) and 0 < len(value) <= MAX_NAME and "\x00" not in value


def _absolute_path(value):
    return (isinstance(value, str) and 0 < len(value) <= MAX_PATH and "\x00" not in value
            and os.path.isabs(value) and os.path.abspath(value) == value)


def _json_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate key")
        result[key] = value
    return result


def evidence_digest(text):
    _evidence(text)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _evidence(text):
    if not isinstance(text, str) or len(text.encode("utf-8", "surrogatepass")) > MAX_EVIDENCE_BYTES:
        raise _invalid("evidence size")
    try:
        data = json.loads(text, object_pairs_hook=_json_object)
    except (ValueError, RecursionError):
        raise _invalid("evidence encoding") from None
    if not isinstance(data, dict) or type(data.get("version")) is not int or data["version"] not in (1, 2):
        raise _invalid("evidence version")
    fields = {"version", "working_dir", "compose_files"}
    if data["version"] == 2:
        fields |= {"revision", "claims", "facts", "receipt"}
    if set(data) != fields:
        raise _invalid("evidence fields")
    files = data["compose_files"]
    if (not _absolute_path(data["working_dir"]) or not isinstance(files, list)
            or not 0 < len(files) <= MAX_COMPOSE_FILES or not all(_absolute_path(item) for item in files)
            or len(set(files)) != len(files)):
        raise _invalid("evidence identity")
    if data["version"] == 2:
        if type(data["revision"]) is not int or not 1 <= data["revision"] <= 2147483647:
            raise _invalid("evidence revision")
        claims = data["claims"]
        if (not isinstance(claims, list) or any(not isinstance(x, list) or len(x) != 2
                or not valid_claim(*x) for x in claims)
                or claims != [list(x) for x in sorted(set(map(tuple, claims)))]):
            raise _invalid("evidence claims")
        facts = data["facts"]
        if not isinstance(facts, dict) or set(facts) - {"target_image_id", "running_image_id", "source_sha256", "backup_sha256"}:
            raise _invalid("evidence facts")
        for key, value in facts.items():
            if key.endswith("image_id"):
                if not valid_claim("image", value):
                    raise _invalid("image fact")
            else:
                if not isinstance(value, dict) or not value:
                    raise _invalid("digest facts")
                for name, digest in value.items():
                    valid_name = (_absolute_path(name) if key == "source_sha256"
                                  else ["backup", name] in claims)
                    if not valid_name or not isinstance(digest, str) or not _LOGICAL_KEY.fullmatch(digest):
                        raise _invalid("digest fact")
        receipt = data["receipt"]
        if receipt is not None:
            if (not isinstance(receipt, dict) or set(receipt) != {"proof", "completion_id", "history_id", "request_digest", "prior_evidence_digest"}
                    or receipt["proof"] not in ("succeeded", "no_effect", "safe_recovery")
                    or not isinstance(receipt["completion_id"], str) or not _OPERATION_ID.fullmatch(receipt["completion_id"])
                    or type(receipt["history_id"]) is not int or receipt["history_id"] <= 0
                    or data["revision"] < 2
                    or any(not isinstance(receipt[k], str) or not _LOGICAL_KEY.fullmatch(receipt[k])
                           for k in ("request_digest", "prior_evidence_digest"))):
                raise _invalid("resolution receipt")
    return data


def _fence(row):
    from .compose_baseline import logical_key
    operation_id, key, stack, compose_service, action, lifecycle, evidence, created, updated = tuple(row)
    if not (isinstance(operation_id, str) and _OPERATION_ID.fullmatch(operation_id)):
        raise _invalid("owner")
    if lifecycle not in (*UNRESOLVED, "resolved"):
        raise _invalid("lifecycle")
    if action not in ACTIONS:
        raise _invalid("action")
    if not (isinstance(key, str) and _LOGICAL_KEY.fullmatch(key)) or not _name(stack) or not _name(compose_service):
        raise _invalid("logical service identity")
    if not (isinstance(created, str) and 0 < len(created) <= 64 and "\x00" not in created
            and isinstance(updated, str) and 0 < len(updated) <= 64 and "\x00" not in updated):
        raise _invalid("timestamps")
    data = _evidence(evidence)
    working_dir, files = data["working_dir"], tuple(data["compose_files"])
    if data["version"] == 2 and ((lifecycle == "resolved") != (data["receipt"] is not None)):
        raise _invalid("receipt lifecycle")
    if data["version"] == 2 and data["receipt"] is not None and data["receipt"]["completion_id"] != operation_id:
        raise _invalid("receipt owner")
    # The stored key must be exactly reproducible from the stable identity.
    if logical_key(stack, compose_service, working_dir, files) != key:
        raise _invalid("logical service identity")
    return Fence(operation_id, key, stack, compose_service, action, lifecycle, working_dir, files,
                 evidence, created, updated)


def unresolved_fences(c):
    """Every unresolved (or unrecognised-lifecycle) fence, strictly validated."""
    rows = c.execute(
        "SELECT operation_id,logical_key,stack,compose_service,action,lifecycle,evidence,created_at,updated_at "
        "FROM completion_fences WHERE lifecycle IS NOT 'resolved' ORDER BY operation_id").fetchall()
    fences = tuple(_with_claims(c, _fence(row)) for row in rows)
    if len({fence.logical_key for fence in fences}) != len(fences):
        raise _invalid("duplicate unresolved logical service")
    return fences


def valid_claim(kind, value):
    if kind == "image":
        return isinstance(value, str) and bool(_IMAGE_ID.fullmatch(value))
    if kind == "backup" and isinstance(value, str) and value.count("/") == 1:
        root, name = value.split("/")
        return root in BACKUP_CLAIM_ROOTS and bool(_BACKUP_NAME.fullmatch(name)) and ".." not in name
    return False


_COLUMNS = "operation_id,logical_key,stack,compose_service,action,lifecycle,evidence,created_at,updated_at"


def _with_claims(c, fence):
    from dataclasses import replace
    claims = tuple(tuple(row) for row in c.execute(
        "SELECT kind,value FROM completion_fence_claims WHERE operation_id=? ORDER BY kind,value",
        (fence.operation_id,)).fetchall())
    if any(not valid_claim(*claim) for claim in claims) or len(set(claims)) != len(claims):
        raise _invalid("claim")
    data = _evidence(fence.evidence)
    if data["version"] == 2 and claims != tuple(map(tuple, data["claims"])):
        raise _invalid("claim evidence mismatch")
    return replace(fence, claims=claims)


def exact_fence(c, operation_id):
    """Read one owner, including manual/resolved receipts, in the caller's snapshot.

    None establishes absence only in this database snapshot. It is never a
    retry or release authorisation. SQLite failures propagate unchanged.
    """
    if not isinstance(operation_id, str) or not _OPERATION_ID.fullmatch(operation_id):
        raise _invalid("owner")
    rows = c.execute("SELECT " + _COLUMNS + " FROM completion_fences WHERE operation_id=?",
                     (operation_id,)).fetchall()
    if len(rows) > 1:
        raise _invalid("duplicate owner")
    return _with_claims(c, _fence(rows[0])) if rows else None


def unresolved_claims(c):
    """Claims of unresolved owners by kind; orphaned or malformed claims refuse.

    An owner is trusted only after the full fence validation used for
    admission, so retention never relies on evidence admission would refuse.
    ``c`` must hold one read transaction so owners and claims are consistent.
    """
    owners = {fence.operation_id for fence in unresolved_fences(c)}
    claims = {kind: set() for kind in CLAIM_KINDS}
    rows = c.execute(
        "SELECT cl.operation_id,cl.kind,cl.value,f.lifecycle FROM completion_fence_claims cl "
        "LEFT JOIN completion_fences f ON f.operation_id=cl.operation_id "
        "WHERE f.lifecycle IS NOT 'resolved'").fetchall()
    for operation_id, kind, value, lifecycle in (tuple(row) for row in rows):
        if operation_id not in owners or lifecycle not in UNRESOLVED:
            raise _invalid("claim owner")
        if not valid_claim(kind, value):
            raise _invalid("claim")
        claims[kind].add(value)
    return {kind: frozenset(values) for kind, values in claims.items()}


def _identity(service):
    files = getattr(service, "compose_files", None) or ()
    return (getattr(service, "stack", None), getattr(service, "compose_service", None),
            getattr(service, "working_dir", None), [str(item) for item in files])


def service_refusal(fences, service):
    """Why ``service`` may not be mutated under ``fences``, or None.

    A fence covers its exact logical service and, to resist identity drift
    (changed file lists, moved projects, project-name aliases), any service
    with the same (stack, Compose service) or the same Compose service in the
    same project directory or Compose source. Sharing only an image never
    blocks. Incomplete Compose identity cannot be proved unrelated.
    """
    if not fences:
        return None
    stack, compose_service, working_dir, files = _identity(service)
    if not stack and not compose_service:
        return None  # A standalone container has no Compose identity a fence can own.
    if not (stack and compose_service and working_dir and files):
        return ("completion_identity_unproven: Compose service identity is incomplete while an unresolved "
                "update/rollback completion requires manual recovery")
    from .compose_baseline import logical_key
    key = logical_key(stack, compose_service, working_dir, files)
    directory = os.path.abspath(str(working_dir))
    sources = {os.path.abspath(item) for item in files}
    for fence in fences:
        if (fence.logical_key == key or (fence.stack, fence.compose_service) == (stack, compose_service)
                or (fence.compose_service == compose_service
                    and (fence.working_dir == directory or sources.intersection(fence.compose_files)))):
            return "completion_unresolved: an unresolved update/rollback completion requires manual recovery"
    return None


def recovery_sources_unfenced(state, paths):
    """Recheck before recovery restores sources an operation wrote.

    A proven conflict or malformed evidence refuses (manual recovery). An
    unreadable database keeps the existing compensation: these sources were
    proved unfenced before the operation's first write and recovery restores
    only bytes it verifies as its own.
    """
    try:
        state.assert_sources_unfenced(paths)
    except sqlite3.Error:
        pass


def _aliases(path):
    path = os.path.abspath(str(path))
    return {path, os.path.realpath(path)}


def _owned(fence):
    return set().union(*map(_aliases, (*fence.compose_files, os.path.join(fence.working_dir, ".env"))))


def tree_refusal(fences, directories):
    """Why recursively deleting ``directories`` would remove a fenced source, or None."""
    roots = set().union(*map(_aliases, directories))
    for fence in fences:
        if any(source == root or source.startswith(root.rstrip(os.sep) + os.sep)
               for source in _owned(fence) for root in roots):
            return "completion_unresolved: an unresolved update/rollback completion requires manual recovery"
    return None


def source_refusal(fences, paths, *, stack=None, compose_service=None):
    """Why rewriting Compose ``paths`` would mutate a fenced service, or None.

    A fence owns its Compose files and its project ``.env`` (interpolation
    input), compared lexically and through symlinks, whichever service writes.
    """
    sources = set().union(*map(_aliases, paths))
    for fence in fences:
        if (sources & _owned(fence)
                or (stack and compose_service and (fence.stack, fence.compose_service) == (stack, compose_service))):
            return "completion_unresolved: an unresolved update/rollback completion requires manual recovery"
    return None
