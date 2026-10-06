"""Crash recovery.

Runs under the export's lease (worker startup and every tick). For each
unfinished export, consult the journal/artifact records plus on-disk digests:

* the staged temp artifact is complete (digest matches the recorded and the
  deterministically recomputed digest) -> converge: publish that very artifact;
* a published file already exists with the expected digest (crash between the
  atomic link and the DB update) -> converge the bookkeeping;
* anything else (partial write, digest mismatch, missing file, orphans) ->
  clean up the残缺 artifacts and requeue the export.

Lease safety: the caller passes its fencing token. Recovery revalidates it at
every mutating step -- at entry, right before the atomic link, and inside the
same IMMEDIATE transaction that records the terminal PUBLISHED state. SQLite
serializes writers, so "token valid" and "terminal state committed" are atomic:
a holder whose lease already expired (e.g. LEASE_TTL_SECONDS=0) or expired mid
recovery publishes nothing, updates no terminal state, and leaves the export
for the still-valid holder to take over.
"""
import hashlib
import os

from . import artifacts, store
from .render import render_artifact_bytes


def lease_resource(export_id):
    return "export:" + export_id


def _expected(export_row):
    data = render_artifact_bytes(export_row)
    return data, hashlib.sha256(data).hexdigest()


def _converge(conn, resource, actor, fencing, export_id, digest, via):
    """Terminal transition + artifact record in one fenced IMMEDIATE txn.

    Raises store.LeaseLostError (whole txn rolled back) when the token is no
    longer valid; the caller must stop without any further mutation.
    """
    pub = artifacts.published_path(export_id)
    return store.mark_published_fenced(conn, resource, actor, fencing,
                                       export_id, digest, pub, actor, via)


def _journal_lease_lost(conn, export_id, actor, when):
    """Best-effort observability; never touches the export's terminal state."""
    try:
        with store.immediate(conn):
            store.journal(conn, export_id, actor, "recovery_lease_lost", when)
    except Exception:
        conn.rollback()


def recover_export(conn, export_id, actor, fencing):
    """Recover one export. Caller must hold the export's lease and pass its
    fencing token; every mutation is re-gated on that token."""
    resource = lease_resource(export_id)

    # Gate 1: a lease dead on arrival (TTL 0, already stolen) does nothing.
    # Not journaled: under a non-positive TTL this would repeat every tick.
    if not store.check_lease(conn, resource, actor, fencing):
        return "lease_lost"

    export = store.get_export(conn, export_id)
    if not export or export["stage"] == "PUBLISHED":
        return "none"
    _, expected_digest = _expected(export)
    pub = artifacts.published_path(export_id)

    # Case 1: published file already on disk (crash between link and DB update).
    if os.path.exists(pub):
        if artifacts.sha256_file(pub) == expected_digest:
            if not store.check_lease(conn, resource, actor, fencing):
                _journal_lease_lost(conn, export_id, actor, "before_converge_published_file")
                return "lease_lost"
            try:
                _converge(conn, resource, actor, fencing, export_id, expected_digest,
                          "recovery_published_file")
            except store.LeaseLostError:
                return "lease_lost"
            artifacts.cleanup_tmp_for(export_id)
            return "converged"
        # A corrupt published file: quarantine it (re-gated) so no later step
        # ever serves it, then fall through to cleanup/requeue.
        if not store.check_lease(conn, resource, actor, fencing):
            _journal_lease_lost(conn, export_id, actor, "before_quarantine")
            return "lease_lost"
        target = artifacts.quarantine(pub)
        try:
            with store.immediate(conn):
                store.assert_lease(conn, resource, actor, fencing)
                store.journal(conn, export_id, actor, "recovery_quarantined_published", target)
        except store.LeaseLostError:
            return "lease_lost"

    # Case 2: a staged temp artifact whose digest matches journal + recompute.
    for row in store.staged_artifacts(conn, export_id):
        path = row["path"]
        if not (
            os.path.exists(path)
            and artifacts.sha256_file(path) == row["digest"] == expected_digest
        ):
            continue
        # Gate 2: lease may have expired naturally during the digest work.
        if not store.check_lease(conn, resource, actor, fencing):
            _journal_lease_lost(conn, export_id, actor, "before_publish_staged_artifact")
            return "lease_lost"
        artifacts.publish(path, pub, row["digest"])
        try:
            _converge(conn, resource, actor, fencing, export_id, row["digest"],
                      "recovery_staged_artifact")
        except store.LeaseLostError:
            # The link may already be on disk; leave it for the valid holder --
            # it is deterministic content and converges via Case 1. It is not
            # downloadable (stage is not PUBLISHED) and must not be deleted.
            return "lease_lost"
        artifacts.cleanup_tmp_for(export_id)
        return "converged"

    # Case 3: incomplete/mismatched remains -> clean up and requeue.
    # Gate 3: the state transition happens first, fenced; temp files are removed
    # only after the committed requeue, so a deposed holder mutates nothing.
    if not store.check_lease(conn, resource, actor, fencing):
        _journal_lease_lost(conn, export_id, actor, "before_cleanup")
        return "lease_lost"
    try:
        with store.immediate(conn):
            store.assert_lease(conn, resource, actor, fencing)
            staged = store.staged_artifacts(conn, export_id)
            for row in staged:
                store.abort_artifact(conn, row["id"])
            store.journal(conn, export_id, actor, "recovery_cleanup", "staged_rows=%d" % len(staged))
            store.requeue(conn, export_id, actor, "recovery_cleanup staged_rows=%d" % len(staged))
    except store.LeaseLostError:
        return "lease_lost"
    if store.check_lease(conn, resource, actor, fencing):
        artifacts.cleanup_tmp_for(export_id)
    return "requeued"


def sweep_orphans(conn, actor, older_than_seconds=30.0):
    """Delete temp files not referenced by any staged artifact record."""
    import time

    removed = []
    known = set()
    for export in store.list_exports(conn, limit=1000):
        for row in store.staged_artifacts(conn, export["export_id"]):
            known.add(os.path.abspath(row["path"]))
    now = time.time()
    for path in artifacts.list_tmp_files():
        if os.path.abspath(path) in known:
            continue
        if now - os.path.getmtime(path) < older_than_seconds:
            continue  # may belong to an in-flight staging; leave it alone
        try:
            os.unlink(path)
            removed.append(path)
        except FileNotFoundError:
            pass
    if removed:
        with store.immediate(conn):
            store.journal(conn, None, actor, "recovery_orphan_sweep", "removed=%d" % len(removed))
    return removed
