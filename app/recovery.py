"""Crash recovery.

Runs under the export's lease (worker startup and every tick). For each
unfinished export, consult the journal/artifact records plus on-disk digests:

* the staged temp artifact is complete (digest matches the recorded and the
  deterministically recomputed digest) -> converge: publish that very artifact;
* a published file already exists with the expected digest (crash between the
  atomic link and the DB update) -> converge the bookkeeping;
* anything else (partial write, digest mismatch, missing file, orphans) ->
  clean up the残缺 artifacts and requeue the export.

Every publish/terminal update is gated on the caller's lease still being
valid (fencing token re-checked). An expired lease -- whether it lapsed
before the recovery round started or in the middle of it -- stops the round
without publishing or terminally updating anything, so the still-valid
holder can take over.
"""
import hashlib
import os

from . import artifacts, store
from .render import render_artifact_bytes


def _expected(export_row):
    data = render_artifact_bytes(export_row)
    return data, hashlib.sha256(data).hexdigest()


def _lease_lost(conn, export_id, actor):
    with store.immediate(conn):
        store.journal(conn, export_id, actor, "lease_lost", "recovery")
    return "lease_lost"


def _converge(conn, export_id, digest, actor, via, fencing):
    """Terminally mark the export published -- only while the lease is valid."""
    with store.immediate(conn):
        if not store.check_lease(conn, store.lease_resource(export_id), actor, fencing):
            store.journal(conn, export_id, actor, "lease_lost", "recovery")
            return False
        store.record_artifact(conn, export_id, "published", artifacts.published_path(export_id), digest)
        store.mark_published(conn, export_id, digest, artifacts.published_path(export_id), actor, via)
        return True


def recover_export(conn, export_id, actor, fencing):
    """Recover one export. Caller must hold the export's lease and pass its
    fencing token; the lease is re-validated before anything is published or
    terminally updated."""
    export = store.get_export(conn, export_id)
    if not export or export["stage"] == "PUBLISHED":
        return "none"
    resource = store.lease_resource(export_id)

    # The lease must be valid before recovery mutates anything at all.
    if not store.check_lease(conn, resource, actor, fencing):
        return _lease_lost(conn, export_id, actor)

    _, expected_digest = _expected(export)
    pub = artifacts.published_path(export_id)

    # Case 1: published file already on disk (crash between link and DB update).
    if os.path.exists(pub):
        if artifacts.sha256_file(pub) == expected_digest:
            if _converge(conn, export_id, expected_digest, actor, "recovery_published_file", fencing):
                artifacts.cleanup_tmp_for(export_id)
                return "converged"
            return "lease_lost"
        target = artifacts.quarantine(pub)
        with store.immediate(conn):
            store.journal(conn, export_id, actor, "recovery_quarantined_published", target)

    # Case 2: a staged temp artifact whose digest matches journal + recompute.
    for row in store.staged_artifacts(conn, export_id):
        path = row["path"]
        if (
            os.path.exists(path)
            and artifacts.sha256_file(path) == row["digest"] == expected_digest
        ):
            # Re-validate before publishing: the lease may have lapsed while
            # recovery was verifying the staged bytes.
            if not store.check_lease(conn, resource, actor, fencing):
                return _lease_lost(conn, export_id, actor)
            artifacts.publish(path, pub, row["digest"])
            if _converge(conn, export_id, row["digest"], actor, "recovery_staged_artifact", fencing):
                artifacts.cleanup_tmp_for(export_id)
                return "converged"
            return "lease_lost"

    # Case 3: incomplete/mismatched remains -> clean up and requeue.
    removed = artifacts.cleanup_tmp_for(export_id)
    with store.immediate(conn):
        for row in store.staged_artifacts(conn, export_id):
            store.abort_artifact(conn, row["id"])
        store.journal(conn, export_id, actor, "recovery_cleanup", "removed=%d" % len(removed))
        store.requeue(conn, export_id, actor, "recovery_cleanup removed=%d" % len(removed))
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
