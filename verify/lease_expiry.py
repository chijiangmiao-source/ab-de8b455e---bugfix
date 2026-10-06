"""Lease-expiry recovery regression against a fresh DATA_DIR (isolated Compose volume).

Reproduces the reported scenario end to end: LEASE_TTL_SECONDS=0, an export
persisted as STAGED with a digest-matching temp artifact, and a worker
recovery round that acquires an instantly-expired lease. The round must not
publish the artifact or advance the export to PUBLISHED; it must stop so a
still-valid holder can take over. Also covers the lease lapsing in the
middle of the recovery round.

Prints lease_expired_recovery_stage=<stage>; exits 0 when recovery stayed
lease-bound in both scenarios (and a valid holder took over), 1 otherwise.
"""
import hashlib
import os
import sys
import time

from app import artifacts, config, recovery, store, worker
from app.render import render_artifact_bytes

RECORDS = [{"ts": "2026-10-06T01:00:00Z", "lat": 31.230416, "depth_m": 42.51, "vessel_id": "HAICE-01"}]


def stage_stuck_export(conn, export_id, token):
    """Persist an export as STAGED with a digest-matching temp artifact."""
    store.submit_export(conn, export_id, RECORDS)
    export = store.get_export(conn, export_id)
    data = render_artifact_bytes(export)
    digest = hashlib.sha256(data).hexdigest()
    tmp = artifacts.tmp_path(export_id, token)
    artifacts.write_tmp(tmp, data)
    with store.immediate(conn):
        store.cas_stage(conn, export_id, "PROCESSING", ("RECEIVED",))
        store.record_artifact(conn, export_id, "staged", tmp, digest)
        store.cas_stage(conn, export_id, "STAGED", ("PROCESSING",))
    return data, digest, tmp


def terminal_state(conn, export_id):
    row = store.get_export(conn, export_id)
    return {
        "stage": row["stage"],
        "artifact_digest": row["artifact_digest"],
        "published_rows": len(store.published_artifacts(conn, export_id)),
        "published_file": os.path.exists(artifacts.published_path(export_id)),
    }


def terminally_clean(conn, export_id):
    state = terminal_state(conn, export_id)
    return (
        state["stage"] != "PUBLISHED"
        and state["artifact_digest"] is None
        and state["published_rows"] == 0
        and not state["published_file"]
    )


def scenario_expired_before_tick(conn, fail):
    """Lease already expired when the recovery round starts (LEASE_TTL_SECONDS=0)."""
    os.environ["LEASE_TTL_SECONDS"] = "0"
    export_id = "LEASEX-1"
    data, digest, _ = stage_stuck_export(conn, export_id, "staged0")

    worker.tick(conn, "lease-expiry-check")  # acquires an instantly-expired lease

    state = terminal_state(conn, export_id)
    print("lease_expired_recovery_stage=%s" % state["stage"], flush=True)
    if not terminally_clean(conn, export_id):
        fail("expired-lease recovery published/terminally updated %s: %s" % (export_id, state))
        return

    # A valid holder takes over and converges to the very same artifact.
    os.environ["LEASE_TTL_SECONDS"] = "30"
    worker.tick(conn, "lease-expiry-valid")
    state = terminal_state(conn, export_id)
    print("lease_expired_recovery_takeover_stage=%s" % state["stage"], flush=True)
    if state["stage"] != "PUBLISHED" or state["artifact_digest"] != digest:
        fail("valid holder failed to take over %s: %s" % (export_id, state))
        return
    with open(artifacts.published_path(export_id), "rb") as fh:
        if fh.read() != data:
            fail("takeover published different bytes for %s" % export_id)


def scenario_expired_mid_recovery(conn, fail):
    """Lease valid at round start but lapses while recovery verifies the bytes."""
    export_id = "LEASEX-2"
    data, digest, _ = stage_stuck_export(conn, export_id, "staged0")
    resource = store.lease_resource(export_id)
    fencing = store.acquire_lease(conn, resource, "lease-expiry-mid", 60)
    if fencing is None:
        fail("could not acquire lease for %s" % export_id)
        return

    original = recovery._expected

    def expire_lease(export_row):  # the TTL elapses mid-recovery
        conn.execute(
            "UPDATE leases SET expires_at = ? WHERE resource = ?",
            (time.time() - 1, resource),
        )
        return original(export_row)

    recovery._expected = expire_lease
    try:
        result = recovery.recover_export(conn, export_id, "lease-expiry-mid", fencing)
    finally:
        recovery._expected = original

    state = terminal_state(conn, export_id)
    print("lease_expired_mid_recovery_result=%s stage=%s" % (result, state["stage"]), flush=True)
    if result != "lease_lost" or not terminally_clean(conn, export_id):
        fail("mid-recovery lease expiry still published/terminally updated %s: result=%s %s"
             % (export_id, result, state))
        return

    # A valid holder takes over and converges to the very same artifact.
    fencing = store.acquire_lease(conn, resource, "lease-expiry-valid", 60)
    if fencing is None:
        fail("valid holder could not acquire lease for %s" % export_id)
        return
    result = recovery.recover_export(conn, export_id, "lease-expiry-valid", fencing)
    state = terminal_state(conn, export_id)
    print("lease_expired_mid_recovery_takeover_stage=%s" % state["stage"], flush=True)
    if result != "converged" or state["stage"] != "PUBLISHED" or state["artifact_digest"] != digest:
        fail("valid holder failed to take over %s: result=%s %s" % (export_id, result, state))
        return
    with open(artifacts.published_path(export_id), "rb") as fh:
        if fh.read() != data:
            fail("takeover published different bytes for %s" % export_id)


def main():
    config.ensure_dirs()
    conn = store.connect()
    failures = []

    def fail(message):
        failures.append(message)
        print("lease-expiry regression: FAIL: %s" % message, flush=True)

    try:
        store.init_db(conn)
        scenario_expired_before_tick(conn, fail)
        scenario_expired_mid_recovery(conn, fail)
    finally:
        conn.close()
    if failures:
        return 1
    print("lease-expiry regression: OK", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
