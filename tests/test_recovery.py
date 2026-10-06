import hashlib
import os
import tempfile
import time
import unittest
from unittest import mock

from app import artifacts, config, recovery, store
from app.render import render_artifact_bytes


class RecoveryTestBase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        os.environ["DATA_DIR"] = self._tmp.name
        config.ensure_dirs()
        self.conn = store.connect()
        store.init_db(self.conn)
        store.submit_export(self.conn, "E-1", [{"ts": "t0", "lat": 31.2, "depth_m": 10}])
        self.resource = recovery.lease_resource("E-1")
        self.fencing = store.acquire_lease(self.conn, self.resource, "test", 30)

    def tearDown(self):
        self.conn.close()
        self._tmp.cleanup()
        os.environ.pop("DATA_DIR", None)

    def export(self):
        return store.get_export(self.conn, "E-1")

    def expected_digest(self):
        return hashlib.sha256(render_artifact_bytes(self.export())).hexdigest()

    def recover(self, actor="test", fencing=None):
        return recovery.recover_export(self.conn, "E-1", actor,
                                       self.fencing if fencing is None else fencing)


class ConvergeTest(RecoveryTestBase):
    def test_complete_staged_artifact_is_published_as_is(self):
        """Crash after staging: recovery converges to the same complete artifact."""
        data = render_artifact_bytes(self.export())
        digest = hashlib.sha256(data).hexdigest()
        tmp = artifacts.tmp_path("E-1", "deadbeef")
        artifacts.write_tmp(tmp, data)
        with store.immediate(self.conn):
            store.cas_stage(self.conn, "E-1", "PROCESSING", ("RECEIVED",))
            store.record_artifact(self.conn, "E-1", "staged", tmp, digest)
            store.cas_stage(self.conn, "E-1", "STAGED", ("PROCESSING",))

        result = self.recover()

        self.assertEqual("converged", result)
        row = self.export()
        self.assertEqual("PUBLISHED", row["stage"])
        self.assertEqual(digest, row["artifact_digest"])
        # the very same bytes were published, not a regenerated copy
        with open(artifacts.published_path("E-1"), "rb") as fh:
            self.assertEqual(data, fh.read())
        self.assertEqual([], artifacts.tmp_files_for("E-1"))

    def test_published_file_present_but_db_not_updated(self):
        """Crash between atomic link and DB update: converge bookkeeping."""
        data = render_artifact_bytes(self.export())
        digest = hashlib.sha256(data).hexdigest()
        tmp = artifacts.tmp_path("E-1", "cafe")
        artifacts.write_tmp(tmp, data)
        artifacts.publish(tmp, artifacts.published_path("E-1"), digest)
        with store.immediate(self.conn):
            store.cas_stage(self.conn, "E-1", "PROCESSING", ("RECEIVED",))
            store.cas_stage(self.conn, "E-1", "STAGED", ("PROCESSING",))

        result = self.recover()

        self.assertEqual("converged", result)
        row = self.export()
        self.assertEqual("PUBLISHED", row["stage"])
        self.assertEqual(digest, row["artifact_digest"])
        self.assertEqual(1, len(store.published_artifacts(self.conn, "E-1")))


class CleanupTest(RecoveryTestBase):
    def test_partial_write_is_cleaned_and_requeued(self):
        """Crash mid-write: partial temp file removed, export back to RECEIVED."""
        data = render_artifact_bytes(self.export())
        tmp = artifacts.tmp_path("E-1", "half")
        artifacts.write_tmp(tmp, data[: len(data) // 2])
        with store.immediate(self.conn):
            store.cas_stage(self.conn, "E-1", "PROCESSING", ("RECEIVED",))

        result = self.recover()

        self.assertEqual("requeued", result)
        row = self.export()
        self.assertEqual("RECEIVED", row["stage"])
        self.assertEqual(1, row["attempts"])
        self.assertEqual([], artifacts.tmp_files_for("E-1"))

    def test_corrupted_staged_artifact_is_aborted(self):
        data = render_artifact_bytes(self.export())
        digest = hashlib.sha256(data).hexdigest()
        tmp = artifacts.tmp_path("E-1", "badc0de")
        artifacts.write_tmp(tmp, data + b"corruption")
        with store.immediate(self.conn):
            store.cas_stage(self.conn, "E-1", "PROCESSING", ("RECEIVED",))
            store.record_artifact(self.conn, "E-1", "staged", tmp, digest)
            store.cas_stage(self.conn, "E-1", "STAGED", ("PROCESSING",))

        result = self.recover()

        self.assertEqual("requeued", result)
        self.assertEqual([], artifacts.tmp_files_for("E-1"))
        self.assertEqual([], store.staged_artifacts(self.conn, "E-1"))
        self.assertEqual("RECEIVED", self.export()["stage"])

    def test_published_export_is_never_touched(self):
        with store.immediate(self.conn):
            store.mark_published(self.conn, "E-1", "d" * 64, "/nowhere", "test", "unit")
        self.assertEqual("none", self.recover())
        self.assertEqual("PUBLISHED", self.export()["stage"])
        self.assertEqual("d" * 64, self.export()["artifact_digest"])

    def test_orphan_sweep_removes_unreferenced_old_temp_files(self):
        orphan = artifacts.tmp_path("E-9", "orphan")
        artifacts.write_tmp(orphan, b"leftover")
        old = 1_600_000_000
        os.utime(orphan, (old, old))
        removed = recovery.sweep_orphans(self.conn, "test", older_than_seconds=1)
        self.assertIn(orphan, removed)
        self.assertFalse(os.path.exists(orphan))


class PublishPrimitiveTest(RecoveryTestBase):
    def test_publish_never_clobbers(self):
        a = artifacts.tmp_path("E-1", "a")
        b = artifacts.tmp_path("E-1", "b")
        artifacts.write_tmp(a, b"same-content")
        artifacts.write_tmp(b, b"same-content")
        dst = artifacts.published_path("E-1")
        digest = artifacts.sha256_bytes(b"same-content")
        self.assertEqual("linked", artifacts.publish(a, dst, digest))
        self.assertEqual("dedup", artifacts.publish(b, dst, digest))
        self.assertFalse(os.path.exists(a))
        self.assertFalse(os.path.exists(b))
        self.assertEqual(1, len(artifacts.list_published_files()))

    def test_publish_refuses_different_content(self):
        a = artifacts.tmp_path("E-1", "a")
        b = artifacts.tmp_path("E-1", "b")
        artifacts.write_tmp(a, b"content-a")
        artifacts.write_tmp(b, b"content-b")
        dst = artifacts.published_path("E-1")
        artifacts.publish(a, dst, artifacts.sha256_bytes(b"content-a"))
        with self.assertRaises(artifacts.PublishedMismatch):
            artifacts.publish(b, dst, artifacts.sha256_bytes(b"content-b"))

    def test_load_verified_rejects_tampered_bytes(self):
        data = render_artifact_bytes(self.export())
        digest = hashlib.sha256(data).hexdigest()
        with store.immediate(self.conn):
            store.mark_published(self.conn, "E-1", digest, artifacts.published_path("E-1"), "t", "unit")
        artifacts.write_tmp(artifacts.published_path("E-1"), data + b"tamper")
        with self.assertRaises(artifacts.DigestMismatch):
            artifacts.load_verified(self.export())


class ExpiredLeaseRecoveryTest(RecoveryTestBase):
    """Regression: an expired lease must never publish an export or move it to
    a terminal state -- neither when the lease was already dead before recovery
    nor when it dies while recovery is in flight."""

    def _stage_complete_temp(self, token="deadbeef"):
        data = render_artifact_bytes(self.export())
        digest = hashlib.sha256(data).hexdigest()
        tmp = artifacts.tmp_path("E-1", token)
        artifacts.write_tmp(tmp, data)
        with store.immediate(self.conn):
            store.cas_stage(self.conn, "E-1", "PROCESSING", ("RECEIVED",))
            store.record_artifact(self.conn, "E-1", "staged", tmp, digest)
            store.cas_stage(self.conn, "E-1", "STAGED", ("PROCESSING",))
        return tmp, digest

    def _assert_not_published(self, digest, tmp=None):
        row = self.export()
        self.assertEqual("STAGED", row["stage"], "terminal state must not advance")
        self.assertIsNone(row["published_at"], "no terminal update allowed")
        self.assertIsNone(row["artifact_digest"])
        self.assertEqual([], store.published_artifacts(self.conn, "E-1"),
                         "no published artifact row may exist")
        self.assertFalse(os.path.exists(artifacts.published_path("E-1")),
                         "temp artifact must never be exposed at the published path")
        # the durable evidence for a valid holder stays intact
        if tmp is not None:
            self.assertTrue(os.path.exists(tmp))
            self.assertEqual(
                [digest], [r["digest"] for r in store.staged_artifacts(self.conn, "E-1")])

    def test_lease_expired_before_recovery_publishes_nothing(self):
        """LEASE_TTL_SECONDS=0: fencing token is dead on arrival (the reported
        bug: lease_expired_recovery_stage=PUBLISHED)."""
        tmp, digest = self._stage_complete_temp()
        store.release_lease(self.conn, self.resource, "test", self.fencing)
        dead_fencing = store.acquire_lease(self.conn, self.resource, "w-dead", 0)
        self.assertFalse(store.check_lease(self.conn, self.resource, "w-dead", dead_fencing))

        result = recovery.recover_export(self.conn, "E-1", "w-dead", dead_fencing)

        self.assertEqual("lease_lost", result)
        self._assert_not_published(digest, tmp)

    def test_lease_expiring_mid_recovery_before_link_publishes_nothing(self):
        """Lease valid at entry, expired after the digest verification but
        before the atomic link: recovery stops."""
        tmp, digest = self._stage_complete_temp()
        calls = {"n": 0}
        real_check = store.check_lease

        def flaky_check(conn, resource, owner, fencing):
            calls["n"] += 1
            return calls["n"] == 1  # gate 1 passes; every later gate fails

        with mock.patch.object(store, "check_lease", flaky_check):
            result = recovery.recover_export(self.conn, "E-1", "test", self.fencing)

        self.assertEqual("lease_lost", result)
        self._assert_not_published(digest, tmp)

    def test_lease_lost_between_link_and_terminal_commit_publishes_nothing(self):
        """The link may land on disk, but the fenced terminal transaction must
        abort; the export is not PUBLISHED and is not downloadable."""
        tmp, digest = self._stage_complete_temp()
        calls = {"n": 0}
        real_check = store.check_lease

        def stolen_check(conn, resource, owner, fencing):
            # gate 1 (entry) and gate 2 (before link) pass; the assert_lease
            # inside the terminal txn is call 3 and fails (lease stolen).
            calls["n"] += 1
            return calls["n"] < 3

        with mock.patch.object(store, "check_lease", stolen_check):
            result = recovery.recover_export(self.conn, "E-1", "test", self.fencing)

        self.assertEqual("lease_lost", result)
        row = self.export()
        self.assertEqual("STAGED", row["stage"])
        self.assertIsNone(row["published_at"])
        self.assertEqual([], store.published_artifacts(self.conn, "E-1"))
        # even if the link raced onto disk it must carry no DB terminal state;
        # rollback left the staged evidence for the valid holder.
        self.assertEqual([digest], [r["digest"] for r in store.staged_artifacts(self.conn, "E-1")])

        # The still-valid holder takes over on the next pass and converges.
        store.check_lease = real_check  # noqa: restore explicitly for clarity
        result2 = recovery.recover_export(self.conn, "E-1", "test", self.fencing)
        self.assertEqual("converged", result2)
        row = self.export()
        self.assertEqual("PUBLISHED", row["stage"])
        self.assertEqual(digest, row["artifact_digest"])
        self.assertEqual(1, len(store.published_artifacts(self.conn, "E-1")))

    def test_lease_expires_naturally_by_ttl_mid_recovery(self):
        """Same guarantee via real wall-clock expiry (not just mocks): a short
        lease that ages out during recovery cannot publish."""
        tmp, digest = self._stage_complete_temp()
        store.release_lease(self.conn, self.resource, "test", self.fencing)
        fencing = store.acquire_lease(self.conn, self.resource, "w-slow", 0.05)
        time.sleep(0.07)  # lease dies while recovery's digest work would run

        result = recovery.recover_export(self.conn, "E-1", "w-slow", fencing)

        self.assertEqual("lease_lost", result)
        self._assert_not_published(digest, tmp)

    def test_valid_holder_takes_over_after_stale_attempt(self):
        """After a stale holder gives up, the next valid fencing token finishes
        the convergence on the same complete artifact."""
        tmp, digest = self._stage_complete_temp()
        store.release_lease(self.conn, self.resource, "test", self.fencing)
        dead = store.acquire_lease(self.conn, self.resource, "w-dead", 0.01)
        time.sleep(0.03)
        self.assertEqual("lease_lost",
                         recovery.recover_export(self.conn, "E-1", "w-dead", dead))

        takeover = store.acquire_lease(self.conn, self.resource, "w-alive", 30)
        self.assertGreater(takeover, dead)
        self.assertEqual("converged",
                         recovery.recover_export(self.conn, "E-1", "w-alive", takeover))
        row = self.export()
        self.assertEqual("PUBLISHED", row["stage"])
        self.assertEqual(digest, row["artifact_digest"])
        with open(artifacts.published_path("E-1"), "rb") as fh:
            self.assertEqual(hashlib.sha256(fh.read()).hexdigest(), digest)
        self.assertEqual([], artifacts.tmp_files_for("E-1"))

    def test_stale_holder_cannot_requeue_either(self):
        """Case 3 (partial artifact -> cleanup/requeue) is fenced too: a stale
        lease leaves the export and the temp file exactly as they were."""
        data = render_artifact_bytes(self.export())
        tmp = artifacts.tmp_path("E-1", "half")
        artifacts.write_tmp(tmp, data[: len(data) // 2])
        with store.immediate(self.conn):
            store.cas_stage(self.conn, "E-1", "PROCESSING", ("RECEIVED",))
        store.release_lease(self.conn, self.resource, "test", self.fencing)
        dead = store.acquire_lease(self.conn, self.resource, "w-dead", 0)

        result = recovery.recover_export(self.conn, "E-1", "w-dead", dead)

        self.assertEqual("lease_lost", result)
        row = self.export()
        self.assertEqual("PROCESSING", row["stage"])
        self.assertEqual(0, row["attempts"])
        self.assertTrue(os.path.exists(tmp))

    def test_fenced_mark_published_primitive(self):
        tmp, digest = self._stage_complete_temp()
        artifacts.publish(tmp, artifacts.published_path("E-1"), digest)
        store.release_lease(self.conn, self.resource, "test", self.fencing)
        dead = store.acquire_lease(self.conn, self.resource, "w-dead", 0)

        with self.assertRaises(store.LeaseLostError):
            store.mark_published_fenced(
                self.conn, self.resource, "w-dead", dead,
                "E-1", digest, artifacts.published_path("E-1"), "w-dead", "unit")
        self.assertEqual("STAGED", self.export()["stage"])
        self.assertEqual([], store.published_artifacts(self.conn, "E-1"))

        valid = store.acquire_lease(self.conn, self.resource, "w-alive", 30)
        self.assertTrue(
            store.mark_published_fenced(
                self.conn, self.resource, "w-alive", valid,
                "E-1", digest, artifacts.published_path("E-1"), "w-alive", "unit"))
        self.assertEqual("PUBLISHED", self.export()["stage"])


if __name__ == "__main__":
    unittest.main()
