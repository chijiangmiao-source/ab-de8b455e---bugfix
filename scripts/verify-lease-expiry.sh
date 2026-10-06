#!/bin/sh
# Lease-expiry recovery regression in an isolated Compose project (fresh volume):
# with LEASE_TTL_SECONDS=0, recovery of a persisted STAGED export must not
# publish the artifact or advance the export to PUBLISHED -- neither when the
# lease lapsed before the round nor when it lapses mid-recovery -- and a valid
# holder must still take over. Exit code is the regression result.
set -u
cd "$(dirname "$0")/.."

PROJECT="track-export-lease-expiry"

docker compose -p "$PROJECT" down -v >/dev/null 2>&1 || true   # guarantee a fresh data volume
docker compose -p "$PROJECT" --profile lease-expiry build lease-expiry-check || exit 1
docker compose -p "$PROJECT" --profile lease-expiry run --rm lease-expiry-check
code=$?
docker compose -p "$PROJECT" down -v
exit $code
