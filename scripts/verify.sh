#!/bin/sh
# One-shot acceptance run: build, start app + 2 workers on an ISOLATED fresh
# volume, run verify, report via exit code. The lease-expiry regression relies
# on a pristine data volume, so any pre-existing state is removed first and the
# volume is always torn down afterwards (even on failure/interrupt).
set -u
cd "$(dirname "$0")/.."

PROJECT="track-export-verify"
COMPOSE="docker compose -p $PROJECT"

cleanup() {
	$COMPOSE down -v --remove-orphans
}
trap cleanup EXIT INT TERM

# Reset any state left by an earlier run of this project before we start.
$COMPOSE down -v --remove-orphans

$COMPOSE up -d --build --wait app worker
$COMPOSE run --rm verify
code=$?
exit $code
