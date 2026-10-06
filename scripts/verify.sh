#!/bin/sh
# One-shot acceptance run: build, start app + 2 workers, run verify, report via exit code.
# Afterwards, run the lease-expiry recovery regression in an isolated Compose
# project with a fresh volume (exit code of this script is the overall result).
set -u
cd "$(dirname "$0")/.."

docker compose up -d --build --wait app worker
docker compose run --rm verify
code=$?
docker compose down
[ "$code" -eq 0 ] || exit "$code"

./scripts/verify-lease-expiry.sh
exit $?
