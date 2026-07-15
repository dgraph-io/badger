#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

MODE="${1:-new}"

EXISTING_TESTS="TestStream$|TestStreamDB|TestStreamWithThreadId|TestBackupRestore1|TestBackupRestore2|TestBackupRestore3|TestStreamWriter1|TestStreamWriter2"
NEW_TESTS="TestPinnedSnapshot|TestOrchestrate|TestBackupDuringConcurrent|TestBackupRestoreRoundTrip"

case "$MODE" in
  base)
    echo "=== BASE mode: running existing tests (should pass) ==="
    go test -v -run "$EXISTING_TESTS" -count=1 -timeout=120s .
    echo ""
    echo "=== BASE mode: running new tests (should fail without solution) ==="
    if go test -v -tags with_snapshot -run "$NEW_TESTS" -count=1 -timeout=120s . 2>&1; then
      echo "FAIL: new tests passed but should have failed without solution"
      exit 1
    else
      echo "OK: new tests correctly fail without solution"
    fi
    ;;
  new)
    echo "=== NEW mode: running new tests ==="
    go test -v -tags with_snapshot -run "$NEW_TESTS" -count=1 -timeout=120s .
    echo "=== All tests passed ==="
    ;;
  *)
    echo "Usage: $0 [base|new]"
    exit 1
    ;;
esac
