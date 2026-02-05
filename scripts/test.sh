#!/bin/bash
# Run tests and report success/failure
# Usage: ./scripts/test.sh [test_file_or_pattern]

set -e

cd "$(dirname "$0")/.."

TEST_TARGET="${1:-tests/}"

echo "Running tests: $TEST_TARGET"
echo "========================================"

if uv run pytest "$TEST_TARGET" -v; then
    echo "========================================"
    echo "SUCCESS: All tests passed"
    exit 0
else
    echo "========================================"
    echo "FAILURE: Some tests failed"
    exit 1
fi
