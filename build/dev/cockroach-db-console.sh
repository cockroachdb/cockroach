#!/usr/bin/env bash
# Script to run standalone DB Console server for development with ibazel
# This script will be executed by ibazel and will restart when UI files change
# while keeping the main CockroachDB node running uninterrupted

set -euo pipefail

# The cockroach binary path is passed as the first argument
COCKROACH_BIN="$1"
shift  # Remove the binary path from arguments

# Clean up function for graceful shutdown
cleanup() {
    echo "Shutting down DB Console server..."
    exit 0
}

trap cleanup SIGINT SIGTERM

echo "Starting standalone DB Console server (minimal build)..."
echo "Hot reload is active - server will restart when UI code changes!"
echo "This uses a minimal binary that only includes the DB Console command."
echo ""

# Start the standalone DB console server
# If no arguments provided, use defaults. Otherwise, pass through all arguments.
if [ $# -eq 0 ]; then
    echo "Using default settings:"
    echo "  Connecting to: localhost:26257"
    echo "  DB Console at: http://localhost:9080/future"
    echo ""
    echo "To customize, pass flags: ibazel run //build/dev:cockroach-db-console -- --host=HOST:PORT --port=PORT"
    echo ""
    exec "$COCKROACH_BIN" start-db-console \
        --host=localhost:26257 \
        --port=9080 \
        --insecure
else
    echo "Using custom flags: $*"
    echo ""
    exec "$COCKROACH_BIN" start-db-console "$@"
fi
