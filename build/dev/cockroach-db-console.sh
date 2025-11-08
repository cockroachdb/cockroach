#!/usr/bin/env bash
# Script to run standalone DB Console server for development with ibazel
# This script will be executed by ibazel and will restart when UI files change
# while keeping the main CockroachDB node running uninterrupted

set -euo pipefail

# The cockroach binary path is passed as the first argument
COCKROACH_BIN="$1"

# Clean up function for graceful shutdown
cleanup() {
    echo "Shutting down DB Console server..."
    exit 0
}

trap cleanup SIGINT SIGTERM

echo "Starting standalone DB Console server (minimal build)..."
echo "Connecting to CockroachDB at: localhost:26257"
echo "DB Console will be available at: http://localhost:9080"
echo "Direct access: http://localhost:9080/future/overview"
echo ""
echo "Hot reload is active - server will restart when UI code changes!"
echo "This uses a minimal binary that only includes the DB Console command."
echo ""
echo "Keep your main CRDB node running separately with:"
echo "  ./cockroach start-single-node --insecure"
echo ""

# Start the standalone DB console server
# Note: The minimal binary just has start-db-console as the default command
exec "$COCKROACH_BIN" start-db-console \
    --host=localhost:26257 \
    --port=9080 \
    --insecure
