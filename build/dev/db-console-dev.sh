#!/usr/bin/env bash
# Ultra-fast development script for DB Console using go run
# This bypasses bazel entirely for maximum iteration speed

set -euo pipefail

# Clean up function for graceful shutdown
cleanup() {
    echo "Shutting down DB Console server..."
    exit 0
}

trap cleanup SIGINT SIGTERM

echo "Starting DB Console server with go run (ultra-fast mode)..."
echo "Connecting to CockroachDB at: localhost:26257"
echo "DB Console will be available at: http://localhost:9080"
echo ""
echo "This uses 'go run' for instant rebuilds!"
echo "Keep your CRDB node running: ./cockroach start-single-node --insecure"
echo ""

cd "$(dirname "$0")/../.."

# Use go run directly - much faster than building a binary
exec go run ./pkg/cmd/cockroach-db-console start-db-console \
    --host=localhost:26257 \
    --port=9080 \
    --insecure
