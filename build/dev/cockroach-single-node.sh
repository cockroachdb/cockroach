#!/usr/bin/env bash
# Script to run CockroachDB single-node server for development with ibazel
# This script will be executed by ibazel and will restart when files change

set -euo pipefail

# The cockroach binary path is passed as the first argument
COCKROACH_BIN="$1"

# Set up a data directory in the temp folder to avoid cluttering the project
DATA_DIR="/tmp/cockroach-single-node-dev"

# Clean up function for graceful shutdown
cleanup() {
    echo "Shutting down CockroachDB..."
    exit 0
}

trap cleanup SIGINT SIGTERM

# Remove existing data directory if it exists (for clean restarts)
if [ -d "$DATA_DIR" ]; then
    echo "Removing existing data directory: $DATA_DIR"
    rm -rf "$DATA_DIR"
fi

# Create fresh data directory
mkdir -p "$DATA_DIR"

echo "Starting CockroachDB single-node server..."
echo "Data directory: $DATA_DIR"
echo "Web UI will be available at: http://localhost:8080"
echo "Your app at: http://localhost:8080/future/index.html (with auto-refresh!)"
echo "SQL endpoint: localhost:26257"
echo ""
echo "Auto-refresh is active - your browser will reload when the server restarts!"
echo ""

# Start the single-node server
exec "$COCKROACH_BIN" start-single-node \
    --insecure \
    --store="$DATA_DIR" \
    --listen-addr=localhost:26257 \
    --http-addr=localhost:8080 \
    --logtostderr=INFO