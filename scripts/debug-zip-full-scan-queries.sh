#!/usr/bin/env bash

# Copyright 2026 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# debug-zip-full-scan-queries.sh
#
# Extracts SQL statements with full scan count greater than 0 from a
# debug.zip file. This script analyzes statement statistics from
# system.statement_statistics_limit_5000.txt, which contains the latest
# 5000 statements within the last hour. It starts a temporary
# CockroachDB cluster, imports the statement statistics, and searches
# for queries performed full table scans (optionally filtered by table name).

set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  debug-zip-full-scan-queries.sh --crdb-bin <path> --debug-zip-path <path> [--table-name <name>]
            [--crdb-addr <port>] [--crdb-http-addr <port>] [--store <dir>] [--no-cleanup]

Required:
  --crdb-bin           Path to cockroach binary (e.g. ./cockroach or /path/to/cockroach)
  --debug-zip-path     Path to debug.zip

Optional:
  --table-name         Table name substring to search for in statement text
  --crdb-addr          SQL listen port (default: 26257)
  --crdb-http-addr     HTTP port (default: 8080)
  --store              Store dir (default: node1)
  --no-cleanup         Skip tearing down the demo cluster on exit
EOF
}

# ----------------------------
# Arg parsing (flags-only)
# ----------------------------
CRDB_BIN=""
DEBUG_ZIP_PATH=""
TABLE_NAME=""

CRDB_ADDR="26257"
CRDB_HTTP_ADDR="8080"
STORE="node1"
NO_CLEANUP="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --crdb-bin)
      CRDB_BIN="${2:-}"; shift 2;;
    --debug-zip-path)
      DEBUG_ZIP_PATH="${2:-}"; shift 2;;
    --table-name)
      TABLE_NAME="${2:-}"; shift 2;;
    --crdb-addr)
      CRDB_ADDR="${2:-}"; shift 2;;
    --crdb-http-addr)
      CRDB_HTTP_ADDR="${2:-}"; shift 2;;
    --store)
      STORE="${2:-}"; shift 2;;
    --no-cleanup)
      NO_CLEANUP="true"; shift 1;;
    -h|--help)
      usage; exit 0;;
    *)
      echo "Unknown argument: $1" >&2
      usage; exit 2;;
  esac
done

# ----------------------------
# Validation
# ----------------------------
if [[ -z "${CRDB_BIN}" || -z "${DEBUG_ZIP_PATH}" ]]; then
  echo "Missing required flags." >&2
  usage
  exit 2
fi

if [[ ! -x "${CRDB_BIN}" ]]; then
  echo "--crdb-bin must point to an executable: ${CRDB_BIN}" >&2
  exit 2
fi

if [[ ! -f "${DEBUG_ZIP_PATH}" ]]; then
  echo "--debug-zip-path does not exist: ${DEBUG_ZIP_PATH}" >&2
  exit 2
fi

if ! command -v unzip >/dev/null 2>&1; then
  echo "Missing dependency: unzip" >&2
  exit 2
fi

echo "WARNING: This script analyzes only the latest 5000 statements within " \
     "the last hour from system.statement_statistics_limit_5000.txt"
echo

SQL_HOST="localhost"
SQL_PORT="${CRDB_ADDR}"
HTTP_ADDR="localhost:${CRDB_HTTP_ADDR}"
LISTEN_ADDR="localhost:${CRDB_ADDR}"
ADVERTISE_ADDR="localhost:${CRDB_ADDR}"

TMPDIR="$(mktemp -d)"
PID_FILE="${TMPDIR}/cockroach.pid"

cleanup() {
  set +e
  
  # Always clean up temp directory (can be large)
  rm -rf "${TMPDIR}" >/dev/null 2>&1 || true
  
  if [[ "${NO_CLEANUP}" == "true" ]]; then
    echo "Skipping cluster cleanup due to --no-cleanup flag. "\
    "Cluster remains running on port ${SQL_PORT} with store: ${STORE}"
    echo "To manually stop: ${CRDB_BIN} quit --insecure --host=${SQL_HOST} --port=${SQL_PORT}"
    return
  fi

  # Try to stop node nicely (insecure)
  "${CRDB_BIN}" quit --insecure --host="${SQL_HOST}" --port="${SQL_PORT}" >/dev/null 2>&1 || true

  # If pid file exists and process still around, kill it
  if [[ -f "${PID_FILE}" ]]; then
    PID="$(cat "${PID_FILE}" 2>/dev/null || true)"
    if [[ -n "${PID}" ]] && kill -0 "${PID}" >/dev/null 2>&1; then
      kill "${PID}" >/dev/null 2>&1 || true
    fi
  fi
}
trap cleanup EXIT

# ----------------------------
# 1) Start single-node cluster
# ----------------------------
echo "Starting CockroachDB single-node (SQL=${LISTEN_ADDR}, HTTP=${HTTP_ADDR}, store=${STORE})..."
mkdir -p "${STORE}"

"${CRDB_BIN}" start-single-node \
  --listen-addr="${LISTEN_ADDR}" \
  --http-addr="${HTTP_ADDR}" \
  --advertise-addr="${ADVERTISE_ADDR}" \
  --store="${STORE}" \
  --insecure \
  --background \
  --pid-file="${PID_FILE}"

# Wait for SQL to be ready
echo "Waiting for SQL to be ready..."
"${CRDB_BIN}" sql --insecure --host="${SQL_HOST}" --port="${SQL_PORT}" -e "SELECT 1;" >/dev/null

# ----------------------------
# 2) Unzip debug.zip
# ----------------------------
UNZIP_DIR="${TMPDIR}/debug_unzipped"
mkdir -p "${UNZIP_DIR}"
echo "Unzipping ${DEBUG_ZIP_PATH} -> ${UNZIP_DIR}"
unzip -q "${DEBUG_ZIP_PATH}" -d "${UNZIP_DIR}"

# ----------------------------
# 3) Find system.statement_statistics_limit_5000.txt
# ----------------------------
STATS_TXT_PATH="$(find "${UNZIP_DIR}" -type f -name "system.statement_statistics_limit_5000.txt" -print -quit || true)"
if [[ -z "${STATS_TXT_PATH}" ]]; then
  echo "Could not find system.statement_statistics_limit_5000.txt inside the unzipped debug.zip." >&2
  exit 1
fi
echo "Found: ${STATS_TXT_PATH}"

# ----------------------------
# 4) Upload to userfile with timestamped filename
# ----------------------------
TIMESTAMP="$(date +%s)"
CSV_FILENAME="stats_${TIMESTAMP}.csv"
echo "Uploading to userfile: ${CSV_FILENAME}"
"${CRDB_BIN}" userfile upload \
  --insecure \
  --host="${SQL_HOST}" \
  --port="${SQL_PORT}" \
  "${STATS_TXT_PATH}" \
  "${CSV_FILENAME}"

# ----------------------------
# 5) Create stats table & truncate
# ----------------------------
echo "Creating table stats (schema from system.statement_statistics) and truncating..."
"${CRDB_BIN}" sql --insecure --host="${SQL_HOST}" --port="${SQL_PORT}" -e \
  "SET allow_unsafe_internals = true; CREATE TABLE IF NOT EXISTS stats AS SELECT * FROM system.statement_statistics LIMIT 1; TRUNCATE stats;"

# ----------------------------
# 6) IMPORT INTO stats from userfile CSV (tab-delimited)
# ----------------------------
echo "Importing data into stats from userfile:///${CSV_FILENAME}"
"${CRDB_BIN}" sql --insecure --host="${SQL_HOST}" --port="${SQL_PORT}" -e \
  "IMPORT INTO stats (aggregated_ts, fingerprint_id,
  transaction_fingerprint_id, plan_hash, app_name,
  agg_interval, metadata, statistics, plan,
  index_recommendations)
   CSV DATA ('userfile:///${CSV_FILENAME}')
   WITH delimiter = e'\t', skip = '1';"

# ----------------------------
# 7) Function: find queries with fullScanCount > 0 for a table name substring
# ----------------------------
find_full_scans_for_table() {
  local table_name="$1"

  local where_clause="WHERE (metadata->'fullScanCount')::int > 0"
  
  if [[ -n "${table_name}" ]]; then
    # Escape single quotes for SQL literal (basic)
    local escaped
    escaped="${table_name//\'/\'\'}"
    where_clause="WHERE metadata->>'query' LIKE '%${escaped}%' AND (metadata->'fullScanCount')::int > 0"
  fi

  "${CRDB_BIN}" sql --insecure --host="${SQL_HOST}" --port="${SQL_PORT}" --format=tsv -e \
    "SELECT DISTINCT metadata->>'query'
     FROM stats
     ${where_clause};"
}

echo
if [[ -n "${TABLE_NAME}" ]]; then
  echo "Queries mentioning '${TABLE_NAME}' with fullScanCount > 0:"
else
  echo "All queries with fullScanCount > 0:"
fi
find_full_scans_for_table "${TABLE_NAME}"
