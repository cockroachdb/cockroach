#!/usr/bin/env sh
set -euo pipefail

CRDB=${COCKROACH:-../../../../cockroach}
WORKDIR=$(mktemp -d)
CERTS="$WORKDIR/certs"
CERTS_SAFE="$WORKDIR/certs-safe"
COCKROACH_DATA="$WORKDIR/cockroach-data"
PIDFILE="$WORKDIR/crdb.pid"

mkdir $CERTS $CERTS_SAFE $COCKROACH_DATA

printf "Temporary files stored in '${WORKDIR}'\n"
cleanup() {
  rm -rf $WORKDIR
}

trap 'cleanup' EXIT;

printf "Creating certificates..."
$CRDB cert create-ca --certs-dir=$CERTS --ca-key=$CERTS_SAFE/ca.key
$CRDB cert create-node localhost --certs-dir=$CERTS --ca-key=$CERTS_SAFE/ca.key
$CRDB cert create-client root --certs-dir=$CERTS --ca-key=$CERTS_SAFE/ca.key
printf " DONE\n"

# Start a temporary single-node cluster in the background (listening on port
# 9090 to hide it from the tests).
printf "Starting temporary cluster..."
$CRDB start-single-node \
  --certs-dir=$CERTS \
  --listen-addr=localhost \
  --http-addr=localhost:9090 \
  --store=$COCKROACH_DATA \
  --accept-sql-without-tls \
  --pid-file=$PIDFILE \
  --background > /dev/null
printf " DONE\n"

# Set a known password for the root user.
printf "Setting root user's password..."
$CRDB sql --certs-dir=$CERTS -e "ALTER USER root WITH PASSWORD 'cypress'" >/dev/null
printf " DONE\n"

# Initialize some tables with the "movr" workload.
printf "Initializing 'movr' workload..."
$CRDB workload init movr --secure --password 'cypress' 2>/dev/null
printf " DONE\n"

# Kill the initial CRDB cluster and wait for it to exit.
printf "Waiting for cluster to stop..."
kill $(cat $PIDFILE)
while pgrep -F $PIDFILE > /dev/null; do
  sleep 1
done
printf " DONE\n"

# Restart the single-node cluster (listening on port 8080 this time) so tests
# know when to start executing.
printf "Restarting cluster for testing...\n"
$CRDB start-single-node \
  --certs-dir=$CERTS \
  --listen-addr=localhost \
  --http-addr=localhost:8080 \
  --accept-sql-without-tls \
  --unencrypted-localhost-http \
  --store=$COCKROACH_DATA \
  --pid-file=${CERTS}/crdb.pid >/dev/null 2>&1
