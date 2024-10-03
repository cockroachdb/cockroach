#!/bin/bash

# Copyright 2021 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

set -euo pipefail

# This script sets up an sql proxy, test directory server and a host server to help with end to end testing.
# The directory server starts tenant sql processes on demand and is configured to start the cockroach executable through
# a shell script called run_tenant.sh. There is another script called run_proxy_direct_crdb.sh that does the same thing
# but starts the tenant processes by directly executing the cockroach executable.
# The script will start all processes and then will block waiting for a keypress. It will shutdown all related
# processes and clean up the temporary folders when a key is pressed.

COCKROACH=${1:-'cockroach'}
BASE=${2:-$(mktemp -d -t 'tenant-test-XXXX')}

# Setup host
HOST=$BASE/host
CLIENT=$BASE/client
TENANT=$BASE/tenant
DIR=$BASE/dir
PROXY=$BASE/proxy

mkdir -p "$HOST"
mkdir -p "$CLIENT"
mkdir -p "$TENANT"
mkdir -p "$DIR"
mkdir -p "$PROXY"

cleanup() {
  echo "cleaning up..."
  rm -rf "$BASE"
}

trap 'trap "" TERM EXIT; kill 0; wait; cleanup; exit' INT TERM EXIT

#Ports
HOST_P=55501
HOST_HTTP_P=55502
DIR_P=55505
PROXY_P=55506
PROXY_HTTP_P=55507

echo Create node CA and node cert
COCKROACH_CA_KEY=$HOST/ca.key COCKROACH_CERTS_DIR=$HOST $COCKROACH cert create-ca
COCKROACH_CA_KEY=$HOST/ca.key COCKROACH_CERTS_DIR=$HOST $COCKROACH cert create-node 127.0.0.1 localhost

echo Create client CA
COCKROACH_CA_KEY=$CLIENT/ca.key COCKROACH_CERTS_DIR=$CLIENT $COCKROACH cert create-client-ca
mv "$CLIENT"/ca-client.crt "$CLIENT"/ca.crt
# SQL connections may have client CA or host CA issued certs on either side.
cat "$HOST"/ca.crt >> "$CLIENT"/ca.crt
cp "$CLIENT"/ca.crt "$HOST"/ca-client.crt

echo Create tenant CA
COCKROACH_CA_KEY=$TENANT/ca.key COCKROACH_CERTS_DIR=$TENANT $COCKROACH cert create-tenant-client-ca
mv "$TENANT"/ca-client-tenant.crt "$TENANT"/ca.crt
cp "$TENANT"/ca.crt "$HOST"/ca-client-tenant.crt
cat "$HOST"/ca.crt >> "$TENANT"/ca.crt

echo Start KV layer
$COCKROACH start-single-node --listen-addr=localhost:$HOST_P --http-addr=:$HOST_HTTP_P --background --certs-dir="$HOST" --store="$HOST"/store

echo Create client host root cert
COCKROACH_CA_KEY=$CLIENT/ca.key COCKROACH_CERTS_DIR=$CLIENT $COCKROACH cert create-client root

echo Start test directory server
$COCKROACH mt test-directory --port=$DIR_P --log="{sinks: {file-groups: {default: {dir: $DIR, channels: ALL}}}}" -- /bin/bash -c $(dirname "$0")'/run_tenant.sh "$@"' "" "$COCKROACH" "$BASE" 0 --kv-addrs=localhost:$HOST_P 2>"$DIR"/stderr.log &

echo "Start the sql proxy server (with self signed client facing cert)"
$COCKROACH mt start-proxy  --listen-addr=localhost:$PROXY_P --listen-cert=* --listen-key=* --directory=:$DIR_P --listen-metrics=:$PROXY_HTTP_P --skip-verify --log="{sinks: {file-groups: {default: {dir: $PROXY, channels: ALL}}}}" 2>"$PROXY"/stderr.log &

echo "All files are in $BASE"
echo "To connect to a specific tenant (123 for example):"
echo "  $COCKROACH sql --url=\"postgresql://root:secret@127.0.0.1:$PROXY_P?sslmode=require&options=--cluster=tenant-cluster-123\""
echo "Press any key to shutdown all processes and cleanup."
read -r
