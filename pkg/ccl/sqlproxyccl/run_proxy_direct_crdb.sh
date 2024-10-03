#!/bin/bash

# Copyright 2021 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

set -euo pipefail

# This script sets up an sql proxy, test directory server and a host server to help with end to end testing.
# The directory server starts a tenant sql process on demand and is configured to start directly the
# cockroach executable. It only support a single tenant (with id 123) as the setup is done before the test directory
# server is started. There is another script called run_proxy_indirect_crdb.sh that does the same thing, but starts
# the tenant processes by executing a shell script to allow for better customization. It also support multiple tenants.
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
TENANT_P=55503
TENANT_HTTP_P=55504
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
echo Connect as root to the host cluster and create tenant
COCKROACH_CA_KEY=$CLIENT/ca.key COCKROACH_CERTS_DIR=$CLIENT $COCKROACH sql --port=$HOST_P  -e " 
SELECT CASE WHEN NOT EXISTS(SELECT * FROM system.tenants WHERE id = 123)
THEN crdb_internal.create_tenant(123)
END" > /dev/null

echo Create cert for tenant 123
# shellcheck disable=SC2035
COCKROACH_CA_KEY=$TENANT/ca.key COCKROACH_CERTS_DIR=$TENANT $COCKROACH cert create-tenant-client 123 127.0.0.1 ::1 :: localhost "*.local"
echo Start the tenant to set a root password
$COCKROACH mt start-sql  --certs-dir="$TENANT" --kv-addrs=:$HOST_P --sql-addr=:$TENANT_P --http-addr=:$TENANT_HTTP_P --tenant-id=123 --store="$TENANT"/store.0.123 --log="{sinks: {stderr: {}}}" 2>"$TENANT"/tenant.stderr.log &
TENANT_PID=$!
echo Tenant PID is $TENANT_PID
sleep 1

echo Create client tenant root cert
COCKROACH_CA_KEY=$TENANT/ca.key COCKROACH_CERTS_DIR=$TENANT $COCKROACH cert create-client root
echo Set the password
COCKROACH_CA_KEY=$TENANT/ca.key COCKROACH_CERTS_DIR=$TENANT $COCKROACH sql --port=$TENANT_P -e "alter user root with password 'secret'" > "$TENANT"/alter_root.log

echo Shutdown the tenant
kill $TENANT_PID
sleep 1

echo Start test directory server 
$COCKROACH mt test-directory --port=$DIR_P --log="{sinks: {file-groups: {default: {dir: $DIR, channels: ALL}}}}" -- "$COCKROACH" mt start-sql --kv-addrs=localhost:$HOST_P --certs-dir="$TENANT" --store="$TENANT"/store.0.123 2>"$DIR"/stderr.log &

echo "Start the sql proxy server (with self signed client facing cert)"
$COCKROACH mt start-proxy  --listen-addr=localhost:$PROXY_P --listen-cert=* --listen-key=* --directory=:$DIR_P --listen-metrics=:$PROXY_HTTP_P --skip-verify --log="{sinks: {file-groups: {default: {dir: $PROXY, channels: ALL}}}}" 2>"$PROXY"/stderr.log &

echo "All files are in $BASE"
echo "To connect:"
echo "  $COCKROACH sql --url=\"postgresql://root:secret@127.0.0.1:$PROXY_P?sslmode=require&options=--cluster%3Dtenant-cluster-123\""
echo "Press any key to shutdown all processes and cleanup."
read -r
