#!/bin/bash

# Sample script to run a minimal cockroachdb deployment using sqlproxy
# listening on localhost:5432.
# This consists of a single-node db cluster, a sql tenant server, and a proxy.
# The proxy listens on :5432, forwarding to tenant SQL server at :36257.
# Finally a sql shell is opened for tenant id 123 and user `bob`.
# The password for `bob` is `builder`.
#
# WARNING: directory `~/.cockroach-certs` will be DELETED.
# WARNING: all cockroach and mtproxy processes will be killed.

set -euxo pipefail

# Prep work.
rm -rf ~/.cockroach-certs cockroach-data
killall -9 cockroach || true
killall -9 mtproxy || true

# Create certificates.
export CERTSDIR=$HOME/.cockroach-certs
export COCKROACH_CA_KEY=$CERTSDIR/ca.key
./cockroach cert create-ca
./cockroach cert create-client root
./cockroach cert create-node 127.0.0.1 localhost
./cockroach mt cert create-tenant-client 123 --certs-dir=${HOME}/.cockroach-certs

#  Start KV layer. (:26257)
./cockroach start-single-node --host 127.0.0.1 --background
# Create tenant
./cockroach sql --host 127.0.0.1 -e 'select crdb_internal.create_tenant(123);'

# Spawn tenant SQL server (:36257) pointing at KV layer (:26257)
./cockroach mt start-sql --tenant-id 123 --kv-addrs 127.0.0.1:26257 --sql-addr 127.0.0.1:36257 &
sleep 1

# Create user on tenant (proxy does not forward client certs)
./cockroach sql --port 36257 -e "create user bob with password 'builder';"

# Spawn the proxy on :5432, forwarding to tenant SQL server (:36257)
go run ./pkg/cmd/mtproxy/ -cert-file $CERTSDIR/node.crt -key-file $CERTSDIR/node.key -verify=false --target 127.0.0.1:36257 &

sleep 5

# Connect to proxy.
./cockroach sql --host 127.0.0.1 --port 5432 --user bob
