#!/bin/bash

# Sample script to run a minimal cockroachdb deployment using sqlproxy
# listening on its default port at localhost:46257.
# This consists of a single-node db cluster, a sql tenant server, and a proxy.
# The proxy listens on :46257, forwarding to tenant SQL server at :36257.
# Finally a sql shell is opened for tenant id 123 and user `bob`.
# The password for `bob` is `builder`.
#
# WARNING: directory `~/.cockroach-certs` will be DELETED.
# WARNING: all cockroach and sqlproxy processes will be killed.

COCKROACH=${1:-'./cockroach'}
SQLPROXY=${2:-$COCKROACH mt start-proxy --target-addr 127.0.0.1:36257}

set -euxo pipefail

# Prep work.
rm -rf ~/.cockroach-certs cockroach-data
killall -9 cockroach || true
killall -9 sqlproxy || true

# Create certificates.
export CERTSDIR=$HOME/.cockroach-certs
export COCKROACH_CA_KEY=$CERTSDIR/ca.key
$COCKROACH cert create-ca
$COCKROACH cert create-client root
$COCKROACH cert create-node 127.0.0.1 localhost
$COCKROACH mt cert create-tenant-client 123 --certs-dir=${HOME}/.cockroach-certs

#  Start KV layer. (:26257)
$COCKROACH start-single-node --host 127.0.0.1 --background
# Create tenant
$COCKROACH sql --host 127.0.0.1 -e 'select crdb_internal.create_tenant(123);'

# Spawn tenant SQL server (:36257) pointing at KV layer (:26257)
COCKROACH_TRUST_CLIENT_PROVIDED_SQL_REMOTE_ADDR=true $COCKROACH mt start-sql --tenant-id 123 --kv-addrs 127.0.0.1:26257 --sql-addr 127.0.0.1:36257 --logtostderr=NONE &
sleep 1

# Create user on tenant (proxy does not forward client certs)
$COCKROACH sql --port 36257 -e "create user bob with password 'builder';"

# Spawn the proxy on :46257, forwarding to tenant SQL server (:36257)
$SQLPROXY --listen-addr 127.0.0.1:46257 --listen-cert $CERTSDIR/node.crt --listen-key $CERTSDIR/node.key &

sleep 2

# Connect to proxy to `defaultdb`. Note the need to prefix the db name with the
# magic phrase 'prancing-pony'.
# Password for user `bob` is `builder`.
$COCKROACH sql --url "postgresql://bob:builder@127.0.0.1:46257/prancing-pony.defaultdb"
