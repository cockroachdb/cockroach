#!/usr/bin/env bash

# Copyright 2020 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


CERTS_DIR=${CERTS_DIR:-/certs}
crdb=$1
trap "set -x; cat /tmp/server_pid | xargs kill -9 || true" EXIT HUP

set -euo pipefail

# Disable automatic network access by psql.
unset PGHOST
unset PGPORT
# Use root access.
export PGUSER=root

echo "Testing Unix socket connection via insecure server."
set -x

# Start an insecure CockroachDB server.
# We use a different port number from standard for an extra guarantee that
# "psql" is not going to find it.
"$crdb" start-single-node --background --insecure \
              --socket-dir=/tmp --pid-file=/tmp/server_pid \
              --listen-addr=localhost:12345

# Wait for server ready.
"$crdb" sql --insecure -e "select 1" -p 12345

# Verify that psql can connect to the server.
psql -h /tmp -p 12345 -c "select 1" | grep "1 row"

# It worked.
kill `cat /tmp/server_pid` || true
sleep 1; kill -9 `cat /tmp/server_pid` || true

set +x
echo "Testing Unix socket connection via secure server."
set -x

# Restart the server in secure mode.
"$crdb" start-single-node --background \
              --certs-dir="$CERTS_DIR" --socket-dir=/tmp --pid-file=/tmp/server_pid \
              --listen-addr=localhost:12345

# Wait for server ready; also create a user that can log in.
"$crdb" sql --certs-dir="$CERTS_DIR" -e "create user foo with password 'pass'" -p 12345

# Also verify that psql can connect to the server.
env PGPASSWORD=pass psql -U foo -h /tmp -p 12345 -c "select 1" | grep "1 row"

