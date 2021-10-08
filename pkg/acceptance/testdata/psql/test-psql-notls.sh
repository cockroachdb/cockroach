#!/usr/bin/env bash

CERTS_DIR=${CERTS_DIR:-/certs}
crdb=$1
trap "set -x; killall cockroach cockroachshort || true" EXIT HUP

set -euo pipefail

# Disable automatic network access by psql.
unset PGHOST
unset PGPORT
# Use root access.
export PGUSER=root

set +x
echo "Testing non-TLS TCP connection via secure server."
set -x

# Start a server in secure mode and allow non-TLS SQL clients.
"$crdb" start-single-node --background \
        --certs-dir="$CERTS_DIR" --socket-dir=/tmp \
        --accept-sql-without-tls \
        --listen-addr=:12345

# Wait for server ready; also create a user that can log in.
"$crdb" sql --certs-dir="$CERTS_DIR" -e "create user foo with password 'pass'" -p 12345

# verify that psql can connect to the server without TLS but auth
# fails if they present the wrong password.
(env PGPASSWORD=wrongpass psql 'postgres://foo@localhost:12345?sslmode=disable' -c "select 1" 2>&1 || true) | grep "password authentication failed"

# now verify that psql can connect to the server without TLS with
# the proper password.
env PGPASSWORD=pass psql 'postgres://foo@localhost:12345?sslmode=disable' -c "select 1" | grep "1 row"

set +x
# Done.
"$crdb" quit --certs-dir="$CERTS_DIR" -p 12345
