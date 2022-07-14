#!/usr/bin/env bash
set -euo pipefail

print_usage() {
  cat<< EOF
USAGE: $0 COMMAND

Starts a CockroachDB 'movr' demo, executing COMMAND when the database serves
HTTP traffic on port 8080. The path to a file containing the connection URL
(from which a username and password can be extracted with standard URL parsing)
is provided to COMMAND via the \$CONN_URL_FILE environment variable.

When COMMAND exits, the database is stopped.

EXAMPLES:
    $0 'curl http://localhost:8080'
        Load index.html once the database has started.

    $0 'cat \$CONN_URL_FILE'
        Print the connection string once the database has started.
EOF
}

EXIT_CODE=-1
cleanup() {
  # Send an exit signal to the background CRDB job.
  kill %1

  # Wait for the background CRDB job to exit, using 'set +e' to allow 'wait' to
  # exit with non-zero. CRDB will exit non-zero (and 'wait' will too), but with
  # 'set -e' this entire script would normally exit *immediately*.
  set +e; wait %1; set -e

  # Forward the exit code from COMMAND to the calling shell.
  exit $EXIT_CODE;
}

# Command used improperly; print help and exit non-zero.
if [ $# -lt 1 ] || [ -z "$1" ]; then
  print_usage
  exit 1
fi

# Help explicitly requested; print help and exit zero.
if [ "$1" == "help" ] || [ "$1" == "-h" ] || [ "$1" == "--help" ]; then
  print_usage
  exit 0
fi

if curl --silent --head http://localhost:8080 > /dev/null; then
  cat<< EOF
ERROR: Another process is already listening on port 8080.  It may be a CRDB
       instance, but to be safe, this script is exiting early.

       Here's what we know about that process, courtesy of 'ps':\n
EOF

  ps -o pid -o command -p $(lsof -t -i :8080)
  exit 2
fi

# Accept an arbitrary Cockroach binary via the $COCKROACH environment variable
CRDB=${COCKROACH:-../../../../cockroach}

# Start a 'movr' demo cluster, writing the connection URL to a file.
export CONN_URL_FILE=$(mktemp)
$CRDB demo movr \
  --nodes=1 \
  --multitenant=false \
  --http-port=8080 \
  --listening-url-file=$CONN_URL_FILE \
  --execute 'select pg_sleep(1000000)' &

# Close that cluster whenever this script exits.
trap 'cleanup' EXIT

# Wait for the connection URL file to exist and for the database's HTTP server
# to be available.
until [ -s $CONN_URL_FILE ] && curl --fail --silent --head http://localhost:8080 > /dev/null; do
  sleep 0.25
done

# Use eval() to execute the provided command so that environment variables are
# expanded properly (e.g. `$0 'cat $CONN_URL_FILE'`, where $CONN_URL_FILE must
# be evaluated after this file is executed, not before).
eval $1

# Stash COMMAND's exit code so we can return it in the cleanup hook.
EXIT_CODE=$?
