#!/usr/bin/env bash

# Copyright 2022 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

set -ueo pipefail

# Compute the absolute path to the root of the cockroach repo.
root=$(dirname $(dirname $(dirname $(dirname $(dirname $(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd ))))))
# Set up a working directory to ensure the original working directory isn't littered.
WORKDIR=$(mktemp -d)
# Stash the original working directory to return to later
ORIGINAL_PWD=$PWD

print_usage() {
  cat<< EOF
USAGE: $0 COMMAND [ARGS...]

Starts a CockroachDB node with an empty 'movr' database, executing COMMAND with
ARGS when the database serves HTTP traffic on port 8080.

When COMMAND exits, the database is stopped.

EXAMPLES:
    $0 'curl http://localhost:8080'
        Load index.html once the database has started.

    $0 'pnpm run cy:run'
        Run Cypress tests once the database has started.
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

  # Clean up the working directory.
  cd $ORIGINAL_PWD
  rm -rf $WORKDIR

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

# Use an arbitrary Cockroach binary via the $COCKROACH_ENTRYPOINT environment
# variable
COCKROACH=${COCKROACH:-$root/cockroach}

# Run cockroach from within the temporary directory, so extra files don't
# clutter the original PWD.
pushd $WORKDIR

# Start a single-node cluster using the cockroach.sh script, which automatically
# creates a user and database from provided environment variables.
# Use standard shell backgrounding (*not* the cockroach --background flag) so
# that this cluster can be shut down when $COMMAND exits.
COCKROACH=$COCKROACH \
COCKROACH_USER=cypress \
COCKROACH_PASSWORD=tests \
COCKROACH_DATABASE=movr \
$root/build/deploy/cockroach.sh \
  start-single-node \
  --http-port=8080 & &> /dev/null

# Close that cluster whenever this script exits.
trap 'cleanup' EXIT

# Wait for the connection URL file to exist and for the database's HTTP server
# to be available.
until [ -r ./init_success ] && curl --fail --silent --head http://localhost:8080 > /dev/null; do
  sleep 0.25
done

# Return to the original PWD before executing $COMMAND
popd

# Use eval() to execute the provided command so that environment variables are
# expanded properly (e.g. `$0 'cat $SHLVL'`, where $SHLVL must
# be evaluated after this file is executed, not before).
(
  # Temporarily disable globbing, so strings containing ** (e.g.
  # '--spec "cypress/e2e/health-check/**"') aren't evaluated.
  set -f; eval "$@"; set +f
)

# Stash COMMAND's exit code so we can return it in the cleanup hook.
EXIT_CODE=$?
