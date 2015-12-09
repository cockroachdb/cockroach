#!/bin/bash
# Build various binaries.

set -euo pipefail

# This is mildly tricky: This script runs itself recursively. The
# first time it is run it does not take the if-branch below and
# executes on the host computer. It uses the builder.sh script to run
# itself inside of docker passing "docker" as the argument causing the
# commands in the if-branch to be executed within the docker
# container.
if [ "${1-}" = "docker" ]; then
    time make build
    time make testbuild PKG=./sql
    time make testbuild PKG=./acceptance TAGS=acceptance

    strip -S cockroach
    strip -S sql/sql.test
    strip -S acceptance/acceptance.test
    exit 0
fi

# Build the cockroach and test binaries.
$(dirname $0)/builder.sh $0 docker
