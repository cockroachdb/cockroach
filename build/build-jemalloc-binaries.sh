#!/bin/bash
# Build cockroach binary using jemalloc as the allocator.

set -euo pipefail

source $(dirname $0)/build-common.sh

# This is mildly tricky: This script runs itself recursively. The
# first time it is run it does not take the if-branch below and
# executes on the host computer. It uses the builder.sh script to run
# itself inside of docker passing "docker" as the argument causing the
# commands in the if-branch to be executed within the docker
# container.
if [ "${1-}" = "docker" ]; then
    time make STATIC=1 build TAGS="jemalloc"
    check_static cockroach
    strip -S cockroach
    exit 0
fi

# Build the cockroach and test binaries.
$(dirname $0)/builder.sh $0 docker
