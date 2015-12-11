#!/bin/bash
# Build a statically linked Cockroach binary
#
# Author: Peter Mattis (peter@cockroachlabs.com)

set -euo pipefail

function check_static() {
    local libs=$(ldd $1 | egrep -v '(linux-vdso\.|librt\.|libpthread\.|libm\.|libc\.|ld-linux-)')
    if [ -n "${libs}" ]; then
	echo "$1 is not properly statically linked"
	ldd $1
	exit 1
    fi
}

# This is mildly tricky: This script runs itself recursively. The
# first time it is run it does not take the if-branch below and
# executes on the host computer. It uses the builder.sh script to run
# itself inside of docker passing "docker" as the argument causing the
# commands in the if-branch to be executed within the docker
# container.
if [ "${1-}" = "docker" ]; then
    time make STATIC=1 release

    check_static cockroach

    mv cockroach build/deploy/cockroach

    exit 0
fi

# Build the cockroach and test binaries.
$(dirname $0)/builder.sh $0 docker

# Build the image.
docker build --tag=cockroachdb/cockroach "$(dirname $0)/deploy"
