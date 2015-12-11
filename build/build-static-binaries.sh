#!/bin/bash
# Build various statically linked binaries.

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
    time make STATIC=1 build
    time make STATIC=1 testbuild PKG=./sql
    time make STATIC=1 testbuild PKG=./acceptance TAGS=acceptance

    check_static cockroach
    check_static sql/sql.test
    check_static acceptance/acceptance.test

    strip -S cockroach
    strip -S sql/sql.test
    strip -S acceptance/acceptance.test
    exit 0
fi

# Build the cockroach and test binaries.
$(dirname $0)/builder.sh $0 docker
