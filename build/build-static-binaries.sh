#!/bin/bash
# Build various statically linked binaries.

set -euo pipefail

source $(dirname $0)/build-common.sh

# This is mildly tricky: This script runs itself recursively. The
# first time it is run it does not take the if-branch below and
# executes on the host computer. It uses the builder.sh script to run
# itself inside of docker passing "docker" as the argument causing the
# commands in the if-branch to be executed within the docker
# container.
if [ "${1-}" = "docker" ]; then
    test_build_dir=$(mktemp -d test-binaries.XXXX)
    time make STATIC=1 build
    # sql/sql.test is built standalone as well to simplify the nightly logictest runs.
    time make STATIC=1 testbuild STRIPPED=1 PKG=./sql
    time make STATIC=1 testbuild STRIPPED=1 PKG=./acceptance TAGS=acceptance
    time make STATIC=1 testbuild STRIPPED=1 DIR=${test_build_dir}

    # We don't check all test binaries, but one from each invocation.
    check_static cockroach
    check_static sql/sql.test
    check_static acceptance/acceptance.test
    check_static ${test_build_dir}/github.com/cockroachdb/cockroach/sql/sql.test

    strip -S cockroach

    rm -f static-tests.tar.gz
    # Skip the project/repo part of the path inside the tarball.
    # Even for stripped binaries, gzip results in 167MB (21s) vs 512MB (3s).
    # It makes a big difference when fetching from outside AWS.
    time tar cfz static-tests.tar.gz -C ${test_build_dir}/github.com/cockroachdb/ cockroach/
    exit 0
fi

# Build the cockroach and test binaries.
$(dirname $0)/builder.sh $0 docker
