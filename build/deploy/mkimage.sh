#!/bin/bash
# Build a statically linked Cockroach binary
#
# Requires a working cockroach/cockroach-dev image from which the cockroach
# binary and some other necessary resources are taken. Additionally, we built
# test binaries which are mounted into the appropriate location on the deploy
# image, running them once. These are not a part of the resulting image but
# make sure that at least on the machine that creates the deploy image, the
# tests all pass.
#
# Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

set -euo pipefail

# This is mildly trick: This script runs itself recursively. The first
# time it is run it does not take the if-branch below and executes on
# the host computer. It uses the builder.sh script to run itself
# inside of docker passing "docker" as the argument causing the
# commands in the if-branch to be executed within the docker
# container.
if [ "${1:-}" = "docker" ]; then
    time make testbuild
    time make STATIC=1 release

    # Make sure the created binary is statically linked.  Seems
    # awkward to do this programmatically, but this should work.
    file cockroach | grep -F 'statically linked' > /dev/null

    rm -fr build/deploy/build
    mkdir -p build/deploy/build
    mv cockroach *.test build/deploy/build
    cp build/deploy/test.sh build/deploy/build/test.sh

    exit 0
fi

# Build the cockroach and test binaries.
$(dirname $0)/../builder.sh $0 docker

cd -P "$(dirname $0)"
DIR=$(pwd -P)

# Build the image.
docker build -t cockroachdb/cockroach .
# Run the tests.
docker run -v "${DIR}/build":/build cockroachdb/cockroach
