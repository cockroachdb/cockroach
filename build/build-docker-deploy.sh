#!/bin/bash
# Build a statically linked Cockroach binary
#
# Author: Peter Mattis (peter@cockroachlabs.com)

set -euo pipefail

source $(dirname $0)/build-common.sh

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

# Relative path from the top-level cockroach directory to the current script.
relative_path_to_self="$(basename $(cd $(dirname $0); pwd))/$(basename $0)"

# Build the CockroachDB binary.
$(dirname $0)/builder.sh ${relative_path_to_self} docker

# Build the image.
docker build --tag=cockroachdb/cockroach "$(dirname $0)/deploy"
