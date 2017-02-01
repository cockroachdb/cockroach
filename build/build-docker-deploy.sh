#!/usr/bin/env bash
# Build a statically linked Cockroach binary
#
# Author: Peter Mattis (peter@cockroachlabs.com)

set -euo pipefail

build_dir="$(dirname $0)"

source "${build_dir}"/build-common.sh

# This is mildly tricky: This script runs itself recursively. The
# first time it is run it does not take the if-branch below and
# executes on the host computer. It uses the builder.sh script to run
# itself inside of docker passing "docker" as the argument causing the
# commands in the if-branch to be executed within the docker
# container.
if [ "${1-}" = "docker" ]; then
    time make STATIC=1 build

    check_static cockroach
    strip -S cockroach

    mv cockroach ${build_dir}/deploy/cockroach

    exit 0
fi

# Relative path from the top-level cockroach directory to the current script.
relative_path_to_self="$(basename "$(cd "$(dirname "${0}")"; pwd)")/$(basename "${0}")"

# Build the CockroachDB binary.
"${build_dir}"/builder.sh "${relative_path_to_self}" docker

# Build the image.
docker build --tag=cockroachdb/cockroach "${build_dir}"/deploy
