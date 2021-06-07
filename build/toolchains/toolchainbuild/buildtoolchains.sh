#!/usr/bin/env bash

set -euxo pipefail

this_dir="$(cd "$(dirname "${0}")"; pwd)"
toplevel="$(dirname $(dirname $(dirname $this_dir)))"

mkdir -p "${toplevel}"/artifacts

# note: the Docker image should match the base image of
# `cockroachdb/builder`.
docker run --rm -i ${tty-} -v $this_dir:/bootstrap \
       -v "${toplevel}"/artifacts:/artifacts \
       ubuntu:focal-20210119 /bootstrap/perform-build.sh
