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
set -ex
cd -P "$(dirname $0)"
DIR=$(pwd -P)

function cleanup() {
  # Files in ./build may belong to root, so let's delete that folder via this hack.
  docker run -v "${DIR}/build":/build "cockroachdb/cockroach-dev" shell "rm -rf /build/*"
}
trap cleanup EXIT

mkdir -p build
docker run -v "${DIR}/build":/build "cockroachdb/cockroach-dev" shell "cd /cockroach && \
  rm -rf /build/* && \
  make testbuild && \
  make STATIC=1 release && \
  cp -r cockroach *.test /build/"

# Make sure the created binary is statically linked.
# Seems awkward to do this programmatically, but
# this should work.
file build/cockroach | grep 'statically linked' &>/dev/null

docker build -t cockroachdb/cockroach .
docker run -v "${DIR}/build":/build cockroachdb/cockroach
