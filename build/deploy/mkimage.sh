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

rm -rf resources ui cockroach
mkdir -p build
docker run -v "$(pwd)/build":/build "cockroachdb/cockroach-dev" shell "cd /cockroach && \
  rm -rf /build/*
  make clean build testbuild >/dev/null 2>&1 && \
  find . -name '*.test' -type f -printf "\"/build/%h \"" | xargs mkdir -p && \
  find . -name '*.test' -type f -exec mv {} "/build/{}" \; && \
  cp -r resources /build/resources && \
  cp -r ui /build/ui && \
  cp cockroach /build/cockroach"

cp -r build/resources resources
cp -r build/ui ./ui
cp build/cockroach cockroach
docker build -t cockroachdb/cockroach .
docker run -v "${DIR}/build":/build cockroachdb/cockroach
