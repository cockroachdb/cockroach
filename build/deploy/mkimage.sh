#!/bin/bash
# Build a statically linked Cockroach binary
#
# Requires a working cockroach/cockroach-dev image, in which a statically
# linked (linux64) binary is built. Using this binary, a deploy image
# based on BusyBox is created.
# Additionally, we built statically linked tests which are mounted into
# the appropriate location on the deploy image, running them once. These
# are not a part of the resulting image but make sure that at least on
# the machine that creates the deploy image, the tests all pass.
#
# Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)
set -ex
cd -P "$(dirname $0)"
DIR=$(pwd -P)

rm -rf resources cockroach .out && mkdir -p .out
docker run "cockroachdb/cockroach-dev" shell "export STATIC=1 && \
  cd /cockroach && (rm -f cockroach && make clean build testbuild) >/dev/null 2>&1 && \
  tar -cf - cockroach \$(find . -name '*.test' -type f -printf '"%p" ') resources" \
> .out/files.tar;
tar -xvC .out/ -f .out/files.tar && rm -f .out/files.tar
mv .out/cockroach .
cp -r .out/resources ./resources
cp -r .out/ui ./ui
docker build -t cockroachdb/cockroach .
docker run -v "${DIR}/.out":/test/.out cockroachdb/cockroach
