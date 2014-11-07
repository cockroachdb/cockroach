#!/bin/bash
set -ex
cd -P "$(dirname $0)"
DIR=$(pwd -P)

rm -rf resources cockroach .out && mkdir -p .out
docker run "cockroachdb/cockroach-dev" shell "export STATIC=1 && \
  cd /cockroach && (rm -f cockroach && make clean build testbuild) >/dev/null 2>&1 && \
  tar -cf - cockroach \$(find . -name '*.test' -type f -printf '"%p" ') resources" \
| tar -xvC .out/ -f -
mv .out/cockroach .
cp -r .out/resources ./resources
docker build -t cockroachdb/cockroach .
docker run -v "${DIR}/.out":/test/.out cockroachdb/cockroach
