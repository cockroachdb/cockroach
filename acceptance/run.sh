#!/bin/bash

set -eu

cd $(dirname $0)/..
source build/init-docker.sh
build/builder.sh make install
set -x
go test -v -tags acceptance ./acceptance ${GOFLAGS:-} -run "${TESTS:-.*}" -timeout ${TESTTIMEOUT:-5m} ${TESTFLAGS:-}
