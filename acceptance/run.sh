#!/bin/bash

cd $(dirname $0)/..
build/builder.sh make install
set -x
go test -v -tags acceptance ./acceptance ${GOFLAGS} -run "${TESTS:-.*}" -timeout ${TESTTIMEOUT:-5m} ${TESTFLAGS}
