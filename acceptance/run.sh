#!/bin/bash

set -eu

source $(dirname $0)/../build/init-docker.sh
$(dirname $0)/../build/builder.sh make install

set -x
go test -tags acceptance ./acceptance ${GOFLAGS-} -run "${TESTS-.*}" -timeout ${TESTTIMEOUT-5m} ${TESTFLAGS-}
