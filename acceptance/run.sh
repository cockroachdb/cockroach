#!/usr/bin/env bash

set -eu

source $(dirname $0)/../build/init-docker.sh
$(dirname $0)/../build/builder.sh make install GOFLAGS='-tags clockoffset'

set -x
go test -tags acceptance ./acceptance ${GOFLAGS-} -run "${TESTS-.}" -timeout ${TESTTIMEOUT-10m} ${TESTFLAGS--v -nodes 3}
