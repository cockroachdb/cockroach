#!/bin/bash

set -e
cd $(dirname $0)/..
if [ x"${DOCKER_HOST}" = "x" -a "$(uname)" = "Darwin" ]; then
    if ! type -P "boot2docker" >& /dev/null; then
	echo "boot2docker not found!"
	exit 1
    fi
    echo "boot2docker shellinit # initializing DOCKER_* env variables"
    eval $(boot2docker shellinit 2>/dev/null)
fi
build/verify-docker.sh
build/builder.sh make install
set -x
go test -v -tags acceptance ./acceptance ${GOFLAGS} -run "${TESTS:-.*}" -timeout ${TESTTIMEOUT:-5m} ${TESTFLAGS}
