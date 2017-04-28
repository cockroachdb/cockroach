#!/usr/bin/env bash
set -euxo pipefail

mkdir -p artifacts

# make bigtest needs the sqllogictest repo from the host's GOPATH, so we can't
# hide it like we do in the other teamcity build scripts.
# TODO(jordan) improve builder.sh to allow partial GOPATH hiding rather than
# the all-on/all-off strategy BULIDER_HIDE_GOPATH_SRC gives us.
export BUILDER_HIDE_GOPATH_SRC=0

for config in default distsql; do
    build/builder.sh env \
        make test TESTFLAGS="-v -bigtest -config ${config}" TESTTIMEOUT='24h' PKG='./pkg/sql' TESTS='^TestLogic$$' 2>&1 \
        | tee "artifacts/${config}.log" \
        | go-test-teamcity
done
