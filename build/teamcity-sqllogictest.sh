#!/usr/bin/env bash
set -euxo pipefail

mkdir -p artifacts

# TestSqlLiteLogic needs the sqllogictest repo from the host's GOPATH, so we
# can't hide it like we do in the other teamcity build scripts.
# TODO(jordan) improve builder.sh to allow partial GOPATH hiding rather than
# the all-on/all-off strategy BUILDER_HIDE_GOPATH_SRC gives us.
export BUILDER_HIDE_GOPATH_SRC=0

# Run SqlLite tests that do not contain correlated subqueries first, since the
# heuristic planner does not support that feature. Afterwards, run additional
# tests that do require correlated subquery support, but only with the cost-
# based optimizer.
for config in local local-opt fakedist fakedist-opt fakedist-disk; do
    build/builder.sh \
        stdbuf -oL -eL \
        make test TESTFLAGS="-v -bigtest -config ${config}" TESTTIMEOUT='24h' PKG='./pkg/sql/logictest' TESTS='^TestSqlLiteLogic$$' 2>&1 \
        | tee "artifacts/${config}.log" \
        | go-test-teamcity
done

# Need to specify the flex-types flag in order to skip past variations that have
# numeric typing differences.
for config in local-opt fakedist-opt; do
    build/builder.sh \
        stdbuf -oL -eL \
        make test TESTFLAGS="-v -bigtest -config ${config} -flex-types" TESTTIMEOUT='24h' PKG='./pkg/sql/logictest' TESTS='^TestSqlLiteCorrelatedLogic$$' 2>&1 \
        | tee "artifacts/${config}.log" \
        | go-test-teamcity
done
