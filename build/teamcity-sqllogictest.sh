#!/usr/bin/env bash
set -euxo pipefail

source "$(dirname "${0}")/teamcity-support.sh"

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
USE_BUILDER_IMAGE=20210205-000935 run_json_test build/builder.sh \
  stdbuf -oL -eL \
  make test GOTESTFLAGS=-json TESTFLAGS="-v -bigtest" TESTTIMEOUT='24h' PKG='./pkg/sql/logictest' TESTS='^TestSqlLiteLogic$$'

# Need to specify the flex-types flag in order to skip past variations that have
# numeric typing differences.
USE_BUILDER_IMAGE=20210205-000935 run_json_test build/builder.sh \
  stdbuf -oL -eL \
  make test GOTESTFLAGS=-json TESTFLAGS="-v -bigtest -config local,fakedist -flex-types" TESTTIMEOUT='24h' PKG='./pkg/sql/logictest' TESTS='^TestSqlLiteCorrelatedLogic$$'
