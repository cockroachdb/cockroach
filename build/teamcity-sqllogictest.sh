#!/usr/bin/env bash
set -euxo pipefail

source "$(dirname "${0}")/teamcity-support.sh"

mkdir -p artifacts

# TestSqlLiteLogic needs the sqllogictest repo from the host's GOPATH, so we
# can't hide it like we do in the other teamcity build scripts.
# TODO(jordan) improve builder.sh to allow partial GOPATH hiding rather than
# the all-on/all-off strategy BUILDER_HIDE_GOPATH_SRC gives us.
export BUILDER_HIDE_GOPATH_SRC=0

# Run SqlLite tests.
# Need to specify the flex-types flag in order to skip past variations that have
# numeric typing differences.
# TODO(yuzefovich): remove crdb_test_off tag once sqllite tests have been
# adjusted to run in reasonable time with batch size randomizations.
run_json_test build/builder.sh \
  stdbuf -oL -eL \
  make test GOTESTFLAGS=-json TESTFLAGS="-v -bigtest -flex-types" TESTTIMEOUT='24h' PKG='./pkg/sql/logictest' TESTS='^TestSqlLiteLogic$$' TAGS=crdb_test_off

# Run the tests with a multitenant configuration.
run_json_test build/builder.sh \
  stdbuf -oL -eL \
  make test GOTESTFLAGS=-json TESTFLAGS="-v -bigtest -flex-types" TESTTIMEOUT='24h' PKG='./pkg/ccl/logictestccl' TESTS='^TestTenantSQLLiteLogic$$' TAGS=crdb_test_off
