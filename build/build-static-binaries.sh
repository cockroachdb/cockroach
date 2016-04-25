#!/bin/bash
# Build various statically linked binaries.

set -euo pipefail

source $(dirname $0)/build-common.sh

test_build_dir=$(mktemp -d test-binaries.XXXX)
time make STATIC=1 build
# sql/sql.test is built standalone as well to simplify the nightly logictest runs.
time make STATIC=1 testbuild PKG=./sql
time make STATIC=1 testbuild PKG=./acceptance TAGS=acceptance
time make STATIC=1 testbuildall DIR=${test_build_dir}

# We don't check all test binaries, but one from each invocation.
check_static cockroach
check_static sql/sql.test
check_static acceptance/acceptance.test
check_static ${test_build_dir}/github.com/cockroachdb/cockroach/sql/sql.test

strip -S cockroach
strip -S sql/sql.test
strip -S acceptance/acceptance.test

rm -f static-tests.tar.gz
# Skip the project/repo part of the path inside the tarball.
# Even for stripped binaries, gzip results in 167MB (21s spent) vs 512MB (3s spent).
# It makes a big difference when fetching from outside AWS.
time tar cfz static-tests.tar.gz -C ${test_build_dir}/github.com/cockroachdb/ cockroach/
