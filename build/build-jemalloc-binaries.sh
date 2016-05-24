#!/bin/bash
# Build cockroach binary using jemalloc as the allocator.

set -euo pipefail

source $(dirname $0)/build-common.sh

test_build_dir=$(mktemp -d test-binaries.XXXX)
time make STATIC=1 build TAGS="jemalloc"
time make STATIC=1 testbuildall DIR=${test_build_dir} TAGS="jemalloc"

# We don't check all test binaries, but one from each invocation.
check_static cockroach
check_static ${test_build_dir}/github.com/cockroachdb/cockroach/sql/sql.test

strip -S cockroach

rm -f static-tests.jemalloc.tar.gz
time tar cfz static-tests.jemalloc.tar.gz -C ${test_build_dir}/github.com/cockroachdb/ cockroach/
