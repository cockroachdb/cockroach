#!/bin/bash
# Build cockroach binary using the default allocator (as opposed to jemalloc).

set -euo pipefail

source $(dirname $0)/build-common.sh

time make STATIC=1 build TAGS=stdmalloc
time make STATIC=1 testbuild PKG=./... TAGS='acceptance stdmalloc'

# We don't check all test binaries, but one from each invocation.
check_static cockroach
check_static cli/cli.test

# Strip the binary and all the tests.
strip -S cockroach
find . -name '*.test' | xargs strip -S

cd ..
rm -f static-tests.stdmalloc.tar.gz
time tar cfz static-tests.stdmalloc.tar.gz $(find cockroach -name '*.test') $(git -C cockroach ls-files)
