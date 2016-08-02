#!/bin/bash
# Build various statically linked binaries.

set -euo pipefail

archive=$1
tags=${2-}

source $(dirname $0)/build-common.sh

time make STATIC=1 build
time make STATIC=1 testbuild PKG=./... TAGS="$tags acceptance"

# We don't check all test binaries, but one from each invocation.
check_static cockroach
check_static cli/cli.test

strip -S cockroach
find . -type f -name '*.test' | xargs strip -S

rm -f $archive
time tar cfz $archive -C .. $(git ls-files | sed -r 's,^,cockroach/,') $(find . -type f -name '*.test' -and -not -name acceptance.test | sed 's,^\./,cockroach/,')
