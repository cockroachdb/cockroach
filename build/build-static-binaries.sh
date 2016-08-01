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

binaries=$(find cockroach -name '*.test' -or -name cockroach)
echo $binaries | xargs strip -S

rm -f $archive
time tar cfz $archive -C .. $(echo $binaries $(git ls-files) | sed s,^,cockroach/,)
