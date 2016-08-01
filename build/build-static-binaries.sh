#!/bin/bash
# Build various statically linked binaries.

set -euo pipefail

source $(dirname $0)/build-common.sh

time make STATIC=1 build
time make STATIC=1 testbuild PKG=./... TAGS=acceptance

# We don't check all test binaries, but one from each invocation.
check_static cockroach
check_static cli/cli.test

# Strip the binary and all the tests.
strip -S cockroach
find . -name '*.test' | xargs strip -S

rm -f static-tests.tar.gz
time tar cfz static-tests.tar.gz -C ../ cockroach/ --exclude '.git'
