#!/usr/bin/env bash
# Build various statically linked binaries with race detection enabled.
#
# TODO(tamird): consolidate with build-static-binaries. We shouldn't have two
# files that do the same thing.

set -euo pipefail

archive=$1

source "$(dirname "${0}")"/build-common.sh

time make STATIC=1 build GOFLAGS=-race
# Build test binaries. Note that the acceptance binary will not be included in
# the archive, but is instead uploaded directly by push-aws.sh.
time make STATIC=1 testbuild PKG=./... TAGS=acceptance GOFLAGS=-race TESTBUILDPREFIX=race.

# We don't check all test binaries, but one from each invocation.
check_static cockroach
check_static cli/race.cli.test

strip -S cockroach
find . -type f -name '*.test' -exec strip -S {} ';'

rm -f "$archive"
time tar cfz "$archive" -C .. $(git ls-files | sed -r 's,^,cockroach/,') $(find . -type f -name '*.test' -and -not -name '*acceptance.test' | sed 's,^\./,cockroach/,')
