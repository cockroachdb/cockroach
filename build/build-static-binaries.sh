#!/usr/bin/env bash
# Build various statically linked binaries.

set -euo pipefail

archive=$1

source "$(dirname "${0}")"/build-common.sh

time make build TYPE=release-linux-gnu  GOFLAGS="${GOFLAGS-}" SUFFIX="${SUFFIX-}" TAGS="${TAGS-}"
time make build TYPE=release-linux-musl GOFLAGS="${GOFLAGS-}" SUFFIX="${SUFFIX-}" TAGS="${TAGS-}"
# Build test binaries. Note that the acceptance binary will not be included in
# the archive, but is instead uploaded directly by push-aws.sh.
time make testbuild TYPE=release-linux-gnu GOFLAGS="${GOFLAGS-}" SUFFIX="${SUFFIX-}" TAGS="${TAGS-} acceptance"

# We don't check all test binaries, but one from each invocation.
check_static "cockroach${SUFFIX-}"
check_static "cli/cli.test${SUFFIX-}"

# Try running the cockroach binary.
MALLOC_CONF=prof:true ./cockroach${SUFFIX-} version

rm -f "$archive"
time tar cfz "$archive" -C .. $(git ls-files | sed -r 's,^,cockroach/,') $(find . -type f -name '*.test*' -and -not -name 'acceptance.test*' | sed 's,^\./,cockroach/,')
