#!/usr/bin/env bash
set -euo pipefail

# This script can be used as the `-exec` value of a `go test` invocation when
# we have a precompiled pkg.test binary that we want to run through `go test`.
# Usually the reason is wanting to use `go test -json` in a situation where
# we are forced to operate outside of the builder image (for docker reasons).

actual=$1
shift
# Throw `go test`s binary away.
shift

echo "$actual" "$@"

