#!/usr/bin/env bash
set -euo pipefail

# This script can be used as the `-exec` value of a `go test` invocation when
# we have a precompiled pkg.test binary that we want to run through `go test`.
# Usually the reason is wanting to use `go test -json` in a situation where
# we are forced to operate outside of the builder image (for docker reasons).

# Change from anywhere to repo root.
cd "$(dirname $0)/.."

actual=$1

# Change to directory the test binary is in.
# For example, ./pkg/acceptance/acceptance.test --> cd ./pkg/acceptance
cd "$(dirname "${actual}")"

# Throw path to script away.
shift
# Throw `go test`s binary away.
shift

# Following the example, invoke `./acceptance.test`.
GOROOT=/home/agent/work/.go "./$(basename "$actual")" "$@"

