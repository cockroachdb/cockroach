#!/usr/bin/env bash
set -exuo pipefail


# All packages need to be installed before we can run (some) of the checks
# and code generators reliably. More precisely, anything that using
# x/tools/go/loader is fragile (this includes stringer, vet and others).
#
# The blocking issue is https://github.com/golang/go/issues/14120; see
# https://github.com/golang/go/issues/10249 for some more concrete discussion
# on `stringer` and https://github.com/golang/go/issues/16086 for `vet`.
export BUILDER_HIDE_UNVENDORED=1
build/builder.sh make gotestdashi

mkdir -p artifacts

build/builder.sh make check 2>&1 | tee artifacts/check.log | go-test-teamcity

build/builder.sh go generate ./pkg/...
build/builder.sh /bin/bash -c '! git status --porcelain | read || (git status; git diff -a 1>&2; exit 1)'

# If the code is new enough to have go generate not
# run the ui tests, run the ui tests.
if grep "make generate" pkg/ui/ui.go; then
    build/builder.sh make -C pkg/ui
fi
