#!/usr/bin/env bash
set -exuo pipefail
build/builder.sh make check 2>&1 | go-test-teamcity
build/builder.sh go generate ./pkg/... 2>&1
build/builder.sh /bin/bash -c '! git status --porcelain | read || (git status; git diff -a; exit 1)' 2>&1

# If the code is new enough to have go generate not
# run the ui tests, run the ui tests.
if grep "make generate" pkg/ui/ui.go; then
    build/builder.sh make -C pkg/ui
fi
