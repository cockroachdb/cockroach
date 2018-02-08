#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "${0}")/teamcity-support.sh"

tc_prepare

tc_start_block "Ensure dependencies are up-to-date"
run build/builder.sh go install ./vendor/github.com/golang/dep/cmd/dep ./pkg/cmd/github-pull-request-make
run build/builder.sh env BUILD_VCS_NUMBER="$BUILD_VCS_NUMBER" TARGET=checkdeps github-pull-request-make
tc_end_block "Ensure dependencies are up-to-date"

tc_start_block "Ensure generated code is up-to-date"
run build/builder.sh make generate
# The workspace is clean iff `git status --porcelain` produces no output. Any
# output is either an error message or a listing of an untracked/dirty file.
if [[ "$(git status --porcelain 2>&1)" != "" ]]; then
  git status >&2 || true
  git diff -a >&2 || true
  exit 1
fi
tc_end_block "Ensure generated code is up-to-date"

tc_start_block "Lint"
run build/builder.sh make lint 2>&1 | tee artifacts/lint.log | go-test-teamcity
tc_end_block "Lint"

tc_start_block "Test web UI"
# Run the UI tests. This logically belongs in teamcity-test.sh, but we do it
# here to minimize total build time since this build has already generated the
# UI.
run build/builder.sh make -C pkg/ui
tc_end_block "Test web UI"
