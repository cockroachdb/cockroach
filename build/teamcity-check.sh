#!/usr/bin/env bash

# Set this to 1 to require a "release justification" note in the commit message
# or the PR description.
require_justification=0

set -euo pipefail

source "$(dirname "${0}")/teamcity-support.sh"
source "$(dirname "${0}")/teamcity-bazel-support.sh"  # For run_bazel

function check_clean() {
  # The workspace is clean iff `git status --porcelain` produces no output. Any
  # output is either an error message or a listing of an untracked/dirty file.
  if [[ "$(git status --porcelain 2>&1)" != "" ]]; then
    git status >&2 || true
    git diff -a >&2 || true
    echo "====================================================" >&2
    echo "Some automatically generated code is not up to date." >&2
    echo $1 >&2
    exit 1
  fi
}

tc_prepare

if [ "$require_justification" = 1 ]; then
  tc_start_block "Ensure commit message contains a release justification"
  # Ensure master branch commits have a release justification.
  if [[ $(git log -n1 | grep -ci "Release justification: \S\+") == 0 ]]; then
    echo "Build Failed. No Release justification in the commit message or in the PR description." >&2
    echo "Commits must have a Release justification of the form:" >&2
    echo "Release justification: <some description of why this commit is safe to add to the release branch.>" >&2
    exit 1
  fi
  tc_end_block "Ensure commit message contains a release justification"
fi

tc_start_block "Ensure generated code is up-to-date"
# Buffer noisy output and only print it on failure.
if ! run run_bazel build/bazelutil/check.sh &> artifacts/buildshort.log || (cat artifacts/buildshort.log && false); then
    # The command will output instructions on how to fix the error.
    exit 1
fi
rm artifacts/buildshort.log
TEAMCITY_BAZEL_SUPPORT_LINT=1  # See teamcity-bazel-support.sh.
run run_bazel build/bazelutil/bazel-generate.sh &> artifacts/buildshort.log || (cat artifacts/buildshort.log && false)
rm artifacts/buildshort.log
check_clean "Run \`make bazel-generate\` to automatically regenerate these."
run build/builder.sh make generate &> artifacts/generate.log || (cat artifacts/generate.log && false)
rm artifacts/generate.log
check_clean "Run \`make generate\` to automatically regenerate these."
run build/builder.sh make buildshort &> artifacts/buildshort.log || (cat artifacts/buildshort.log && false)
rm artifacts/buildshort.log
check_clean "Run \`make buildshort\` to automatically regenerate these."
tc_end_block "Ensure generated code is up-to-date"

# generated code can generate new dependencies; check dependencies after generated code.
tc_start_block "Ensure dependencies are up-to-date"
# Run go mod tidy and `make -k vendor_rebuild` and ensure nothing changes.
run build/builder.sh go mod tidy
check_clean "Run \`go mod tidy\` and \`make -k vendor_rebuild\` to automatically regenerate these."
run build/builder.sh make -k vendor_rebuild
cd vendor
check_clean "Run \`make -k vendor_rebuild\` to automatically regenerate these."
cd ..
tc_end_block "Ensure dependencies are up-to-date"

tc_start_block "Lint"
# Disable ccache so that Go doesn't try to install dependencies into GOROOT,
# where it doesn't have write permissions. (Using ccache busts the Go package
# cache because it appears to the Go toolchain as a different C compiler than
# the toolchain was compiled with.) We've already built the C dependencies
# above, so we're not losing anything by turning it off.
#
# TODO(benesch): once GOPATH/pkg goes away because Go static analysis tools can
# rebuild on demand, remove this. Upstream issue: golang/go#25650.
run_json_test env COCKROACH_BUILDER_CCACHE= \
  build/builder.sh stdbuf -eL -oL make GOTESTFLAGS=-json lint
tc_end_block "Lint"

tc_start_block "Test web UI"
# Run the UI tests. This logically belongs in teamcity-test.sh, but we do it
# here to minimize total build time since this build has already generated the
# UI.
run build/builder.sh make -C pkg/ui
tc_end_block "Test web UI"
