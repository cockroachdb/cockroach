#!/usr/bin/env bash

# Set this to 1 to require a "release justification" note in the commit message
# or the PR description.
require_justification=1

set -euo pipefail

source "$(dirname "${0}")/teamcity-support.sh"

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
