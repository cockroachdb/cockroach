#!/usr/bin/env bash

# Copyright 2021 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -euo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"

source "$dir/teamcity-support.sh"  # For $root, changed_go_pkgs
source "$dir/teamcity-bazel-support.sh"  # For run_bazel

# The full race suite takes >4h and is only suitable for nightlies. On master
# and staging, run just the `server` package which (at the time of writing)
# takes <30m to run and exercises most of the code in the system at least
# rudimentarily. This (hopefully) causes obvious races to fail in bors,
# before spraying failures over the nightly stress suite.
canaryspec=./pkg/server
tc_start_block "Determine changed packages"
if tc_release_branch; then
  pkgspec="${canaryspec}"
  echo "On release branch ($TC_BUILD_BRANCH), so running canary testrace ($pkgspec)"
elif tc_bors_branch; then
  pkgspec="${canaryspec}"
  echo "On bors branch ($TC_BUILD_BRANCH), so running canary testrace ($pkgspec)"
else
  pkgspec=$(changed_go_pkgs)
  if [[ $(echo "$pkgspec" | wc -w) -gt 10 ]]; then
    echo "PR #$TC_BUILD_BRANCH changed many packages; skipping race detector tests"
    exit 0
  elif [[ -z "$pkgspec" ]]; then
    echo "PR #$TC_BUILD_BRANCH has no changed packages; skipping race detector tests"
    exit 0
  fi
  if [[ $pkgspec == *"./pkg/sql/opt"* ]]; then
    # If one opt package was changed, run all opt packages (the optimizer puts
    # various checks behind the race flag to keep them out of release builds).
    echo "$pkgspec" | sed 's$./pkg/sql/opt/[^ ]*$$g'
    pkgspec=$(echo "$pkgspec" | sed 's$./pkg/sql/opt[^ ]*$$g')
    pkgspec="$pkgspec ./pkg/sql/opt/..."
  fi
  echo "PR #$TC_BUILD_BRANCH has changed packages; running race detector tests on $pkgspec"
fi
tc_end_block "Determine changed packages"

tc_start_block "Run race detector tests"
run_bazel build/teamcity/cockroach/ci/tests/testrace_impl.sh $pkgspec
tc_end_block "Run race detector tests"
