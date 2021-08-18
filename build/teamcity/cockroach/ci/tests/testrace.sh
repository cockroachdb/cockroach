#!/usr/bin/env bash

set -euo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"

source "$dir/teamcity-support.sh"  # For $root
source "$dir/teamcity-bazel-support.sh"  # For run_bazel

tc_prepare

tc_start_block "Determine changed packages"
if tc_release_branch; then
  pkgspec=./pkg/...
  echo "On release branch ($TC_BUILD_BRANCH), so running testrace on all packages ($pkgspec)"
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
