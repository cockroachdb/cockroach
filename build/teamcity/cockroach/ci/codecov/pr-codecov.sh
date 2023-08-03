#!/usr/bin/env bash

set -euo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"

source "$dir/teamcity-support.sh"  # For changed_go_pkgs
source "$dir/teamcity-bazel-support.sh"  # For run_bazel

tc_start_block "Determine changed packages"
if tc_release_branch; then
  echo "Build not intended to run on release branch ($TC_BUILD_BRANCH)"
  exit 0
elif tc_bors_branch; then
  echo "Build not intended to run on bors branch ($TC_BUILD_BRANCH)"
  exit 0
fi

PR=${TC_BUILD_BRANCH}

pkgspec=$(changed_go_pkgs)
if [[ $(echo "$pkgspec" | wc -w) -gt 10 ]]; then
  echo "PR #$PR changed too many packages; skipping code coverage"
  exit 0
elif [[ -z "$pkgspec" ]]; then
  echo "PR #$PR has no changed packages; skipping code coverage"
  exit 0
fi

echo "PR #$PR has changed packages; running code coverage on ${pkgspec}"
tc_end_block "Determine changed packages"

tc_start_block "Run tests with coverage after the change"
sha=$(git rev-parse HEAD)
outputfile="gs://radu-codecov-public/pr-cockroach/${PR}-${sha}.json"
run_bazel build/teamcity/cockroach/ci/codecov/pr-codecov-run-tests.sh "${outputfile}" "${pkgspec}"
tc_end_block "Run tests with coverage after the change"
