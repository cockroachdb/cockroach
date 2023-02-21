#!/usr/bin/env bash

set -xeuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"

source "$dir/teamcity-support.sh"  # for 'tc_release_branch'

#bazel build //pkg/cmd/bazci --config=ci

EXTRA_PARAMS=""

if tc_release_branch; then
  # enable up to 2 retries (3 attempts, worst-case) per test executable to report flakes but only on release branches (i.e., not staging)
  EXTRA_PARAMS=" --flaky_test_attempts=3" 
fi

bazel test --config=cinolint -c fastbuild \
      //pkg:small_tests //pkg:medium_tests //pkg:large_tests //pkg:enormous_tests \
      --bes_results_url=https://app.buildbuddy.io/invocation/ \
      --bes_backend=grpcs://remote.buildbuddy.io \
      --remote_cache=grpcs://remote.buildbuddy.io \
      --remote_download_toplevel \
      --remote_timeout=3600 \
      --remote_header=x-buildbuddy-api-key=$BUILDBUDDY_API_KEY \
      --build_metadata=ROLE=CI \
      --experimental_remote_cache_compression \
      --build_metadata=COMMIT_SHA=$(git rev-parse HEAD) \
      --build_metadata=REPO_URL=https://github.com/cockroachdb/cockroach.git \
      --profile=/artifacts/profile.gz $EXTRA_PARAMS
