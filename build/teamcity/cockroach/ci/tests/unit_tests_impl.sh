#!/usr/bin/env bash

set -xeuo pipefail

bazel build //pkg/cmd/bazci --config=ci

extra_params=""

if tc_release_branch; then
  # enable up to 2 retries (3 attempts, worst-case) per test executable to report flakes but only on release branches (i.e., not staging)
  extra_params=" --flaky_test_attempts=3" 
fi

$(bazel info bazel-bin --config=ci)/pkg/cmd/bazci/bazci_/bazci -- test --config=cinolint --config=simplestamp -c fastbuild \
                                  //pkg:small_tests //pkg:medium_tests //pkg:large_tests //pkg:enormous_tests \
                                   --profile=/artifacts/profile.gz $extra_params
