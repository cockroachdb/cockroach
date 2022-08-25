#!/usr/bin/env bash

set -xeuo pipefail

bazel build //pkg/cmd/bazci --config=ci

if tc_release_branch; then
  # enable up to 2 attempts per test executable to report flakes but only on release branches (i.e., not staging)
  $(bazel info bazel-bin --config=ci)/pkg/cmd/bazci/bazci_/bazci --config=cinolint --config=simplestamp --compilation_mode=fastbuild \
		                   test //pkg:small_tests //pkg:medium_tests //pkg:large_tests //pkg:enormous_tests -- \
                                   --profile=/artifacts/profile.gz --flaky_test_attempts=3
else 
  $(bazel info bazel-bin --config=ci)/pkg/cmd/bazci/bazci_/bazci --config=cinolint --config=simplestamp --compilation_mode=fastbuild \
                                   test //pkg:small_tests //pkg:medium_tests //pkg:large_tests //pkg:enormous_tests -- \
fi
