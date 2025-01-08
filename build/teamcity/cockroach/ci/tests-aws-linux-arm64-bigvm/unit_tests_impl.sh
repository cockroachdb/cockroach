#!/usr/bin/env bash

# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -xeuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"

source "$dir/teamcity-support.sh"  # for 'tc_release_branch'

bazel build //pkg/cmd/bazci

EXTRA_PARAMS=""

if tc_bors_branch; then
  # enable up to 1 retry (2 attempts, worst-case) per test executable to report flakes but only on release branches and staging.
  EXTRA_PARAMS=" --flaky_test_attempts=2"
fi

$(bazel info bazel-bin)/pkg/cmd/bazci/bazci_/bazci -- test --config=ci --config=use_ci_timeouts -c fastbuild \
    //pkg:all_tests \
    --profile=/artifacts/profile.gz $EXTRA_PARAMS
