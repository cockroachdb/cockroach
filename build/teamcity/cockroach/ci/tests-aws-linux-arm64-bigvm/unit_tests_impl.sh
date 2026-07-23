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

$(bazel info bazel-bin)/pkg/cmd/bazci/bazci_/bazci -- test --config=ci --config=use_ci_timeouts -c fastbuild \
    //pkg:all_tests \
    --profile=/artifacts/profile.gz $EXTRA_PARAMS
