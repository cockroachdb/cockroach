#!/usr/bin/env bash

# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -euo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"

source "$dir/teamcity-support.sh"  # for 'tc_release_branch'

bazel build //pkg/cmd/bazci

# Omit the ui_test as it depends on Javascript stuff; we don't have nodejs stuff
# for s390x and it's expensive to pull in anyway.
TESTS=$(bazel query 'kind(go_test, pkg/...) except attr("tags", "[\[ ]integration[,\]]", kind(go_test, pkg/...))' | grep -v ui_test)

set -x

$(bazel info bazel-bin)/pkg/cmd/bazci/bazci_/bazci --extralabels=s390x-test-failure -- \
		       test --config=ci --config=dev \
		       $TESTS \
		       --profile=/artifacts/profile.gz
