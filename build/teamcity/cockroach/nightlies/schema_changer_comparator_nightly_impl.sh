#!/usr/bin/env bash

# Copyright 2023 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -xeuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"
source "$dir/teamcity-support.sh"

exit_status=0


bazel build //pkg/cmd/bazci
BAZEL_BIN=$(bazel info bazel-bin)

# Run schema changer comparator test.
$BAZEL_BIN/pkg/cmd/bazci/bazci_/bazci test -- --config=ci \
  //pkg/sql/schemachanger:schemachanger_test \
  --test_filter='^TestSchemaChangeComparator' \
  --test_env=GO_TEST_WRAP_TESTV=1 \
  --test_env=GO_TEST_WRAP=1 \
  --test_env=COCKROACH_SCHEMA_CHANGE_COMPARATOR_SKIP=false \
  --test_timeout=7200 \
  || exit_status=$?

exit $exit_status
