#!/usr/bin/env bash

set -xeuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"
source "$dir/teamcity-support.sh"

exit_status=0


bazel build //pkg/cmd/bazci --config=ci
BAZEL_BIN=$(bazel info bazel-bin --config=ci)

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
