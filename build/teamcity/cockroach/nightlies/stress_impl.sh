#!/usr/bin/env bash

set -xeuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"

if [ -z "${TAGS-}" ]
then
    TAGS=bazel,gss
else
    TAGS="bazel,gss,$TAGS"
fi

bazel build //pkg/cmd/bazci --config=ci
BAZEL_BIN=$(bazel info bazel-bin --config=ci)
ARTIFACTS_DIR=/artifacts

if [[ ! -z $(bazel query "attr(tags, \"integration\", $TARGET)") ]]
then
    echo "Skipping test $TARGET as it is an integration test"
    exit 0
fi

GOTESTTIMEOUTSECS=$(($TESTTIMEOUTSECS - 5))
COCKROACH_NIGHTLY_STRESS=true $BAZEL_BIN/pkg/cmd/bazci/bazci_/bazci -- test --config=ci "$TARGET" \
                                        --test_env=COCKROACH_NIGHTLY_STRESS=true \
                                        --test_env=GOTRACEBACK=all \
                                        --test_timeout="$TESTTIMEOUTSECS" \
                                        --test_arg=-test.timeout="${GOTESTTIMEOUTSECS}s" \
                                        --run_under "@com_github_cockroachdb_stress//:stress -bazel -shardable-artifacts 'XML_OUTPUT_FILE=$BAZEL_BIN/pkg/cmd/bazci/bazci_/bazci merge-test-xmls' $STRESSFLAGS" \
                                        --define "gotags=$TAGS" \
                                        --nocache_test_results \
                                        --test_output streamed \
                                        ${EXTRA_BAZEL_FLAGS}
