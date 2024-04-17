#!/usr/bin/env bash

set -xeuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"

if [ -z "${TAGS-}" ]
then
    TAGS=bazel,gss
else
    TAGS="bazel,gss,$TAGS"
fi

GIT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
if [ "$TC_BUILD_BRANCH" != "$GIT_BRANCH" ]; then
    echo "Skipping test $TARGET, as the expected branch is $TC_BUILD_BRANCH, but actual branch is $GIT_BRANCH"
    exit 0
else
    echo "Confirmed that git branch is $GIT_BRANCH matches build branch $TC_BUILD_BRANCH"
fi

bazel build //pkg/cmd/bazci --config=ci
BAZEL_BIN=$(bazel info bazel-bin --config=ci)
ARTIFACTS_DIR=/artifacts

if [[ ! -z $(bazel query "attr(tags, \"broken_in_bazel\", $TARGET)") ]]
then
    echo "Skipping test $TARGET as it is broken in bazel"
    exit 0
fi
if [[ ! -z $(bazel query "attr(tags, \"integration\", $TARGET)") ]]
then
    echo "Skipping test $TARGET as it is an integration test"
    exit 0
fi

GOTESTTIMEOUTSECS=$(($TESTTIMEOUTSECS - 5))
$BAZEL_BIN/pkg/cmd/bazci/bazci_/bazci -- test --config=ci "$TARGET" \
                                        --test_env=COCKROACH_NIGHTLY_STRESS=true \
                                        --test_env=GOTRACEBACK=all \
                                        --test_timeout="$TESTTIMEOUTSECS" \
                                        --test_arg=-test.timeout="${GOTESTTIMEOUTSECS}s" \
                                        --run_under "@com_github_cockroachdb_stress//:stress -bazel -shardable-artifacts 'XML_OUTPUT_FILE=$BAZEL_BIN/pkg/cmd/bazci/bazci_/bazci merge-test-xmls' $STRESSFLAGS" \
                                        --define "gotags=$TAGS" \
                                        --nocache_test_results \
                                        --test_output streamed \
                                        ${EXTRA_BAZEL_FLAGS}
