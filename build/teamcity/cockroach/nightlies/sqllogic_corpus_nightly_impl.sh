#!/usr/bin/env bash

set -xeuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"
source "$dir/teamcity-bazel-support.sh"  # For process_test_json
source "$dir/teamcity-support.sh"  # For process_test_json

bazel build //pkg/cmd/bazci //pkg/cmd/github-post //pkg/cmd/testfilter --config=ci
BAZEL_BIN=$(bazel info bazel-bin --config=ci)
google_credentials="$GOOGLE_EPHEMERAL_CREDENTIALS"

log_into_gcloud

ARTIFACTS_DIR=/artifacts
GO_TEST_JSON_OUTPUT_FILE=$ARTIFACTS_DIR/test.json.txt
GO_TEST_GEN_JSON_OUTPUT_FILE=$ARTIFACTS_DIR/test-gen.json.txt
GO_TEST_GEN_CCL_JSON_OUTPUT_FILE=$ARTIFACTS_DIR/test-gen-ccl.json.txt
GO_TEST_VALIDATE_JSON_OUTPUT_FILE=$ARTIFACTS_DIR/test-validate.json.txt
GO_TEST_JSON_OUTPUT_FILE_MIXED=$ARTIFACTS_DIR/test-mixed.json.txt
GO_TEST_VALIDATE_JSON_OUTPUT_FILE_MIXED=$ARTIFACTS_DIR/test-validate-mixed.json.txt

mkdir -p $ARTIFACTS_DIR/corpus
mkdir -p $ARTIFACTS_DIR/corpus-mixed
exit_status=0

# Generate a corpus for all non-mixed version variants
$BAZEL_BIN/pkg/cmd/bazci/bazci_/bazci --config=ci \
    test //pkg/sql/logictest:logictest_test -- \
    --test_arg=--declarative-corpus=$ARTIFACTS_DIR/corpus \
    --test_filter='^TestLogic/(local|local-legacy-schema-changer|3node|multiregion).*$' \
    --test_env=GO_TEST_WRAP_TESTV=1 \
    --test_env=GO_TEST_WRAP=1 \
    --test_env=GO_TEST_JSON_OUTPUT_FILE=$GO_TEST_JSON_OUTPUT_FILE \
    --test_timeout=7200 \
    || exit_status=$?


process_test_json \
  $BAZEL_BIN/pkg/cmd/testfilter/testfilter_/testfilter \
  $BAZEL_BIN/pkg/cmd/github-post/github-post_/github-post \
  $ARTIFACTS_DIR \
  $GO_TEST_JSON_OUTPUT_FILE \
  $exit_status

$BAZEL_BIN/pkg/cmd/bazci/bazci_/bazci --config=ci \
    test //pkg/ccl/logictestccl:logictestccl_test -- \
    --test_arg=--declarative-corpus=$ARTIFACTS_DIR/corpus \
    --test_filter='^(TestCCLLogic|TestTenantLogic)/(local|local-legacy-schema-changer|3node|multiregion).*$' \
    --test_env=GO_TEST_WRAP_TESTV=1 \
    --test_env=GO_TEST_WRAP=1 \
    --test_env=GO_TEST_JSON_OUTPUT_FILE=$GO_TEST_JSON_OUTPUT_FILE \
    --test_timeout=7200 \
    || exit_status=$?

process_test_json \
  $BAZEL_BIN/pkg/cmd/testfilter/testfilter_/testfilter \
  $BAZEL_BIN/pkg/cmd/github-post/github-post_/github-post \
  $ARTIFACTS_DIR \
  $GO_TEST_JSON_OUTPUT_FILE \
  $exit_status

# Generate corpuses from end-to-end-schema changer tests
$BAZEL_BIN/pkg/cmd/bazci/bazci_/bazci --config=ci \
  test //pkg/sql/schemachanger:schemachanger_test -- \
  --test_arg=--declarative-corpus=$ARTIFACTS_DIR/corpus \
  --test_filter='^TestGenerateCorpus$' \
  --test_env=GO_TEST_WRAP_TESTV=1 \
  --test_env=GO_TEST_WRAP=1 \
  --test_env=GO_TEST_JSON_OUTPUT_FILE=$GO_TEST_GEN_JSON_OUTPUT_FILE \
  --test_timeout=7200 \
  || exit_status=$?

process_test_json \
$BAZEL_BIN/pkg/cmd/testfilter/testfilter_/testfilter \
$BAZEL_BIN/pkg/cmd/github-post/github-post_/github-post \
$ARTIFACTS_DIR \
$GO_TEST_GEN_JSON_OUTPUT_FILE \
$exit_status

# Generate corpuses from end-to-end-schema changer tests
$BAZEL_BIN/pkg/cmd/bazci/bazci_/bazci --config=ci \
  test //pkg/ccl/schemachangerccl:schemachangerccl_test -- \
  --test_arg=--declarative-corpus=$ARTIFACTS_DIR/corpus \
  --test_filter='^TestGenerateCorpus$' \
  --test_env=GO_TEST_WRAP_TESTV=1 \
  --test_env=GO_TEST_WRAP=1 \
  --test_env=GO_TEST_JSON_OUTPUT_FILE=$GO_TEST_GEN_CCL_JSON_OUTPUT_FILE \
  --test_timeout=7200 \
  || exit_status=$?

process_test_json \
$BAZEL_BIN/pkg/cmd/testfilter/testfilter_/testfilter \
$BAZEL_BIN/pkg/cmd/github-post/github-post_/github-post \
$ARTIFACTS_DIR \
$GO_TEST_GEN_CCL_JSON_OUTPUT_FILE \
$exit_status


# Any generated corpus should be validated on the current version first, which
# indicates we can replay it on the same version.
$BAZEL_BIN/pkg/cmd/bazci/bazci_/bazci --config=ci \
  test //pkg/sql/schemachanger/corpus:corpus_test -- \
  --test_arg=--declarative-corpus=$ARTIFACTS_DIR/corpus \
  --test_filter='^TestValidateCorpuses$' \
  --test_env=GO_TEST_WRAP_TESTV=1 \
  --test_env=GO_TEST_WRAP=1 \
  --test_env=GO_TEST_JSON_OUTPUT_FILE=$GO_TEST_VALIDATE_JSON_OUTPUT_FILE \
  --test_timeout=7200 \
  || exit_status=$?

process_test_json \
$BAZEL_BIN/pkg/cmd/testfilter/testfilter_/testfilter \
$BAZEL_BIN/pkg/cmd/github-post/github-post_/github-post \
$ARTIFACTS_DIR \
$GO_TEST_VALIDATE_JSON_OUTPUT_FILE \
$exit_status

# If validation passes its safe to update the copy in storage.
if [ $exit_status = 0 ]; then
  gsutil cp  $ARTIFACTS_DIR/corpus/* gs://cockroach-corpus/corpus-$TC_BUILD_BRANCH/
fi

# Generate a corpus for all mixed version variants
$BAZEL_BIN/pkg/cmd/bazci/bazci_/bazci --config=ci \
    test //pkg/sql/logictest:logictest_test -- \
    --test_arg=--declarative-corpus=$ARTIFACTS_DIR/corpus-mixed\
    --test_filter='^TestLogic/(mixed).*$' \
    --test_env=GO_TEST_WRAP_TESTV=1 \
    --test_env=GO_TEST_WRAP=1 \
    --test_env=GO_TEST_JSON_OUTPUT_FILE=$GO_TEST_JSON_OUTPUT_FILE_MIXED \
    --test_timeout=7200 \
    || exit_status=$?

process_test_json \
  $BAZEL_BIN/pkg/cmd/testfilter/testfilter_/testfilter \
  $BAZEL_BIN/pkg/cmd/github-post/github-post_/github-post \
  $ARTIFACTS_DIR \
  $GO_TEST_JSON_OUTPUT_FILE_MIXED \
  $exit_status

$BAZEL_BIN/pkg/cmd/bazci/bazci_/bazci --config=ci \
    test //pkg/ccl/logictestccl:logictestccl_test -- \
    --test_arg=--declarative-corpus=$ARTIFACTS_DIR/corpus \
    --test_filter='^(TestCCLLogic|TestTenantLogic)/(mixed).*$' \
    --test_env=GO_TEST_WRAP_TESTV=1 \
    --test_env=GO_TEST_WRAP=1 \
    --test_env=GO_TEST_JSON_OUTPUT_FILE=$GO_TEST_JSON_OUTPUT_FILE \
    --test_timeout=7200 \
    || exit_status=$?

process_test_json \
  $BAZEL_BIN/pkg/cmd/testfilter/testfilter_/testfilter \
  $BAZEL_BIN/pkg/cmd/github-post/github-post_/github-post \
  $ARTIFACTS_DIR \
  $GO_TEST_JSON_OUTPUT_FILE \
  $exit_status


# Any generated corpus should be validated on the current version first, which
# indicates we can replay it on the same version.
$BAZEL_BIN/pkg/cmd/bazci/bazci_/bazci --config=ci \
    test //pkg/sql/schemachanger/corpus:corpus_test -- \
    --test_arg=--declarative-corpus=$ARTIFACTS_DIR/corpus-mixed \
    --test_filter='^TestValidateCorpuses$' \
    --test_env=GO_TEST_WRAP_TESTV=1 \
    --test_env=GO_TEST_WRAP=1 \
    --test_env=GO_TEST_JSON_OUTPUT_FILE=$GO_TEST_VALIDATE_JSON_OUTPUT_FILE_MIXED \
    --test_timeout=7200 \
    || exit_status=$?

process_test_json \
  $BAZEL_BIN/pkg/cmd/testfilter/testfilter_/testfilter \
  $BAZEL_BIN/pkg/cmd/github-post/github-post_/github-post \
  $ARTIFACTS_DIR \
  $GO_TEST_VALIDATE_JSON_OUTPUT_FILE_MIXED \
  $exit_status

# If validation passes its safe to update the copy in storage.
if [ $exit_status = 0 ]; then
  gsutil cp  $ARTIFACTS_DIR/corpus-mixed/* gs://cockroach-corpus/corpus-mixed-$TC_BUILD_BRANCH/
fi
