#!/usr/bin/env bash

# Copyright 2022 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -xeuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"
source "$dir/teamcity-support.sh"

bazel build //pkg/cmd/bazci
BAZEL_BIN=$(bazel info bazel-bin)
google_credentials="$GOOGLE_EPHEMERAL_CREDENTIALS"

log_into_gcloud

ARTIFACTS_DIR=/artifacts

mkdir -p $ARTIFACTS_DIR/corpus
mkdir -p $ARTIFACTS_DIR/corpus-mixed
exit_status=0

# Generate a corpus for all non-mixed version variants
for config in local multiregion-9node-3region-3azs; do
$BAZEL_BIN/pkg/cmd/bazci/bazci_/bazci test -- --config=ci \
    //pkg/sql/logictest/tests/$config/... \
    --test_arg=--declarative-corpus=$ARTIFACTS_DIR/corpus \
    --test_env=GO_TEST_WRAP_TESTV=1 \
    --test_env=GO_TEST_WRAP=1 \
    --test_timeout=7200 \
    || exit_status=$?
done

for config in local multiregion-9node-3region-3azs multiregion-9node-3region-3azs-no-los multiregion-9node-3region-3azs-tenant multiregion-9node-3region-3azs-vec-off multiregion-15node-5region-3azs 3node-tenant 3node-tenant-multiregion; do
$BAZEL_BIN/pkg/cmd/bazci/bazci_/bazci test -- --config=ci \
    //pkg/ccl/logictestccl/tests/$config/... \
    --test_arg=--declarative-corpus=$ARTIFACTS_DIR/corpus \
    --test_env=GO_TEST_WRAP_TESTV=1 \
    --test_env=GO_TEST_WRAP=1 \
    --test_timeout=7200 \
    || exit_status=$?
done

# Generate corpuses from end-to-end-schema changer tests
$BAZEL_BIN/pkg/cmd/bazci/bazci_/bazci test -- --config=ci \
  //pkg/sql/schemachanger:schemachanger_test \
  --test_arg=--declarative-corpus=$ARTIFACTS_DIR/corpus \
  --test_filter='^TestGenerateCorpus.*$' \
  --test_env=GO_TEST_WRAP_TESTV=1 \
  --test_env=GO_TEST_WRAP=1 \
  --test_timeout=7200 \
  || exit_status=$?

# Generate corpuses from end-to-end-schema changer tests
$BAZEL_BIN/pkg/cmd/bazci/bazci_/bazci test -- --config=ci \
  //pkg/ccl/schemachangerccl:schemachangerccl_test \
  --test_arg=--declarative-corpus=$ARTIFACTS_DIR/corpus \
  --test_filter='^TestGenerateCorpus.*$' \
  --test_env=GO_TEST_WRAP_TESTV=1 \
  --test_env=GO_TEST_WRAP=1 \
  --test_timeout=7200 \
  || exit_status=$?

# Any generated corpus should be validated on the current version first, which
# indicates we can replay it on the same version.
$BAZEL_BIN/pkg/cmd/bazci/bazci_/bazci test -- --config=ci \
  //pkg/sql/schemachanger/corpus:corpus_test \
  --test_arg=--declarative-corpus=$ARTIFACTS_DIR/corpus \
  --test_filter='^TestValidateCorpuses$' \
  --test_env=GO_TEST_WRAP_TESTV=1 \
  --test_env=GO_TEST_WRAP=1 \
  --test_timeout=7200 \
  || exit_status=$?

# If validation passes its safe to update the copy in storage.
if [ $exit_status = 0 ]; then
  gsutil cp  $ARTIFACTS_DIR/corpus/* gs://cockroach-corpus/corpus-$TC_BUILD_BRANCH/
fi

# Generate a corpus for all mixed version variants
for config in local-mixed-24.3; do
  $BAZEL_BIN/pkg/cmd/bazci/bazci_/bazci test -- --config=ci \
      //pkg/sql/logictest/tests/$config/... \
      --test_arg=--declarative-corpus=$ARTIFACTS_DIR/corpus-mixed \
      --test_env=GO_TEST_WRAP_TESTV=1 \
      --test_env=GO_TEST_WRAP=1 \
      --test_timeout=7200 \
      || exit_status=$?
done

# Any generated corpus should be validated on the current version first, which
# indicates we can replay it on the same version.
$BAZEL_BIN/pkg/cmd/bazci/bazci_/bazci test -- --config=ci \
    //pkg/sql/schemachanger/corpus:corpus_test \
    --test_arg=--declarative-corpus=$ARTIFACTS_DIR/corpus-mixed \
    --test_filter='^TestValidateCorpuses$' \
    --test_env=GO_TEST_WRAP_TESTV=1 \
    --test_env=GO_TEST_WRAP=1 \
    --test_timeout=7200 \
    || exit_status=$?

# If validation passes its safe to update the copy in storage.
if [ $exit_status = 0 ]; then
  gsutil cp  $ARTIFACTS_DIR/corpus-mixed/* gs://cockroach-corpus/corpus-mixed-$TC_BUILD_BRANCH/
fi
