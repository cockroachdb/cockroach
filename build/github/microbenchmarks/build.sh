#!/bin/bash

# Copyright 2025 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

set -euxo pipefail

pkg_last=$(basename "${TEST_PKG}")
pkg_bin=$(echo "${TEST_PKG}" | tr '/' '_')

build_sha=$(git rev-parse HEAD)
storage_bucket="$BUCKET"
output_url="gs://${storage_bucket}/builds/${build_sha}/bin"

# Disable parallel uploads, as it requires specific permissions
gcloud config set storage/parallel_composite_upload_enabled False

if gcloud storage ls "${output_url}" &>/dev/null; then
  echo "Build for $build_sha already exists. Skipping..."
  exit 0
fi

# Build test binary
bazel build "//${TEST_PKG}:tests_test" \
  --jobs 100 \
  --crdb_test_off \
  --bes_keywords integration-test-artifact-build \
  --config=crosslinux \
  --remote_download_minimal \
  $(./build/github/engflow-args.sh)

# Copy to GCS
bazel_bin=$(bazel info bazel-bin --config=crosslinux)
gcloud storage cp -n "${bazel_bin}/pkg/sql/tests/${pkg_last}_test_/${pkg_last}_test" "${output_url}/${pkg_bin}"
