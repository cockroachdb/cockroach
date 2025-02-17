#!/bin/bash

# Copyright 2025 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

set -euxo pipefail

working_dir=$(mktemp -d)
temp_dir=$(mktemp -d)
storage_bucket="$BUCKET"
shas=("$BASE_SHA" "$HEAD_SHA")

# Disable parallel uploads, as it requires specific permissions
gcloud config set storage/parallel_composite_upload_enabled False

# Retrieve required binaries from the base and head builds
for pkg in "${TEST_PACKAGES[@]}"; do
  for sha in "${shas[@]}"; do
    pkg_bin=$(echo "${pkg}" | tr '/' '_')
    url="gs://${storage_bucket}/builds/${sha}/bin/${pkg_bin}"
    dest="$working_dir/$sha/bin"
    mkdir -p "$dest"
    gcloud storage cp "${url}" "$dest/${pkg_bin}"
    chmod +x "$dest/${pkg_bin}"
  done
done

# Run the microbenchmarks
./build/github/microbenchmarks/util.sh run \
  --group="$GROUP" \
  --working-dir="$working_dir" \
  --old="$BASE_SHA" \
  --new="$HEAD_SHA"

# Copy benchmark results to GCS
curl -H "Metadata-Flavor: Google" \
    http://metadata.google.internal/computeMetadata/v1/instance/machine-type | awk -F'/' '{print $NF}' > "$temp_dir/machine_type.txt"
for sha in "${shas[@]}"; do
  gcloud storage cp -n "$temp_dir/machine_type.txt" "gs://${storage_bucket}/artifacts/${sha}/${BUILD_ID}/machine_type.txt"
  gcloud storage cp -r "${working_dir}/${sha}/artifacts/*" "gs://${storage_bucket}/artifacts/${sha}/${BUILD_ID}/"
done
