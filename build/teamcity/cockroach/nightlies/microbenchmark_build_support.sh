#!/usr/bin/env bash

# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -exuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"
source "$dir/teamcity-support.sh"

# Set up credentials
google_credentials="$GOOGLE_EPHEMERAL_CREDENTIALS"
log_into_gcloud

# Build and copy binaries, for the given SHA, to GCS bucket
function build_and_upload_binaries() {
  local sha=$1
  archive_name="$sha-${SANITIZED_BENCH_PACKAGE}.tar.gz"
  if check_gcs_path_exists "gs://$BENCH_BUCKET/builds/$archive_name"; then
    echo "Build for $sha already exists. Skipping..."
    return
  fi

  config_args="--config=crosslinux --crdb_test_off"
  bazel clean
  go_test_targets=$(bazel query kind\(go_test, //$BENCH_PACKAGE:all\))
  bazel build $config_args $go_test_targets
  bazel_bin=$(bazel info bazel-bin $config_args)
  stage_dir=$(mktemp -d)
  for pkg in $go_test_targets; do
    # `pkg` is in the form //path/to/package:test_name
    path=$(echo "$pkg" | cut -d ':' -f 1 | cut -c 3-)
    test_name=$(echo "$pkg" | cut -d ':' -f 2)

    mkdir -p "$stage_dir/$path/bin"
    ln -s "$bazel_bin/$path/${test_name}_/${test_name}.runfiles" "$stage_dir/$path/bin/"
    ln -s "$bazel_bin/$path/${test_name}_/${test_name}" "$stage_dir/$path/bin/${test_name}"

    # Create a run script for each package
    cat << EOF > "$stage_dir/$path/bin/run.sh"
export RUNFILES_DIR=\$(pwd)/${test_name}.runfiles
export TEST_WORKSPACE=com_github_cockroachdb_cockroach/$path

find . -maxdepth 1 -type l -delete
ln -f -s ./${test_name}.runfiles/com_github_cockroachdb_cockroach/$path/* .
./${test_name} "\$@"
EOF
    chmod +x "$stage_dir/$path/bin/run.sh"
  done

  # Create a tarball of the binaries and copy to GCS
  out_dir=$(mktemp -d)
  tar -chf - -C "$stage_dir" . | ./bin/roachprod-microbench compress > "$out_dir/$archive_name"
  rm -rf "$stage_dir"
  gsutil -q -m cp "$out_dir/$archive_name" "gs://$BENCH_BUCKET/builds/$archive_name"
  rm -rf "$out_dir"
}

# Build and copy binaries for all passed SHAs
current_sha=$(git rev-parse HEAD)
shas=("$@")
for sha in "${shas[@]}"; do
  git fetch origin "$sha"
  git checkout "$sha"
  build_and_upload_binaries "$sha"
done
git checkout "$current_sha"


