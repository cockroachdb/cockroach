#!/usr/bin/env bash

set -exuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"
source "$dir/teamcity-support.sh"

# Set up credentials
google_credentials="$GOOGLE_EPHEMERAL_CREDENTIALS"
log_into_gcloud

# Check if a given GCS path exists
function check_gcs_path_exists() {
  local path=$1
  if gsutil ls "$path" > /dev/null 2>&1; then
    return 0
  else
    return 1
  fi
}

# Build and copy binaries, for the given SHA, to GCS bucket
function build_and_copy_binaries() {
  local sha=$1
  archive_name=${BENCH_PACKAGE//\//-}
  archive_name="$sha-${archive_name/.../all}.tar.gz"
  if check_gcs_path_exists "gs://$BUILDS_BUCKET/builds/$archive_name"; then
    echo "Build for $sha already exists. Skipping..."
    return
  fi

  config_args="--config=crosslinux --config=ci"
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
  gsutil -m cp "$out_dir/$archive_name" "gs://$BUILDS_BUCKET/builds/$archive_name"
  rm -rf "$out_dir"
}

# Build and copy binaries for all passed SHAs
current_sha=$(git rev-parse HEAD)
shas=("$@")
for sha in "${shas[@]}"; do
  git checkout "$sha"
  build_and_copy_binaries "$sha"
done
git checkout "$current_sha"


