#!/usr/bin/env bash

set -exuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"
source "$dir/teamcity-support.sh"

# Set up credentials
google_credentials="$GOOGLE_EPHEMERAL_CREDENTIALS"
log_into_gcloud

function build_binaries() {
  config_args="--config=crosslinux --config=ci"
  bazel clean
  go_test_targets=$(bazel query kind\(go_test, //pkg/util:all\))
  bazel build $config_args $go_test_targets
  bazel_bin=$(bazel info bazel-bin $config_args)
  stage_dir=$(mktemp -d)
  for pkg in $go_test_targets; do
    # pkg is in the form //path/to/package:test_name
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

  out_dir=$(mktemp -d)

  tar -I 'xz -T0' -chf "$out_dir/stage.tar.xz" -C "$stage_dir" .
  gsutil -m cp "$out_dir/stage.tar.xz" "gs://cockroach-microbench/builds/$1.tar.xz"
}

build_binaries b1

git fetch origin release-24.1
git checkout -b release-24.1 origin/release-24.1

build_binaries b2
