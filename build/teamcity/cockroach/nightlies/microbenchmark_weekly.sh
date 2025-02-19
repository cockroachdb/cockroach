#!/usr/bin/env bash

# Copyright 2023 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

#
# This script runs microbenchmarks across a roachprod cluster. It will build microbenchmark binaries
# for the given revisions if they do not already exist in the BENCH_BUCKET. It will then create a
# roachprod cluster, stage the binaries on the cluster, and run the microbenchmarks.
# Acceptable values for a revision:
#   - A branch name (e.g. master, release-23.2)
#   - A tag name (e.g. v21.1.0)
#   - A full commit SHA (e.g. 0123456789abcdef0123456789abcdef01234567)
#   - LATEST_PATCH_RELEASE, which will use the latest patch release tag (e.g. vX.Y.Z)
#
# Parameters (and suggested defaults):
#   BENCH_REVISION: revision to build and run benchmarks against (default: master)
#   BENCH_COMPARE_REVISION: revision to compare against (default: latest release branch)
#   BENCH_BUCKET: GCS bucket to store the built binaries and compare cache (default: cockroach-microbench)
#   BENCH_PACKAGE: package to build and run benchmarks against (default: ./pkg/...)
#   BENCH_ITERATIONS: number of iterations to run each microbenchmark (default: 10)
#   GCE_NODE_COUNT: number of nodes to use in the roachprod cluster (default: 12)
#   CLUSTER_LIFETIME: lifetime of the roachprod cluster (default: 24h)
#   GCE_MACHINE_TYPE: machine type to use in the roachprod cluster (default: n2-standard-32)
#   GCE_ZONE: zone to use in the roachprod cluster (default: us-central-1b)
#   BENCH_SHELL: command to run before each iteration (default: export COCKROACH_RANDOM_SEED=1)
#   BENCH_TIMEOUT: timeout for each microbenchmark on a function level (default: 20m)
#   BENCH_EXCLUDE: comma-separated list of benchmarks to exclude (default: none)
#   BENCH_IGNORE_PACKAGES: comma-separated list of packages to exclude completely from listing and execution (default: none)
#   TEST_ARGS: additional arguments to pass to the test binary (default: none)
#   MICROBENCH_SLACK_TOKEN: token to use to post to slack (default: none)
#   MICROBENCH_INFLUX_HOST: Influx host path to use to push results to InfluxDB (default: none)
#   MICROBENCH_INFLUX_TOKEN: auth token to use to push results to InfluxDB (default: none)

set -exuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"
source "$dir/teamcity-support.sh"  # For $root
source "$dir/teamcity-bazel-support.sh"  # For run_bazel
output_dir="./artifacts/microbench"
remote_dir="/mnt/data1"
benchmarks_commit=$(git rev-parse HEAD)
exit_status=0

# Check if a string is a valid SHA (otherwise it's a branch or tag name).
is_sha() {
  local sha="$1"
  [[ "$sha" =~ ^[0-9a-f]{40}$ ]]
}

get_latest_patch_tag() {
  local latest_tag
  latest_tag=$(git tag -l 'v*' \
    | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' \
    | sort -t. -k1,1 -k2,2n -k3,3n \
    | tail -n 1)
  echo "$latest_tag"
}

# Create arrays for the revisions and their corresponding SHAs.
revisions=("${BENCH_REVISION}" "${BENCH_COMPARE_REVISION}")
declare -a sha_arr
declare -a name_arr
for rev in "${revisions[@]}"; do
  if [ "$rev" == "LATEST_PATCH_RELEASE" ]; then
    git fetch origin --tags
    rev=$(get_latest_patch_tag)
  fi
  if is_sha "$rev"; then
    sha="$rev"
    name="${sha:0:8}"
  else
    sha=$(git ls-remote origin "$rev" | awk '{print $1}')
    name="$rev (${sha:0:8})"
  fi
  name_arr+=("$name")
  sha_arr+=("$sha")
done

# Set up credentials
google_credentials="$GOOGLE_EPHEMERAL_CREDENTIALS"
log_into_gcloud
export GOOGLE_APPLICATION_CREDENTIALS="$PWD/.google-credentials.json"
export ROACHPROD_USER=teamcity
export ROACHPROD_CLUSTER=teamcity-microbench-${TC_BUILD_ID}
generate_ssh_key

# Sanatize the package name for use in paths
SANITIZED_BENCH_PACKAGE=${BENCH_PACKAGE//\//-}
export SANITIZED_BENCH_PACKAGE=${SANITIZED_BENCH_PACKAGE/.../all}

# Build rochprod and roachprod-microbench
run_bazel <<'EOF'
bazel build --config crosslinux //pkg/cmd/roachprod //pkg/cmd/roachprod-microbench
BAZEL_BIN=$(bazel info bazel-bin --config crosslinux)
mkdir -p bin
cp $BAZEL_BIN/pkg/cmd/roachprod/roachprod_/roachprod bin
cp $BAZEL_BIN/pkg/cmd/roachprod-microbench/roachprod-microbench_/roachprod-microbench bin
chmod a+w bin/roachprod bin/roachprod-microbench
EOF

# Check if the baseline cache exists and copy it to the output directory.
baseline_cache_path="gs://$BENCH_BUCKET/cache/$GCE_MACHINE_TYPE/$SANITIZED_BENCH_PACKAGE/${sha_arr[1]}"
declare -a build_sha_arr
build_sha_arr+=("${sha_arr[0]}")
if check_gcs_path_exists "$baseline_cache_path"; then
  mkdir -p "$output_dir/baseline"
  gsutil -mq cp -r "$baseline_cache_path/*" "$output_dir/baseline"
  echo "Baseline cache found for ${name_arr[1]}. Using it for comparison."
else
  build_sha_arr+=("${sha_arr[1]}")
fi

# Builds binaries for the given SHAs.
BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="-e GOOGLE_EPHEMERAL_CREDENTIALS -e SANITIZED_BENCH_PACKAGE -e BENCH_PACKAGE -e BENCH_BUCKET" \
  run_bazel build/teamcity/cockroach/nightlies/microbenchmark_build_support.sh "${build_sha_arr[@]}"

# Log into gcloud again (credentials are removed by teamcity-support in the build script)
log_into_gcloud

# Create roachprod cluster
./bin/roachprod create "$ROACHPROD_CLUSTER" -n "$GCE_NODE_COUNT" \
  --lifetime "$CLUSTER_LIFETIME" \
  --clouds gce \
  --gce-machine-type "$GCE_MACHINE_TYPE" \
  --gce-zones="$GCE_ZONE" \
  --gce-managed \
  --gce-use-spot \
  --local-ssd=false \
  --gce-pd-volume-size=384 \
  --os-volume-size=50

# Stage binaries on the cluster
for sha in "${build_sha_arr[@]}"; do
  archive_name=${BENCH_PACKAGE//\//-}
  archive_name="$sha-${archive_name/.../all}.tar.gz"
  ./bin/roachprod-microbench stage --quiet "$ROACHPROD_CLUSTER" "gs://$BENCH_BUCKET/builds/$archive_name" "$remote_dir/$sha"
done

# Execute microbenchmarks
./bin/roachprod-microbench run "$ROACHPROD_CLUSTER" \
  --binaries experiment="$remote_dir/${build_sha_arr[0]}" \
  ${build_sha_arr[1]:+--binaries baseline="$remote_dir/${build_sha_arr[1]}"} \
  --output-dir="$output_dir" \
  --iterations "$BENCH_ITERATIONS" \
  --shell="$BENCH_SHELL" \
  ${BENCH_TIMEOUT:+--timeout="$BENCH_TIMEOUT"} \
  ${BENCH_EXCLUDE:+--exclude="$BENCH_EXCLUDE"} \
  ${BENCH_IGNORE_PACKAGES:+--ignore-package="$BENCH_IGNORE_PACKAGES"} \
  --quiet \
  -- "$TEST_ARGS" \
  || exit_status=$?

# Write metadata to a file for each set of benchmarks
declare -A metadata
metadata["run-time"]=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
metadata["baseline-commit"]=${sha_arr[1]}
metadata["benchmarks-commit"]=$benchmarks_commit
metadata["machine"]=$GCE_MACHINE_TYPE
metadata["goarch"]=amd64
metadata["goos"]=linux
metadata["repository"]=cockroach
echo "" > "$output_dir/baseline/metadata.log"
for key in "${!metadata[@]}"; do
  echo "$key": "${metadata[$key]}" >> "$output_dir/baseline/metadata.log"
done

metadata["experiment-commit"]=${sha_arr[0]}
metadata["experiment-commit-time"]=$(git show -s --format=%cI "${sha_arr[0]}")
for key in "${!metadata[@]}"; do
  echo "$key": "${metadata[$key]}" >> "$output_dir/experiment/metadata.log"
done

# Push baseline to cache if we ran both benchmarks
if [[ ${#build_sha_arr[@]} -gt 1 ]]; then
  gsutil -mq cp -r "$output_dir/baseline" "$baseline_cache_path"
fi

# Push experiment results to cache
experiment_cache_path="gs://$BENCH_BUCKET/cache/$GCE_MACHINE_TYPE/$SANITIZED_BENCH_PACKAGE/${sha_arr[0]}"
gsutil -mq cp -r "$output_dir/experiment" "$experiment_cache_path"

# Compare the results, if both sets of benchmarks were run.
# These should exist if the benchmarks were run successfully.
if [ -d "$output_dir/experiment" ] && [ "$(ls -A "$output_dir/experiment")" ] \
&& [ -d "$output_dir/baseline" ] && [ "$(ls -A "$output_dir/baseline")" ]; then
  # Set up slack token only if the build was triggered by TeamCity (not a manual run)
  if [ -n "${TRIGGERED_BUILD:-}" ]; then
    slack_token="${MICROBENCH_SLACK_TOKEN}"
    influx_token="${MICROBENCH_INFLUX_TOKEN}"
    influx_host="${MICROBENCH_INFLUX_HOST}"
  fi
  # Sheet description is in the form: `baseline` to `experiment`
  sheet_description="${name_arr[1]} -> ${name_arr[0]}"
  ./bin/roachprod-microbench compare "$output_dir/experiment" "$output_dir/baseline" \
    ${slack_token:+--slack-token="$slack_token"} \
    --sheet-desc="$sheet_description" \
    ${influx_token:+--influx-token="$influx_token"} \
    ${influx_host:+--influx-host="$influx_host"} \
    2>&1 | tee "$output_dir/sheets.txt"
else
  echo "No microbenchmarks were run. Skipping comparison."
fi

# Exit with the code from roachprod-microbench
exit $exit_status
