#!/usr/bin/env bash
#
# This script runs microbenchmarks across a roachprod cluster. It will build microbenchmark binaries
# for the given revisions if they do not already exist in the BUILDS_BUCKET. It will then create a
# roachprod cluster, stage the binaries on the cluster, and run the microbenchmarks.
# Parameters (and suggested defaults):
#   BENCH_REVISION: revision to build and run benchmarks against (default: master)
#   BENCH_COMPARE_REVISION: revision to compare against (default: latest release branch)
#   BUILDS_BUCKET: GCS bucket to store the built binaries
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

set -exuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"
source "$dir/teamcity-support.sh"  # For $root
source "$dir/teamcity-bazel-support.sh"  # For run_bazel
output_dir="./artifacts/microbench"
exit_status=0

# Build rochprod and roachprod-microbench
run_bazel <<'EOF'
bazel build --config ci --config crosslinux //pkg/cmd/roachprod //pkg/cmd/roachprod-microbench
BAZEL_BIN=$(bazel info bazel-bin --config ci --config crosslinux)
mkdir -p bin
cp $BAZEL_BIN/pkg/cmd/roachprod/roachprod_/roachprod bin
cp $BAZEL_BIN/pkg/cmd/roachprod-microbench/roachprod-microbench_/roachprod-microbench bin
chmod a+w bin/roachprod bin/roachprod-microbench
EOF

# Check if a string is a valid SHA (otherwise it's a branch name).
is_sha() {
  local sha="$1"
  [[ "$sha" =~ ^[0-9a-f]{40}$ ]]
}

# Create arrays for the revisions and their corresponding SHAs.
revisions=("${BENCH_REVISION}" "${BENCH_COMPARE_REVISION}")
declare -a sha_arr
declare -a name_arr
for rev in "${revisions[@]}"; do
  git fetch origin "$rev"
  if is_sha "$rev"; then
    sha="$rev"
    name="${sha:0:8}"
  else
    sha=$(git rev-parse origin/"$rev")
    name="$rev (${sha:0:8})"
  fi
  name_arr+=("$name")
  sha_arr+=("$sha")
done

# Builds binaries for the given SHAs.
BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="-e GOOGLE_EPHEMERAL_CREDENTIALS -e BENCH_PACKAGE -e BUILDS_BUCKET" \
  run_bazel build/teamcity/cockroach/nightlies/microbenchmark_build_support.sh "${sha_arr[@]}"

# Set up credentials (needs to be done after the build phase)
google_credentials="$GOOGLE_EPHEMERAL_CREDENTIALS"
log_into_gcloud
export GOOGLE_APPLICATION_CREDENTIALS="$PWD/.google-credentials.json"
export ROACHPROD_USER=teamcity
export ROACHPROD_CLUSTER=teamcity-microbench-${TC_BUILD_ID}
generate_ssh_key

# Create roachprod cluster
./bin/roachprod create "$ROACHPROD_CLUSTER" -n "$GCE_NODE_COUNT" \
  --lifetime "$CLUSTER_LIFETIME" \
  --clouds gce \
  --gce-machine-type "$GCE_MACHINE_TYPE" \
  --gce-zones="$GCE_ZONE" \
  --gce-managed \
  --gce-use-spot \
  --os-volume-size=384

# Stage binaries on the cluster
for sha in "${sha_arr[@]}"; do
  archive_name=${BENCH_PACKAGE//\//-}
  archive_name="$sha-${archive_name/.../all}.tar.gz"
  ./bin/roachprod-microbench stage "$ROACHPROD_CLUSTER" "gs://$BUILDS_BUCKET/builds/$archive_name" "$sha"
done

# Execute microbenchmarks
./bin/roachprod-microbench run "$ROACHPROD_CLUSTER" \
  --binaries "${sha_arr[0]}" \
  --compare-binaries "${sha_arr[1]}" \
  --output-dir="$output_dir" \
  --iterations "$BENCH_ITERATIONS" \
  --shell="$BENCH_SHELL" \
  ${BENCH_TIMEOUT:+--timeout="$BENCH_TIMEOUT"} \
  ${BENCH_EXCLUDE:+--exclude="$BENCH_EXCLUDE"} \
  ${BENCH_IGNORE_PACKAGES:+--ignore-package="$BENCH_IGNORE_PACKAGES"} \
  --quiet \
  -- "$TEST_ARGS" \
  || exit_status=$?


# Compare the results, if both sets of benchmarks were run
if [ -d "$output_dir/0" ] && [ "$(ls -A "$output_dir/0")" ] \
&& [ -d "$output_dir/1" ] && [ "$(ls -A "$output_dir/1")" ]; then
  # Set up slack token only if the build was triggered by TeamCity (not a manual run)
  if [ -n "${TRIGGERED_BUILD:-}" ]; then
    slack_token="${MICROBENCH_SLACK_TOKEN}"
  fi
  sheet_description="${name_arr[0]} -> ${name_arr[1]}"
  ./bin/roachprod-microbench compare "$output_dir/0" "$output_dir/1" \
    ${slack_token:+--slack-token="$slack_token"} \
    --sheet-desc="$sheet_description" 2>&1 | tee "$output_dir/sheets.txt"
else
  echo "No microbenchmarks were run. Skipping comparison."
fi

# Exit with the code from roachprod-microbench
exit $exit_status
