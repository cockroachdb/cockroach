#!/usr/bin/env bash

# Copyright 2025 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

set -exuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"

source "$dir/teamcity-support.sh"  # For $root
source "$dir/teamcity-bazel-support.sh"  # For run_bazel

cleanup() {
    rm -f ~/.config/gcloud/application_default_credentials.json
}
trap cleanup EXIT


# Install `gh` CLI tool if not already available
tc_start_block "Install gh CLI"
if ! command -v gh &> /dev/null; then
    echo "Installing gh CLI tool..."
    wget -O /tmp/gh.tar.gz https://github.com/cli/cli/releases/download/v2.13.0/gh_2.13.0_linux_amd64.tar.gz
    echo "9e833e02428cd49e0af73bc7dc4cafa329fe3ecba1bfe92f0859bf5b11916401  /tmp/gh.tar.gz" | sha256sum -c -
    tar --strip-components 1 -xf /tmp/gh.tar.gz -C /tmp
    export PATH=/tmp/bin:$PATH
    echo "gh CLI installed successfully"
fi
tc_end_block "Install gh CLI"

tc_start_block "Configuration"
export TESTS="${TESTS:-tpcc-nowait/literal/w=1000/nodes=5/cpu=16}"
export COUNT="${COUNT:-20}"
export CLOUD="${CLOUD:-gce}"
# Do not use spot instances for PGO profile generation to ensure consistent
# performance and random VM preemptions.
export USE_SPOT="${USE_SPOT:-never}"

docker_env_args=(
  SELECT_PROBABILITY=1.0
  ARM_PROBABILITY=0.0
  COCKROACH_EA_PROBABILITY=0.0
  FIPS_PROBABILITY=0.0
  LITERAL_ARTIFACTS_DIR=$root/artifacts
  BUILD_VCS_NUMBER
  CLOUD
  COCKROACH_DEV_LICENSE
  COCKROACH_RANDOM_SEED
  COUNT
  EXPORT_OPENMETRICS
  GITHUB_API_TOKEN
  GITHUB_ORG
  GITHUB_REPO
  GOOGLE_CREDENTIALS_ASSUME_ROLE
  GOOGLE_EPHEMERAL_CREDENTIALS
  GOOGLE_KMS_KEY_A
  GOOGLE_KMS_KEY_B
  GOOGLE_SERVICE_ACCOUNT
  GRAFANA_SERVICE_ACCOUNT_AUDIENCE
  GRAFANA_SERVICE_ACCOUNT_JSON
  ROACHPERF_OPENMETRICS_CREDENTIALS
  ROACHTEST_ASSERTIONS_ENABLED_SEED
  ROACHTEST_FORCE_RUN_INVALID_RELEASE_BRANCH
  SELECTIVE_TESTS
  SLACK_TOKEN
  SNOWFLAKE_PVT_KEY
  SNOWFLAKE_USER
  TC_BUILDTYPE_ID
  TC_BUILD_BRANCH
  TC_BUILD_ID
  TC_SERVER_URL
  TESTS
  USE_SPOT
)
BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="$(printf -- "-e %s " "${docker_env_args[@]}")"

benchstat_before="artifacts/benchstat_before.txt"
benchstat_after="artifacts/benchstat_after.txt"
tc_end_block "Configuration"

tc_start_block "Baseline tpcc-nowait tests"

BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="$BAZEL_SUPPORT_EXTRA_DOCKER_ARGS" \
    run_bazel build/teamcity/cockroach/nightlies/roachtest_nightly_impl.sh

# Benchmark results are zipped in artifacts; extract them for analysis in a temp dir.
tmp_artifacts_before="$(mktemp -d)"
find "${root}/artifacts" -type f -name "artifacts.zip" -exec sh -c '
  src="$1"
  dest="'"$tmp_artifacts_before"'"
  base="'"${root}/artifacts"'"
  rel="${src#$base/}"
  rel_dir="$(dirname "$rel")"
  outdir="$dest/$rel_dir"
  mkdir -p "$outdir"
  unzip -o "$src" -d "$outdir"
' _ {} \;

bash -xe ./scripts/tpcc_results.sh "$tmp_artifacts_before" > "$benchstat_before"
rm -rf "$tmp_artifacts_before"

echo "=== Baseline benchstat saved to $benchstat_before ==="
tc_end_block "Baseline tpcc-nowait tests"

tc_start_block "Generate new PGO profile"
echo "=== Step 2: Generating new PGO profile ==="
filename="$(date +"%Y%m%d%H%M%S")-$(git rev-parse HEAD).pb.gz"
bazel build //pkg/cmd/run-pgo-build
"$(bazel info bazel-bin)"/pkg/cmd/run-pgo-build/run-pgo-build_/run-pgo-build -out "artifacts/$filename"
sha256_hash=$(shasum -a 256 "artifacts/$filename" | awk '{print $1}')

# Upload to GCS
google_credentials="$GCS_GOOGLE_CREDENTIALS" log_into_gcloud
gcs_url="gs://cockroach-profiles/$filename"
gsutil cp "artifacts/$filename" "$gcs_url"

# Convert GCS URL to HTTPS URL for WORKSPACE
https_url="https://storage.googleapis.com/cockroach-profiles/$filename"

echo "=== PGO profile uploaded to $gcs_url ==="
echo "=== SHA256: $sha256_hash ==="

# Use sed to update the sha256 and url in the pgo_profile block
# This is more robust than string replacement
sed -i.bak -e '/pgo_profile(/,/)/ {
    s|sha256 = ".*"|sha256 = "'"$sha256_hash"'"|
    s|url = ".*"|url = "'"$https_url"'"|
}' WORKSPACE
rm -f WORKSPACE.bak

tc_end_block "Generate new PGO profile"

tc_start_block "Rebuild with new PGO profile"
# Clear build cache to ensure new profile is used
bazel clean
rm -rf "artifacts/tpcc-nowait"

# Run the same tests again with the new PGO profile
BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="$BAZEL_SUPPORT_EXTRA_DOCKER_ARGS" \
    run_bazel build/teamcity/cockroach/nightlies/roachtest_nightly_impl.sh

# Benchmark results are zipped in artifacts; extract them for analysis in a temp dir.
tmp_artifacts_after="$(mktemp -d)"
find "${root}/artifacts" -type f -name "artifacts.zip" -exec sh -c '
  src="$1"
  dest="'"$tmp_artifacts_after"'"
  base="'"${root}/artifacts"'"
  rel="${src#$base/}"
  rel_dir="$(dirname "$rel")"
  outdir="$dest/$rel_dir"
  mkdir -p "$outdir"
  unzip -o "$src" -d "$outdir"
' _ {} \;

bash -xe ./scripts/tpcc_results.sh "$tmp_artifacts_after" > "$benchstat_after"
rm -rf "$tmp_artifacts_after"

echo "=== Post-PGO benchstat saved to $benchstat_after ==="
tc_end_block "Rebuild with new PGO profile"

tc_start_block "Benchstat comparison"
# Run benchstat comparison using bazel and save to file
benchstat_comparison="artifacts/benchstat_comparison.txt"

bazel build @org_golang_x_perf//cmd/benchstat
"$(bazel info bazel-bin)"/external/org_golang_x_perf/cmd/benchstat/benchstat_/benchstat \
    "$benchstat_before" "$benchstat_after" | tee "$benchstat_comparison"
tc_end_block "Benchstat comparison"

tc_start_block "Create PR"
export GIT_AUTHOR_NAME="Justin Beaver"
export GIT_COMMITTER_NAME="Justin Beaver"
export GIT_AUTHOR_EMAIL="teamcity@cockroachlabs.com"
export GIT_COMMITTER_EMAIL="teamcity@cockroachlabs.com"
gh_username="cockroach-teamcity"

# Create a new branch for the PR
branch_name="pgo-profile-update-$(date +"%Y%m%d%H%M%S")"
git checkout -b "$branch_name"

# Stage the WORKSPACE file changes
git add WORKSPACE

# Create commit with descriptive message
# Note: Full benchstat comparison is in the PR description, not in commit message
commit_message="build: update PGO profile to $filename

This commit updates the PGO profile used for building CockroachDB.

The new profile was generated from tpcc-nowait benchmarks.
See PR description for full benchmark comparison results.

Epic: none
Release note: none"

git commit -m "$commit_message"
set +x
git push "https://$gh_username:$GH_TOKEN@github.com/$gh_username/cockroach.git" "$branch_name"
set -x

# Create PR using gh CLI
pr_body="This PR updates the PGO (Profile-Guided Optimization) profile used for building CockroachDB.

The new profile was validated using tpcc-nowait benchmarks. Results comparing the old profile (before) vs new profile (after):

\`\`\`
$(cat "$benchstat_comparison")
\`\`\`

Epic: none
Release note: none"

gh pr create \
    --head "$gh_username:$branch_name" \
    --title "build: update PGO profile to $filename" \
    --body "$pr_body" \
    --reviewer "cockroachdb/release-eng-prs" \
    --base master | tee artifacts/pgo_profile_pr_url.txt
tc_end_block "Create PR"
