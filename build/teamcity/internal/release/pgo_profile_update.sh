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
if ! command -v gh &> /dev/null; then
    echo "Installing gh CLI tool..."
    wget -O /tmp/gh.tar.gz https://github.com/cli/cli/releases/download/v2.13.0/gh_2.13.0_linux_amd64.tar.gz
    echo "9e833e02428cd49e0af73bc7dc4cafa329fe3ecba1bfe92f0859bf5b11916401  /tmp/gh.tar.gz" | sha256sum -c -
    tar --strip-components 1 -xf /tmp/gh.tar.gz -C /tmp
    export PATH=/tmp/bin:$PATH
    echo "gh CLI installed successfully"
fi

# Configuration
export TESTS="${TESTS:-tpcc-nowait/literal/w=1000/nodes=5/cpu=16}"
export COUNT="${COUNT:-20}"
export CLOUD="${CLOUD:-gce}"
export USE_SPOT="${USE_SPOT:-never}"

docker_args=(
  ARM_PROBABILITY=0.0
  SELECT_PROBABILITY=1.0
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
BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="$(printf -- "-e %s " "${docker_args[@]}")"

benchstat_before="$root/artifacts/benchstat_before.txt"
benchstat_after="$root/artifacts/benchstat_after.txt"

# Step 1: Run initial benchmark tests to establish baseline
echo "=== Step 1: Running baseline tpcc-nowait tests ==="

BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="$BAZEL_SUPPORT_EXTRA_DOCKER_ARGS" \
    run_bazel build/teamcity/cockroach/nightlies/roachtest_nightly_impl.sh

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

bash -xe "$root/scripts/tpcc_results.sh" "$tmp_artifacts_before" > "$benchstat_before"
rm -rf "$tmp_artifacts_before"

echo "=== Baseline benchstat saved to $benchstat_before ==="

# Step 2: Generate new PGO profile
echo "=== Step 2: Generating new PGO profile ==="
filename="$(date +"%Y%m%d%H%M%S")-$(git rev-parse HEAD).pb.gz"
bazel build //pkg/cmd/run-pgo-build
_bazel/bin/pkg/cmd/run-pgo-build/run-pgo-build_/run-pgo-build -out "artifacts/$filename"
sha256_hash=$(shasum -a 256 "artifacts/$filename" | awk '{print $1}')
echo "$sha256_hash" | tee "artifacts/location.txt"

# Upload to GCS
google_credentials="$GCS_GOOGLE_CREDENTIALS" log_into_gcloud
gcs_url="gs://cockroach-profiles/$filename"
gsutil cp "artifacts/$filename" "$gcs_url"
echo "$gcs_url" >> "artifacts/location.txt"

# Convert GCS URL to HTTPS URL for WORKSPACE
https_url="https://storage.googleapis.com/cockroach-profiles/$filename"

echo "=== PGO profile uploaded to $gcs_url ==="
echo "=== SHA256: $sha256_hash ==="

# Step 3: Update WORKSPACE file with new PGO profile
echo "=== Step 3: Updating WORKSPACE file ==="

# Update the pgo_profile stanza in WORKSPACE
workspace_file="$root/WORKSPACE"

# Use sed to update the sha256 and url in the pgo_profile block
# This is more robust than string replacement
sed -i.bak -e '/pgo_profile(/,/)/ {
    s|sha256 = ".*"|sha256 = "'"$sha256_hash"'"|
    s|url = ".*"|url = "'"$https_url"'"|
}' "$workspace_file"

# Remove backup file
rm -f "${workspace_file}.bak"

echo "=== WORKSPACE file updated ==="

# Step 4: Rebuild with new PGO profile and run tests again
echo "=== Step 4: Running tests with new PGO profile ==="

# Clear build cache to ensure new profile is used
bazel clean
rm -rf "$root/artifacts/tpcc-nowait"

# Run the same tests again with the new PGO profile
BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="$BAZEL_SUPPORT_EXTRA_DOCKER_ARGS" \
    run_bazel build/teamcity/cockroach/nightlies/roachtest_nightly_impl.sh
#
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

bash -xe "$root/scripts/tpcc_results.sh" "$tmp_artifacts_after" > "$benchstat_after"
rm -rf "$tmp_artifacts_after"
rm -rf "$root/artifacts/tpcc-nowait"

echo "=== Post-PGO benchstat saved to $benchstat_after ==="

# Step 5: Compare benchstats
echo "=== Step 5: Comparing benchmark results ==="

# Run benchstat comparison using bazel and save to file
benchstat_comparison="${root}/artifacts/benchstat_comparison.txt"
bazel run @org_golang_x_perf//cmd/benchstat -- "$benchstat_before" "$benchstat_after" | sed "s,$root/artifacts/,,g" | tee "$benchstat_comparison"

echo "=== Benchstat comparison saved to $benchstat_comparison ==="

# Step 6: Create PR with changes
echo "=== Step 6: Creating pull request ==="
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
    --base master
