#!/usr/bin/env bash

# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

#
# This script runs roachprod E2E tests across cloud providers. It creates
# clusters with randomized configurations to validate roachprod's cluster
# management capabilities across different machine types, storage options,
# and cloud configurations.
#
# Parameters:
#   TEST_TIMEOUT: timeout for the entire test suite in seconds (default: 1800)
#   TEST_COUNT: number of times to run each test (default: 3)
#   GOOGLE_PROJECT: Google Cloud project to use (default: cockroach-ephemeral)

set -exuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"
source "$dir/teamcity-support.sh"  # For $root
source "$dir/teamcity-bazel-support.sh"  # For run_bazel

# Set defaults
TEST_TIMEOUT="${TEST_TIMEOUT:-1800}"
TEST_COUNT="${TEST_COUNT:-1}" # Set to not 1 when this is working
GOOGLE_PROJECT="${GOOGLE_PROJECT:-cockroach-ephemeral}"

exit_status=0

# Set up Google Cloud credentials
google_credentials="$GOOGLE_EPHEMERAL_CREDENTIALS"
log_into_gcloud
export GOOGLE_APPLICATION_CREDENTIALS="$PWD/.google-credentials.json"

## Set up AWS credentials
#aws_access_key_id="$AWS_ACCESS_KEY_ID"
#aws_secret_access_key="$AWS_SECRET_ACCESS_KEY"
#aws_default_region="$AWS_DEFAULT_REGION"
#mkdir -p "$PWD/.aws"
#export AWS_SHARED_CREDENTIALS_FILE="$PWD/.aws/credentials"
#export AWS_CONFIG_FILE="$PWD/.aws/config"
#log_into_aws
#
## Validate Azure credentials
#if [[ -z "${AZURE_CLIENT_ID:-}" ]] || [[ -z "${AZURE_CLIENT_SECRET:-}" ]] || \
#   [[ -z "${AZURE_TENANT_ID:-}" ]] || [[ -z "${AZURE_SUBSCRIPTION_ID:-}" ]]; then
#  echo "ERROR: Azure credentials not set. Required: AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID, AZURE_SUBSCRIPTION_ID" >&2
#  exit 1
#fi
#
## Validate IBM credentials
#if [[ -z "${IBM_APIKEY:-}" ]]; then
#  echo "ERROR: IBM_APIKEY not set" >&2
#  exit 1
#fi

# Configure roachprod to use TeamCity user
# Each build gets a unique cluster prefix to avoid conflicts
export ROACHPROD_USER=teamcity

# Generate SSH key for roachprod to access VMs
generate_ssh_key

# Set Google Cloud project for roachprod
gcloud config set project "$GOOGLE_PROJECT"

# Build bazci and roachprod
tc_start_block "Build bazci and roachprod"
run_bazel <<'EOF'
bazel build --config crosslinux //pkg/cmd/bazci //pkg/cmd/roachprod
BAZEL_BIN=$(bazel info bazel-bin --config crosslinux)
mkdir -p bin
cp $BAZEL_BIN/pkg/cmd/roachprod/roachprod_/roachprod bin
chmod a+w bin/roachprod
cp $BAZEL_BIN/pkg/cmd/bazci/bazci_/bazci bin
chmod a+w bin/bazci
EOF
tc_end_block "Build bazci and roachprod"

# Set up artifacts directory
ARTIFACTS_DIR=$PWD/artifacts
mkdir -p "$ARTIFACTS_DIR"

# Run the roachprod E2E tests using bazci
# bazci wraps bazel test and automatically:
# - Collects test.xml and test.log files
# - Stages them in the artifacts directory
# - Munges XML for TeamCity compatibility
# - Handles multiple test attempts (--runs_per_test)
# - Adds --config=ci automatically
# The TestMain function handles gcloud authentication using GOOGLE_EPHEMERAL_CREDENTIALS
tc_start_block "Run roachprod E2E tests"
BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="-e GOOGLE_EPHEMERAL_CREDENTIALS -e GOOGLE_PROJECT -e ROACHPROD_USER" \
  run_bazel <<EOF
# Inside Docker container, artifacts are mounted at /artifacts
./bin/bazci --artifacts_dir=/artifacts -- test \
  //pkg/cmd/roachprod/test:all \
  --config=ci \
  --test_env=GOOGLE_EPHEMERAL_CREDENTIALS \
  --test_env=GOOGLE_PROJECT \
  --test_env=ROACHPROD_USER \
  --test_output=all \
  --test_arg=-test.v \
  --test_timeout="$TEST_TIMEOUT" \
  --runs_per_test="$TEST_COUNT" \
  --test_tmpdir=/tmp \
  || exit_status=\$?
EOF
tc_end_block "Run roachprod E2E tests"

# Ensure gcloud is authenticated for final cluster listing
#log_into_gcloud
#
## List any remaining clusters for debugging (in case cleanup failed)
## NEVER RUN roachprod destroy [-m,--all-mine]
#tc_start_block "List remaining clusters"
#./bin/roachprod list --mine || true
#tc_end_block "List remaining clusters"

# Exit with the test status
exit $exit_status
