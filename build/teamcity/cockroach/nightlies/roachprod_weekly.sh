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
#   GCE_PROJECT: GCE project to use (default: cockroach-ephemeral)

set -exuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"
source "$dir/teamcity-support.sh"  # For $root
source "$dir/teamcity-bazel-support.sh"  # For run_bazel

# Set defaults
TEST_TIMEOUT="${TEST_TIMEOUT:-1800}"
TEST_COUNT="${TEST_COUNT:-3}"
GCE_PROJECT="${GCE_PROJECT:-cockroach-ephemeral}"

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

# Set GCE project for roachprod
gcloud config set project "$GCE_PROJECT"

# Build roachprod binary
tc_start_block "Build roachprod"
run_bazel <<'EOF'
bazel build --config crosslinux //pkg/cmd/roachprod
BAZEL_BIN=$(bazel info bazel-bin --config crosslinux)
mkdir -p bin
cp $BAZEL_BIN/pkg/cmd/roachprod/roachprod_/roachprod bin
chmod a+w bin/roachprod
EOF
tc_end_block "Build roachprod"

# Build roachprod test binary
tc_start_block "Build roachprod tests"
run_bazel <<'EOF'
bazel build --config crosslinux //pkg/cmd/roachprod/roachprodtest:roachprodtest_test
BAZEL_BIN=$(bazel info bazel-bin --config crosslinux)
mkdir -p bin
cp $BAZEL_BIN/pkg/cmd/roachprod/roachprodtest/roachprodtest_test_/roachprodtest_test bin
chmod a+w bin/roachprodtest_test
EOF
tc_end_block "Build roachprod tests"

# Log into gcloud again (credentials are removed by teamcity-support in the build script)
log_into_gcloud

# Install go-junit-report for XML test output
tc_start_block "Install go-junit-report"
go install github.com/jstemmer/go-junit-report/v2@latest
tc_end_block "Install go-junit-report"

# Create logs directory for test artifacts
mkdir -p logs

# Run the roachprod E2E tests directly (not through bazel test)
# These tests will:
# - Create clusters with randomized configurations
# - Test basic operations (create, list, destroy)
# - Verify roachprod functionality across different VM types and options
# - Clean up clusters automatically on test completion
# Running the binary directly allows roachprod to use the gcloud CLI
tc_start_block "Run roachprod E2E tests"
export GOTRACEBACK=all
export ROACHPROD_BINARY="$PWD/bin/roachprod"
export ROACHPROD_USER=teamcity
for i in $(seq 1 "$TEST_COUNT"); do
  echo "Test iteration $i of $TEST_COUNT"

  # Run test and capture output to log file, also pipe to junit report
  set +e
  ./bin/roachprodtest_test \
    -test.run="TestCloud.*" \
    -test.v \
    -test.timeout="${TEST_TIMEOUT}s" \
    2>&1 | tee "logs/roachprodtest-iteration-${i}.log"

  # Capture the test exit status from the pipeline
  test_exit_status=${PIPESTATUS[0]}
  set -e

  # Generate JUnit XML from the log file
  cat "logs/roachprodtest-iteration-${i}.log" | \
    go-junit-report > "logs/roachprodtest-iteration-${i}.xml"

  if [ $test_exit_status -ne 0 ]; then
    echo "Test iteration $i failed with exit status $test_exit_status"
    exit_status=$test_exit_status
    break
  fi
done
tc_end_block "Run roachprod E2E tests"

# Import JUnit XML results into TeamCity
tc_start_block "Import test results"
echo "##teamcity[importData type='junit' path='logs/*.xml']"
tc_end_block "Import test results"

# Publish logs as artifacts
echo "##teamcity[publishArtifacts 'logs/ => roachprodtest-logs.zip']"

# Log into gcloud again (credentials may be removed after bazel test)
log_into_gcloud

# List any remaining clusters for debugging (in case cleanup failed)
# NEVER RUN roachprod destroy [-m,--all-mine]
tc_start_block "List remaining clusters"
./bin/roachprod list --mine || true
tc_end_block "List remaining clusters"

# Exit with the test status
exit $exit_status
