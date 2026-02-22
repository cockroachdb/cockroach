#!/usr/bin/env bash

# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# This script runs roachprod functional tests across cloud providers. It creates
# clusters with randomized configurations to validate roachprod's cluster
# management capabilities across different machine types, storage options,
# and cloud configurations.
#
# Parameters / Configurables:
#   TEST_TIMEOUT: test target (e.g. create, destroy, etc.) timeout in seconds
#   TEST_COUNT: number of times to run each test, for stressing or running randomization tests
#   GOOGLE_PROJECT: Google Cloud project to use

set -exuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"
source "$dir/teamcity-support.sh"  # For $root
source "$dir/teamcity-bazel-support.sh"  # For run_bazel

# Set defaults
TEST_TIMEOUT="${TEST_TIMEOUT:-1800}"
TEST_COUNT="${TEST_COUNT:-1}"
GOOGLE_PROJECT="${GOOGLE_PROJECT:-cockroach-ephemeral}"

# Set up Google Cloud credentials
google_credentials="$GOOGLE_EPHEMERAL_CREDENTIALS"
log_into_gcloud
export GOOGLE_APPLICATION_CREDENTIALS="$PWD/.google-credentials.json"

# TODO: AWS, Azure, IBM credentials

# Configure roachprod to use TeamCity user
export ROACHPROD_USER=teamcity

# Generate SSH key for roachprod to access VMs
generate_ssh_key

# Set Google Cloud project for roachprod
gcloud config set project "$GOOGLE_PROJECT"

# Set up artifacts directory
ARTIFACTS_DIR=$PWD/artifacts
mkdir -p "$ARTIFACTS_DIR"

# Build bazci and run the roachprod functional tests
# bazci wraps bazel test and:
# --config=ci is added early because bazci will use .bazelrc to expand --config=ci into other bazel args that we want to override
# i.e. --test_tmpdir so unnecessary roachprod artifacts are not collected by bazci / teamcity
# - Collects test.xml and test.log files, munges XML for TeamCity compatibility, and stages them in the artifacts directory
# - Teamcity's XML Report Processing Build Feature finds the xml and log files to create the Teamcity test report view
# - Passes build configuration arguments: TEST_TIMEOUT, TEST_COUNT
tc_start_block "Run roachprod functional tests"
bazci_exit=0
BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="-e GOOGLE_EPHEMERAL_CREDENTIALS -e GOOGLE_PROJECT -e ROACHPROD_USER" \
  run_bazel <<EOF || bazci_exit=$?
bazel build --config crosslinux //pkg/cmd/bazci
BAZEL_BIN=\$(bazel info bazel-bin --config crosslinux)
mkdir -p bin
cp \$BAZEL_BIN/pkg/cmd/bazci/bazci_/bazci bin
chmod a+w bin/bazci

./bin/bazci --artifacts_dir=/artifacts -- test \
  //pkg/cmd/roachprod/test/tests:all \
  --config=ci \
  --test_env=GOOGLE_EPHEMERAL_CREDENTIALS \
  --test_env=GOOGLE_PROJECT \
  --test_env=ROACHPROD_USER \
  --test_output=all \
  --test_arg=-test.v \
  --test_timeout="$TEST_TIMEOUT" \
  --runs_per_test="$TEST_COUNT" \
  --test_tmpdir=/tmp
EOF
tc_end_block "Run roachprod functional tests"

# Exit code 0 = all tests passed, 1 = some tests failed, both are OK for build status
# Any other exit code indicates an infrastructure failure (panic, bazci crash, etc.).
if [ $bazci_exit -gt 1 ]; then
  exit $bazci_exit
fi
