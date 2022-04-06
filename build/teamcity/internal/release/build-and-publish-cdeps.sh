#!/usr/bin/env bash

set -euo pipefail

google_credentials="$GOOGLE_EPHEMERAL_CREDENTIALS"
dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"
source "$dir/teamcity-support.sh"  # for root
source "$dir/teamcity-bazel-support.sh"  # for run_bazel
log_into_gcloud

set -x

tc_start_block "Build c-deps"
run_bazel c-deps/buildcdeps.sh
tc_end_block "Build c-deps"

tc_start_block "Publish artifacts"
loc=$(date +%Y%m%d-%H%M%S)
gsutil cp -r $root/artifacts gs://public-bazel-artifacts/c-deps/$loc
tc_end_block "Publish artifacts"
