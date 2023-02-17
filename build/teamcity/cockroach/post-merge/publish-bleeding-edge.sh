#!/usr/bin/env bash

# This script is called by the build configuration
# "Cockroach > Post Merge > Publish Bleeding Edge" in TeamCity.

set -euxo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"
source "$dir/teamcity-support.sh"
source "$dir/teamcity-bazel-support.sh"

gcs_bucket="cockroach-edge-artifacts-prod"
# export the variable to avoid shell escaping
export gcs_credentials="$GCS_CREDENTIALS_PROD"

BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="-e TC_BUILDTYPE_ID -e TC_BUILD_BRANCH -e gcs_credentials -e gcs_bucket=$gcs_bucket" run_bazel << 'EOF'
bazel build --config ci //pkg/cmd/publish-artifacts
BAZEL_BIN=$(bazel info bazel-bin --config ci)
export google_credentials="$gcs_credentials"
source "build/teamcity-support.sh"  # For log_into_gcloud
log_into_gcloud
export GOOGLE_APPLICATION_CREDENTIALS="$PWD/.google-credentials.json"
$BAZEL_BIN/pkg/cmd/publish-artifacts/publish-artifacts_/publish-artifacts --gcs-bucket="$gcs_bucket"
EOF
