#!/usr/bin/env bash

set -euo pipefail

google_credentials="$GOOGLE_EPHEMERAL_CREDENTIALS"
source "$(dirname "${0}")/teamcity-support.sh"
log_into_gcloud

set -x

tc_start_block "Build toolchains"
build/toolchains/toolchainbuild/buildtoolchains.sh
tc_end_block "Build toolchains"

tc_start_block "Publish artifacts"
loc=$(date +%Y%m%d-%H%M%S)
# NB: $root is set by teamcity-support.sh.
gsutil cp -r $root/artifacts gs://public-bazel-artifacts/toolchains/crosstool-ng/$loc
tc_end_block "Publish artifacts"
