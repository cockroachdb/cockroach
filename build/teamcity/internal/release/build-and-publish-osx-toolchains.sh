#!/usr/bin/env bash

# Copyright 2022 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -euo pipefail

google_credentials="$GOOGLE_EPHEMERAL_CREDENTIALS"
dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"
source "$dir/teamcity-support.sh"
log_into_gcloud

set -x

tc_start_block "Build toolchains"
build/toolchains/toolchainbuild/osxcross/buildtoolchains.sh
tc_end_block "Build toolchains"

tc_start_block "Publish artifacts"
loc=$(date +%Y%m%d-%H%M%S)
# NB: $root is set by teamcity-support.sh.
gsutil cp -r $root/artifacts gs://public-bazel-artifacts/toolchains/osxcross/$(uname -m)/$loc
tc_end_block "Publish artifacts"
