#!/usr/bin/env bash

# Copyright 2022 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


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
for FILE in `find $root/artifacts -name '*.tar.gz'`; do
    BASE=$(basename $FILE)
    DEST=${BASE/.tar.gz/".$loc.tar.gz"}
    gsutil cp $FILE gs://public-bazel-artifacts/c-deps/$loc/$DEST
done
tc_end_block "Publish artifacts"
