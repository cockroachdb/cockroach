#!/usr/bin/env bash

# Copyright 2023 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -euo pipefail

google_credentials="$GOOGLE_EPHEMERAL_CREDENTIALS"
dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"
source "$dir/teamcity-support.sh"  # for log_into_gcloud
log_into_gcloud

set -x

loc=$(cat TIMESTAMP.txt)
rm TIMESTAMP.txt
mkdir artifacts

publish() {
    archive=$(echo go*.darwin-$1.tar.gz)
    shasum -a 256 $archive
    gsutil cp $archive gs://public-bazel-artifacts/go/$loc/$archive
    mv $archive artifacts
}
publish amd64
publish arm64

