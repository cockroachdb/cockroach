#!/usr/bin/env bash

# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

set -xeuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname $(dirname "${0}"))))))"

cleanup() {
    rm -f ~/.config/gcloud/application_default_credentials.json
}
trap cleanup EXIT

source "$dir/teamcity-support.sh"
google_credentials="$GOOGLE_CREDENTIALS"
log_into_gcloud

filename="$(date +"%Y%m%d%H%M%S")-$(git rev-parse HEAD).pb.gz"

bazel build //pkg/cmd/run-pgo-build
_bazel/bin/pkg/cmd/run-pgo-build/run-pgo-build_/run-pgo-build -out "artifacts/$filename"
shasum -a 256 "artifacts/$filename" | tee artifacts/location.txt

gsutil cp "artifacts/$filename" "gs://cockroach-profiles/$filename"
echo "gs://cockroach-profiles/$filename" >> artifacts/location.txt
