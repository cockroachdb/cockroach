#!/usr/bin/env bash

# Copyright 2023 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -euxo pipefail

if [[ -n "${DRY_RUN}" ]] ; then
  echo "Skipping this step in dry-run mode"
  exit
fi

dir="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"
source "$dir/release/teamcity-support.sh"
version=$(grep -v "^#" "$dir/../pkg/build/version.txt" | head -n1)

google_credentials="$GCS_CREDENTIALS_PROD" log_into_gcloud
gsutil cp "gs://cockroach-release-artifacts-staged-prod/cockroach-$version.linux-amd64.tgz" ./
tar xf "cockroach-$version.linux-amd64.tgz"
echo "select crdb_internal.force_panic('testing');" | "./cockroach-${version}.linux-amd64/cockroach" demo --insecure || true
rm -rf "cockroach-$version.linux-amd64.tgz" "cockroach-${version}.linux-amd64"
