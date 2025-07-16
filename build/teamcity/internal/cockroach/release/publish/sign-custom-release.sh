#!/usr/bin/env bash

# Copyright 2023 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

set -xeuo pipefail

service_account=$(curl --header "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/email" || echo "")
if [[ $service_account != "signing-agent@crl-teamcity-agents.iam.gserviceaccount.com" ]]; then
  echo "Not running on a signing agent, skipping signing"
  exit 1
fi

dir="$(dirname $(dirname $(dirname $(dirname $(dirname $(dirname "${0}"))))))"
source "$dir/teamcity-support.sh"  # For log_into_gcloud

curr_dir=$(pwd)

remove_files_on_exit() {
  rm -f "$curr_dir/.google-credentials.json"
  rm -rf "$curr_dir/.secrets"
}
trap remove_files_on_exit EXIT

mkdir -p .secrets
# Explicitly set the account to the signing agent. This is helpful if one of the previous
# commands failed and left the account set to something else.
gcloud config set account "signing-agent@crl-teamcity-agents.iam.gserviceaccount.com"
gcloud secrets versions access latest --secret=gpg-private-key | base64 -d > "$curr_dir/.secrets/gpg-private-key"
gcloud secrets versions access latest --secret=gpg-private-key-password | base64 -d > "$curr_dir/.secrets/gpg-private-key-password"

gpg --homedir "$curr_dir/.secrets" --pinentry-mode loopback \
  --passphrase-file "$curr_dir/.secrets/gpg-private-key-password" \
  --import "$curr_dir/.secrets/gpg-private-key"

# By default, set dry-run variables
google_credentials="$GCS_CREDENTIALS_DEV"
gcs_staged_bucket="cockroach-release-artifacts-staged-dryrun"
version=$(grep -v "^#" "$dir/../pkg/build/version.txt" | head -n1)
cockroach_archive_prefix="${COCKROACH_ARCHIVE_PREFIX:?COCKROACH_ARCHIVE_PREFIX must be set}"

# override dev defaults with production values
if [[ -z "${DRY_RUN}" ]] ; then
  echo "Setting production variable values"
  google_credentials="$GCS_CREDENTIALS_PROD"
  gcs_staged_bucket="cockroach-release-artifacts-staged-prod"
fi

log_into_gcloud

mkdir -p artifacts
cd artifacts

for platform in linux-amd64 linux-arm64; do
  tarball=${cockroach_archive_prefix}-${version}.${platform}.tgz

  gsutil cp "gs://$gcs_staged_bucket/$tarball" "$tarball"
  gsutil cp "gs://$gcs_staged_bucket/$tarball.sha256sum" "$tarball.sha256sum"

  shasum --algorithm 256 --check "$tarball.sha256sum"

  gpg --homedir "$curr_dir/.secrets" --pinentry-mode loopback \
    --passphrase-file "$curr_dir/.secrets/gpg-private-key-password" \
    --detach-sign --armor "$tarball"
  gpg --homedir "$curr_dir/.secrets" --verify "$tarball.asc" "$tarball"

  gsutil cp "$tarball.asc" "gs://$gcs_staged_bucket/$tarball.asc"
done
