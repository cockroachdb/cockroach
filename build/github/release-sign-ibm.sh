#!/usr/bin/env bash

# Copyright 2026 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# GitHub Actions wrapper for IBM GPG signing. Replaces the TeamCity signing
# agent approach: uses WIF-authenticated gcloud to fetch GPG keys from
# Secret Manager.
#
# NOTE: This script intentionally does NOT use set -x. It handles signing
# secrets that must never appear in build logs.

set -euo pipefail

dir="$(dirname $(dirname $(dirname "${0}")))"
source "$dir/build/release/teamcity-support.sh"

secrets_dir="${RUNNER_TEMP:-.}/.secrets"

remove_files_on_exit() {
  rm -f .google-credentials.json
  rm -rf "$secrets_dir"
}
trap remove_files_on_exit EXIT

# Store signing secrets outside the workspace with restrictive permissions.
echo "Fetching GPG signing secrets from Secret Manager..."
umask 077
mkdir -p "$secrets_dir"
gcloud secrets versions access latest --secret=gha-releases-gpg-private-key | base64 -d > "$secrets_dir/gpg-private-key"
gcloud secrets versions access latest --secret=gha-releases-gpg-private-key-password | base64 -d > "$secrets_dir/gpg-private-key-password"
umask 022
echo "Secrets fetched successfully."

echo "Importing GPG key..."
gpg --homedir "$secrets_dir" --pinentry-mode loopback \
  --passphrase-file "$secrets_dir/gpg-private-key-password" \
  --import "$secrets_dir/gpg-private-key"

# By default, set dry-run variables.
gcs_staged_bucket="cockroach-release-artifacts-staged-dryrun"
version=$(grep -v "^#" "$dir/pkg/build/version.txt" | head -n1)
cockroach_archive_prefix="${COCKROACH_ARCHIVE_PREFIX:?COCKROACH_ARCHIVE_PREFIX must be set}"

# Override dev defaults with production values.
if [[ -z "${DRY_RUN}" ]] ; then
  echo "Using production GCS bucket"
  gcs_staged_bucket="cockroach-release-artifacts-staged-prod"
fi

# gcloud is already authenticated via WIF; log_into_gcloud is a no-op.
log_into_gcloud

mkdir -p artifacts
cd artifacts

for platform in linux-amd64 linux-amd64-fips linux-arm64 linux-s390x; do
  tarball=${cockroach_archive_prefix}-${version}.${platform}.tgz

  echo "Downloading $tarball..."
  gsutil -o 'GSUtil:num_retries=5' cp "gs://$gcs_staged_bucket/$tarball" "$tarball"
  gsutil -o 'GSUtil:num_retries=5' cp "gs://$gcs_staged_bucket/$tarball.sha256sum" "$tarball.sha256sum"

  echo "Verifying checksum..."
  shasum --algorithm 256 --check "$tarball.sha256sum"

  echo "GPG signing $tarball..."
  gpg --homedir "$secrets_dir" --pinentry-mode loopback \
    --passphrase-file "$secrets_dir/gpg-private-key-password" \
    --detach-sign --armor "$tarball"
  gpg --homedir "$secrets_dir" --verify "$tarball.asc" "$tarball"

  echo "Uploading $tarball.asc..."
  gsutil -o 'GSUtil:num_retries=5' cp "$tarball.asc" "gs://$gcs_staged_bucket/$tarball.asc"
done
echo "IBM GPG signing complete."
