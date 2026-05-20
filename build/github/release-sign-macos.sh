#!/usr/bin/env bash

# Copyright 2026 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# GitHub Actions wrapper for macOS signing. Replaces the TeamCity signing agent
# approach: instead of relying on a GCE instance SA, we use WIF-authenticated
# gcloud to fetch Apple signing secrets from Secret Manager.
#
# NOTE: This script intentionally does NOT use set -x. It handles signing
# secrets that must never appear in build logs.

set -euo pipefail

dir="$(dirname $(dirname $(dirname "${0}")))"
source "$dir/build/release/teamcity-support.sh"
source "$dir/build/shlib.sh"

secrets_dir="${RUNNER_TEMP:-.}/.secrets"

remove_files_on_exit() {
  rm -f .google-credentials.json
  rm -rf "$secrets_dir"
}
trap remove_files_on_exit EXIT

# Store signing secrets outside the workspace with restrictive permissions.
echo "Fetching Apple signing secrets from Secret Manager..."
umask 077
mkdir -p "$secrets_dir"
gcloud secrets versions access latest --secret=gha-releases-apple-signing-cert | base64 -d > "$secrets_dir/cert.p12"
gcloud secrets versions access latest --secret=gha-releases-apple-signing-cert-password > "$secrets_dir/cert.pass"
gcloud secrets versions access latest --secret=gha-releases-appstoreconnect-api-key > "$secrets_dir/api_key.json"
umask 022
echo "Secrets fetched successfully."

# By default, set dry-run variables.
gcs_staged_bucket="cockroach-release-artifacts-staged-dryrun"
version=$(grep -v "^#" "$dir/pkg/build/version.txt" | head -n1)

# Override dev defaults with production values.
if [[ -z "${DRY_RUN}" ]] ; then
  echo "Using production GCS bucket"
  gcs_staged_bucket="cockroach-release-artifacts-staged-prod"
fi

# gcloud is already authenticated via WIF; log_into_gcloud is a no-op.
log_into_gcloud

mkdir -p artifacts
cd artifacts

for product in cockroach cockroach-sql; do
  for platform in darwin-11.0-arm64; do
    base=${product}-${version}.${platform}
    unsigned_base=${product}-${version}.${platform}.unsigned
    unsigned_file=${unsigned_base}.tgz
    target=${base}.tgz

    echo "Downloading unsigned archive: $unsigned_file"
    gsutil -o 'GSUtil:num_retries=5' cp "gs://$gcs_staged_bucket/$unsigned_file" "$unsigned_file"
    gsutil -o 'GSUtil:num_retries=5' cp "gs://$gcs_staged_bucket/$unsigned_file.sha256sum" "$unsigned_file.sha256sum"

    echo "Verifying checksum..."
    shasum --algorithm 256 --check "$unsigned_file.sha256sum"

    tar -xf "$unsigned_file"
    mv "$unsigned_base" "$base"

    echo "Signing $base/$product..."
    rcodesign sign \
      --p12-file "$secrets_dir/cert.p12" --p12-password-file "$secrets_dir/cert.pass" \
      --code-signature-flags runtime \
      "$base/$product"
    tar -czf "$target" "$base"

    echo "Submitting for notarization..."
    zip crl.zip "$base/$product"
    retry rcodesign notary-submit \
      --api-key-file "$secrets_dir/api_key.json" \
      --wait \
      crl.zip

    rm -rf "$base" "$unsigned_file" "$unsigned_file.sha256sum" crl.zip

    echo "Uploading signed archive: $target"
    shasum --algorithm 256 "$target" > "$target.sha256sum"
    gsutil -o 'GSUtil:num_retries=5' cp "$target" "gs://$gcs_staged_bucket/$target"
    gsutil -o 'GSUtil:num_retries=5' cp "$target.sha256sum" "gs://$gcs_staged_bucket/$target.sha256sum"

  done
done
echo "macOS signing complete."
