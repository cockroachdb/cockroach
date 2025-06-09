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
source "$dir/shlib.sh"

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
gcloud secrets versions access latest --secret=apple-signing-cert | base64 -d > "$curr_dir/.secrets/cert.p12"
gcloud secrets versions access latest --secret=apple-signing-cert-password > "$curr_dir/.secrets/cert.pass"
gcloud secrets versions access latest --secret=appstoreconnect-api-key > "$curr_dir/.secrets/api_key.json"

# By default, set dry-run variables
google_credentials="$GCS_CREDENTIALS_DEV"
gcs_staged_bucket="cockroach-release-artifacts-staged-dryrun"
version=$(grep -v "^#" "$dir/../pkg/build/version.txt" | head -n1)

# override dev defaults with production values
if [[ -z "${DRY_RUN}" ]] ; then
  echo "Setting production variable values"
  google_credentials="$GCS_CREDENTIALS_PROD"
  gcs_staged_bucket="cockroach-release-artifacts-staged-prod"
fi

log_into_gcloud

mkdir -p artifacts
cd artifacts

for product in cockroach cockroach-sql; do
  # TODO: add Intel binaries too.
  for platform in darwin-11.0-arm64; do
    base=${product}-${version}.${platform}
    unsigned_base=${product}-${version}.${platform}.unsigned
    unsigned_file=${unsigned_base}.tgz
    target=${base}.tgz

    gsutil cp "gs://$gcs_staged_bucket/$unsigned_file" "$unsigned_file"
    gsutil cp "gs://$gcs_staged_bucket/$unsigned_file.sha256sum" "$unsigned_file.sha256sum"

    shasum --algorithm 256 --check "$unsigned_file.sha256sum"

    tar -xf "$unsigned_file"
    mv "$unsigned_base" "$base"

    rcodesign sign \
      --p12-file "$curr_dir/.secrets/cert.p12" --p12-password-file "$curr_dir/.secrets/cert.pass" \
      --code-signature-flags runtime \
      "$base/$product"
    tar -czf "$target" "$base"

    zip crl.zip "$base/$product"
    retry rcodesign notary-submit \
      --api-key-file "$curr_dir/.secrets/api_key.json" \
      --wait \
      crl.zip

    rm -rf "$base" "$unsigned_file" "$unsigned_file.sha256sum" crl.zip

    shasum --algorithm 256 "$target" > "$target.sha256sum"
    gsutil cp "$target" "gs://$gcs_staged_bucket/$target"
    gsutil cp "$target.sha256sum" "gs://$gcs_staged_bucket/$target.sha256sum"

  done
done
