#!/usr/bin/env bash

# Copyright 2023 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -xeuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname $(dirname "${0}"))))))"
source "$dir/teamcity-support.sh"  # For log_into_gcloud

KEYCHAIN_NAME=signing
KEYCHAIN_PROFILE=notarization
curr_dir=$(pwd)

remove_files_on_exit() {
  rm -f "$curr_dir/.google-credentials.json"
  security lock-keychain "${KEYCHAIN_NAME}"
}
trap remove_files_on_exit EXIT

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

# Install gcloud/gsutil
GOOGLE_SDK_DIR=$(mktemp -d)
curl https://sdk.cloud.google.com > "$GOOGLE_SDK_DIR/install.sh"
bash "$GOOGLE_SDK_DIR/install.sh" --disable-prompts --install-dir="$GOOGLE_SDK_DIR"
export PATH="$GOOGLE_SDK_DIR/google-cloud-sdk/bin":$PATH

log_into_gcloud
security unlock-keychain -p "${KEYCHAIN_PASSWORD}" "${KEYCHAIN_NAME}"

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

    codesign --timestamp --options=runtime -f --keychain "$KEYCHAIN_NAME" -s "$SIGNING_IDENTITY" \
      "$base/$product"
    tar -czf "$target" "$base"

    zip crl.zip "$base/$product"
    xcrun notarytool submit crl.zip --wait \
      --team-id "$TEAM_ID" --keychain-profile "$KEYCHAIN_PROFILE" \
      --apple-id "$APPLE_ID" --verbose \
      --keychain "${HOME}/Library/Keychains/${KEYCHAIN_NAME}-db"

    rm -rf "$base" "$unsigned_file" "$unsigned_file.sha256sum" crl.zip

    shasum --algorithm 256 "$target" > "$target.sha256sum"
    gsutil cp "$target" "gs://$gcs_staged_bucket/$target"
    gsutil cp "$target.sha256sum" "gs://$gcs_staged_bucket/$target.sha256sum"

  done
done
