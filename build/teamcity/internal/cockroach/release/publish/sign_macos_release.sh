#!/usr/bin/env bash

# Copyright 2022 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -xeuo pipefail

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
gcs_bucket="cockroach-release-artifacts-dryrun"

# override dev defaults with production values
if [[ -z "${DRY_RUN}" ]] ; then
  echo "Setting production variable values"
  google_credentials="$GCS_CREDENTIALS_PROD"
  gcs_bucket="cockroach-release-artifacts-prod"
fi

echo "$google_credentials" > "$curr_dir/.google-credentials.json"
export GOOGLE_APPLICATION_CREDENTIALS="$curr_dir/.google-credentials.json"

# install bazelisk
mkdir -p bazelisk-bin
curl -fsSL https://github.com/bazelbuild/bazelisk/releases/download/v1.13.2/bazelisk-darwin-arm64 > bazelisk-bin/bazel
shasum --algorithm 256 --check - <<EOF
c84fb9ae7409b19aee847af4767639ab86fc3ce110f961107ff278879b019e38  bazelisk-bin/bazel
EOF
chmod 755 bazelisk-bin/bazel
export PATH=$PWD/bazelisk-bin:$PATH

# compile the tool
bazel build --config=crossmacosarm //pkg/cmd/cloudupload
BAZEL_BIN=$(bazel info --config=crossmacosarm bazel-bin)

security unlock-keychain -p "${KEYCHAIN_PASSWORD}" "${KEYCHAIN_NAME}"

mkdir -p artifacts
cd artifacts

for product in cockroach cockroach-sql; do
  # TODO: add Intel binaries too.
  for platform in darwin-11.0-arm64; do
    base=${product}-${VERSION}.${platform}
    unsigned_base=${product}-${VERSION}.${platform}.unsigned
    unsigned_file=${unsigned_base}.tgz
    target=${base}.tgz
    download_prefix="https://storage.googleapis.com/$gcs_bucket"

    curl -o "$unsigned_file" "$download_prefix/$unsigned_file"
    curl -o "$unsigned_file.sha256sum" "$download_prefix/$unsigned_file.sha256sum"
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
    "$BAZEL_BIN/pkg/cmd/cloudupload/cloudupload_/cloudupload" \
      "$target" "gs://$gcs_bucket/$target"
    "$BAZEL_BIN/pkg/cmd/cloudupload/cloudupload_/cloudupload" \
      "$target.sha256sum" "gs://$gcs_bucket/$target.sha256sum"

  done
done
