#!/usr/bin/env bash

set -xeuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname $(dirname "${0}"))))))"
source "$dir/teamcity-support.sh"  # For log_into_gcloud

google_credentials="$GCS_CREDENTIALS_DEV"
s3_bucket="cockroach-builds-test"
gcs_bucket="cockroach-release-artifacts-dryrun"
# Always use prod to download for now
download_prefix="https://storage.googleapis.com/cockroach-release-artifacts-prod"

# override dev defaults with production values
if [[ -z "${DRY_RUN}" ]] ; then
  echo "ooops, not there yet!"
  exit 1
  echo "Setting production values"
  google_credentials="$GCS_CREDENTIALS_PROD"
  s3_bucket="binaries.cockroachdb.com"
  gcs_bucket="cockroach-release-artifacts-prod"
fi

KEYCHAIN_NAME=signing
KEYCHAIN_PROFILE=notarization

security unlock-keychain -p "${KEYCHAIN_PASSWORD}" "${KEYCHAIN_NAME}"

# make sure we can log in to GCS
log_into_gcloud
export GOOGLE_APPLICATION_CREDENTIALS="$PWD/.google-credentials.json"

# install bazelisk
mkdir -p bazelisk-bin
curl -o bazelisk-bin/bazel https://github.com/bazelbuild/bazelisk/releases/download/v1.13.2/bazelisk-darwin-arm64
# TODO: verify checksum
chmod 755 bazel-bin/bazel
export PATH=$PWD/bazelisk-bin:$PATH

# compile the tool
bazel build --config=crosslinux //pkg/cmd/cloudupload
BAZEL_BIN=$(bazel info --config=crosslinux bazel-bin)

mkdir -p artifacts
cd artifacts

for product in cockroach cockroach-sql; do
  for platform in darwin-aarch64; do
    base=${product}-${VERSION}.${platform}
    unsigned_file=${base}.unsigned.tgz
    target=${base}.tgz
    curl -o "$unsigned_file" "$download_prefix/$unsigned_file"
    tar -xf "$unsigned_file"
    codesign --timestamp --options=runtime -f --keychain "$KEYCHAIN_NAME" -s "$SIGNING_IDENTITY" \
      "$base/$product"
    tar -czf "$target" "$base"
    zip crl.zip "$base/$product"
    xcrun notarytool submit crl.zip --wait \
      --team-id "$TEAM_ID" --keychain-profile "$KEYCHAIN_PROFILE" \
      --apple-id "$APPLE_ID" --verbose \
      --keychain "${HOME}/Library/Keychains/${KEYCHAIN_NAME}-db"
    rm -rf "$base" "$unsigned_file" crl.zip

    echo $BAZEL_BIN/pkg/cmd/cloudupload/cloudupload_/cloudupload \
      "$target" "s3://$s3_bucket/$target"
    echo $BAZEL_BIN/pkg/cmd/cloudupload/cloudupload_/cloudupload \
      "$target" "gcs://$gcs_bucket/$target"

  done
done
