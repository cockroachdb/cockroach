#!/usr/bin/env bash

# Copyright 2025 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -euxo pipefail

service_account=$(curl --header "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/email" || echo "")

if [[ $service_account != "signing-agent@crl-teamcity-agents.iam.gserviceaccount.com" ]]; then
  echo "Not running on a signing agent, skipping signing"
  exit 0
fi

dir="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"
source "$dir/release/teamcity-support.sh"
source "$dir/shlib.sh"

tc_start_block "Variable Setup"

build_name=$(git describe --tags --dirty --match=v[0-9]* 2> /dev/null || git rev-parse --short HEAD;)

# On no match, `grep -Eo` returns 1. `|| echo""` makes the script not error.
release_branch="$(echo "$build_name" | grep -Eo "^v[0-9]+\.[0-9]+" || echo"")"
is_customized_build="$(echo "$TC_BUILD_BRANCH" | grep -Eo "^custombuild-" || echo "")"
is_release_build="$(echo "$TC_BUILD_BRANCH" | grep -Eo "^((staging|release|rc)-(v)?[0-9][0-9]\.[0-9](\.0)?).*|master$" || echo "")"

if [[ -z "${DRY_RUN}" ]] ; then
  if [[ -z "${is_release_build}" ]] ; then
    gcs_bucket="cockroach-customized-builds-artifacts-prod"
    google_credentials="$GOOGLE_CREDENTIALS_CUSTOMIZED"
  else
    gcs_bucket="cockroach-builds-artifacts-prod"
    google_credentials="$GCS_CREDENTIALS_PROD"
  fi
else
  gcs_bucket="cockroach-builds-artifacts-dryrun"
  build_name="${build_name}.dryrun"
  google_credentials="$GCS_CREDENTIALS_DEV"
fi

cat << EOF

  build_name:          $build_name
  release_branch:      $release_branch
  is_customized_build: $is_customized_build
  gcs_bucket:          $gcs_bucket
  is_release_build:    $is_release_build

EOF
tc_end_block "Variable Setup"

secrets_dir="$(mktemp -d)"
_on_exit() {
  rm -rf "$secrets_dir"
  gcloud config set account "signing-agent@crl-teamcity-agents.iam.gserviceaccount.com"
}
trap _on_exit EXIT


# Explicitly set the account to the signing agent. This is helpful if one of the previous
# commands failed and left the account set to something else.
gcloud config set account "signing-agent@crl-teamcity-agents.iam.gserviceaccount.com"
gcloud secrets versions access latest --secret=apple-signing-cert | base64 -d > "$secrets_dir/cert.p12"
gcloud secrets versions access latest --secret=apple-signing-cert-password > "$secrets_dir/cert.pass"
gcloud secrets versions access latest --secret=appstoreconnect-api-key > "$secrets_dir/api_key.json"

google_credentials="$google_credentials" log_into_gcloud

workdir="$(mktemp -d)"
cd "$workdir"

for product in cockroach cockroach-sql; do
  # In case we want to sign darwin-10.9-amd64, we can add it here.
  for platform in darwin-11.0-arm64; do
    gsutil cp "gs://$gcs_bucket/$product-$build_name.$platform.unsigned.tgz" "$product-$build_name.$platform.unsigned.tgz"
    tar -xzf "$product-$build_name.$platform.unsigned.tgz"
    mv "$product-$build_name.$platform.unsigned" "$product-$build_name.$platform"
    rcodesign sign \
      --p12-file "$secrets_dir/cert.p12" --p12-password-file "$secrets_dir/cert.pass" \
      --code-signature-flags runtime \
      "$product-$build_name.$platform/$product"
    zip -r crl.zip "$product-$build_name.$platform/$product"
    retry rcodesign notary-submit --api-key-file "$secrets_dir/api_key.json" --wait crl.zip
    tar -czf "$product-$build_name.$platform.tgz" "$product-$build_name.$platform"
    shasum --algorithm 256 "$product-$build_name.$platform.tgz" > "$product-$build_name.$platform.tgz.sha256sum"
    gsutil cp "$product-$build_name.$platform.tgz" "gs://$gcs_bucket/$product-$build_name.$platform.tgz"
    gsutil cp "$product-$build_name.$platform.tgz.sha256sum" "gs://$gcs_bucket/$product-$build_name.$platform.tgz.sha256sum"
    rm -rf "$product-$build_name.$platform" \
      "$product-$build_name.$platform.tgz" \
      "$product-$build_name.$platform.tgz.sha256sum" \
      crl.zip
  done
done
