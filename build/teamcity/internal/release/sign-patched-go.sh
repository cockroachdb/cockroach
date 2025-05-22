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

cleanup() {
    rm -rf darwin.zip staging darwin-amd64 darwin-arm64 ./*.tar.gz TIMESTAMP.txt
    rm -rf .secrets
}
trap cleanup EXIT

mkdir -p .secrets
# Explicitly set the account to the signing agent. This is helpful if one of the previous
# commands failed and left the account set to something else.
gcloud config set account "signing-agent@crl-teamcity-agents.iam.gserviceaccount.com"
gcloud secrets versions access latest --secret=apple-signing-cert | base64 -d > .secrets/cert.p12
gcloud secrets versions access latest --secret=apple-signing-cert-password > .secrets/cert.pass
gcloud secrets versions access latest --secret=appstoreconnect-api-key > .secrets/api_key.json

mkdir artifacts
mv TIMESTAMP.txt artifacts/TIMESTAMP.txt
sign() {
    archive=$(ls -1 go*.darwin-$1.tar.gz | head -n1 | xargs basename)
    mkdir "darwin-$1"
    tar -xf "$archive" -C "darwin-$1"
    rm "$archive"
    for bin in go gofmt; do
        rcodesign sign \
          --p12-file .secrets/cert.p12 --p12-password-file .secrets/cert.pass \
          --code-signature-flags runtime \
          "darwin-$1/go/bin/$bin"
    done
    tar cf - -C "darwin-$1" go | gzip -9 > "artifacts/$archive"
    mkdir staging
    cp "darwin-$1/go/bin/gofmt" staging
    cp "darwin-$1/go/bin/go"    staging
    zip -r darwin.zip staging
    rm -rf staging
    rcodesign notary-submit \
      --api-key-file .secrets/api_key.json \
      --wait \
      darwin.zip
}

sign amd64
sign arm64
