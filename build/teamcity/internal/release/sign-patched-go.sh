#!/usr/bin/env bash

# Copyright 2023 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -xeuo pipefail

KEYCHAIN_NAME=signing
KEYCHAIN_PROFILE=notarization
curr_dir=$(pwd)

cleanup() {
    security lock-keychain "${KEYCHAIN_NAME}"
    rm -rf darwin.zip staging darwin-amd64 darwin-arm64 *.tar.gz TIMESTAMP.txt
}
trap cleanup EXIT

mkdir artifacts
mv TIMESTAMP.txt artifacts/TIMESTAMP.txt
security unlock-keychain -p "${KEYCHAIN_PASSWORD}" "${KEYCHAIN_NAME}"

sign() {
    archive=$(find . -name go*.darwin-$1.tar.gz -d 1 | head -n1 | xargs basename)
    mkdir darwin-$1
    tar -xf $archive -C darwin-$1
    rm $archive
    for bin in go gofmt; do
        codesign --timestamp --options=runtime -f --keychain "$KEYCHAIN_NAME" -s "$SIGNING_IDENTITY" darwin-$1/go/bin/$bin
    done
    tar cf - -C darwin-$1 go | gzip -9 > artifacts/$archive
    mkdir staging
    cp darwin-$1/go/bin/gofmt staging
    cp darwin-$1/go/bin/go    staging
    zip -r darwin.zip staging
    rm -rf staging
    xcrun notarytool submit darwin.zip --wait \
          --team-id "$TEAM_ID" --keychain-profile "$KEYCHAIN_PROFILE" \
          --apple-id "$APPLE_ID" --verbose \
          --keychain "${HOME}/Library/Keychains/${KEYCHAIN_NAME}-db"
}

sign amd64
sign arm64
