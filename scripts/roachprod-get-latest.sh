#!/usr/bin/env bash

# Copyright 2023 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


# Downloads the _latest_ roachprod binary (on master) from GCS to the specified directory
# or the current directory if none is specified.
# Assumes that you have gsutil installed and authenticated; see [1] for those details.
#
# The specified directory, release branch, OS and CPU architecture are optional arguments. They can be overridden by
# simultaneously specifying _all_ in the described order.
#
# [1] https://cockroachlabs.atlassian.net/wiki/spaces/TE/pages/144408811/Roachprod+Tutorial

DEFAULT_BRANCH="master"
DEFAULT_OS=$(uname | tr '[:upper:]' '[:lower:]')
DEFAULT_ARCH=$(arch | tr '[:upper:]' '[:lower:]')
BUCKET="cockroach-nightly"

DEST_DIR=${1:-.}
DEST_FILE="$DEST_DIR/roachprod"
BRANCH=${2:-$DEFAULT_BRANCH}
OS=${3:-$DEFAULT_OS}
ARCH=${4:-$DEFAULT_ARCH}

build_for_plat() {
  plat="$OS-$ARCH"
  if [ "$plat" = "linux-aarch64" ] || [ "$plat" = "linux-arm64" ]; then
    echo "linux/arm64"
  elif [ "$plat" = "linux-x86_64" ] || [ "$plat" = "linux-amd64" ]; then
    echo "linux/amd64"
  elif [ "$plat" = "darwin-arm64" ]; then
    echo "darwin/arm64"
  elif [ "$plat" = "darwin-x86_64" ]; then
    echo "darwin/amd64"
  else
    echo "No available 'roachprod' binary for your OS/CPU: $PLAT"
    exit 1
  fi
}

prompt_to_overwrite() {
  read -p "File already exists. Overwrite? [y/N] " -n 1 -r
  echo
  [[ $REPLY =~ ^[Yy]$ ]]
}

BUILD=$(build_for_plat)
URL_PREFIX="gs://$BUCKET/binaries/$BRANCH/$BUILD"
LATEST_SHA=$(gsutil cat "${URL_PREFIX}/latest_sha")

# error out if LASTEST_SHA is empty
if [ -z "$LATEST_SHA" ]; then
  echo "No latest SHA found for $BUILD. (Debugging hint: check the GCS bucket at $URL_PREFIX)"
  exit 1
fi

URL="${URL_PREFIX}/roachprod.$LATEST_SHA"
echo "Found latest SHA: $LATEST_SHA"

echo "Download URL: $URL"
if command -v gsutil &> /dev/null; then
  if [[ ! -f "$DEST_FILE" ]] || prompt_to_overwrite; then
    gsutil cp "$URL" "$DEST_FILE"
    chmod +x "$DEST_FILE"
  fi
else
  echo "gsutil is not installed. Consult the wiki: https://cockroachlabs.atlassian.net/wiki/spaces/TE/pages/144408811/Roachprod+Tutorial"
  exit 1
fi
