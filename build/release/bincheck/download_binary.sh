#!/usr/bin/env bash

# Copyright 2022 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -exuo pipefail

download_and_extract() {
  cockroach_version=$1
  binary_suffix=$2
  binary_source="https://binaries.cockroachdb.com"
  binary_url="${binary_source}/cockroach-${cockroach_version}.${binary_suffix}"

  mkdir -p mnt

  # Check if this is a tarball or zip.
  if [[ "${binary_suffix}" == *.tgz ]]; then
    curl --header 'Cache-Control: no-cache' -sSfL "${binary_url}?$RANDOM" > cockroach.tar.gz
    tar zxf cockroach.tar.gz -C mnt --strip-components=1
  else
    curl --header 'Cache-Control: no-cache' -sSfL "${binary_url}?$RANDOM" > cockroach.zip
    7z e -omnt cockroach.zip
  fi

  echo "Downloaded ${binary_url}"
}
