#!/usr/bin/env bash

# Copyright 2022 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -exuo pipefail

download_and_extract() {
  cockroach_version=$1
  binary_suffix=$2
  archive="cockroach-${cockroach_version}.${binary_suffix}"

  mkdir -p mnt

  # Extract to the same local filenames regardless of source, so the
  # .gitignore entries stay valid.
  if [[ "${binary_suffix}" == *.tgz ]]; then
    local_file=cockroach.tar.gz
  else
    local_file=cockroach.zip
  fi

  # When BINCHECK_GCS_BUCKET is set (build-and-sign, pre-publish), pull the
  # staged archive from the GCS API with the BINCHECK_GCS_TOKEN bearer token.
  # curl (rather than gcloud) keeps this portable to the Windows runner, where
  # gcloud isn't readily available from bash. Otherwise pull the published
  # archive from the public CDN.
  if [[ -n "${BINCHECK_GCS_BUCKET:-}" ]]; then
    curl -sSfL -H "Authorization: Bearer ${BINCHECK_GCS_TOKEN:?BINCHECK_GCS_TOKEN must be set when BINCHECK_GCS_BUCKET is set}" \
      "https://storage.googleapis.com/${BINCHECK_GCS_BUCKET}/${archive}" > "${local_file}"
  else
    curl --header 'Cache-Control: no-cache' -sSfL \
      "https://binaries.cockroachdb.com/${archive}?$RANDOM" > "${local_file}"
  fi

  if [[ "${binary_suffix}" == *.tgz ]]; then
    tar zxf "${local_file}" -C mnt --strip-components=1
  else
    7z e -omnt "${local_file}"
  fi

  echo "Fetched ${archive}"
}
