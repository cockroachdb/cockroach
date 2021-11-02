#!/usr/bin/env bash
#
# Copyright 2021 The Cockroach Authors.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.
set -euo pipefail
# These values are substituted in the Go code that uses this.
SOURCE_URL="%s"
CACHE_URL="%s"
SHA256="%s"
OUTPUT_FILE="%s"

checkFile() {
  local file_name="${1}"
  local expected_shasum="${2}"

  local actual_shasum=""
  if command -v sha256sum > /dev/null 2>&1; then
    actual_shasum=$(sha256sum "$file_name" | cut -f1 -d' ')
  elif command -v shasum > /dev/null 2>&1; then
    actual_shasum=$(shasum -a 256 "$file_name" | cut -f1 -d' ')
  else
    echo "sha256sum or shasum not found" >&2
    return 1
  fi

  if [[ "$actual_shasum" == "$expected_shasum" ]]; then
     return 0
  else
    echo "SHASUM MISMATCH: expected: $expected_shasum, actual: $actual_shasum"
    return 1
  fi
}

download() {
  local url="$1"
  local output_file="$2"
  local output_dir
  local tmpfile

  output_dir="$(dirname "$output_file")"
  tmpfile=$(mktemp "$output_dir/.roachprod_download_XXXX")
  trap "rm -f -- ${tmpfile}" EXIT
  # curl options:
  #            -s: silent
  #            -o: output file
  #            -L: follow redirects
  #  --show-error: show errors despite silent flag
  #        --fail: Exit non-0 for non-2XX HTTP status codes
  #       --retry: Retry transient errors
  # --retry-delay: Time to wait (in seconds) between retries
  if curl -s -L --retry 3 --retry-delay 1 --fail --show-error -o "$tmpfile" "$url"; then
      mv "$tmpfile" "$output_file"
  else
      rm -f "$tmpfile"
      return 1
  fi
}

download_with_cache() {
  local source_url="$1"
  local cache_url="$2"
  local sha256="$3"
  local output_file="$4"

  echo "Attempting to download from cache: $cache_url"
  if ! download "$cache_url" "$output_file"; then
    echo "Failed to download from cache ($cache_url), trying source: $source_url"
    download "$source_url" "$output_file"
  fi
  checkFile "$output_file" "$sha256"
}

if ! [[ -f "$OUTPUT_FILE" ]]; then
  echo "File not found on disk, downloading"
  download_with_cache "$SOURCE_URL" "$CACHE_URL" "$SHA256" "$OUTPUT_FILE"
else
  if ! checkFile "$OUTPUT_FILE" "$SHA256"; then
    echo "File found on disk but with wrong SHA256, redownloading"
    download_with_cache "$SOURCE_URL" "$CACHE_URL" "$SHA256" "$OUTPUT_FILE"
  else
    echo "File already downloaded."
  fi
fi
