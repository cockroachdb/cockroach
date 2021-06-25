// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package install

import (
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
)

const (
	gcsCacheBaseURL     = "https://storage.googleapis.com/cockroach-fixtures/tools/"
	localCacheBasePath  = "${HOME}/local/.cache/tools"
	remoteCacheBasePath = "/var/cache/roachprod/tools"
)

var downloadScript = `
#!/usr/bin/env bash
set -euo pipefail

SOURCE_URL="%s"
CACHE_URL="%s"
CACHE_PATH="%s"
SHA256="%s"
TARGET="%s"

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
  #  --show-error: show errors depsite silent flag
  #        --fail: Exit non-0 for non-2XX HTTP status codes
  #       --retry: Retry transient errors
  # --retry-delay: Time to wait (in seconds) between retries
  if curl -s --retry 3 --retry-delay 1 --fail --show-error -o "$tmpfile" "$url"; then
      mv "$tmpfile" "$output_file"
  else
      rm -f "$tmpfile"
  fi
}

download_with_cache() {
  local cache_path="$1"
  local cache_url="$2"
  local source_url="$3"
  local sha256="$4"

  mkdir -p "$(dirname $cache_path)"
  echo "Attempting to download from cache: $cache_url"
  if ! download "$cache_url" "$cache_path"; then
    echo "Failed to download from cache, trying source: $source_url"
    download "$source_url" "$cache_path"
  fi
  checkFile "$cache_path" "$sha256"
}

if ! [[ -f "$CACHE_PATH" ]]; then
  echo "File not found in on-disk cache, downloading"
  download_with_cache "$CACHE_PATH" "$CACHE_URL" "$SOURCE_URL" "$SHA256"
else
  if ! checkFile "$CACHE_PATH" "$SHA256"; then
    echo "File found in on-disk cache but with wrong SHA256, redownloading"
    download_with_cache "$CACHE_PATH" "$CACHE_URL" "$SOURCE_URL" "$SHA256"
  else
    echo "File found in on-disk cache"
  fi
fi
`

// Download downloads the remote resource, preferring the local file
// cache and GCS cache if available.
func Download(c *SyncedCluster, args []string) error {
	sourceURLStr := args[0]
	sourceSHA := args[1]

	sourceURL, err := url.Parse(sourceURLStr)
	if err != nil {
		return err
	}
	filename := path.Base(sourceURL.Path)

	cacheBasePath := fileCacheBasePath(c)
	cachePath := path.Join(cacheBasePath, filename)
	gcsCacheURL, err := url.Parse(path.Join(gcsCacheBaseURL, filename))
	if err != nil {
		return err
	}

	var targetDest string
	if len(args) > 2 {
		targetDest = args[2]
	} else {
		targetDest = path.Join("./", filename)
	}

	cmd := fmt.Sprintf(downloadScript,
		sourceURL.String(),
		gcsCacheURL.String(),
		cachePath,
		sourceSHA,
		targetDest,
	)

	downloadNodes := c.Nodes
	cpNodes := c.Nodes
	// We don't want to deal with cross-platform file locking in
	// shell scripts, so if we are on a local cluster, we download
	// it on a single node and copy it from the cache on a single
	// node if we have a non-relative path.
	if c.IsLocal() {
		downloadNodes = downloadNodes[:1]
		if filepath.IsAbs(targetDest) {
			cpNodes = cpNodes[:1]
		}
	}

	// TODO(ssd): This chmod is unfortunate. Would be nice for a
	// path with the appropriate permissions to be set up during
	// the build.
	createCacheCmd := fmt.Sprintf("sudo mkdir -p %[1]s; sudo chmod o+rwx %[1]s", cacheBasePath)
	if c.IsLocal() {
		createCacheCmd = fmt.Sprintf("mkdir -p %s", cacheBasePath)
	}

	if err := c.Run(os.Stdout, os.Stderr,
		downloadNodes,
		"ensuring cache directory",
		createCacheCmd,
	); err != nil {
		return err
	}

	if err := c.Run(os.Stdout, os.Stderr,
		downloadNodes,
		fmt.Sprintf("downloading %s", filename),
		cmd,
	); err != nil {
		return err
	}

	return c.Run(os.Stdout, os.Stderr,
		cpNodes,
		"copying from cache to dest",
		fmt.Sprintf(`cp "%s" "%s"`, cachePath, targetDest))
}

func fileCacheBasePath(c *SyncedCluster) string {
	if c.IsLocal() {
		return localCacheBasePath
	}
	return remoteCacheBasePath
}
