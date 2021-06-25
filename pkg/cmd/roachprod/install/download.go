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
	_ "embed" // required for go:embed
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
)

const (
	gcsCacheBaseURL       = "https://storage.googleapis.com/cockroach-fixtures/tools/"
	localClusterCacheDir  = "${HOME}/local/.cache/tools"
	remoteClusterCacheDir = "/var/cache/roachprod/tools"
)

//go:embed scripts/download.sh
var downloadScript string

// Download downloads the remote resource, preferring the local file
// cache and GCS cache if available.
func Download(c *SyncedCluster, sourceURLStr string, sha string, dest string) error {
	// https://example.com/foo/bar.txt
	sourceURL, err := url.Parse(sourceURLStr)
	if err != nil {
		return err
	}
	// bar.txt
	basename := path.Base(sourceURL.Path)

	// /path/to/cache
	fileCacheDir := cacheDir(c)
	// SHA-bar.txt
	cacheBasename := fmt.Sprintf("%s-%s", sha, basename)

	// /path/to/cache/SHA-bar.txt
	fileCachePath := filepath.Join(fileCacheDir, cacheBasename)
	// https://storage.googleapis.com/SOME_BUCKET/SHA-bar.txt
	gcsCacheURL, err := url.Parse(path.Join(gcsCacheBaseURL, cacheBasename))
	if err != nil {
		return err
	}

	if dest == "" {
		dest = path.Join("./", basename)
	}

	downloadNodes := c.Nodes
	cpNodes := c.Nodes
	// We don't want to deal with cross-platform file locking in
	// shell scripts, so if we are on a local cluster, we download
	// it on a single node and copy it from the cache on a single
	// node if we have a non-relative path.
	if c.IsLocal() {
		downloadNodes = downloadNodes[:1]
		if filepath.IsAbs(dest) {
			cpNodes = cpNodes[:1]
		}
	}

	createCacheCmd := fmt.Sprintf("sudo mkdir -p %[1]s && sudo chmod o+rwx %[1]s", fileCacheDir)
	if c.IsLocal() {
		createCacheCmd = fmt.Sprintf("mkdir -p %s", fileCacheDir)
	}

	if err := c.Run(os.Stdout, os.Stderr,
		downloadNodes,
		"ensuring cache directory",
		createCacheCmd,
	); err != nil {
		return err
	}

	downloadCmd := fmt.Sprintf(downloadScript,
		sourceURL.String(),
		gcsCacheURL.String(),
		fileCachePath,
		sha,
	)
	if err := c.Run(os.Stdout, os.Stderr,
		downloadNodes,
		fmt.Sprintf("downloading %s", basename),
		downloadCmd,
	); err != nil {
		return err
	}

	return c.Run(os.Stdout, os.Stderr,
		cpNodes,
		"copying from cache to dest",
		fmt.Sprintf(`cp "%s" "%s"`, fileCachePath, dest))
}

func cacheDir(c *SyncedCluster) string {
	if c.IsLocal() {
		return localClusterCacheDir
	}
	return remoteClusterCacheDir
}
