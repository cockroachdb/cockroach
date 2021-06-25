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
	gcsCacheBaseURL     = "https://storage.googleapis.com/cockroach-fixtures/tools/"
	localCacheBasePath  = "${HOME}/local/.cache/tools"
	remoteCacheBasePath = "/var/cache/roachprod/tools"
)

//go:embed scripts/download.sh
var downloadScript string

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
