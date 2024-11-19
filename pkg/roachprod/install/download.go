// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package install

import (
	"context"
	_ "embed" // required for go:embed
	"fmt"
	"net/url"
	"path"
	"path/filepath"

	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
)

const (
	// We store downloadable content in a public bucket to allow for easy curling.
	gcsCacheBaseURL = "https://storage.googleapis.com/cockroach-test-artifacts"
)

//go:embed scripts/download.sh
var downloadScript string

// Download downloads the remote resource, preferring a GCS cache if available.
func Download(
	ctx context.Context,
	l *logger.Logger,
	c *SyncedCluster,
	sourceURLStr string,
	sha string,
	dest string,
) error {
	// https://example.com/foo/bar.txt
	sourceURL, err := url.Parse(sourceURLStr)
	if err != nil {
		return err
	}

	// bar.txt
	basename := path.Base(sourceURL.Path)
	// SHA-bar.txt
	cacheBasename := fmt.Sprintf("%s-%s", sha, basename)
	// https://storage.googleapis.com/SOME_BUCKET/SHA-bar.txt
	gcsCacheURL, err := url.Parse(path.Join(gcsCacheBaseURL, cacheBasename))
	if err != nil {
		return err
	}

	if dest == "" {
		dest = path.Join("./", basename)
	}

	// We don't want to deal with cross-platform file locking in
	// shell scripts, so if we are on a local cluster, we download
	// it on a single node and copy it from the cache on a single
	// node if we have a non-relative path.
	downloadNodes := c.Nodes
	if c.IsLocal() {
		downloadNodes = downloadNodes[:1]
	}

	downloadCmd := fmt.Sprintf(downloadScript,
		sourceURL.String(),
		gcsCacheURL.String(),
		sha,
		dest,
	)
	if err := c.Run(ctx, l, l.Stdout, l.Stderr,
		WithNodes(downloadNodes),
		fmt.Sprintf("downloading %s", basename),
		downloadCmd,
	); err != nil {
		return err
	}

	// If we are local and the destination is relative, then copy the file from
	// the download node to the other nodes.
	if c.IsLocal() && !filepath.IsAbs(dest) {
		src := filepath.Join(c.localVMDir(downloadNodes[0]), dest)
		cpCmd := fmt.Sprintf(`cp "%s" "%s"`, src, dest)
		return c.Run(ctx, l, l.Stdout, l.Stderr, WithNodes(c.Nodes[1:]), "copying to remaining nodes", cpCmd)
	}

	return nil
}
