// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"bufio"
	"compress/gzip"
	_ "embed"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/urlcheck/lib/urlcheck"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

const (
	// URL Cache is per-release branch and defaults to 7 days TTL.
	CacheTTL        = 7 * 24 * time.Hour
	CacheBlobPrefix = "gs://cockroach-nightly/urlchecker"
	CacheFile       = "cache.txt.gz"
)

func main() {
	cmd := exec.Command("git", "grep", "-nE",
		urlcheck.URLRE,
		"--",
		":!*.bzl",
		":!*.config",
		":!*.sh",
		":!cloud/kubernetes/*",
		":!scripts/*",
		":!licenses/*",
		":!*_test.go",
		":!*/testdata/*",
	)

	// Let's derive the blob path for the optional cache abd try to load it.
	var blobPath string

	buildBranch := os.Getenv("TC_BUILD_BRANCH")
	if buildBranch == "master" {
		blobPath = fmt.Sprintf("%s/%s", CacheBlobPrefix, CacheFile)
	} else {
		blobPath = fmt.Sprintf("%s/%s/%s", CacheBlobPrefix, buildBranch, CacheFile)
	}
	// Try to load the cache.
	log.Printf("Attemptign to fetch cache from: %s", blobPath)
	cached, fresh, err := LoadURLCache(blobPath)
	if err != nil {
		log.Printf("WARN: URL cache load error: %+v", err)
	} else if !fresh {
		log.Printf("WARN: URL cache stale")
	}
	if fresh {
		log.Printf("Loaded %d URLs from cache", len(cached))
	}
	uniqueURLs, err := urlcheck.CheckURLsFromGrepOutput(cmd, cached)
	// Persist all URLs if cache was stale or missing.
	if !fresh {
		if err := SaveURLCache(blobPath, uniqueURLs); err != nil {
			log.Printf("WARN: URL cache save error: %+v", err)
		} else {
			log.Printf("Saved %d URLs to cache: %q", len(uniqueURLs), blobPath)
		}
	}
	if err != nil {
		log.Fatalf("%+v\nFAIL", err)
	}
	fmt.Println("PASS")
}

// LoadURLCache downloads a gzipped flat-file from GCS (remoteGCSPath), checks TTL using
// the gzip header ModTime, and if fresh returns (map, true, nil).
// If the object is missing, unreadable, or stale, it returns (nil, false, nil).
func LoadURLCache(remoteGCSPath string) (map[string]struct{}, bool, error) {
	localPath, cleanup, err := tempPathCWD(remoteGCSPath, ".download")
	if err != nil {
		return nil, false, err
	}
	defer cleanup()

	// Try to download. If it fails, treat as cache miss (not an error).
	if err := runGsutilCp(remoteGCSPath, localPath); err != nil {
		return nil, false, err
	}

	f, err := os.Open(localPath)
	if err != nil {
		return nil, false, err
	}
	defer f.Close()

	gr, err := gzip.NewReader(f)
	if err != nil {
		// Corrupt gzip? Treat as miss.
		return nil, false, err
	}
	defer gr.Close()

	mod := gr.Header.ModTime
	if mod.IsZero() {
		// Missing mtime. Treat as miss.
		return nil, false, errors.New("missing gzip ModTime")
	}

	// Check if cache TTL expired.
	if timeutil.Since(mod) > CacheTTL {
		return nil, false, nil
	}

	// Cache is still fresh, let's parse it.
	sc := bufio.NewScanner(gr)
	buf := make([]byte, 0, 64*1024)
	sc.Buffer(buf, len(buf))

	urls := make(map[string]struct{}, 1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		urls[line] = struct{}{}
	}
	if err := sc.Err(); err != nil && !errors.Is(err, io.EOF) {
		return nil, false, err
	}
	return urls, true, nil
}

// SaveURLCache writes the given set of URLs to a local gz file with gzip ModTime=now,
// then uploads it to GCS (remoteGCSPath). It overwrites any existing object.
func SaveURLCache(remoteGCSPath string, urls []string) error {
	localPath, cleanup, err := tempPathCWD(remoteGCSPath, ".upload")
	if err != nil {
		return err
	}
	// Clean up local temp whether upload succeeds or not.
	defer cleanup()

	lf, err := os.Create(localPath)
	if err != nil {
		return fmt.Errorf("create temp cache file: %w", err)
	}
	defer lf.Close()

	gw, err := gzip.NewWriterLevel(lf, gzip.BestSpeed)
	if err != nil {
		return fmt.Errorf("gzip writer: %w", err)
	}
	// Record modtime inside gzip header to drive TTL checks later.
	gw.ModTime = timeutil.Now()

	w := bufio.NewWriter(gw)
	for _, u := range urls {
		if _, err := w.WriteString(u + "\n"); err != nil {
			_ = gw.Close()
			return fmt.Errorf("write cache contents: %w", err)
		}
	}
	if err := w.Flush(); err != nil {
		_ = gw.Close()
		return fmt.Errorf("flush writer: %w", err)
	}
	if err := gw.Close(); err != nil {
		return fmt.Errorf("close gzip writer: %w", err)
	}
	if err := lf.Close(); err != nil {
		return fmt.Errorf("close temp file: %w", err)
	}

	// Upload
	if err := runGsutilCp(localPath, remoteGCSPath); err != nil {
		return fmt.Errorf("gsutil cp upload: %w", err)
	}
	return nil
}

func runGsutilCp(src, dst string) error {
	cmd := exec.Command("gsutil", "cp", src, dst)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return errors.NewAssertionErrorWithWrappedErrf(err, "gsutil cp %s %s: %s", src, dst, out)
	}
	return nil
}

// tempPathCWD creates a temp file path in CWD derived from the remote name.
// It returns (path, cleanup func, error).
func tempPathCWD(remote, suffix string) (string, func(), error) {
	base := filepath.Base(remote)
	if base == "." || base == "/" || base == "" {
		base = CacheFile
	}
	// Pattern must contain a * for uniqueness; remove slashes if any.
	pattern := strings.ReplaceAll(base, string(filepath.Separator), "_") + ".*" + suffix
	f, err := os.CreateTemp(".", pattern)
	if err != nil {
		return "", func() {}, fmt.Errorf("create temp file in cwd: %w", err)
	}
	path := f.Name()
	_ = f.Close()
	cleanup := func() { _ = os.Remove(path) }
	return path, cleanup, nil
}
