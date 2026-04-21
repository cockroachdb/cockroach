// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package modelcache downloads and caches ONNX embedding model files.
// On first use it retrieves the default all-MiniLM-L6-v2 model from
// Hugging Face, verifies SHA256 checksums, and stores the files locally.
// Subsequent calls return the cached paths without network access.
package modelcache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// modelFile describes a single file in the model manifest.
type modelFile struct {
	// Name is the filename within the cache directory.
	Name string
	// URL is the download location.
	URL string
	// SHA256 is the hex-encoded expected checksum.
	SHA256 string
}

// defaultModel is the all-MiniLM-L6-v2 manifest from Hugging Face.
// 384 dimensions, 30522 vocab, 256 max sequence length.
var defaultModel = []modelFile{
	{
		Name:   "model.onnx",
		URL:    "https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2/resolve/main/onnx/model.onnx",
		SHA256: "6fd5d72fe4589f189f8ebc006442dbb529bb7ce38f8082112682524616046452",
	},
	{
		Name:   "vocab.txt",
		URL:    "https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2/resolve/main/vocab.txt",
		SHA256: "07eced375cec144d27c900241f3e339478dec958f92fddbc551f295c992038a3",
	},
}

// downloadTimeout is the maximum time allowed for downloading a single file.
const downloadTimeout = 5 * time.Minute

// progressInterval is the number of bytes between progress log messages.
const progressInterval = 10 * 1024 * 1024 // 10 MB

// Result holds the resolved paths after EnsureModel completes.
type Result struct {
	ModelPath string
	VocabPath string
}

// EnsureModel ensures that the default model files are present in
// cacheDir, downloading them from Hugging Face if necessary. Returns
// the paths to the model and vocabulary files.
func EnsureModel(ctx context.Context, cacheDir string) (Result, error) {
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		return Result{}, errors.Wrap(err, "creating embedding cache directory")
	}

	paths := make(map[string]string, len(defaultModel))
	for _, mf := range defaultModel {
		p, err := ensureFile(ctx, cacheDir, mf)
		if err != nil {
			return Result{}, err
		}
		paths[mf.Name] = p
	}

	return Result{
		ModelPath: paths["model.onnx"],
		VocabPath: paths["vocab.txt"],
	}, nil
}

// ensureFile checks if a file exists in cacheDir. If not, it downloads
// the file, verifies its SHA256 checksum, and atomically moves it into
// place.
func ensureFile(ctx context.Context, cacheDir string, mf modelFile) (string, error) {
	destPath := filepath.Join(cacheDir, mf.Name)

	// Cache hit: file already exists with non-zero size.
	if info, err := os.Stat(destPath); err == nil && info.Size() > 0 {
		return destPath, nil
	}

	log.Ops.Infof(ctx, "downloading embedding model file %s from %s",
		mf.Name, log.SafeManaged(mf.URL))

	if err := downloadFile(ctx, destPath, mf.URL, mf.SHA256, mf.Name); err != nil {
		return "", errors.Wrapf(err, "downloading %s", mf.Name)
	}

	return destPath, nil
}

// downloadFile downloads url to a temporary file, verifies the SHA256
// checksum, and atomically renames it to destPath.
func downloadFile(ctx context.Context, destPath, url, expectedSHA256, displayName string) error {
	// Use a dedicated context with a timeout for the download.
	dlCtx, cancel := context.WithTimeout(ctx, downloadTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(dlCtx, "GET", url, nil)
	if err != nil {
		return errors.Wrap(err, "creating HTTP request")
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return errors.Wrap(err, "HTTP GET")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return errors.Newf("HTTP %d from %s", resp.StatusCode, url)
	}

	// Write to a temp file so that incomplete downloads don't corrupt
	// the cache.
	tmpPath := destPath + ".download"
	f, err := os.Create(tmpPath)
	if err != nil {
		return errors.Wrap(err, "creating temp file")
	}
	defer func() {
		f.Close()
		// Clean up temp file on error.
		if _, statErr := os.Stat(tmpPath); statErr == nil {
			_ = os.Remove(tmpPath)
		}
	}()

	// Hash the content as we write it.
	hash := sha256.New()
	pw := &progressWriter{
		ctx:         ctx,
		displayName: displayName,
		logEvery:    int64(progressInterval),
		totalSize:   resp.ContentLength,
	}
	writer := io.MultiWriter(f, hash, pw)

	if _, err := io.Copy(writer, resp.Body); err != nil {
		return errors.Wrap(err, "writing file")
	}

	if err := f.Close(); err != nil {
		return errors.Wrap(err, "closing temp file")
	}

	// Verify checksum.
	actualSHA256 := hex.EncodeToString(hash.Sum(nil))
	if actualSHA256 != expectedSHA256 {
		return errors.Newf(
			"SHA256 mismatch for %s: expected %s, got %s",
			displayName, expectedSHA256, actualSHA256,
		)
	}

	// Atomic rename.
	if err := os.Rename(tmpPath, destPath); err != nil {
		return errors.Wrap(err, "renaming temp file to final path")
	}

	log.Ops.Infof(ctx, "downloaded %s (%s)",
		displayName, formatBytes(pw.written.Load()))

	return nil
}

// progressWriter logs download progress at regular intervals.
type progressWriter struct {
	ctx         context.Context
	displayName string
	written     atomic.Int64
	logEvery    int64
	totalSize   int64 // from Content-Length; -1 if unknown
	nextLogAt   int64
}

func (pw *progressWriter) Write(p []byte) (int, error) {
	n := len(p)
	total := pw.written.Add(int64(n))
	if total >= pw.nextLogAt {
		if pw.totalSize > 0 {
			pct := float64(total) / float64(pw.totalSize) * 100
			log.Ops.Infof(pw.ctx, "downloading %s: %s / %s (%.0f%%)",
				pw.displayName, formatBytes(total), formatBytes(pw.totalSize), pct)
		} else {
			log.Ops.Infof(pw.ctx, "downloading %s: %s",
				pw.displayName, formatBytes(total))
		}
		pw.nextLogAt = total + pw.logEvery
	}
	return n, nil
}

// formatBytes returns a human-readable byte count.
func formatBytes(b int64) string {
	const mb = 1024 * 1024
	if b >= mb {
		return fmt.Sprintf("%.1f MB", float64(b)/float64(mb))
	}
	const kb = 1024
	if b >= kb {
		return fmt.Sprintf("%.1f KB", float64(b)/float64(kb))
	}
	return fmt.Sprintf("%d B", b)
}
