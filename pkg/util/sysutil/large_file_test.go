// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sysutil

import (
	"bytes"
	"compress/flate"
	"os"
	"path/filepath"
	"testing"
)

func TestResizeLargeFile(t *testing.T) {
	fname := filepath.Join(t.TempDir(), "ballast")

	lens := []int64{2000, 1000, 64<<20 + 10, 0, 1}
	for _, n := range lens {
		if err := ResizeLargeFile(fname, n); err != nil {
			t.Fatal(err)
		}
		fi, err := os.Stat(fname)
		if err != nil {
			t.Fatal(err)
		}
		if n != fi.Size() {
			t.Fatalf("expected size of file %d, got %d", n, fi.Size())
		}
	}
}

// TestResizeLargeFileNaiveIncompressible verifies that the naive fallback
// (used on non-Linux platforms, and on Linux when fallocate is unsupported)
// fills files with data that resists compression. Filesystems like ZFS
// compress long runs of zeroes down to almost nothing, which would defeat
// the purpose of a ballast file: reserving real, reclaimable disk space
// (see #78606).
func TestResizeLargeFileNaiveIncompressible(t *testing.T) {
	fname := filepath.Join(t.TempDir(), "ballast")
	const size = 4 << 20 // 4MiB, enough for a meaningful compression ratio
	if err := resizeLargeFileNaive(fname, size); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(fname)
	if err != nil {
		t.Fatal(err)
	}
	if int64(len(data)) != size {
		t.Fatalf("expected file of size %d, got %d", size, len(data))
	}

	var compressed bytes.Buffer
	w, err := flate.NewWriter(&compressed, flate.BestCompression)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// A standard compressor should not be able to shrink pseudo-random data
	// by more than a small margin; all-zero data compresses by orders of
	// magnitude more than this.
	if ratio := float64(compressed.Len()) / float64(len(data)); ratio < 0.9 {
		t.Fatalf("ballast data compressed too well: %d bytes -> %d bytes (ratio %.2f)",
			len(data), compressed.Len(), ratio)
	}
}
