// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sysutil

import (
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
