// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sysutil

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

func TestResizeLargeFile(t *testing.T) {
	d, err := ioutil.TempDir("", t.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.RemoveAll(d); err != nil {
			t.Fatal(err)
		}
	}()
	fname := filepath.Join(d, "ballast")

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
