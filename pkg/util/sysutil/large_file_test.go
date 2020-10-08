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
	"testing"

	"github.com/cockroachdb/errors"
)

func TestLargeFile(t *testing.T) {
	f, err := ioutil.TempFile("", "input")
	if err != nil {
		t.Fatal(err)
	}
	fname := f.Name()
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	const n int64 = 1013
	if err := CreateLargeFile(fname, n); err != nil {
		t.Fatal(err)
	}
	s, err := os.Stat(fname)
	if err != nil {
		t.Fatal(err)
	}
	if s.Size() != n {
		t.Fatal(errors.Errorf("expected size of file %d, got %d", n, s.Size()))
	}
}
