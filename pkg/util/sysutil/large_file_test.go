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
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
)

func TestLargeFile(t *testing.T) {
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

	// Check that an existing file cannot be overwritten.
	if err = CreateLargeFile(fname, n); !(oserror.IsExist(err) || strings.Contains(err.Error(), "exists")) {
		t.Fatalf("expected 'already exists' error, got (%T) %+v", err, err)
	}
}
