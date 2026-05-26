// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build !windows && !plan9

package sysutil

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetFileACLInfo(t *testing.T) {
	certsDir := t.TempDir()

	exampleData := []byte("example")

	type testFile struct {
		filename string
		mode     os.FileMode
	}

	for testNum, f := range []testFile{
		{
			filename: "test.txt",
			mode:     0777,
		},
	} {
		filename := filepath.Join(certsDir, f.filename)

		if err := os.WriteFile(filename, exampleData, f.mode); err != nil {
			t.Fatalf("#%d: could not write file %s: %v", testNum, f.filename, err)
		}

		// Explicitly set file ownership to account for
		// different file GID behaviour amongst different
		// unixes and filesystems.
		expectedUID := os.Getuid()
		expectedGID := os.Getgid()
		if err := os.Chown(filename, expectedUID, expectedGID); err != nil {
			t.Fatalf("#%d: could not chown file: %s: %v", testNum, f.filename, err)
		}
		info, err := os.Stat(filename)
		if nil != err {
			t.Errorf("#%d: failed to stat new test file %s: %v", testNum, f.filename, err)
		}
		aclInfo := GetFileACLInfo(info)
		assert.True(t, aclInfo.IsOwnedByUID(uint64(expectedUID)))
		assert.True(t, aclInfo.IsOwnedByGID(uint64(expectedGID)))
		assert.False(t, ExceedsPermissions(aclInfo.Mode(), f.mode))
	}
}
