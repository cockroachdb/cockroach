// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// +build !windows
// +build !plan9

package sysutil

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetFileACLInfo(t *testing.T) {
	certsDir, err := ioutil.TempDir("", "acl_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.RemoveAll(certsDir); err != nil {
			t.Fatal(err)
		}
	}()

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

		if err := ioutil.WriteFile(filename, exampleData, f.mode); err != nil {
			t.Fatalf("#%d: could not write file %s: %v", testNum, f.filename, err)
		}
		info, err := os.Stat(filename)
		if nil != err {
			t.Errorf("#%d: failed to stat new test file %s: %v", testNum, f.filename, err)
		}
		aclInfo := GetFileACLInfo(info)
		assert.True(t, aclInfo.IsOwnedByUID(uint64(os.Getuid())))
		assert.True(t, aclInfo.IsOwnedByGID(uint64(os.Getgid())))
		assert.False(t, ExceedsPermissions(aclInfo.Mode(), f.mode))
	}
}
