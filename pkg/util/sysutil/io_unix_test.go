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
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestGetUmask(t *testing.T) {
	originalUmask := unix.Umask(0022)
	defer func() {
		_ = unix.Umask(originalUmask)
	}()

	umask := os.FileMode(unix.Umask(0022))
	require.Equal(t, umask, GetUmask())
	require.Equal(t, "-----w--w-", GetUmask().String())
	require.Equal(t, 0022, int(GetUmask()))
}

func TestChangingOtherBits(t *testing.T) {
	in := os.FileMode(0777)
	expected := os.FileMode(0770)
	removed := removeOtherBits(in)
	assert.Equal(t, expected, removed, "expected %s, got %s", expected, removed)
	added := addOtherBits(os.FileMode(removed))
	assert.Equal(t, in, added, "expected %s, got %s", in, added)
}

func TestModeAfterUmask(t *testing.T) {
	originalUmask := unix.Umask(0022)
	defer func() {
		_ = unix.Umask(originalUmask)
	}()

	for _, test := range []struct {
		in       os.FileMode
		expected os.FileMode
	}{
		{
			in:       0666,
			expected: 0640,
		},
		{
			in:       0600,
			expected: 0600,
		},
		{
			in:       0622,
			expected: 0600,
		},
	} {

		actual := modeAfterUmask(test.in)
		assert.Equal(t, test.expected, actual, "expected %s, got %s", test.expected, actual)
	}
}

func TestOpenFile(t *testing.T) {
	originalUmask := unix.Umask(0000)

	testIODir, err := ioutil.TempDir("", "iowrapper_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = unix.Umask(originalUmask)
		if err := os.RemoveAll(testIODir); err != nil {
			t.Fatal(err)
		}
	}()

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
		filename := filepath.Join(testIODir, f.filename)

		file, err := OpenFile(filename, os.O_CREATE, f.mode)
		if err != nil {
			t.Fatalf("#%d: could not create file %s: %v", testNum, f.filename, err)
		}
		_ = file.Close()

		info, err := os.Stat(filename)
		if nil != err {
			t.Errorf("#%d: failed to stat new test file %s: %v", testNum, f.filename, err)
		}
		assert.Equal(t, f.mode.String(), info.Mode().String())
	}
}

func TestCreate(t *testing.T) {
	originalUmask := unix.Umask(0000)

	testIODir, err := ioutil.TempDir("", "iowrapper_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = unix.Umask(originalUmask)
		if err := os.RemoveAll(testIODir); err != nil {
			t.Fatal(err)
		}
	}()

	type testFile struct {
		filename string
		mode     os.FileMode
	}

	for testNum, f := range []testFile{
		{
			filename: "test.txt",
			mode:     0660,
		},
	} {
		filename := filepath.Join(testIODir, f.filename)

		file, err := Create(filename)
		if err != nil {
			t.Fatalf("#%d: could not create file %s: %v", testNum, f.filename, err)
		}
		_ = file.Close()

		info, err := os.Stat(filename)
		if nil != err {
			t.Errorf("#%d: failed to stat new test file %s: %v", testNum, f.filename, err)
		}
		assert.Equal(t, f.mode.String(), info.Mode().String())
	}
}
