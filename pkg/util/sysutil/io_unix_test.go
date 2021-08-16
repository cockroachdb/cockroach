// Copyright 2021 The Cockroach Authors.
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
	"golang.org/x/sys/unix"
)

func TestModeAfterMask(t *testing.T) {
	for _, test := range []struct {
		in       os.FileMode
		expected os.FileMode
		mask     os.FileMode
	}{
		{
			in:       0666,
			expected: 0640,
			mask:     0027,
		},
		{
			in:       0600,
			expected: 0600,
			mask:     0022,
		},
		{
			in:       0622,
			expected: 0600,
			mask:     0022,
		},
		{
			in:       0000,
			expected: 0000,
			mask:     0022,
		},
	} {

		actual := ModeAfterMask(test.in, test.mask)
		assert.Equal(t, test.expected, actual, "when masking %s with %s, got %s instead of expected %ss", test.in, test.mask, actual, test.expected)
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
		filename     string
		inputMode    os.FileMode
		expectedMode os.FileMode
	}

	for testNum, f := range []testFile{
		{
			filename:     "test.txt",
			inputMode:    0777,
			expectedMode: 0770,
		},
	} {
		filename := filepath.Join(testIODir, f.filename)

		file, err := OpenFile(filename, os.O_CREATE, f.inputMode)
		if err != nil {
			t.Fatalf("#%d: could not create file %s: %v", testNum, f.filename, err)
		}
		_ = file.Close()

		info, err := os.Stat(filename)
		if nil != err {
			t.Errorf("#%d: failed to stat new test file %s: %v", testNum, f.filename, err)
		}
		assert.Equal(t, f.expectedMode.String(), info.Mode().String())
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
	}

	for testNum, f := range []testFile{
		{
			filename: "test.txt",
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
		assert.Equal(t, DefaultCreateMode.String(), info.Mode().String())
	}
}

func TestWriteFile(t *testing.T) {
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
		filename     string
		inputMode    os.FileMode
		expectedMode os.FileMode
	}

	for testNum, f := range []testFile{
		{
			filename:     "test.txt",
			inputMode:    0666,
			expectedMode: 0660,
		},
	} {
		filename := filepath.Join(testIODir, f.filename)

		err := WriteFile(filename, []byte(filename), f.inputMode)
		if err != nil {
			t.Fatalf("#%d: could not WriteFile %s: %v", testNum, f.filename, err)
		}

		info, err := os.Stat(filename)
		if nil != err {
			t.Errorf("#%d: failed to stat new test file %s: %v", testNum, f.filename, err)
		}
		assert.Equal(t, f.expectedMode.String(), info.Mode().String())

		b, err := ioutil.ReadFile(filename)
		if err != nil {
			t.Fatalf("#%d: could not read file created by WriteFile %s: %v", testNum, f.filename, err)
		}
		assert.Equal(t, []byte(filename), b)
	}
}
