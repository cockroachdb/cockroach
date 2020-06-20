// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package heapprofiler

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMakeFileName(t *testing.T) {
	ts := time.Date(2020, 6, 15, 13, 19, 19, 543000000, time.UTC)

	joy := profileStore{dir: "mydir"}
	assert.Equal(t,
		filepath.Join("mydir", "memprof.2020-06-15T13_19_19.543.123456"),
		joy.makeNewFileName(ts, 123456))
}

func TestParseFileName(t *testing.T) {
	z := time.Time{}
	testData := []struct {
		f         string
		ts        time.Time
		heapUsage uint64
		expError  bool
	}{
		{"hello", z, 0, true},
		{"", z, 0, true},

		// Old (obsolete) formats.
		{"memprof.fraction_system_memory.000000019331059712_2020-03-04T16_58_39.54.pprof", z, 0, true},
		{"memprof.000000000030536024_2020-06-15T13_19_19.543", z, 0, true},

		// New format.
		{"memprof.2020-06-15T13_19_19.543.123456", time.Date(2020, 6, 15, 13, 19, 19, 543000000, time.UTC), 123456, false},
	}

	for _, tc := range testData {
		ts, heapUsage, err := parseFileName(tc.f)
		if (err != nil) != tc.expError {
			t.Fatalf("%s: expected error %v, got %v", tc.f, tc.expError, err)
		}

		assert.Equal(t, tc.ts, ts)
		assert.Equal(t, tc.heapUsage, heapUsage)
	}
}

func TestCleanupLastRampup(t *testing.T) {
	testData := []struct {
		startFiles []string
		maxP       int64
		cleaned    []string
		preserved  map[int]bool
	}{
		// When the directory is empty, nothing happens.
		{[]string{}, 0, []string{}, map[int]bool{}},
		{[]string{}, 5, []string{}, map[int]bool{}},
		// General case, just one file.
		{
			// Starting files.
			[]string{
				"memprof.2020-06-15T13_19_19.543.123456",
			},
			// max files to keep in upramp.
			1,
			// Nothing gets cleaned.
			[]string{},
			// The one file gets preserved.
			map[int]bool{0: true},
		},
		// General case.
		{
			// Two upramps.
			[]string{
				// First upramp.
				"memprof.2020-06-15T13_19_19.000.1",
				"memprof.2020-06-15T13_19_19.001.2",
				"memprof.2020-06-15T13_19_19.002.3",
				"memprof.2020-06-15T13_19_19.003.4",
				"memprof.2020-06-15T13_19_19.004.5",
				// Second upramp.
				"memprof.2020-06-15T13_19_19.005.1",
				"memprof.2020-06-15T13_19_19.006.10",
				"memprof.2020-06-15T13_19_19.007.20",
				"memprof.2020-06-15T13_19_19.008.100",
			},
			// max files to keep in one upramp.
			2,
			// We expect files to only get cleaned in the last upramp.
			// The deletion goes in decreasing order (the algorithm
			// starts from the end.)
			[]string{
				"memprof.2020-06-15T13_19_19.006.10",
				"memprof.2020-06-15T13_19_19.005.1",
			},
			// The last two files in the 2nd upramp are preserved.
			map[int]bool{7: true, 8: true},
		},
		// Odd case: num files to keep = 0 per upramp.  Everything gets
		// deleted _inside the last upramp_ but previous upramps are
		// unaffected. This is intended to preserve upramps that occur
		// when a process crashes.
		{
			[]string{
				// First upramp.
				"memprof.2020-06-15T13_19_19.000.1",
				"memprof.2020-06-15T13_19_19.001.2",
				"memprof.2020-06-15T13_19_19.002.3",
				"memprof.2020-06-15T13_19_19.003.4",
				"memprof.2020-06-15T13_19_19.004.5",
				// Second upramp.
				"memprof.2020-06-15T13_19_19.005.1",
				"memprof.2020-06-15T13_19_19.006.10",
				"memprof.2020-06-15T13_19_19.007.20",
				"memprof.2020-06-15T13_19_19.008.100",
			},
			// max files to keep in one upramp.
			0,
			// The deletion goes in decreasing order (the algorithm
			// starts from the end.)
			[]string{
				"memprof.2020-06-15T13_19_19.008.100",
				"memprof.2020-06-15T13_19_19.007.20",
				"memprof.2020-06-15T13_19_19.006.10",
				"memprof.2020-06-15T13_19_19.005.1",
			},
			// No file preserved.
			map[int]bool{},
		},
		// Odd case: bogus filenames "in the middle".
		{
			// Two upramps.
			[]string{
				// Unexpected junk files at beginning.
				"abc_unexpected0",
				"def_unexpected1",
				// First upramp.
				"memprof.2020-06-15T13_19_19.002.1",
				"memprof.2020-06-15T13_19_19.003.2",
				"memprof.2020-06-15T13_19_19.004.invalid",
				"memprof.2020-06-15T13_19_19.005.4",
				// Some middle junk.
				"memprof.2020-06-15T13_19_19.006.unexpected",
				// Second upramp.
				"memprof.2020-06-15T13_19_19.007.1",
				"memprof.2020-06-15T13_19_19.008.10",
				"memprof.2020-06-15T13_19_19.009.invalid",
				"memprof.2020-06-15T13_19_19.010.100",
				// Unexpected junk files at end.
				"yyy_unexpected11",
				"zzz_unexpected12",
			},
			// max files to keep in one upramp.
			2,
			// We expect files to only get cleaned in the last upramp.
			// The deletion goes in decreasing order (the algorithm
			// starts from the end.)
			[]string{
				"memprof.2020-06-15T13_19_19.007.1",
			},
			// The last two _valid_ files in the 2nd upramp are preserved.
			map[int]bool{8: true, 10: true},
		},
	}

	for i, tc := range testData {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			path, err := ioutil.TempDir("", "cleanup")
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = os.RemoveAll(path) }()

			// NB: cleanupLastRampup does not use mtimes.
			files := populate(t, path, tc.startFiles, nil /* sizes */)

			cleaned := []string{}
			cleanupFn := func(s string) error {
				cleaned = append(cleaned, s)
				return nil
			}

			preserved := cleanupLastRampup(context.Background(), files, tc.maxP, cleanupFn)
			assert.EqualValues(t, tc.cleaned, cleaned)
			assert.EqualValues(t, tc.preserved, preserved)
		})
	}
}

func TestRemoveOldAndTooBig(t *testing.T) {
	now := time.Date(2020, 6, 15, 13, 19, 19, 543000000, time.UTC)
	testData := []struct {
		startFiles []string
		sizes      []int64
		maxS       int64
		preserved  map[int]bool
		cleaned    []string
	}{
		// Simple case: no files.
		{[]string{}, []int64{}, 10, nil, []string{}},

		// Some files spanning a few days.
		{
			[]string{
				"memprof.2020-06-11T13_19_19.000.1",
				"memprof.2020-06-12T13_19_19.001.2",
				"memprof.2020-06-13T13_19_19.002.3",
				"memprof.2020-06-14T13_19_19.003.4",
				"memprof.2020-06-15T13_19_19.004.5",
			},
			// The actual sizes for the 5 names above.
			[]int64{
				10, // June 11
				10, // June 12
				10, // June 13
				10, // June 14
				10, // June 15
			},
			// The max size to keep.
			35,
			// The preserved files (no file preserved).
			nil,
			// Expected files to clean up.
			[]string{
				"memprof.2020-06-12T13_19_19.001.2",
				"memprof.2020-06-11T13_19_19.000.1",
			},
		},

		// Some files spanning a few days with some unknown files
		// interleaved.
		{
			[]string{
				"memprof.2020-06-11T13_19_19.000.1",
				"memprof.2020-06-12T13_19_19.001.2",
				"unknown",
			},
			// The actual sizes for the 5 names above.
			[]int64{
				10, // June 11
				10, // June 12
				10, // unknown
			},
			// The max size to keep.
			25,
			// The preserved files (no file preserved).
			nil,
			// Expected files to clean up: none.
			[]string{},
		},

		// Ditto, with some files preserved.
		{
			[]string{
				"memprof.2020-06-11T13_19_19.000.1",
				"memprof.2020-06-12T13_19_19.001.2",
				"memprof.2020-06-13T13_19_19.002.3",
				"memprof.2020-06-14T13_19_19.003.4",
				"memprof.2020-06-15T13_19_19.004.5",
			},
			// The actual time.Times for the 5 names above.
			[]int64{
				10, // June 11
				10, // June 12
				10, // June 13
				10, // June 14
				10, // June 15
			},
			// The max size to keep.
			25,
			// The preserved files. This takes priority over size-based
			// deletion.
			map[int]bool{
				0: true, // June 11
				2: true, // June 13
				3: true, // June 14
			},
			// Expected files to clean up. The other files
			// are preserved.
			[]string{
				"memprof.2020-06-12T13_19_19.001.2",
			},
		},
	}

	for i, tc := range testData {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			path, err := ioutil.TempDir("", "remove")
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = os.RemoveAll(path) }()

			files := populate(t, path, tc.startFiles, tc.sizes)

			cleaned := []string{}
			cleanupFn := func(s string) error {
				cleaned = append(cleaned, s)
				return nil
			}

			removeOldAndTooBigExcept(context.Background(), files, now, tc.maxS, tc.preserved, cleanupFn)
			assert.EqualValues(t, tc.cleaned, cleaned)
		})
	}
}

func populate(t *testing.T, dirName string, fileNames []string, sizes []int64) []os.FileInfo {
	if len(sizes) > 0 {
		require.Equal(t, len(fileNames), len(sizes))
	}

	for i, fn := range fileNames {
		f, err := os.Create(filepath.Join(dirName, fn))
		if err != nil {
			t.Fatal(err)
		}

		if len(sizes) > 0 {
			// Populate a size if requested.
			fmt.Fprintf(f, "%*s", sizes[i], " ")
		}

		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}

	// Retrieve the file list for the remainder of the test.
	files, err := ioutil.ReadDir(dirName)
	if err != nil {
		t.Fatal(err)
	}
	return files
}
