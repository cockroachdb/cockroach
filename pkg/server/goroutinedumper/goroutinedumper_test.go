// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package goroutinedumper

import (
	"compress/gzip"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/server/dumpstore"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
)

type goroutinesVal struct {
	secs       time.Duration // the time at which this goroutines value was emitted
	threshold  int64
	goroutines int64
}

func TestHeuristic(t *testing.T) {
	const dumpDir = "dump_dir"
	st := &cluster.Settings{}

	cases := []struct {
		name          string
		heuristics    []heuristic
		vals          []goroutinesVal
		dumpsToFail   []string
		expectedDumps []string
	}{
		// N is the number of goroutines
		{
			name:       "Use only doubleSinceLastDumpHeuristic",
			heuristics: []heuristic{doubleSinceLastDumpHeuristic},
			vals: []goroutinesVal{
				{0, 100, 30},    // not trigger since N < numGoroutinesThreshold
				{10, 100, 40},   // not trigger since N < numGoroutinesThreshold
				{20, 100, 120},  // trigger since N >= numGoroutinesThreshold
				{50, 100, 35},   // not trigger since N has not doubled since last dump
				{70, 100, 150},  // not trigger since N has not doubled since last dump
				{80, 100, 250},  // trigger since N has doubled since last dump
				{100, 100, 135}, // not trigger since N has not doubled since last dump
				{180, 100, 30},  // not trigger since N has not doubled since last dump
				{190, 100, 80},  // not trigger since N has not doubled since last dump
				{220, 100, 500}, // trigger since N has doubled since last dump
			},
			expectedDumps: []string{
				"goroutine_dump.2019-01-01T00_00_20.000.double_since_last_dump.000000120",
				"goroutine_dump.2019-01-01T00_01_20.000.double_since_last_dump.000000250",
				"goroutine_dump.2019-01-01T00_03_40.000.double_since_last_dump.000000500",
			},
		},
		{
			name: "Fail some dumps when doubleSinceLastDumpHeuristic is used",
			heuristics: []heuristic{
				doubleSinceLastDumpHeuristic,
			},
			vals: []goroutinesVal{
				{0, 100, 20},    // not trigger since N < numGoroutinesThreshold
				{10, 100, 35},   // not trigger since N < numGoroutinesThreshold
				{20, 100, 110},  // trigger since N >= numGoroutinesThreshold
				{50, 100, 150},  // not trigger since N has not doubled since last dump
				{70, 100, 170},  // not trigger since N has not doubled since last dump
				{80, 100, 230},  // trigger but dump will fail
				{100, 100, 220}, // trigger since N has doubled since last dump
				{180, 100, 85},  // not trigger since N has not doubled since last dump
				{200, 100, 450}, // trigger since N has doubled since last dump
				{220, 100, 500}, // not trigger since N has not doubled since last dump
			},
			expectedDumps: []string{
				"goroutine_dump.2019-01-01T00_00_20.000.double_since_last_dump.000000110",
				"goroutine_dump.2019-01-01T00_01_40.000.double_since_last_dump.000000220",
				"goroutine_dump.2019-01-01T00_03_20.000.double_since_last_dump.000000450",
			},
			dumpsToFail: []string{
				"goroutine_dump.2019-01-01T00_01_20.000.double_since_last_dump.000000230",
			},
		},
		{
			name:       "Change in threshold resets the maxGoroutinesDumped",
			heuristics: []heuristic{doubleSinceLastDumpHeuristic},
			vals: []goroutinesVal{
				{0, 100, 30},    // not trigger since N < numGoroutinesThreshold
				{10, 100, 40},   // not trigger since N < numGoroutinesThreshold
				{20, 100, 120},  // trigger since N >= numGoroutinesThreshold
				{50, 100, 135},  // not trigger since N has not doubled since last dump
				{70, 100, 150},  // not trigger since N has not doubled since last dump
				{80, 200, 150},  // update numGoroutinesThreshold, which resets maxGoroutinesDumped
				{90, 200, 210},  // trigger since maxGoroutinesDumped was reset and N >= threshold
				{100, 200, 235}, // not trigger since N has not doubled since last dump
				{180, 200, 230}, // not trigger since N has not doubled since last dump
				{190, 200, 280}, // not trigger since N has not doubled since last dump
				{220, 200, 500}, // trigger since N has doubled since last dump
			},
			expectedDumps: []string{
				"goroutine_dump.2019-01-01T00_00_20.000.double_since_last_dump.000000120",
				"goroutine_dump.2019-01-01T00_01_30.000.double_since_last_dump.000000210",
				"goroutine_dump.2019-01-01T00_03_40.000.double_since_last_dump.000000500",
			},
		},
		{
			name:       "No heuristic is used",
			heuristics: []heuristic{},
			vals: []goroutinesVal{
				{0, 100, 10},
				{10, 100, 15},
				{20, 100, 50},
				{50, 100, 35},
				{70, 100, 80},
				{80, 100, 150},
				{100, 100, 120},
				{180, 100, 85},
				{200, 100, 130},
				{220, 100, 500},
			},
			expectedDumps: nil,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			baseTime := time.Date(2019, time.January, 1, 0, 0, 0, 0, time.UTC)
			var dumps []string
			var currentTime time.Time
			gd := GoroutineDumper{
				maxGoroutinesDumped: 0,
				heuristics:          c.heuristics,
				currentTime: func() time.Time {
					return currentTime
				},
				takeGoroutineDump: func(path string) error {
					for _, d := range c.dumpsToFail {
						if path == filepath.Join(dumpDir, d) {
							return errors.New("this dump is set to fail")
						}
					}
					dumps = append(dumps, filepath.Base(path))
					return nil
				},
				store: dumpstore.NewStore(dumpDir, nil, nil),
			}

			ctx := context.Background()
			for _, v := range c.vals {
				currentTime = baseTime.Add(v.secs * time.Second)
				numGoroutinesThreshold.Override(ctx, &st.SV, v.threshold)
				gd.MaybeDump(ctx, st, v.goroutines)
			}
			assert.Equal(t, c.expectedDumps, dumps)
		})
	}
}

func TestNewGoroutineDumper(t *testing.T) {
	t.Run("fails because no directory is specified", func(t *testing.T) {
		_, err := NewGoroutineDumper(context.Background(), "", nil)
		assert.EqualError(t, err, "directory to store dumps could not be determined")
	})

	t.Run("succeeds", func(t *testing.T) {
		tempDir, dirCleanupFn := testutils.TempDir(t)
		defer dirCleanupFn()
		gd, err := NewGoroutineDumper(context.Background(), tempDir, nil)
		assert.NoError(t, err, "unexpected error in NewGoroutineDumper")
		assert.Equal(t, int64(0), gd.goroutinesThreshold)
		assert.Equal(t, int64(0), gd.maxGoroutinesDumped)
	})
}

func TestTakeGoroutineDump(t *testing.T) {
	t.Run("fails because dump already exists as a directory", func(t *testing.T) {
		tempDir, dirCleanupFn := testutils.TempDir(t)
		defer dirCleanupFn()
		path := filepath.Join(tempDir, "goroutine_dump.txt.gz")
		err := os.Mkdir(path, 0755)
		assert.NoError(t, err, "failed to make dump directory %s", path)

		filename := filepath.Join(tempDir, "goroutine_dump")
		err = takeGoroutineDump(filename)
		assert.Error(t, err)
		assert.Contains(
			t,
			err.Error(),
			fmt.Sprintf("error creating file %s for goroutine dump", path),
		)
	})

	t.Run("succeeds writing a goroutine dump in gzip format", func(t *testing.T) {
		tempDir, dirCleanupFn := testutils.TempDir(t)
		defer dirCleanupFn()

		path := filepath.Join(tempDir, "goroutine_dump")
		err := takeGoroutineDump(path)
		assert.NoError(t, err, "unexpected error when dumping goroutines")

		expectedFile := filepath.Join(tempDir, "goroutine_dump.txt.gz")
		f, err := os.Open(expectedFile)
		if err != nil {
			t.Fatalf("could not open goroutine dump file %s: %s", expectedFile, err)
		}
		defer f.Close()
		// Test file is in gzip format.
		r, err := gzip.NewReader(f)
		if err != nil {
			t.Fatalf("could not create gzip reader for file %s: %s", expectedFile, err)
		}
		if _, err = ioutil.ReadAll(r); err != nil {
			t.Fatalf("could not read goroutine dump file %s with gzip: %s", expectedFile, err)
		}
		if err = r.Close(); err != nil {
			t.Fatalf("error closing gzip reader: %s", err)
		}
	})
}
