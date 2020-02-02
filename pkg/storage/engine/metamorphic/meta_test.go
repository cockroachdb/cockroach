// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package metamorphic

import (
	"context"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)


var (
	keep  = flag.Bool("keep", false, "keep temp directories after test")
	check = flag.String("check", "", "run operations in specified file and check output for equality")
	seed  = flag.Int64("seed", 456, "specify seed to use for random number generator")
)

func runMetaTestForEngines(ctx context.Context, t *testing.T, seed int64, checkFile io.Reader, outputFile io.Writer, engineImpls []engineImpl) {
	tempDir, cleanup := testutils.TempDir(t)
	defer func() {
		if !*keep {
			cleanup()
		}
	}()

	testRunner := metaTestRunner{
		ctx:         ctx,
		t:           t,
		w:           outputFile,
		seed:        seed,
		engineImpls: engineImpls,
		path:        filepath.Join(tempDir, "store"),
	}

	testRunner.init()
	defer testRunner.closeAll()
	if checkFile != nil {
		testRunner.parseFileAndRun(checkFile)
	} else {
		// TODO(itsbilal): Make this configurable.
		testRunner.generateAndRun(10000)
	}
}

func runMetaTest(ctx context.Context, t *testing.T, seed int64, checkFile string, engineSequences [][]engineImpl) {
	outerTempDir, cleanup := testutils.TempDir(t)
	defer func() {
		if !*keep {
			cleanup()
		}
	}()

	// The test run with the first engine sequence writes its output to this file.
	// All subsequent engine sequence runs compare their output against this file.
	firstRunOutput := filepath.Join(outerTempDir, "output.meta")
	firstRunExecuted := false
	fmt.Printf("first run output file: %s\n", firstRunOutput)

	for _, engineSequence := range engineSequences {
		var engineNames []string
		for _, engineImpl := range engineSequence {
			engineNames = append(engineNames, engineImpl.name)
		}

		t.Run(strings.Join(engineNames, ","), func(t *testing.T) {
			innerTempDir, cleanup := testutils.TempDir(t)
			defer func() {
				if !*keep {
					cleanup()
				}
			}()

			// If this is not the first sequence run and a "check" file was not passed
			// in, use the first run's output file as the check file.
			var checkFileReader io.ReadCloser
			if checkFile == "" && firstRunExecuted {
				checkFile = firstRunOutput
			}
			if checkFile != "" {
				var err error
				checkFileReader, err = os.Open(checkFile)
				defer checkFileReader.Close()
				if err != nil {
					t.Fatal(err)
				}
			}

			var outputFileWriter io.WriteCloser
			outputFile := firstRunOutput
			if firstRunExecuted {
				outputFile = filepath.Join(innerTempDir, "output.meta")
			}
			var err error
			outputFileWriter, err = os.Create(outputFile)
			defer outputFileWriter.Close()
			if err != nil {
				t.Fatal(err)
			}
			fmt.Printf("check file = %s\noutput file = %s\n", checkFile, outputFile)
			runMetaTestForEngines(ctx, t, seed, checkFileReader, outputFileWriter, engineSequence)
			firstRunExecuted = true
		})
	}
}

// TestRocksPebbleEquivalence runs the MVCC Metamorphic test suite, and checks
// for matching outputs by the test suite between RocksDB and Pebble.
func TestRocksPebbleEquivalence(t *testing.T) {
	defer leaktest.AfterTest(t)
	ctx := context.Background()
	if util.RaceEnabled {
		// This test times out with the race detector enabled.
		return
	}

	// Have one fixed seed, one user-specified seed, and one random seed.
	seeds := []int64{123, *seed, rand.Int63()}

	for _, seed := range seeds {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			runMetaTest(ctx, t, seed, "", [][]engineImpl{
				{engineImplRocksDB},
				{engineImplPebble},
			})
		})
	}
}

// TestRocksPebbleRestarts runs the MVCC Metamorphic test suite with restarts
// enabled, and ensures that the output remains the same across different
// engine sequences with restarts in between.
func TestRocksPebbleRestarts(t *testing.T) {
	defer leaktest.AfterTest(t)
	ctx := context.Background()
	// TODO(itsbilal): Allow this to be specified by a command line flag.
	seeds := []int64{123,456,789}

	for _, seed := range seeds {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			// 2 engine sequences with 1 restart each.
			runMetaTest(ctx, t, seed, "", [][]engineImpl{
				{engineImplRocksDB, engineImplRocksDB},
				{engineImplPebble, engineImplPebble},
				{engineImplRocksDB, engineImplPebble},
				{engineImplPebble, engineImplRocksDB},
			})

			// 3 engine sequences with 2 restarts each.
			runMetaTest(ctx, t, seed, "", [][]engineImpl{
				{engineImplRocksDB, engineImplRocksDB, engineImplRocksDB},
				{engineImplRocksDB, engineImplPebble, engineImplRocksDB},
				{engineImplPebble, engineImplRocksDB, engineImplPebble},
			})
		})
	}
}

// TestRocksPebbleCheck checks whether the output file specified with --check has
// matching behaviour across rocks/pebble.
func TestRocksPebbleCheck(t *testing.T) {
	defer leaktest.AfterTest(t)
	ctx := context.Background()

	if *check != "" {
		if _, err := os.Stat(*check); os.IsNotExist(err) {
			t.Fatal(err)
		}

		runMetaTest(ctx, t, 0, *check, [][]engineImpl{
			{engineImplRocksDB},
			{engineImplPebble},
		})
	}
}
