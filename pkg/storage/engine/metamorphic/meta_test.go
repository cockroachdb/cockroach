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
	"os"
	"path"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// createTestRocksDBEngine returns a new in-memory RocksDB engine with 1MB of
// storage capacity.
func createTestRocksDBEngine(path string) (engine.Engine, error) {
	return engine.NewEngine(enginepb.EngineTypeRocksDB, 1<<20, base.StorageConfig{
		Attrs:           roachpb.Attributes{},
		Dir:             path,
		MustExist:       false,
		MaxSize:         0,
		Settings:        cluster.MakeTestingClusterSettings(),
		UseFileRegistry: false,
		ExtraOptions:    nil,
	})
}

// createTestPebbleEngine returns a new in-memory Pebble storage engine.
func createTestPebbleEngine(path string) (engine.Engine, error) {
	return engine.NewEngine(enginepb.EngineTypePebble, 1<<20, base.StorageConfig{
		Attrs:           roachpb.Attributes{},
		Dir:             path,
		MustExist:       false,
		MaxSize:         0,
		Settings:        cluster.MakeTestingClusterSettings(),
		UseFileRegistry: false,
		ExtraOptions:    nil,
	})
}

var mvccEngineImpls = []struct {
	name   string
	create func(path string) (engine.Engine, error)
}{
	{"rocksdb", createTestRocksDBEngine},
	{"pebble", createTestPebbleEngine},
}

var (
	keep  = flag.Bool("keep", false, "keep temp directories after test")
	check = flag.String("check", "", "run operations in specified file and check output for equality")
)

func runMetaTest(ctx context.Context, t *testing.T, seed int64, checkFile io.Reader) {
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			tempDir, cleanup := testutils.TempDir(t)
			defer func() {
				if !*keep {
					cleanup()
				}
			}()

			eng, err := engineImpl.create(path.Join(tempDir, engineImpl.name))
			if err != nil {
				t.Fatal(err)
			}
			defer eng.Close()

			outputFilePath := path.Join(tempDir, fmt.Sprintf("%s.meta", engineImpl.name))
			fmt.Printf("output file path: %s\n", outputFilePath)

			outputFile, err := os.Create(outputFilePath)
			if err != nil {
				t.Fatal(err)
			}
			defer outputFile.Close()

			testRunner := metaTestRunner{
				ctx:    ctx,
				t:      t,
				w:      outputFile,
				seed:   seed,
				engine: eng,
			}

			testRunner.init()
			if checkFile != nil {
				testRunner.parseFileAndRun(checkFile)
			} else {
				testRunner.generateAndRun(10000)
			}
		})
	}
}

// TestMeta runs the MVCC Metamorphic test suite.
func TestMeta(t *testing.T) {
	defer leaktest.AfterTest(t)
	ctx := context.Background()
	seeds := []int64{123}

	if *check != "" {
		t.Run("check", func(t *testing.T) {
			if _, err := os.Stat(*check); os.IsNotExist(err) {
				t.Fatal(err)
			}
			checkFile, err := os.Open(*check)
			if err != nil {
				t.Fatal(err)
			}
			defer checkFile.Close()

			runMetaTest(ctx, t, 0, checkFile)
		})
	}
	for _, seed := range seeds {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			runMetaTest(ctx, t, seed, nil)
		})
	}
}
