// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package engine

import (
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestNewTempEngine(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Create the temporary directory for the RocksDB engine.
	tempDir, tempDirCleanup := testutils.TempDir(t)
	defer tempDirCleanup()

	engine, err := NewTempEngine(base.TempStorageConfig{Path: tempDir})
	if err != nil {
		t.Fatalf("error encountered when invoking NewTempEngine: %v", err)
	}
	defer engine.Close()

	tempEngine := engine.(*tempEngine)
	if tempEngine.RocksDB == nil {
		t.Fatalf("a rocksdb instance was not returned in NewTempEngine")
	}

	// Temp engine initialized with the temporary directory.
	if tempDir != tempEngine.RocksDB.cfg.Dir {
		t.Fatalf("rocksdb instance initialized with unexpected parent directory.\nexpected %s\nactual %s", tempDir, tempEngine.RocksDB.cfg.Dir)
	}
}

// TestTempEngineClose verifies that Close indeeds removes the associated
// temporary directory for the engine.
func TestTempEngineClose(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, dirCleanup := testutils.TempDir(t)
	defer dirCleanup()

	engine, err := NewTempEngine(base.TempStorageConfig{Path: dir})
	if err != nil {
		t.Fatalf("error encountered when invoking NewTempEngine: %v", err)
	}

	// Close the engine to cleanup temporary directory.
	engine.Close()

	tempEngine := engine.(*tempEngine)

	// Check that the temp store directory is removed.
	_, err = os.Stat(tempEngine.RocksDB.cfg.Dir)
	// os.Stat returns a nil err if the file exists, so we need to check
	// the NOT of this condition instead of os.IsExist() (which returns
	// false and misses this error).
	if !os.IsNotExist(err) {
		if err == nil {
			t.Fatalf("temp storage directory %s was not cleaned up during Close", tempEngine.RocksDB.cfg.Dir)
		} else {
			// Unexpected error.
			t.Fatal(err)
		}
	}
}
