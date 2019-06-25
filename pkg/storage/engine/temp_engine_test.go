// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package engine

import (
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

	engine, err := NewTempEngine(base.TempStorageConfig{Path: tempDir}, base.StoreSpec{Path: tempDir})
	if err != nil {
		t.Fatalf("error encountered when invoking NewTempEngine: %v", err)
	}
	defer engine.Close()

	tempEngine, ok := engine.(*RocksDB)
	if !ok {
		t.Fatalf("temp engine could not be asserted as a rocksdb instance")
	}
	// Temp engine initialized with the temporary directory.
	if tempDir != tempEngine.cfg.Dir {
		t.Fatalf("temp engine initialized with unexpected parent directory.\nexpected %s\nactual %s", tempDir, tempEngine.cfg.Dir)
	}
}
