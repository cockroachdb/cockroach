// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestNewRocksDBTempEngine(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Create the temporary directory for the RocksDB engine.
	tempDir, tempDirCleanup := testutils.TempDir(t)
	defer tempDirCleanup()

	engine, _, err := NewRocksDBTempEngine(base.TempStorageConfig{Path: tempDir}, base.StoreSpec{Path: tempDir})
	if err != nil {
		t.Fatalf("error encountered when invoking NewRocksDBTempEngine: %+v", err)
	}
	defer engine.Close()

	// Temp engine initialized with the temporary directory.
	if dir := engine.(*rocksDBTempEngine).db.cfg.StorageConfig.Dir; tempDir != dir {
		t.Fatalf("temp engine initialized with unexpected parent directory.\nexpected %s\nactual %s",
			tempDir, dir)
	}
}
