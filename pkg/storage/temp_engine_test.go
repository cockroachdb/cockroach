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
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestNewPebbleTempEngine(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Create the temporary directory for the RocksDB engine.
	tempDir, tempDirCleanup := testutils.TempDir(t)
	defer tempDirCleanup()

	db, fs, err := NewPebbleTempEngine(context.Background(), base.TempStorageConfig{Path: tempDir}, base.StoreSpec{Path: tempDir})
	if err != nil {
		t.Fatalf("error encountered when invoking NewRocksDBTempEngine: %+v", err)
	}
	defer db.Close()

	// Temp engine initialized with the temporary directory.
	if dir := fs.(*Pebble).path; tempDir != dir {
		t.Fatalf("temp engine initialized with unexpected parent directory.\nexpected %s\nactual %s",
			tempDir, dir)
	}
}
