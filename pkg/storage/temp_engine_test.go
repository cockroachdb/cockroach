// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
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

	db, fs, err := NewPebbleTempEngine(context.Background(), base.TempStorageConfig{
		Path:     tempDir,
		Settings: cluster.MakeTestingClusterSettings(),
	}, base.StoreSpec{Path: tempDir})
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
