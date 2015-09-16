// Copyright 2015 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Marc Berhault (marc@cockroachlabs.com)

package sql_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

// getFastScanContext returns a test context with fast scan.
func getFastScanContext() *server.Context {
	c := server.NewTestContext()
	c.ScanInterval = time.Millisecond
	c.ScanMaxIdleTime = time.Millisecond
	return c
}

// getRangeKeys returns the end keys of all ranges.
func getRangeKeys(db *client.DB) ([]proto.Key, error) {
	rows, err := db.Scan(keys.Meta2Prefix, keys.MetaMax, 0)
	if err != nil {
		return nil, err
	}
	ret := make([]proto.Key, len(rows), len(rows))
	for i := 0; i < len(rows); i++ {
		ret[i] = bytes.TrimPrefix(rows[i].Key, keys.Meta2Prefix)
	}
	return ret, nil
}

func getNumRanges(db *client.DB) (int, error) {
	rows, err := getRangeKeys(db)
	if err != nil {
		return 0, err
	}
	return len(rows), nil
}

func rangesMatchSplits(ranges []proto.Key, splits []proto.Key) bool {
	if len(ranges) != len(splits) {
		return false
	}
	for i := 0; i < len(ranges); i++ {
		if !splits[i].Equal(ranges[i]) {
			continue
		}
	}
	return true
}

// TestSplitOnTableBoundaries verifies that ranges get split
// as new tables get created.
func TestSplitOnTableBoundaries(t *testing.T) {
	defer leaktest.AfterTest(t)
	s, sqlDB, kvDB := setupWithContext(t, getFastScanContext())
	defer cleanup(s, sqlDB)

	num, err := getNumRanges(kvDB)
	if err != nil {
		t.Fatalf("failed to retrieve range list: %s", err)
	}
	if num != 1 {
		t.Fatalf("expected no splits, but found %d ranges", num)
	}

	if _, err := sqlDB.Exec(`CREATE DATABASE test`); err != nil {
		t.Fatal(err)
	}

	// We split up to the largest allocated descriptor ID, be it a table
	// or a database.
	if err := util.IsTrueWithin(func() bool {
		num, err := getNumRanges(kvDB)
		if err != nil {
			t.Fatalf("failed to retrieve range list: %s", err)
		}
		return num == 2
	}, 500*time.Millisecond); err != nil {
		t.Errorf("missing split: %s", err)
	}

	// Verify the actual splits.
	objectID := uint32(keys.MaxReservedDescID + 1)
	splits := proto.KeySlice{keys.MakeTablePrefix(objectID), proto.KeyMax}
	ranges, err := getRangeKeys(kvDB)
	if err != nil {
		t.Fatal(err)
	}
	if !rangesMatchSplits(ranges, splits) {
		t.Fatalf("Found ranges: %v\nexpected: %v", ranges, splits)
	}

	// Let's create a table.
	if _, err := sqlDB.Exec(`CREATE TABLE test.test (k INT PRIMARY KEY, v INT)`); err != nil {
		t.Fatal(err)
	}

	if err := util.IsTrueWithin(func() bool {
		num, err := getNumRanges(kvDB)
		if err != nil {
			t.Fatalf("failed to retrieve range list: %s", err)
		}
		t.Logf("Num ranges: %d", num)
		return num == 3
	}, 500*time.Millisecond); err != nil {
		t.Errorf("missing split: %s", err)
	}

	// Verify the actual splits.
	splits = proto.KeySlice{keys.MakeTablePrefix(objectID), keys.MakeTablePrefix(objectID + 1), proto.KeyMax}
	ranges, err = getRangeKeys(kvDB)
	if err != nil {
		t.Fatal(err)
	}
	if !rangesMatchSplits(ranges, splits) {
		t.Fatalf("Found ranges: %v\nexpected: %v", ranges, splits)
	}
}
