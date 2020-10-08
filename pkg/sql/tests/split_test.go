// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// getRangeKeys returns the end keys of all ranges.
func getRangeKeys(db *kv.DB) ([]roachpb.Key, error) {
	rows, err := db.Scan(context.Background(), keys.Meta2Prefix, keys.MetaMax, 0)
	if err != nil {
		return nil, err
	}
	ret := make([]roachpb.Key, len(rows))
	for i := 0; i < len(rows); i++ {
		ret[i] = bytes.TrimPrefix(rows[i].Key, keys.Meta2Prefix)
	}
	return ret, nil
}

func getNumRanges(db *kv.DB) (int, error) {
	rows, err := getRangeKeys(db)
	if err != nil {
		return 0, err
	}
	return len(rows), nil
}

func rangesMatchSplits(ranges []roachpb.Key, splits []roachpb.RKey) bool {
	if len(ranges) != len(splits) {
		return false
	}
	for i := 0; i < len(ranges); i++ {
		if !splits[i].Equal(ranges[i]) {
			return false
		}
	}
	return true
}

// TestSplitOnTableBoundaries verifies that ranges get split
// as new tables get created.
func TestSplitOnTableBoundaries(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	params, _ := tests.CreateTestServerParams()
	// We want fast scan.
	params.ScanInterval = time.Millisecond
	params.ScanMinIdleTime = time.Millisecond
	params.ScanMaxIdleTime = time.Millisecond
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	expectedInitialRanges, err := server.ExpectedInitialRangeCount(kvDB, &s.(*server.TestServer).Cfg.DefaultZoneConfig, &s.(*server.TestServer).Cfg.DefaultSystemZoneConfig)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := sqlDB.Exec(`CREATE DATABASE test`); err != nil {
		t.Fatal(err)
	}

	// We split up to the largest allocated descriptor ID, if it's a table.
	// Ensure that no split happens if a database is created.
	testutils.SucceedsSoon(t, func() error {
		num, err := getNumRanges(kvDB)
		if err != nil {
			return err
		}
		if e := expectedInitialRanges; num != e {
			return errors.Errorf("expected %d splits, found %d", e, num)
		}
		return nil
	})

	// Verify the actual splits.
	objectID := uint32(keys.MinUserDescID)
	splits := []roachpb.RKey{roachpb.RKeyMax}
	ranges, err := getRangeKeys(kvDB)
	if err != nil {
		t.Fatal(err)
	}
	if a, e := ranges[expectedInitialRanges-1:], splits; !rangesMatchSplits(a, e) {
		t.Fatalf("Found ranges: %v\nexpected: %v", a, e)
	}

	// Let's create a table.
	if _, err := sqlDB.Exec(`CREATE TABLE test.test (k INT PRIMARY KEY, v INT)`); err != nil {
		t.Fatal(err)
	}

	testutils.SucceedsSoon(t, func() error {
		num, err := getNumRanges(kvDB)
		if err != nil {
			return err
		}
		if e := expectedInitialRanges + 1; num != e {
			return errors.Errorf("expected %d splits, found %d", e, num)
		}
		return nil
	})

	// Verify the actual splits.
	splits = []roachpb.RKey{roachpb.RKey(keys.SystemSQLCodec.TablePrefix(objectID + 3)), roachpb.RKeyMax}
	ranges, err = getRangeKeys(kvDB)
	if err != nil {
		t.Fatal(err)
	}
	if a, e := ranges[expectedInitialRanges-1:], splits; !rangesMatchSplits(a, e) {
		t.Fatalf("Found ranges: %v\nexpected: %v", a, e)
	}
}
