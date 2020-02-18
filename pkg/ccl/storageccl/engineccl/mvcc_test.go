// Copyright 2016 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package engineccl

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestMVCCIterateTimeBound(t *testing.T) {
	defer leaktest.AfterTest(t)()

	dir, cleanupFn := testutils.TempDir(t)
	defer cleanupFn()

	const numKeys = 1000
	const numBatches = 10
	const batchTimeSpan = 10
	const valueSize = 8

	eng, err := loadTestData(filepath.Join(dir, "mvcc_data"),
		numKeys, numBatches, batchTimeSpan, valueSize)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	for _, testCase := range []struct {
		start hlc.Timestamp
		end   hlc.Timestamp
	}{
		// entire time range
		{hlc.Timestamp{WallTime: 0, Logical: 0}, hlc.Timestamp{WallTime: 110, Logical: 0}},
		// one SST
		{hlc.Timestamp{WallTime: 10, Logical: 0}, hlc.Timestamp{WallTime: 19, Logical: 0}},
		// one SST, plus the min of the following SST
		{hlc.Timestamp{WallTime: 10, Logical: 0}, hlc.Timestamp{WallTime: 20, Logical: 0}},
		// one SST, plus the max of the preceding SST
		{hlc.Timestamp{WallTime: 9, Logical: 0}, hlc.Timestamp{WallTime: 19, Logical: 0}},
		// one SST, plus the min of the following and the max of the preceding SST
		{hlc.Timestamp{WallTime: 9, Logical: 0}, hlc.Timestamp{WallTime: 21, Logical: 0}},
		// one SST, not min or max
		{hlc.Timestamp{WallTime: 17, Logical: 0}, hlc.Timestamp{WallTime: 18, Logical: 0}},
		// one SST's max
		{hlc.Timestamp{WallTime: 18, Logical: 0}, hlc.Timestamp{WallTime: 19, Logical: 0}},
		// one SST's min
		{hlc.Timestamp{WallTime: 19, Logical: 0}, hlc.Timestamp{WallTime: 20, Logical: 0}},
		// random endpoints
		{hlc.Timestamp{WallTime: 32, Logical: 0}, hlc.Timestamp{WallTime: 78, Logical: 0}},
	} {
		t.Run(fmt.Sprintf("%s-%s", testCase.start, testCase.end), func(t *testing.T) {
			defer leaktest.AfterTest(t)()

			var expectedKVs []engine.MVCCKeyValue
			iter := eng.NewIterator(engine.IterOptions{UpperBound: roachpb.KeyMax})
			defer iter.Close()
			iter.SeekGE(engine.MVCCKey{})
			for {
				ok, err := iter.Valid()
				if err != nil {
					t.Fatal(err)
				} else if !ok {
					break
				}
				ts := iter.Key().Timestamp
				if (ts.Less(testCase.end) || testCase.end == ts) && testCase.start.Less(ts) {
					expectedKVs = append(expectedKVs, engine.MVCCKeyValue{Key: iter.Key(), Value: iter.Value()})
				}
				iter.Next()
			}
			if len(expectedKVs) < 1 {
				t.Fatalf("source of truth had no expected KVs; likely a bug in the test itself")
			}

			fn := (*engine.MVCCIncrementalIterator).NextKey
			assertEqualKVs(eng, fn, keys.MinKey, keys.MaxKey, testCase.start, testCase.end, expectedKVs)(t)
		})
	}
}

func assertEqualKVs(
	e engine.Engine,
	iterFn func(*engine.MVCCIncrementalIterator),
	startKey, endKey roachpb.Key,
	startTime, endTime hlc.Timestamp,
	expected []engine.MVCCKeyValue,
) func(*testing.T) {
	return func(t *testing.T) {
		t.Helper()
		iter := engine.NewMVCCIncrementalIterator(e, engine.MVCCIncrementalIterOptions{
			IterOptions: engine.IterOptions{UpperBound: endKey},
			StartTime:   startTime,
			EndTime:     endTime,
		})
		defer iter.Close()
		var kvs []engine.MVCCKeyValue
		for iter.SeekGE(engine.MakeMVCCMetadataKey(startKey)); ; iterFn(iter) {
			if ok, err := iter.Valid(); err != nil {
				t.Fatalf("unexpected error: %+v", err)
			} else if !ok || iter.UnsafeKey().Key.Compare(endKey) >= 0 {
				break
			}
			kvs = append(kvs, engine.MVCCKeyValue{Key: iter.Key(), Value: iter.Value()})
		}

		if len(kvs) != len(expected) {
			t.Fatalf("got %d kvs but expected %d: %v", len(kvs), len(expected), kvs)
		}
		for i := range kvs {
			if !kvs[i].Key.Equal(expected[i].Key) {
				t.Fatalf("%d key: got %v but expected %v", i, kvs[i].Key, expected[i].Key)
			}
			if !bytes.Equal(kvs[i].Value, expected[i].Value) {
				t.Fatalf("%d value: got %x but expected %x", i, kvs[i].Value, expected[i].Value)
			}
		}
	}
}
