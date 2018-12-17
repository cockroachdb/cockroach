// Copyright 2018 The Cockroach Authors.
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
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestIterStats(t *testing.T) {
	defer leaktest.AfterTest(t)()

	db := setupMVCCInMemRocksDB(t, "test_iter_stats")
	defer db.Close()

	k := MakeMVCCMetadataKey(roachpb.Key("foo"))
	if err := db.Put(k, []byte("abc")); err != nil {
		t.Fatal(err)
	}

	if err := db.Clear(k); err != nil {
		t.Fatal(err)
	}

	batch := db.NewBatch()
	defer batch.Close()

	testCases := []Iterator{
		db.NewIterator(IterOptions{UpperBound: roachpb.KeyMax, WithStats: true}),
		batch.NewIterator(IterOptions{UpperBound: roachpb.KeyMax, WithStats: true}),
	}

	defer func() {
		for _, iter := range testCases {
			iter.Close()
		}
	}()

	for _, iter := range testCases {
		t.Run("", func(t *testing.T) {
			// Seeking past the tombstone manually counts it.
			for i := 0; i < 10; i++ {
				iter.Seek(NilKey)
				iter.Seek(MVCCKeyMax)
				stats := iter.Stats()
				if e, a := i+1, stats.InternalDeleteSkippedCount; a != e {
					t.Errorf("expected internal delete skipped count of %d, not %d", e, a)
				}
			}
			// Scanning a key range containing the tombstone sees it.
			for i := 0; i < 10; i++ {
				if _, _, _, _, err := iter.MVCCScan(
					roachpb.KeyMin, roachpb.KeyMax, math.MaxInt64, hlc.Timestamp{}, MVCCScanOptions{},
				); err != nil {
					t.Fatal(err)
				}
				stats := iter.Stats()
				if e, a := i+11, stats.InternalDeleteSkippedCount; a != e {
					t.Errorf("expected internal delete skipped count of %d, not %d", e, a)
				}
			}

			// Getting the key with the tombstone sees it.
			for i := 0; i < 10; i++ {
				if _, _, err := iter.MVCCGet(k.Key, hlc.Timestamp{}, MVCCGetOptions{}); err != nil {
					t.Fatal(err)
				}
				stats := iter.Stats()
				if e, a := i+21, stats.InternalDeleteSkippedCount; a != e {
					t.Errorf("expected internal delete skipped count of %d, not %d", e, a)
				}
			}
			// Getting KeyMax doesn't see it.
			for i := 0; i < 10; i++ {
				if _, _, err := iter.MVCCGet(roachpb.KeyMax, hlc.Timestamp{}, MVCCGetOptions{}); err != nil {
					t.Fatal(err)
				}
				stats := iter.Stats()
				if e, a := 30, stats.InternalDeleteSkippedCount; a != e {
					t.Errorf("expected internal delete skipped count of %d, not %d", e, a)
				}
			}

		})
	}
}
