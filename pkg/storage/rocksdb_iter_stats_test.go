// Copyright 2018 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestIterStats(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

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
				iter.SeekGE(NilKey)
				iter.SeekGE(MVCCKeyMax)
				stats := iter.Stats()
				if e, a := i+1, stats.InternalDeleteSkippedCount; a != e {
					t.Errorf("expected internal delete skipped count of %d, not %d", e, a)
				}
			}
			// Scanning a key range containing the tombstone sees it.
			for i := 0; i < 10; i++ {
				if _, err := mvccScanToKvs(
					ctx, iter, roachpb.KeyMin, roachpb.KeyMax, hlc.Timestamp{}, MVCCScanOptions{},
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
				if _, _, err := mvccGet(ctx, iter, k.Key, hlc.Timestamp{}, MVCCGetOptions{}); err != nil {
					t.Fatal(err)
				}
				stats := iter.Stats()
				if e, a := i+21, stats.InternalDeleteSkippedCount; a != e {
					t.Errorf("expected internal delete skipped count of %d, not %d", e, a)
				}
			}
			// Getting KeyMax doesn't see it.
			for i := 0; i < 10; i++ {
				if _, _, err := mvccGet(ctx, iter, roachpb.KeyMax, hlc.Timestamp{}, MVCCGetOptions{}); err != nil {
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
