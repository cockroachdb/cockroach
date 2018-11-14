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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package batcheval

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// TestRefreshRangeTimeBoundIterator is a regression test for
// https://github.com/cockroachdb/cockroach/issues/31823. RefreshRange
// uses a time-bound iterator, which has a bug that can cause old
// resolved intents to incorrectly appear to be pending. This test
// constructs the necessary arrangement of sstables to reproduce the
// bug and ensures that the workaround (and later, the permanent fix)
// are effective.
//
// The bug is that resolving an intent does not contribute to the
// sstable's timestamp bounds, so that if there is no other
// timestamped data expanding the bounds, time-bound iterators may
// open fewer sstables than necessary and only see the intent, not its
// resolution.
//
// This test creates two sstables. The first contains a pending intent
// at ts1 and another key at ts4, giving it timestamp bounds 1-4 (and
// putting it in scope for transactions at timestamps higher than
// ts1).
func TestRefreshRangeTimeBoundIterator(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	k := roachpb.Key("a")
	v := roachpb.MakeValueFromString("hi")
	ts1 := hlc.Timestamp{WallTime: 1}
	ts2 := hlc.Timestamp{WallTime: 2}
	ts3 := hlc.Timestamp{WallTime: 3}
	ts4 := hlc.Timestamp{WallTime: 4}

	// Create an sstable containing an unresolved intent. To reduce the
	// amount of knowledge of MVCC internals we must embed here, we
	// write to a temporary engine and extract the RocksDB KV data. The
	// sstable also contains an unrelated key at a higher timestamp to
	// widen its bounds.
	intentSSTContents := func() []byte {
		db := engine.NewInMem(roachpb.Attributes{}, 10<<20)
		defer db.Close()

		txn := &roachpb.Transaction{
			TxnMeta: enginepb.TxnMeta{
				Key:       k,
				ID:        uuid.MakeV4(),
				Epoch:     1,
				Timestamp: ts1,
			},
		}
		if err := engine.MVCCPut(ctx, db, nil, k, txn.Timestamp, v, txn); err != nil {
			t.Fatal(err)
		}

		if err := engine.MVCCPut(ctx, db, nil, roachpb.Key("unused1"), ts4, v, nil); err != nil {
			t.Fatal(err)
		}

		sstWriter, err := engine.MakeRocksDBSstFileWriter()
		if err != nil {
			t.Fatal(err)
		}
		it := db.NewIterator(engine.IterOptions{
			UpperBound: keys.MaxKey,
		})
		defer it.Close()
		it.Seek(engine.MVCCKey{Key: keys.MinKey})
		for {
			ok, err := it.Valid()
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				break
			}
			if err := sstWriter.Add(engine.MVCCKeyValue{Key: it.Key(), Value: it.Value()}); err != nil {
				t.Fatal(err)
			}
			it.Next()
		}

		sstContents, err := sstWriter.Finish()
		if err != nil {
			t.Fatal(err)
		}

		return sstContents
	}()

	// Create a second sstable containing the resolution of the intent
	// (committed). This is a rocksdb tombstone and there's no good way
	// to construct that as we did above, so we do it by hand. The
	// sstable also has a second write at a different (older) timestamp,
	// because if it were empty other than the deletion tombstone, it
	// would not have any timestamp bounds and would be selected for
	// every read.
	resolveSSTContents := func() []byte {
		sstWriter, err := engine.MakeRocksDBSstFileWriter()
		if err != nil {
			t.Fatal(err)
		}
		if err := sstWriter.Delete(engine.MakeMVCCMetadataKey(k)); err != nil {
			t.Fatal(err)
		}
		if err := sstWriter.Add(engine.MVCCKeyValue{
			Key: engine.MVCCKey{
				Key:       roachpb.Key("unused2"),
				Timestamp: ts1,
			},
			Value: nil,
		}); err != nil {
			t.Fatal(err)
		}
		sstContents, err := sstWriter.Finish()
		if err != nil {
			t.Fatal(err)
		}
		return sstContents
	}()

	// Create a new DB and ingest our two sstables.
	db := engine.NewInMem(roachpb.Attributes{}, 10<<20)
	defer db.Close()

	for i, contents := range [][]byte{intentSSTContents, resolveSSTContents} {
		filename := fmt.Sprintf("intent-%d", i)
		if err := db.WriteFile(filename, contents); err != nil {
			t.Fatal(err)
		}
		if err := db.IngestExternalFiles(ctx, []string{filename}, true); err != nil {
			t.Fatal(err)
		}
	}

	// We should now have a committed value at k@ts1. Read it back to make
	// sure our fake intent resolution did the right thing.
	if val, intents, err := engine.MVCCGet(ctx, db, k, ts1, true, nil); err != nil {
		t.Fatal(err)
	} else if len(intents) > 0 {
		t.Fatalf("got unexpected intents: %v", intents)
	} else if !val.EqualData(v) {
		t.Fatalf("expected %s, got %s", v, val)
	}

	// Now the real test: a transaction at ts2 has been pushed to ts3
	// and must refresh. It overlaps with our committed intent on k@ts1,
	// which is fine because our timestamp is higher (but if that intent
	// were still pending, the new txn would be blocked). Prior to
	// https://github.com/cockroachdb/cockroach/pull/32211, a bug in the
	// time-bound iterator meant that we would see the first sstable but
	// not the second and incorrectly report the intent as pending,
	// resulting in an error from RefreshRange.
	var resp roachpb.RefreshRangeResponse
	_, err := RefreshRange(ctx, db, CommandArgs{
		Args: &roachpb.RefreshRangeRequest{
			RequestHeader: roachpb.RequestHeader{
				Key:    k,
				EndKey: keys.MaxKey,
			},
		},
		Header: roachpb.Header{
			Txn: &roachpb.Transaction{
				TxnMeta: enginepb.TxnMeta{
					Timestamp: ts3,
				},
				OrigTimestamp: ts2,
			},
		},
	}, &resp)
	if err != nil {
		t.Fatal(err)
	}
}
