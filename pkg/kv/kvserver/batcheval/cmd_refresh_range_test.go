// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package batcheval

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
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
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	k := roachpb.Key("a")
	v := roachpb.MakeValueFromString("hi")
	ts1 := hlc.Timestamp{WallTime: 1}
	ts2 := hlc.Timestamp{WallTime: 2}
	ts3 := hlc.Timestamp{WallTime: 3}
	ts4 := hlc.Timestamp{WallTime: 4}

	db := storage.NewDefaultInMemForTesting()
	defer db.Close()

	// Create an sstable containing an unresolved intent.
	txn := &roachpb.Transaction{
		TxnMeta: enginepb.TxnMeta{
			Key:            k,
			ID:             uuid.MakeV4(),
			Epoch:          1,
			WriteTimestamp: ts1,
		},
		ReadTimestamp: ts1,
	}
	if err := storage.MVCCPut(ctx, db, nil, k, txn.ReadTimestamp, hlc.ClockTimestamp{}, v, txn); err != nil {
		t.Fatal(err)
	}
	if err := storage.MVCCPut(ctx, db, nil, roachpb.Key("unused1"), ts4, hlc.ClockTimestamp{}, v, nil); err != nil {
		t.Fatal(err)
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}

	// Create a second sstable containing the resolution of the intent
	// (committed). The sstable also has a second write at a different (older)
	// timestamp, because if it were empty other than the deletion tombstone, it
	// would not have any timestamp bounds and would be selected for every read.
	intent := roachpb.MakeLockUpdate(txn, roachpb.Span{Key: k})
	intent.Status = roachpb.COMMITTED
	if _, err := storage.MVCCResolveWriteIntent(ctx, db, nil, intent); err != nil {
		t.Fatal(err)
	}
	if err := storage.MVCCPut(ctx, db, nil, roachpb.Key("unused2"), ts1, hlc.ClockTimestamp{}, v, nil); err != nil {
		t.Fatal(err)
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}

	// We should now have a committed value at k@ts1. Read it back to make sure.
	// This represents real-world use of time-bound iterators where callers must
	// have previously performed a consistent read at the lower time-bound to
	// prove that there are no intents present that would be missed by the time-
	// bound iterator.
	if val, intent, err := storage.MVCCGet(ctx, db, k, ts1, storage.MVCCGetOptions{}); err != nil {
		t.Fatal(err)
	} else if intent != nil {
		t.Fatalf("got unexpected intent: %v", intent)
	} else if !val.EqualTagAndData(v) {
		t.Fatalf("expected %v, got %v", v, val)
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
		EvalCtx: (&MockEvalCtx{
			ClusterSettings: cluster.MakeTestingClusterSettings(),
		}).EvalContext(),
		Args: &roachpb.RefreshRangeRequest{
			RequestHeader: roachpb.RequestHeader{
				Key:    k,
				EndKey: keys.MaxKey,
			},
			RefreshFrom: ts2,
		},
		Header: roachpb.Header{
			Txn: &roachpb.Transaction{
				TxnMeta: enginepb.TxnMeta{
					WriteTimestamp: ts3,
				},
				ReadTimestamp: ts2,
			},
			Timestamp: ts3,
		},
	}, &resp)
	if err != nil {
		t.Fatal(err)
	}
}

// TestRefreshRangeError verifies we get an error. We are trying to refresh from
// time 1 to 3, but the key was written at time 2.
func TestRefreshRangeError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Intents behave the same, but the error is a bit different. Verify resolved and unresolved intents.
	testutils.RunTrueAndFalse(t, "resolve_intent", func(t *testing.T, resolveIntent bool) {
		ctx := context.Background()
		v := roachpb.MakeValueFromString("hi")
		ts1 := hlc.Timestamp{WallTime: 1}
		ts2 := hlc.Timestamp{WallTime: 2}
		ts3 := hlc.Timestamp{WallTime: 3}

		db := storage.NewDefaultInMemForTesting()
		defer db.Close()

		var k roachpb.Key
		if resolveIntent {
			k = roachpb.Key("resolved_key")
		} else {
			k = roachpb.Key("unresolved_key")
		}

		// Write to a key at time ts2 by creating an sstable containing an unresolved intent.
		txn := &roachpb.Transaction{
			TxnMeta: enginepb.TxnMeta{
				Key:            k,
				ID:             uuid.MakeV4(),
				Epoch:          1,
				WriteTimestamp: ts2,
			},
			ReadTimestamp: ts2,
		}
		if err := storage.MVCCPut(ctx, db, nil, k, txn.ReadTimestamp, hlc.ClockTimestamp{}, v, txn); err != nil {
			t.Fatal(err)
		}

		if resolveIntent {
			intent := roachpb.MakeLockUpdate(txn, roachpb.Span{Key: k})
			intent.Status = roachpb.COMMITTED
			if _, err := storage.MVCCResolveWriteIntent(ctx, db, nil, intent); err != nil {
				t.Fatal(err)
			}
		}

		// We are trying to refresh from time 1 to 3, but the key was written at time
		// 2, therefore the refresh should fail.
		var resp roachpb.RefreshRangeResponse
		_, err := RefreshRange(ctx, db, CommandArgs{
			EvalCtx: (&MockEvalCtx{
				ClusterSettings: cluster.MakeTestingClusterSettings(),
			}).EvalContext(),
			Args: &roachpb.RefreshRangeRequest{
				RequestHeader: roachpb.RequestHeader{
					Key:    k,
					EndKey: keys.MaxKey,
				},
				RefreshFrom: ts1,
			},
			Header: roachpb.Header{
				Txn: &roachpb.Transaction{
					TxnMeta: enginepb.TxnMeta{
						WriteTimestamp: ts3,
					},
					ReadTimestamp: ts3,
				},
				Timestamp: ts3,
			},
		}, &resp)
		require.IsType(t, &roachpb.RefreshFailedError{}, err)
		if resolveIntent {
			require.Equal(t, "encountered recently written committed value \"resolved_key\" @0.000000002,0",
				err.Error())
		} else {
			require.Equal(t, "encountered recently written intent \"unresolved_key\" @0.000000002,0",
				err.Error())
		}
	})
}

// TestRefreshRangeTimestampBounds verifies that a RefreshRange treats its
// RefreshFrom timestamp as exclusive and its txn.ReadTimestamp (i.e. its
// "RefreshTo" timestamp) as inclusive.
func TestRefreshRangeTimestampBounds(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	db := storage.NewDefaultInMemForTesting()
	defer db.Close()

	k := roachpb.Key("key")
	v := roachpb.MakeValueFromString("val")
	ts1 := hlc.Timestamp{WallTime: 1}
	ts2 := hlc.Timestamp{WallTime: 2}
	ts3 := hlc.Timestamp{WallTime: 3}

	// Write to a key at time ts2.
	require.NoError(t, storage.MVCCPut(ctx, db, nil, k, ts2, hlc.ClockTimestamp{}, v, nil))

	for _, tc := range []struct {
		from, to hlc.Timestamp
		expErr   bool
	}{
		// Sanity-check.
		{ts1, ts3, true},
		// RefreshTo is inclusive, so expect error on collision.
		{ts1, ts2, true},
		// RefreshFrom is exclusive, so expect no error on collision.
		{ts2, ts3, false},
	} {
		var resp roachpb.RefreshRangeResponse
		_, err := RefreshRange(ctx, db, CommandArgs{
			EvalCtx: (&MockEvalCtx{
				ClusterSettings: cluster.MakeTestingClusterSettings(),
			}).EvalContext(),
			Args: &roachpb.RefreshRangeRequest{
				RequestHeader: roachpb.RequestHeader{
					Key:    k,
					EndKey: k.Next(),
				},
				RefreshFrom: tc.from,
			},
			Header: roachpb.Header{
				Txn: &roachpb.Transaction{
					TxnMeta: enginepb.TxnMeta{
						WriteTimestamp: tc.to,
					},
					ReadTimestamp: tc.to,
				},
				Timestamp: tc.to,
			},
		}, &resp)

		if tc.expErr {
			require.Error(t, err)
			require.Regexp(t, "encountered recently written committed value", err)
		} else {
			require.NoError(t, err)
		}
	}
}
