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
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
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

	ctx := context.Background()
	k := roachpb.Key("a")
	v := roachpb.MakeValueFromString("hi")
	ts1 := hlc.Timestamp{WallTime: 1}
	ts2 := hlc.Timestamp{WallTime: 2}
	ts3 := hlc.Timestamp{WallTime: 3}
	ts4 := hlc.Timestamp{WallTime: 4}

	db := engine.NewInMem(roachpb.Attributes{}, 10<<20)
	defer db.Close()

	// Create an sstable containing an unresolved intent.
	txn := &roachpb.Transaction{
		TxnMeta: enginepb.TxnMeta{
			Key:       k,
			ID:        uuid.MakeV4(),
			Epoch:     1,
			Timestamp: ts1,
		},
		OrigTimestamp: ts1,
	}
	if err := engine.MVCCPut(ctx, db, nil, k, txn.OrigTimestamp, v, txn); err != nil {
		t.Fatal(err)
	}
	if err := engine.MVCCPut(ctx, db, nil, roachpb.Key("unused1"), ts4, v, nil); err != nil {
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
	if err := engine.MVCCResolveWriteIntent(ctx, db, nil, roachpb.Intent{
		Span:   roachpb.Span{Key: k},
		Txn:    txn.TxnMeta,
		Status: roachpb.COMMITTED,
	}); err != nil {
		t.Fatal(err)
	}
	if err := engine.MVCCPut(ctx, db, nil, roachpb.Key("unused2"), ts1, v, nil); err != nil {
		t.Fatal(err)
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}

	// Double-check that we've created the SSTs we intended to.
	userProps, err := db.GetUserProperties()
	if err != nil {
		t.Fatal(err)
	}
	require.Len(t, userProps.Sst, 2)
	require.Equal(t, userProps.Sst[0].TsMin, &ts1)
	require.Equal(t, userProps.Sst[0].TsMax, &ts4)
	require.Equal(t, userProps.Sst[1].TsMin, &ts1)
	require.Equal(t, userProps.Sst[1].TsMax, &ts1)

	// We should now have a committed value at k@ts1. Read it back to make sure.
	// This represents real-world use of time-bound iterators where callers must
	// have previously performed a consistent read at the lower time-bound to
	// prove that there are no intents present that would be missed by the time-
	// bound iterator.
	if val, intent, err := engine.MVCCGet(ctx, db, k, ts1, engine.MVCCGetOptions{}); err != nil {
		t.Fatal(err)
	} else if intent != nil {
		t.Fatalf("got unexpected intent: %v", intent)
	} else if !val.EqualData(v) {
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
	_, err = RefreshRange(ctx, db, CommandArgs{
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
