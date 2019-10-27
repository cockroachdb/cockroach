// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package engine

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/require"
)

// assertEq compares the given ms and expMS and errors when they don't match. It
// also recomputes the stats over the whole engine with all known
// implementations and errors on mismatch with any of them.
func assertEq(t *testing.T, engine ReadWriter, debug string, ms, expMS *enginepb.MVCCStats) {
	t.Helper()

	msCpy := *ms // shallow copy
	ms = &msCpy
	ms.AgeTo(expMS.LastUpdateNanos)
	if !ms.Equal(expMS) {
		pretty.Ldiff(t, ms, expMS)
		t.Errorf("%s: diff(ms, expMS) nontrivial", debug)
	}

	it := engine.NewIterator(IterOptions{UpperBound: roachpb.KeyMax})
	defer it.Close()
	from, to := MVCCKey{}, MVCCKey{Key: roachpb.KeyMax}

	for _, mvccStatsTest := range mvccStatsTests {
		compMS, err := mvccStatsTest.fn(it, from, to, ms.LastUpdateNanos)
		if err != nil {
			t.Fatal(err)
		}
		if !compMS.Equal(*ms) {
			t.Errorf("%s: diff(ms, %s) = %s", debug, mvccStatsTest.name, pretty.Diff(*ms, compMS))
		}
	}
}

// TestMVCCStatsDeleteCommitMovesTimestamp exercises the case in which a value
// is written, later deleted via an intent and the deletion committed at an even
// higher timestamp. This exercises subtleties related to the implicit push of
// the intent (before resolution) and the accumulation of GCByteAge.
func TestMVCCStatsDeleteCommitMovesTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()

			ctx := context.Background()
			aggMS := &enginepb.MVCCStats{}

			assertEq(t, engine, "initially", aggMS, &enginepb.MVCCStats{})

			key := roachpb.Key("a")
			ts1 := hlc.Timestamp{WallTime: 1E9}
			// Put a value.
			value := roachpb.MakeValueFromString("value")
			if err := MVCCPut(ctx, engine, aggMS, key, ts1, value, nil); err != nil {
				t.Fatal(err)
			}

			mKeySize := int64(mvccKey(key).EncodedSize()) // 2
			vKeySize := MVCCVersionTimestampSize          // 12
			vValSize := int64(len(value.RawBytes))        // 10

			expMS := enginepb.MVCCStats{
				LiveBytes:       mKeySize + vKeySize + vValSize, // 24
				LiveCount:       1,
				KeyBytes:        mKeySize + vKeySize, // 14
				KeyCount:        1,
				ValBytes:        vValSize, // 10
				ValCount:        1,
				LastUpdateNanos: 1E9,
			}
			assertEq(t, engine, "after put", aggMS, &expMS)

			// Delete the value at ts=3. We'll commit this at ts=4 later.
			ts3 := hlc.Timestamp{WallTime: 3 * 1E9}
			txn := &roachpb.Transaction{
				TxnMeta:       enginepb.TxnMeta{ID: uuid.MakeV4(), Timestamp: ts3},
				OrigTimestamp: ts3,
			}
			if err := MVCCDelete(ctx, engine, aggMS, key, txn.OrigTimestamp, txn); err != nil {
				t.Fatal(err)
			}

			// Now commit the value, but with a timestamp gap (i.e. this is a
			// push-commit as it would happen for a SNAPSHOT txn)
			ts4 := hlc.Timestamp{WallTime: 4 * 1E9}
			txn.Status = roachpb.COMMITTED
			txn.Timestamp.Forward(ts4)
			if err := MVCCResolveWriteIntent(ctx, engine, aggMS, roachpb.Intent{
				Span: roachpb.Span{Key: key}, Status: txn.Status, Txn: txn.TxnMeta,
			}); err != nil {
				t.Fatal(err)
			}

			expAggMS := enginepb.MVCCStats{
				LastUpdateNanos: 4E9,
				LiveBytes:       0,
				LiveCount:       0,
				KeyCount:        1,
				ValCount:        2,
				// The implicit meta record (deletion tombstone) counts for len("a")+1=2.
				// Two versioned keys count for 2*vKeySize.
				KeyBytes: mKeySize + 2*vKeySize,
				ValBytes: vValSize, // the initial write (10)
				// No GCBytesAge has been accrued yet, as the value just got non-live at 4s.
				GCBytesAge: 0,
			}

			assertEq(t, engine, "after committing", aggMS, &expAggMS)
		})
	}
}

// TestMVCCStatsPutCommitMovesTimestamp is similar to
// TestMVCCStatsDeleteCommitMovesTimestamp, but is simpler: a first intent is
// written and then committed at a later timestamp.
func TestMVCCStatsPutCommitMovesTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()

			ctx := context.Background()
			aggMS := &enginepb.MVCCStats{}

			assertEq(t, engine, "initially", aggMS, &enginepb.MVCCStats{})

			key := roachpb.Key("a")
			ts1 := hlc.Timestamp{WallTime: 1E9}
			txn := &roachpb.Transaction{
				TxnMeta:       enginepb.TxnMeta{ID: uuid.MakeV4(), Timestamp: ts1},
				OrigTimestamp: ts1,
			}
			// Write an intent at t=1s.
			value := roachpb.MakeValueFromString("value")
			if err := MVCCPut(ctx, engine, aggMS, key, ts1, value, txn); err != nil {
				t.Fatal(err)
			}

			mKeySize := int64(mvccKey(key).EncodedSize()) // 2
			mValSize := int64((&enginepb.MVCCMetadata{    // 44
				Timestamp: hlc.LegacyTimestamp(ts1),
				Deleted:   false,
				Txn:       &txn.TxnMeta,
			}).Size())
			vKeySize := MVCCVersionTimestampSize   // 12
			vValSize := int64(len(value.RawBytes)) // 10

			expMS := enginepb.MVCCStats{
				LastUpdateNanos: 1E9,
				LiveBytes:       mKeySize + mValSize + vKeySize + vValSize, // 2+44+12+10 = 68
				LiveCount:       1,
				KeyBytes:        mKeySize + vKeySize, // 2+12 =14
				KeyCount:        1,
				ValBytes:        mValSize + vValSize, // 44+10 = 54
				ValCount:        1,
				IntentCount:     1,
				IntentBytes:     vKeySize + vValSize, // 12+10 = 22
				GCBytesAge:      0,
			}
			assertEq(t, engine, "after put", aggMS, &expMS)

			// Now commit the intent, but with a timestamp gap (i.e. this is a
			// push-commit as it would happen for a SNAPSHOT txn)
			ts4 := hlc.Timestamp{WallTime: 4 * 1E9}
			txn.Status = roachpb.COMMITTED
			txn.Timestamp.Forward(ts4)
			if err := MVCCResolveWriteIntent(ctx, engine, aggMS, roachpb.Intent{
				Span: roachpb.Span{Key: key}, Status: txn.Status, Txn: txn.TxnMeta,
			}); err != nil {
				t.Fatal(err)
			}

			expAggMS := enginepb.MVCCStats{
				LastUpdateNanos: 4E9,
				LiveBytes:       mKeySize + vKeySize + vValSize, // 2+12+20 = 24
				LiveCount:       1,
				KeyCount:        1,
				ValCount:        1,
				// The implicit meta record counts for len("a")+1=2.
				// One versioned key counts for vKeySize.
				KeyBytes:   mKeySize + vKeySize,
				ValBytes:   vValSize,
				GCBytesAge: 0, // this was once erroneously negative
			}

			assertEq(t, engine, "after committing", aggMS, &expAggMS)
		})
	}
}

// TestMVCCStatsPutPushMovesTimestamp is similar to TestMVCCStatsPutCommitMovesTimestamp:
// An intent is written and then re-written at a higher timestamp. This formerly messed up
// the IntentAge computation.
func TestMVCCStatsPutPushMovesTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()

			ctx := context.Background()
			aggMS := &enginepb.MVCCStats{}

			assertEq(t, engine, "initially", aggMS, &enginepb.MVCCStats{})

			key := roachpb.Key("a")
			ts1 := hlc.Timestamp{WallTime: 1E9}
			txn := &roachpb.Transaction{
				TxnMeta:       enginepb.TxnMeta{ID: uuid.MakeV4(), Timestamp: ts1},
				OrigTimestamp: ts1,
			}
			// Write an intent.
			value := roachpb.MakeValueFromString("value")
			if err := MVCCPut(ctx, engine, aggMS, key, txn.OrigTimestamp, value, txn); err != nil {
				t.Fatal(err)
			}

			mKeySize := int64(mvccKey(key).EncodedSize()) // 2
			mValSize := int64((&enginepb.MVCCMetadata{    // 44
				Timestamp: hlc.LegacyTimestamp(ts1),
				Deleted:   false,
				Txn:       &txn.TxnMeta,
			}).Size())
			vKeySize := MVCCVersionTimestampSize   // 12
			vValSize := int64(len(value.RawBytes)) // 10

			expMS := enginepb.MVCCStats{
				LastUpdateNanos: 1E9,
				LiveBytes:       mKeySize + mValSize + vKeySize + vValSize, // 2+44+12+10 = 68
				LiveCount:       1,
				KeyBytes:        mKeySize + vKeySize, // 2+12 = 14
				KeyCount:        1,
				ValBytes:        mValSize + vValSize, // 44+10 = 54
				ValCount:        1,
				IntentAge:       0,
				IntentCount:     1,
				IntentBytes:     vKeySize + vValSize, // 12+10 = 22
			}
			assertEq(t, engine, "after put", aggMS, &expMS)

			// Now push the value, but with a timestamp gap (i.e. this is a
			// push as it would happen for a SNAPSHOT txn)
			ts4 := hlc.Timestamp{WallTime: 4 * 1E9}
			txn.Timestamp.Forward(ts4)
			if err := MVCCResolveWriteIntent(ctx, engine, aggMS, roachpb.Intent{
				Span: roachpb.Span{Key: key}, Status: txn.Status, Txn: txn.TxnMeta,
			}); err != nil {
				t.Fatal(err)
			}

			expAggMS := enginepb.MVCCStats{
				LastUpdateNanos: 4E9,
				LiveBytes:       mKeySize + mValSize + vKeySize + vValSize, // 2+44+12+20 = 78
				LiveCount:       1,
				KeyCount:        1,
				ValCount:        1,
				// The explicit meta record counts for len("a")+1=2.
				// One versioned key counts for vKeySize.
				KeyBytes: mKeySize + vKeySize,
				// The intent is still there, so we see mValSize.
				ValBytes:    vValSize + mValSize, // 44+10 = 54
				IntentAge:   0,                   // this was once erroneously positive
				IntentCount: 1,                   // still there
				IntentBytes: vKeySize + vValSize, // still there
			}

			assertEq(t, engine, "after pushing", aggMS, &expAggMS)
		})
	}
}

// TestMVCCStatsDeleteMovesTimestamp is similar to TestMVCCStatsPutCommitMovesTimestamp:
// An intent is written and then re-written at a higher timestamp. This formerly messed up
// the GCBytesAge computation.
func TestMVCCStatsDeleteMovesTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()

			ctx := context.Background()
			aggMS := &enginepb.MVCCStats{}

			assertEq(t, engine, "initially", aggMS, &enginepb.MVCCStats{})

			ts1 := hlc.Timestamp{WallTime: 1E9}
			ts2 := hlc.Timestamp{WallTime: 2 * 1E9}

			key := roachpb.Key("a")
			txn := &roachpb.Transaction{
				TxnMeta:       enginepb.TxnMeta{ID: uuid.MakeV4(), Timestamp: ts1},
				OrigTimestamp: ts1,
			}

			// Write an intent.
			value := roachpb.MakeValueFromString("value")
			if err := MVCCPut(ctx, engine, aggMS, key, txn.OrigTimestamp, value, txn); err != nil {
				t.Fatal(err)
			}

			mKeySize := int64(mvccKey(key).EncodedSize())
			require.EqualValues(t, mKeySize, 2)

			mVal1Size := int64((&enginepb.MVCCMetadata{
				Timestamp: hlc.LegacyTimestamp(ts1),
				Deleted:   false,
				Txn:       &txn.TxnMeta,
			}).Size())
			require.EqualValues(t, mVal1Size, 46)

			m1ValSize := int64((&enginepb.MVCCMetadata{
				Timestamp: hlc.LegacyTimestamp(ts2),
				Deleted:   false,
				Txn:       &txn.TxnMeta,
			}).Size())
			require.EqualValues(t, m1ValSize, 46)

			vKeySize := MVCCVersionTimestampSize
			require.EqualValues(t, vKeySize, 12)

			vValSize := int64(len(value.RawBytes))
			require.EqualValues(t, vValSize, 10)

			expMS := enginepb.MVCCStats{
				LastUpdateNanos: 1E9,
				LiveBytes:       mKeySize + m1ValSize + vKeySize + vValSize, // 2+44+12+10 = 68
				LiveCount:       1,
				KeyBytes:        mKeySize + vKeySize, // 2+12 = 14
				KeyCount:        1,
				ValBytes:        mVal1Size + vValSize, // 44+10 = 54
				ValCount:        1,
				IntentAge:       0,
				IntentCount:     1,
				IntentBytes:     vKeySize + vValSize, // 12+10 = 22
			}
			assertEq(t, engine, "after put", aggMS, &expMS)

			// Now replace our intent with a deletion intent, but with a timestamp gap.
			// This could happen if a transaction got restarted with a higher timestamp
			// and ran logic different from that in the first attempt.
			txn.Timestamp.Forward(ts2)

			txn.Sequence++

			// Annoyingly, the new meta value is actually a little larger thanks to the
			// sequence number. Also since there was a write previously on the same
			// transaction, the IntentHistory will add a few bytes to the metadata.
			m2ValSize := int64((&enginepb.MVCCMetadata{
				Timestamp: hlc.LegacyTimestamp(ts2),
				Txn:       &txn.TxnMeta,
				IntentHistory: []enginepb.MVCCMetadata_SequencedIntent{
					{Sequence: 0, Value: value.RawBytes},
				},
			}).Size())
			require.EqualValues(t, m2ValSize, 64)

			if err := MVCCDelete(ctx, engine, aggMS, key, txn.OrigTimestamp, txn); err != nil {
				t.Fatal(err)
			}

			expAggMS := enginepb.MVCCStats{
				LastUpdateNanos: 2E9,
				LiveBytes:       0,
				LiveCount:       0,
				KeyCount:        1,
				ValCount:        1,
				// The explicit meta record counts for len("a")+1=2.
				// One versioned key counts for vKeySize.
				KeyBytes: mKeySize + vKeySize,
				// The intent is still there, but this time with mVal2Size, and a zero vValSize.
				ValBytes:    m2ValSize, // 10+46 = 56
				IntentAge:   0,
				IntentCount: 1,        // still there
				IntentBytes: vKeySize, // still there, but now without vValSize
				GCBytesAge:  0,        // this was once erroneously negative
			}

			assertEq(t, engine, "after deleting", aggMS, &expAggMS)
		})
	}
}

// TestMVCCStatsPutMovesDeletionTimestamp is similar to TestMVCCStatsPutCommitMovesTimestamp: A
// tombstone intent is written and then replaced by a value intent at a higher timestamp. This
// formerly messed up the GCBytesAge computation.
func TestMVCCStatsPutMovesDeletionTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()

			ctx := context.Background()
			aggMS := &enginepb.MVCCStats{}

			assertEq(t, engine, "initially", aggMS, &enginepb.MVCCStats{})

			ts1 := hlc.Timestamp{WallTime: 1E9}
			ts2 := hlc.Timestamp{WallTime: 2 * 1E9}

			key := roachpb.Key("a")
			txn := &roachpb.Transaction{
				TxnMeta:       enginepb.TxnMeta{ID: uuid.MakeV4(), Timestamp: ts1},
				OrigTimestamp: ts1,
			}

			// Write a deletion tombstone intent.
			if err := MVCCDelete(ctx, engine, aggMS, key, txn.OrigTimestamp, txn); err != nil {
				t.Fatal(err)
			}

			value := roachpb.MakeValueFromString("value")

			mKeySize := int64(mvccKey(key).EncodedSize())
			require.EqualValues(t, mKeySize, 2)

			mVal1Size := int64((&enginepb.MVCCMetadata{
				Timestamp: hlc.LegacyTimestamp(ts1),
				Deleted:   false,
				Txn:       &txn.TxnMeta,
			}).Size())
			require.EqualValues(t, mVal1Size, 46)

			m1ValSize := int64((&enginepb.MVCCMetadata{
				Timestamp: hlc.LegacyTimestamp(ts2),
				Deleted:   false,
				Txn:       &txn.TxnMeta,
			}).Size())
			require.EqualValues(t, m1ValSize, 46)

			vKeySize := MVCCVersionTimestampSize
			require.EqualValues(t, vKeySize, 12)

			vValSize := int64(len(value.RawBytes))
			require.EqualValues(t, vValSize, 10)

			expMS := enginepb.MVCCStats{
				LastUpdateNanos: 1E9,
				LiveBytes:       0,
				LiveCount:       0,
				KeyBytes:        mKeySize + vKeySize, // 2 + 12 = 24
				KeyCount:        1,
				ValBytes:        mVal1Size, // 44
				ValCount:        1,
				IntentAge:       0,
				IntentCount:     1,
				IntentBytes:     vKeySize, // 12
				GCBytesAge:      0,
			}
			assertEq(t, engine, "after delete", aggMS, &expMS)

			// Now replace our deletion with a value intent, but with a timestamp gap.
			// This could happen if a transaction got restarted with a higher timestamp
			// and ran logic different from that in the first attempt.
			txn.Timestamp.Forward(ts2)

			txn.Sequence++

			// Annoyingly, the new meta value is actually a little larger thanks to the
			// sequence number. Also the value is larger because the previous intent on the
			// transaction is recorded in the IntentHistory.
			m2ValSize := int64((&enginepb.MVCCMetadata{
				Timestamp: hlc.LegacyTimestamp(ts2),
				Txn:       &txn.TxnMeta,
				IntentHistory: []enginepb.MVCCMetadata_SequencedIntent{
					{Sequence: 0, Value: []byte{}},
				},
			}).Size())
			require.EqualValues(t, m2ValSize, 54)

			if err := MVCCPut(ctx, engine, aggMS, key, txn.OrigTimestamp, value, txn); err != nil {
				t.Fatal(err)
			}

			expAggMS := enginepb.MVCCStats{
				LastUpdateNanos: 2E9,
				LiveBytes:       mKeySize + m2ValSize + vKeySize + vValSize, // 2+46+12+10 = 70
				LiveCount:       1,
				KeyCount:        1,
				ValCount:        1,
				// The explicit meta record counts for len("a")+1=2.
				// One versioned key counts for vKeySize.
				KeyBytes: mKeySize + vKeySize,
				// The intent is still there, but this time with mVal2Size, and a zero vValSize.
				ValBytes:    vValSize + m2ValSize, // 10+46 = 56
				IntentAge:   0,
				IntentCount: 1,                   // still there
				IntentBytes: vKeySize + vValSize, // still there, now bigger
				GCBytesAge:  0,                   // this was once erroneously negative
			}

			assertEq(t, engine, "after put", aggMS, &expAggMS)
		})
	}
}

// TestMVCCStatsDelDelCommit writes a non-transactional tombstone, and then adds an intent tombstone
// on top that is then committed or aborted at a higher timestamp. This random-looking sequence was
// the smallest failing example that highlighted a stats computation error once, and exercises a
// code path in which MVCCResolveIntent has to read the pre-intent value in order to arrive at the
// correct stats.
func TestMVCCStatsDelDelCommitMovesTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()

			ctx := context.Background()
			aggMS := &enginepb.MVCCStats{}

			assertEq(t, engine, "initially", aggMS, &enginepb.MVCCStats{})

			key := roachpb.Key("a")

			ts1 := hlc.Timestamp{WallTime: 1E9}
			ts2 := hlc.Timestamp{WallTime: 2E9}
			ts3 := hlc.Timestamp{WallTime: 3E9}

			// Write a non-transactional tombstone at t=1s.
			if err := MVCCDelete(ctx, engine, aggMS, key, ts1, nil /* txn */); err != nil {
				t.Fatal(err)
			}

			mKeySize := int64(mvccKey(key).EncodedSize())
			require.EqualValues(t, mKeySize, 2)
			vKeySize := MVCCVersionTimestampSize
			require.EqualValues(t, vKeySize, 12)

			expMS := enginepb.MVCCStats{
				LastUpdateNanos: 1E9,
				KeyBytes:        mKeySize + vKeySize,
				KeyCount:        1,
				ValBytes:        0,
				ValCount:        1,
			}

			assertEq(t, engine, "after non-transactional delete", aggMS, &expMS)

			// Write an tombstone intent at t=2s.
			txn := &roachpb.Transaction{
				TxnMeta:       enginepb.TxnMeta{ID: uuid.MakeV4(), Timestamp: ts2},
				OrigTimestamp: ts2,
			}
			if err := MVCCDelete(ctx, engine, aggMS, key, txn.OrigTimestamp, txn); err != nil {
				t.Fatal(err)
			}

			mValSize := int64((&enginepb.MVCCMetadata{
				Timestamp: hlc.LegacyTimestamp(ts1),
				Deleted:   true,
				Txn:       &txn.TxnMeta,
			}).Size())
			require.EqualValues(t, mValSize, 46)

			expMS = enginepb.MVCCStats{
				LastUpdateNanos: 2E9,
				KeyBytes:        mKeySize + 2*vKeySize, // 2+2*12 = 26
				KeyCount:        1,
				ValBytes:        mValSize, // 44
				ValCount:        2,
				IntentCount:     1,
				IntentBytes:     vKeySize, // TBD
				// The original non-transactional write (at 1s) has now aged one second.
				GCBytesAge: 1 * vKeySize,
			}
			assertEq(t, engine, "after put", aggMS, &expMS)

			// Now commit or abort the intent, respectively, but with a timestamp gap
			// (i.e. this is a push-commit as it would happen for a SNAPSHOT txn).
			t.Run("Commit", func(t *testing.T) {
				aggMS := *aggMS
				engine := engine.NewBatch()
				defer engine.Close()

				txnCommit := txn.Clone()
				txnCommit.Status = roachpb.COMMITTED
				txnCommit.Timestamp.Forward(ts3)
				if err := MVCCResolveWriteIntent(ctx, engine, &aggMS, roachpb.Intent{
					Span: roachpb.Span{Key: key}, Status: txnCommit.Status, Txn: txnCommit.TxnMeta,
				}); err != nil {
					t.Fatal(err)
				}

				expAggMS := enginepb.MVCCStats{
					LastUpdateNanos: 3E9,
					KeyBytes:        mKeySize + 2*vKeySize, // 2+2*12 = 26
					KeyCount:        1,
					ValBytes:        0,
					ValCount:        2,
					IntentCount:     0,
					IntentBytes:     0,
					// The very first write picks up another second of age. Before a bug fix,
					// this was failing to do so.
					GCBytesAge: 2 * vKeySize,
				}

				assertEq(t, engine, "after committing", &aggMS, &expAggMS)
			})
			t.Run("Abort", func(t *testing.T) {
				aggMS := *aggMS
				engine := engine.NewBatch()
				defer engine.Close()

				txnAbort := txn.Clone()
				txnAbort.Status = roachpb.ABORTED
				txnAbort.Timestamp.Forward(ts3)
				if err := MVCCResolveWriteIntent(ctx, engine, &aggMS, roachpb.Intent{
					Span: roachpb.Span{Key: key}, Status: txnAbort.Status, Txn: txnAbort.TxnMeta,
				}); err != nil {
					t.Fatal(err)
				}

				expAggMS := enginepb.MVCCStats{
					LastUpdateNanos: 3E9,
					KeyBytes:        mKeySize + vKeySize, // 2+12 = 14
					KeyCount:        1,
					ValBytes:        0,
					ValCount:        1,
					IntentCount:     0,
					IntentBytes:     0,
					// We aborted our intent, but the value we first wrote was a tombstone, and
					// so it's expected to retain its age. Since it's now the only value, it
					// also contributes as a meta key.
					GCBytesAge: 2 * (mKeySize + vKeySize),
				}

				assertEq(t, engine, "after aborting", &aggMS, &expAggMS)
			})
		})
	}
}

// TestMVCCStatsPutDelPut is similar to TestMVCCStatsDelDelCommit, but its first
// non-transactional write is not a tombstone but a real value. This exercises a
// different code path as a tombstone starts accruing GCBytesAge at its own timestamp,
// but values only when the next value is written, which makes the computation tricky
// when that next value is an intent that changes its timestamp before committing.
// Finishing the sequence with a Put in particular exercises a case in which the
// final correction is done in the put path and not the commit path.
func TestMVCCStatsPutDelPutMovesTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()

			ctx := context.Background()
			aggMS := &enginepb.MVCCStats{}

			assertEq(t, engine, "initially", aggMS, &enginepb.MVCCStats{})

			key := roachpb.Key("a")

			ts1 := hlc.Timestamp{WallTime: 1E9}
			ts2 := hlc.Timestamp{WallTime: 2E9}
			ts3 := hlc.Timestamp{WallTime: 3E9}

			// Write a non-transactional value at t=1s.
			value := roachpb.MakeValueFromString("value")
			if err := MVCCPut(ctx, engine, aggMS, key, ts1, value, nil /* txn */); err != nil {
				t.Fatal(err)
			}

			mKeySize := int64(mvccKey(key).EncodedSize())
			require.EqualValues(t, mKeySize, 2)

			vKeySize := MVCCVersionTimestampSize
			require.EqualValues(t, vKeySize, 12)

			vValSize := int64(len(value.RawBytes))
			require.EqualValues(t, vValSize, 10)

			expMS := enginepb.MVCCStats{
				LastUpdateNanos: 1E9,
				KeyBytes:        mKeySize + vKeySize,
				KeyCount:        1,
				ValBytes:        vValSize,
				ValCount:        1,
				LiveBytes:       mKeySize + vKeySize + vValSize,
				LiveCount:       1,
			}

			assertEq(t, engine, "after non-transactional put", aggMS, &expMS)

			// Write a tombstone intent at t=2s.
			txn := &roachpb.Transaction{
				TxnMeta:       enginepb.TxnMeta{ID: uuid.MakeV4(), Timestamp: ts2},
				OrigTimestamp: ts2,
			}
			if err := MVCCDelete(ctx, engine, aggMS, key, txn.OrigTimestamp, txn); err != nil {
				t.Fatal(err)
			}

			mValSize := int64((&enginepb.MVCCMetadata{
				Timestamp: hlc.LegacyTimestamp(ts1),
				Deleted:   true,
				Txn:       &txn.TxnMeta,
			}).Size())
			require.EqualValues(t, mValSize, 46)

			expMS = enginepb.MVCCStats{
				LastUpdateNanos: 2E9,
				KeyBytes:        mKeySize + 2*vKeySize, // 2+2*12 = 26
				KeyCount:        1,
				ValBytes:        mValSize + vValSize, // 44+10 = 56
				ValCount:        2,
				IntentCount:     1,
				IntentBytes:     vKeySize, // 12
				// The original non-transactional write becomes non-live at 2s, so no age
				// is accrued yet.
				GCBytesAge: 0,
			}
			assertEq(t, engine, "after txn delete", aggMS, &expMS)

			// Now commit or abort the intent, but with a timestamp gap (i.e. this is a push-commit as it
			// would happen for a SNAPSHOT txn)

			txn.Timestamp.Forward(ts3)
			txn.Sequence++

			// Annoyingly, the new meta value is actually a little larger thanks to the
			// sequence number.
			m2ValSize := int64((&enginepb.MVCCMetadata{
				Timestamp: hlc.LegacyTimestamp(ts3),
				Txn:       &txn.TxnMeta,
			}).Size())

			require.EqualValues(t, m2ValSize, 48)

			t.Run("Abort", func(t *testing.T) {
				aggMS := *aggMS
				engine := engine.NewBatch()
				defer engine.Close()

				txnAbort := txn.Clone()
				txnAbort.Status = roachpb.ABORTED // doesn't change m2ValSize, fortunately
				if err := MVCCResolveWriteIntent(ctx, engine, &aggMS, roachpb.Intent{
					Span: roachpb.Span{Key: key}, Status: txnAbort.Status, Txn: txnAbort.TxnMeta,
				}); err != nil {
					t.Fatal(err)
				}

				expAggMS := enginepb.MVCCStats{
					LastUpdateNanos: 3E9,
					KeyBytes:        mKeySize + vKeySize,
					KeyCount:        1,
					ValBytes:        vValSize,
					ValCount:        1,
					LiveCount:       1,
					LiveBytes:       mKeySize + vKeySize + vValSize,
					IntentCount:     0,
					IntentBytes:     0,
					// The original value is visible again, so no GCBytesAge is present. Verifying this is the
					// main point of this test (to prevent regression of a bug).
					GCBytesAge: 0,
				}
				assertEq(t, engine, "after abort", &aggMS, &expAggMS)
			})
			t.Run("Put", func(t *testing.T) {
				aggMS := *aggMS
				engine := engine.NewBatch()
				defer engine.Close()

				val2 := roachpb.MakeValueFromString("longvalue")
				vVal2Size := int64(len(val2.RawBytes))
				require.EqualValues(t, vVal2Size, 14)

				txn.Timestamp.Forward(ts3)
				if err := MVCCPut(ctx, engine, &aggMS, key, txn.OrigTimestamp, val2, txn); err != nil {
					t.Fatal(err)
				}

				// Annoyingly, the new meta value is actually a little larger thanks to the
				// sequence number.
				m2ValSizeWithHistory := int64((&enginepb.MVCCMetadata{
					Timestamp: hlc.LegacyTimestamp(ts3),
					Txn:       &txn.TxnMeta,
					IntentHistory: []enginepb.MVCCMetadata_SequencedIntent{
						{Sequence: 0, Value: []byte{}},
					},
				}).Size())

				require.EqualValues(t, m2ValSizeWithHistory, 54)

				expAggMS := enginepb.MVCCStats{
					LastUpdateNanos: 3E9,
					KeyBytes:        mKeySize + 2*vKeySize, // 2+2*12 = 26
					KeyCount:        1,
					ValBytes:        m2ValSizeWithHistory + vValSize + vVal2Size,
					ValCount:        2,
					LiveCount:       1,
					LiveBytes:       mKeySize + m2ValSizeWithHistory + vKeySize + vVal2Size,
					IntentCount:     1,
					IntentBytes:     vKeySize + vVal2Size,
					// The original write was previously non-live at 2s because that's where the
					// intent originally lived. But the intent has moved to 3s, and so has the
					// moment in time at which the shadowed put became non-live; it's now 3s as
					// well, so there's no contribution yet.
					GCBytesAge: 0,
				}
				assertEq(t, engine, "after txn put", &aggMS, &expAggMS)
			})
		})
	}
}

// TestMVCCStatsDelDelGC prevents regression of a bug in MVCCGarbageCollect
// that was exercised by running two deletions followed by a specific GC.
func TestMVCCStatsDelDelGC(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()

			ctx := context.Background()
			aggMS := &enginepb.MVCCStats{}

			assertEq(t, engine, "initially", aggMS, &enginepb.MVCCStats{})

			key := roachpb.Key("a")
			ts1 := hlc.Timestamp{WallTime: 1E9}
			ts2 := hlc.Timestamp{WallTime: 2E9}

			// Write tombstones at ts1 and ts2.
			if err := MVCCDelete(ctx, engine, aggMS, key, ts1, nil); err != nil {
				t.Fatal(err)
			}
			if err := MVCCDelete(ctx, engine, aggMS, key, ts2, nil); err != nil {
				t.Fatal(err)
			}

			mKeySize := int64(mvccKey(key).EncodedSize()) // 2
			vKeySize := MVCCVersionTimestampSize          // 12

			expMS := enginepb.MVCCStats{
				LastUpdateNanos: 2E9,
				KeyBytes:        mKeySize + 2*vKeySize, // 26
				KeyCount:        1,
				ValCount:        2,
				GCBytesAge:      1 * vKeySize, // first tombstone, aged from ts1 to ts2
			}
			assertEq(t, engine, "after two puts", aggMS, &expMS)

			// Run a GC invocation that clears it all. There used to be a bug here when
			// we allowed limiting the number of deleted keys. Passing zero (i.e. remove
			// one key and then bail) would mess up the stats, since the implementation
			// would assume that the (implicit or explicit) meta entry was going to be
			// removed, but this is only true when all values actually go away.
			if err := MVCCGarbageCollect(
				ctx,
				engine,
				aggMS,
				[]roachpb.GCRequest_GCKey{{
					Key:       key,
					Timestamp: ts2,
				}},
				ts2,
			); err != nil {
				t.Fatal(err)
			}

			expAggMS := enginepb.MVCCStats{
				LastUpdateNanos: 2E9,
			}

			assertEq(t, engine, "after GC", aggMS, &expAggMS)
		})
	}
}

// TestMVCCStatsPutIntentTimestampNotPutTimestamp exercises a scenario in which
// an intent is rewritten to a lower timestamp. This formerly caused bugs
// because when computing the stats updates, there was an implicit assumption
// that the meta entries would always move forward in time.
// UPDATE: since there should be no way for a txn to write older intents,
//   mvccPutInternal now makes sure that writes are always done at the most
//   recent intent timestamp within the same txn. Note that this case occurs
//   when the txn timestamp is moved forward due to a write too old condition,
//   which writes the first intent at a higher timestamp. We don't allow the
//   second intent to then be written at a lower timestamp, because that breaks
//   the contract that the intent is always the newest version.
//   This test now merely verifies that even when we try to write an older
//   version, we're upgraded to write the MVCCMetadata.Timestamp.
func TestMVCCStatsPutIntentTimestampNotPutTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()

			ctx := context.Background()
			aggMS := &enginepb.MVCCStats{}

			assertEq(t, engine, "initially", aggMS, &enginepb.MVCCStats{})

			key := roachpb.Key("a")
			ts201 := hlc.Timestamp{WallTime: 2E9 + 1}
			ts099 := hlc.Timestamp{WallTime: 1E9 - 1}
			txn := &roachpb.Transaction{
				TxnMeta:       enginepb.TxnMeta{ID: uuid.MakeV4(), Timestamp: ts201},
				OrigTimestamp: ts099,
			}
			// Write an intent at 2s+1.
			value := roachpb.MakeValueFromString("value")
			if err := MVCCPut(ctx, engine, aggMS, key, txn.OrigTimestamp, value, txn); err != nil {
				t.Fatal(err)
			}

			mKeySize := int64(mvccKey(key).EncodedSize()) // 2
			m1ValSize := int64((&enginepb.MVCCMetadata{   // 44
				Timestamp: hlc.LegacyTimestamp(ts201),
				Txn:       &txn.TxnMeta,
			}).Size())
			vKeySize := MVCCVersionTimestampSize   // 12
			vValSize := int64(len(value.RawBytes)) // 10

			expMS := enginepb.MVCCStats{
				LastUpdateNanos: 2E9 + 1,
				LiveBytes:       mKeySize + m1ValSize + vKeySize + vValSize, // 2+44+12+10 = 68
				LiveCount:       1,
				KeyBytes:        mKeySize + vKeySize, // 14
				KeyCount:        1,
				ValBytes:        m1ValSize + vValSize, // 44+10 = 54
				ValCount:        1,
				IntentCount:     1,
				IntentBytes:     vKeySize + vValSize, // 12+10 = 22
			}
			assertEq(t, engine, "after first put", aggMS, &expMS)

			// Replace the intent with an identical one, but we write it at 1s-1 now. If
			// you're confused, don't worry. There are two timestamps here: the one in
			// the txn (which is, perhaps surprisingly, only really used when
			// committing/aborting intents), and the timestamp passed directly to
			// MVCCPut (which is where the intent will actually end up being written at,
			// and which usually corresponds to txn.OrigTimestamp).
			txn.Sequence++
			txn.Timestamp = ts099

			// Annoyingly, the new meta value is actually a little larger thanks to the
			// sequence number.
			m2ValSize := int64((&enginepb.MVCCMetadata{ // 46
				Timestamp: hlc.LegacyTimestamp(ts201),
				Txn:       &txn.TxnMeta,
				IntentHistory: []enginepb.MVCCMetadata_SequencedIntent{
					{Sequence: 0, Value: value.RawBytes},
				},
			}).Size())
			if err := MVCCPut(ctx, engine, aggMS, key, txn.OrigTimestamp, value, txn); err != nil {
				t.Fatal(err)
			}

			expAggMS := enginepb.MVCCStats{
				// Even though we tried to put a new intent at an older timestamp, it
				// will have been written at 2E9+1, so the age will be 0.
				IntentAge: 0,

				LastUpdateNanos: 2E9 + 1,
				LiveBytes:       mKeySize + m2ValSize + vKeySize + vValSize, // 2+46+12+10 = 70
				LiveCount:       1,
				KeyBytes:        mKeySize + vKeySize, // 14
				KeyCount:        1,
				ValBytes:        m2ValSize + vValSize, // 46+10 = 56
				ValCount:        1,
				IntentCount:     1,
				IntentBytes:     vKeySize + vValSize, // 12+10 = 22
			}

			assertEq(t, engine, "after second put", aggMS, &expAggMS)
		})
	}
}

// TestMVCCStatsPutWaitDeleteGC puts a value, deletes it, and runs a GC that
// deletes the original write, but not the deletion tombstone.
func TestMVCCStatsPutWaitDeleteGC(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()

			ctx := context.Background()
			aggMS := &enginepb.MVCCStats{}

			assertEq(t, engine, "initially", aggMS, &enginepb.MVCCStats{})

			key := roachpb.Key("a")

			ts1 := hlc.Timestamp{WallTime: 1E9}
			ts2 := hlc.Timestamp{WallTime: 2E9}

			// Write a value at ts1.
			val1 := roachpb.MakeValueFromString("value")
			if err := MVCCPut(ctx, engine, aggMS, key, ts1, val1, nil /* txn */); err != nil {
				t.Fatal(err)
			}

			mKeySize := int64(mvccKey(key).EncodedSize())
			require.EqualValues(t, mKeySize, 2)

			vKeySize := MVCCVersionTimestampSize
			require.EqualValues(t, vKeySize, 12)

			vValSize := int64(len(val1.RawBytes))
			require.EqualValues(t, vValSize, 10)

			expMS := enginepb.MVCCStats{
				LastUpdateNanos: 1E9,
				KeyCount:        1,
				KeyBytes:        mKeySize + vKeySize, // 2+12 = 14
				ValCount:        1,
				ValBytes:        vValSize, // 10
				LiveCount:       1,
				LiveBytes:       mKeySize + vKeySize + vValSize, // 2+12+10 = 24
			}
			assertEq(t, engine, "after first put", aggMS, &expMS)

			// Delete the value at ts5.

			if err := MVCCDelete(ctx, engine, aggMS, key, ts2, nil /* txn */); err != nil {
				t.Fatal(err)
			}

			expMS = enginepb.MVCCStats{
				LastUpdateNanos: 2E9,
				KeyCount:        1,
				KeyBytes:        mKeySize + 2*vKeySize, // 2+2*12 = 26
				ValBytes:        vValSize,              // 10
				ValCount:        2,
				LiveBytes:       0,
				LiveCount:       0,
				GCBytesAge:      0, // before a fix, this was vKeySize + vValSize
			}

			assertEq(t, engine, "after delete", aggMS, &expMS)

			if err := MVCCGarbageCollect(ctx, engine, aggMS, []roachpb.GCRequest_GCKey{{
				Key:       key,
				Timestamp: ts1,
			}}, ts2); err != nil {
				t.Fatal(err)
			}

			expMS = enginepb.MVCCStats{
				LastUpdateNanos: 2E9,
				KeyCount:        1,
				KeyBytes:        mKeySize + vKeySize, // 2+12 = 14
				ValBytes:        0,
				ValCount:        1,
				LiveBytes:       0,
				LiveCount:       0,
				GCBytesAge:      0, // before a fix, this was vKeySize + vValSize
			}

			assertEq(t, engine, "after GC", aggMS, &expMS)
		})
	}
}

// TestMVCCStatsSysTxnPutPut prevents regression of a bug that, when rewriting an intent
// on a sys key, would lead to overcounting `ms.SysBytes`.
func TestMVCCStatsTxnSysPutPut(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()

			ctx := context.Background()
			aggMS := &enginepb.MVCCStats{}

			assertEq(t, engine, "initially", aggMS, &enginepb.MVCCStats{})

			key := keys.RangeDescriptorKey(roachpb.RKey("a"))

			ts1 := hlc.Timestamp{WallTime: 1E9}
			ts2 := hlc.Timestamp{WallTime: 2E9}

			txn := &roachpb.Transaction{
				TxnMeta:       enginepb.TxnMeta{ID: uuid.MakeV4(), Timestamp: ts1},
				OrigTimestamp: ts1,
			}

			// Write an intent at ts1.
			val1 := roachpb.MakeValueFromString("value")
			if err := MVCCPut(ctx, engine, aggMS, key, txn.OrigTimestamp, val1, txn); err != nil {
				t.Fatal(err)
			}

			mKeySize := int64(mvccKey(key).EncodedSize())
			require.EqualValues(t, mKeySize, 11)

			mValSize := int64((&enginepb.MVCCMetadata{
				Timestamp: hlc.LegacyTimestamp(ts1),
				Deleted:   false,
				Txn:       &txn.TxnMeta,
			}).Size())
			require.EqualValues(t, mValSize, 46)

			vKeySize := MVCCVersionTimestampSize
			require.EqualValues(t, vKeySize, 12)

			vVal1Size := int64(len(val1.RawBytes))
			require.EqualValues(t, vVal1Size, 10)

			val2 := roachpb.MakeValueFromString("longvalue")
			vVal2Size := int64(len(val2.RawBytes))
			require.EqualValues(t, vVal2Size, 14)

			expMS := enginepb.MVCCStats{
				LastUpdateNanos: 1E9,
				SysBytes:        mKeySize + mValSize + vKeySize + vVal1Size, // 11+44+12+10 = 77
				SysCount:        1,
			}
			assertEq(t, engine, "after first put", aggMS, &expMS)

			// Rewrite the intent to ts2 with a different value.
			txn.Timestamp.Forward(ts2)
			txn.Sequence++

			// The new meta value grows because we've bumped `txn.Sequence`.
			// The value also grows as the older value is part of the same
			// transaction and so contributes to the intent history.
			mVal2Size := int64((&enginepb.MVCCMetadata{
				Timestamp: hlc.LegacyTimestamp(ts2),
				Deleted:   false,
				Txn:       &txn.TxnMeta,
				IntentHistory: []enginepb.MVCCMetadata_SequencedIntent{
					{Sequence: 0, Value: val1.RawBytes},
				},
			}).Size())
			require.EqualValues(t, mVal2Size, 64)

			if err := MVCCPut(ctx, engine, aggMS, key, txn.OrigTimestamp, val2, txn); err != nil {
				t.Fatal(err)
			}

			expMS = enginepb.MVCCStats{
				LastUpdateNanos: 1E9,
				SysBytes:        mKeySize + mVal2Size + vKeySize + vVal2Size, // 11+46+12+14 = 83
				SysCount:        1,
			}

			assertEq(t, engine, "after intent rewrite", aggMS, &expMS)
		})
	}
}

// TestMVCCStatsTxnSysPutAbort prevents regression of a bug that, when aborting
// an intent on a sys key, would lead to undercounting `ms.IntentBytes` and
// `ms.IntentCount`.
func TestMVCCStatsTxnSysPutAbort(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()

			ctx := context.Background()
			aggMS := &enginepb.MVCCStats{}

			assertEq(t, engine, "initially", aggMS, &enginepb.MVCCStats{})

			key := keys.RangeDescriptorKey(roachpb.RKey("a"))

			ts1 := hlc.Timestamp{WallTime: 1E9}
			txn := &roachpb.Transaction{
				TxnMeta:       enginepb.TxnMeta{ID: uuid.MakeV4(), Timestamp: ts1},
				OrigTimestamp: ts1,
			}

			// Write a system intent at ts1.
			val1 := roachpb.MakeValueFromString("value")
			if err := MVCCPut(ctx, engine, aggMS, key, txn.OrigTimestamp, val1, txn); err != nil {
				t.Fatal(err)
			}

			mKeySize := int64(mvccKey(key).EncodedSize())
			require.EqualValues(t, mKeySize, 11)

			mValSize := int64((&enginepb.MVCCMetadata{
				Timestamp: hlc.LegacyTimestamp(ts1),
				Deleted:   false,
				Txn:       &txn.TxnMeta,
			}).Size())
			require.EqualValues(t, mValSize, 46)

			vKeySize := MVCCVersionTimestampSize
			require.EqualValues(t, vKeySize, 12)

			vVal1Size := int64(len(val1.RawBytes))
			require.EqualValues(t, vVal1Size, 10)

			val2 := roachpb.MakeValueFromString("longvalue")
			vVal2Size := int64(len(val2.RawBytes))
			require.EqualValues(t, vVal2Size, 14)

			expMS := enginepb.MVCCStats{
				LastUpdateNanos: 1E9,
				SysBytes:        mKeySize + mValSize + vKeySize + vVal1Size, // 11+44+12+10 = 77
				SysCount:        1,
			}
			assertEq(t, engine, "after first put", aggMS, &expMS)

			// Now abort the intent.
			txn.Status = roachpb.ABORTED
			if err := MVCCResolveWriteIntent(ctx, engine, aggMS, roachpb.Intent{
				Span: roachpb.Span{Key: key}, Status: txn.Status, Txn: txn.TxnMeta,
			}); err != nil {
				t.Fatal(err)
			}

			expMS = enginepb.MVCCStats{
				LastUpdateNanos: 1E9,
			}
			assertEq(t, engine, "after aborting", aggMS, &expMS)
		})
	}
}

// TestMVCCStatsSysPutPut prevents regression of a bug that, when writing a new
// value on top of an existing system key, would undercount.
func TestMVCCStatsSysPutPut(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()

			ctx := context.Background()
			aggMS := &enginepb.MVCCStats{}

			assertEq(t, engine, "initially", aggMS, &enginepb.MVCCStats{})

			key := keys.RangeDescriptorKey(roachpb.RKey("a"))

			ts1 := hlc.Timestamp{WallTime: 1E9}
			ts2 := hlc.Timestamp{WallTime: 2E9}

			// Write a value at ts1.
			val1 := roachpb.MakeValueFromString("value")
			if err := MVCCPut(ctx, engine, aggMS, key, ts1, val1, nil /* txn */); err != nil {
				t.Fatal(err)
			}

			mKeySize := int64(mvccKey(key).EncodedSize())
			require.EqualValues(t, mKeySize, 11)

			vKeySize := MVCCVersionTimestampSize
			require.EqualValues(t, vKeySize, 12)

			vVal1Size := int64(len(val1.RawBytes))
			require.EqualValues(t, vVal1Size, 10)

			val2 := roachpb.MakeValueFromString("longvalue")
			vVal2Size := int64(len(val2.RawBytes))
			require.EqualValues(t, vVal2Size, 14)

			expMS := enginepb.MVCCStats{
				LastUpdateNanos: 1E9,
				SysBytes:        mKeySize + vKeySize + vVal1Size, // 11+12+10 = 33
				SysCount:        1,
			}
			assertEq(t, engine, "after first put", aggMS, &expMS)

			// Put another value at ts2.

			if err := MVCCPut(ctx, engine, aggMS, key, ts2, val2, nil /* txn */); err != nil {
				t.Fatal(err)
			}

			expMS = enginepb.MVCCStats{
				LastUpdateNanos: 1E9,
				SysBytes:        mKeySize + 2*vKeySize + vVal1Size + vVal2Size,
				SysCount:        1,
			}

			assertEq(t, engine, "after second put", aggMS, &expMS)
		})
	}
}

var mvccStatsTests = []struct {
	name string
	fn   func(Iterator, MVCCKey, MVCCKey, int64) (enginepb.MVCCStats, error)
}{
	{
		name: "ComputeStats",
		fn: func(iter Iterator, start, end MVCCKey, nowNanos int64) (enginepb.MVCCStats, error) {
			return iter.ComputeStats(start, end, nowNanos)
		},
	},
	{
		name: "ComputeStatsGo",
		fn: func(iter Iterator, start, end MVCCKey, nowNanos int64) (enginepb.MVCCStats, error) {
			return ComputeStatsGo(iter, start, end, nowNanos)
		},
	},
}

type state struct {
	MS  *enginepb.MVCCStats
	TS  hlc.Timestamp
	Txn *roachpb.Transaction

	eng Engine
	rng *rand.Rand
	key roachpb.Key
}

func (s *state) intent(status roachpb.TransactionStatus) roachpb.Intent {
	return roachpb.Intent{
		Span:   roachpb.Span{Key: s.key},
		Txn:    s.Txn.TxnMeta,
		Status: status,
	}
}

func (s *state) intentRange(status roachpb.TransactionStatus) roachpb.Intent {
	return roachpb.Intent{
		Span:   roachpb.Span{Key: roachpb.KeyMin, EndKey: roachpb.KeyMax},
		Txn:    s.Txn.TxnMeta,
		Status: status,
	}
}

func (s *state) rngVal() roachpb.Value {
	return roachpb.MakeValueFromBytes(randutil.RandBytes(s.rng, int(s.rng.Int31n(128))))
}

type randomTest struct {
	state

	inline      bool
	actions     map[string]func(*state) string
	actionNames []string // auto-populated
	cycle       int
}

func (s *randomTest) step(t *testing.T) {
	if !s.inline {
		// Jump up to a few seconds into the future. In ~1% of cases, jump
		// backwards instead (this exercises intactness on WriteTooOld, etc).
		s.TS = hlc.Timestamp{
			WallTime: s.TS.WallTime + int64((s.state.rng.Float32()-0.01)*4E9),
			Logical:  int32(s.rng.Intn(10)),
		}
		if s.TS.WallTime < 0 {
			// See TestMVCCStatsDocumentNegativeWrites. Negative MVCC timestamps
			// aren't something we're interested in, and besides, they corrupt
			// everything.
			s.TS.WallTime = 0
		}
	} else {
		s.TS = hlc.Timestamp{}
	}

	restart := s.Txn != nil && s.rng.Intn(2) == 0
	if restart {
		// TODO(tschottdorf): experiment with s.TS != s.Txn.TS. Which of those
		// cases are reasonable and which should we catch and error out?
		//
		// Note that we already exercise txn.Timestamp > s.TS since we call
		// Forward() here (while s.TS may move backwards).
		s.Txn.Restart(0, 0, s.TS)
	}
	s.cycle++

	if s.actionNames == nil {
		for name := range s.actions {
			s.actionNames = append(s.actionNames, name)
		}
		sort.Strings(s.actionNames)
	}
	actName := s.actionNames[s.rng.Intn(len(s.actionNames))]

	preTxn := s.Txn
	info := s.actions[actName](&s.state)

	txnS := "<none>"
	if preTxn != nil {
		txnS = preTxn.Timestamp.String()
	}

	if info != "" {
		info = "\n\t" + info
	}
	log.Infof(context.Background(), "%10s %s txn=%s%s", s.TS, actName, txnS, info)

	// Verify stats agree with recomputations.
	assertEq(t, s.eng, fmt.Sprintf("cycle %d", s.cycle), s.MS, s.MS)

	if t.Failed() {
		t.FailNow()
	}
}

func TestMVCCStatsRandomized(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	// NB: no failure type ever required count five or more. When there is a result
	// found by this test, or any other MVCC code is changed, it's worth reducing
	// this first to two, three, ... and running the test for a minute to get a
	// good idea of minimally reproducing examples.
	const count = 200

	actions := make(map[string]func(*state) string)

	actions["Put"] = func(s *state) string {
		if err := MVCCPut(ctx, s.eng, s.MS, s.key, s.TS, s.rngVal(), s.Txn); err != nil {
			return err.Error()
		}
		return ""
	}
	actions["InitPut"] = func(s *state) string {
		failOnTombstones := (s.rng.Intn(2) == 0)
		desc := fmt.Sprintf("failOnTombstones=%t", failOnTombstones)
		if err := MVCCInitPut(ctx, s.eng, s.MS, s.key, s.TS, s.rngVal(), failOnTombstones, s.Txn); err != nil {
			return desc + ": " + err.Error()
		}
		return desc
	}
	actions["Del"] = func(s *state) string {
		if err := MVCCDelete(ctx, s.eng, s.MS, s.key, s.TS, s.Txn); err != nil {
			return err.Error()
		}
		return ""
	}
	actions["DelRange"] = func(s *state) string {
		returnKeys := (s.rng.Intn(2) == 0)
		max := s.rng.Int63n(5)
		desc := fmt.Sprintf("returnKeys=%t, max=%d", returnKeys, max)
		if _, _, _, err := MVCCDeleteRange(ctx, s.eng, s.MS, roachpb.KeyMin, roachpb.KeyMax, max, s.TS, s.Txn, returnKeys); err != nil {
			return desc + ": " + err.Error()
		}
		return desc
	}
	actions["EnsureTxn"] = func(s *state) string {
		if s.Txn == nil {
			txn := roachpb.MakeTransaction("test", nil, 0, s.TS, 0)
			s.Txn = &txn
		}
		return ""
	}

	resolve := func(s *state, status roachpb.TransactionStatus) string {
		ranged := s.rng.Intn(2) == 0
		desc := fmt.Sprintf("ranged=%t", ranged)
		if s.Txn != nil {
			if !ranged {
				if err := MVCCResolveWriteIntent(ctx, s.eng, s.MS, s.intent(status)); err != nil {
					return desc + ": " + err.Error()
				}
			} else {
				max := s.rng.Int63n(5)
				desc += fmt.Sprintf(", max=%d", max)
				if _, _, err := MVCCResolveWriteIntentRange(ctx, s.eng, s.MS, s.intentRange(status), max); err != nil {
					return desc + ": " + err.Error()
				}
			}
			if status != roachpb.PENDING {
				s.Txn = nil
			}
		}
		return desc
	}

	actions["Abort"] = func(s *state) string {
		return resolve(s, roachpb.ABORTED)
	}
	actions["Commit"] = func(s *state) string {
		return resolve(s, roachpb.COMMITTED)
	}
	actions["Push"] = func(s *state) string {
		return resolve(s, roachpb.PENDING)
	}
	actions["GC"] = func(s *state) string {
		// Sometimes GC everything, sometimes only older versions.
		gcTS := hlc.Timestamp{
			WallTime: s.rng.Int63n(s.TS.WallTime + 1 /* avoid zero */),
		}
		if err := MVCCGarbageCollect(
			ctx,
			s.eng,
			s.MS,
			[]roachpb.GCRequest_GCKey{{
				Key:       s.key,
				Timestamp: gcTS,
			}},
			s.TS,
		); err != nil {
			return err.Error()
		}
		return fmt.Sprint(gcTS)
	}

	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			for _, test := range []struct {
				name string
				key  roachpb.Key
				seed int64
			}{
				{
					name: "userspace",
					key:  roachpb.Key("foo"),
					seed: randutil.NewPseudoSeed(),
				},
				{
					name: "sys",
					key:  keys.RangeDescriptorKey(roachpb.RKey("bar")),
					seed: randutil.NewPseudoSeed(),
				},
			} {
				t.Run(test.name, func(t *testing.T) {
					testutils.RunTrueAndFalse(t, "inline", func(t *testing.T, inline bool) {
						t.Run(fmt.Sprintf("seed=%d", test.seed), func(t *testing.T) {
							eng := engineImpl.create()
							defer eng.Close()

							s := &randomTest{
								actions: actions,
								inline:  inline,
								state: state{
									rng: rand.New(rand.NewSource(test.seed)),
									eng: eng,
									key: test.key,
									MS:  &enginepb.MVCCStats{},
								},
							}

							for i := 0; i < count; i++ {
								s.step(t)
							}
						})
					})
				})
			}
		})
	}
}

func TestMVCCComputeStatsError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			engine := engineImpl.create()
			defer engine.Close()

			// Write a MVCC metadata key where the value is not an encoded MVCCMetadata
			// protobuf.
			if err := engine.Put(mvccKey(roachpb.Key("garbage")), []byte("garbage")); err != nil {
				t.Fatal(err)
			}

			iter := engine.NewIterator(IterOptions{UpperBound: roachpb.KeyMax})
			defer iter.Close()
			for _, mvccStatsTest := range mvccStatsTests {
				t.Run(mvccStatsTest.name, func(t *testing.T) {
					_, err := mvccStatsTest.fn(iter, mvccKey(roachpb.KeyMin), mvccKey(roachpb.KeyMax), 100)
					if e := "unable to decode MVCCMetadata"; !testutils.IsError(err, e) {
						t.Fatalf("expected %s, got %v", e, err)
					}
				})
			}
		})
	}
}
