// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
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
// also recomputes the stats over the whole ReadWriter with all known
// implementations and errors on mismatch with any of them. It is used for global
// keys.
func assertEq(t *testing.T, rw ReadWriter, debug string, ms, expMS *enginepb.MVCCStats) {
	t.Helper()
	assertEqImpl(t, rw, debug, true /* globalKeys */, ms, expMS)
}

func assertEqImpl(
	t *testing.T, rw ReadWriter, debug string, globalKeys bool, ms, expMS *enginepb.MVCCStats,
) {
	t.Helper()

	msCpy := *ms // shallow copy
	ms = &msCpy
	ms.AgeTo(expMS.LastUpdateNanos)
	if !ms.Equal(expMS) {
		pretty.Ldiff(t, ms, expMS)
		t.Errorf("%s: diff(ms, expMS) nontrivial", debug)
	}

	keyMin := roachpb.KeyMin
	keyMax := keys.LocalMax
	if globalKeys {
		keyMin = keys.LocalMax
		keyMax = roachpb.KeyMax
	}
	lockKeyMin, _ := keys.LockTableSingleKey(keyMin, nil)
	lockKeyMax, _ := keys.LockTableSingleKey(keyMax, nil)

	for _, mvccStatsTest := range mvccStatsTests {
		compMS, err := mvccStatsTest.fn(rw, keyMin, keyMax, ms.LastUpdateNanos)
		if err != nil {
			t.Fatal(err)
		}
		// NOTE: we use ComputeStats for the lock table stats because it is not
		// supported by ComputeStatsForIter.
		compLockMS, err := ComputeStats(
			context.Background(), rw, lockKeyMin, lockKeyMax, ms.LastUpdateNanos)
		if err != nil {
			t.Fatal(err)
		}
		compMS.Add(compLockMS)
		require.Equal(t, compMS, *ms, "%s: diff(ms, %s)", debug, mvccStatsTest.name)
	}
}

// assertEqLocal is like assertEq, but for tests that use only local keys.
func assertEqLocal(t *testing.T, rw ReadWriter, debug string, ms, expMS *enginepb.MVCCStats) {
	assertEqImpl(t, rw, debug, false /* globalKeys */, ms, expMS)
}

var emptyMVCCValueHeaderSize = func() int64 {
	var h enginepb.MVCCValueHeader
	return extendedPreludeSize + int64(h.Size())
}()

// TestMVCCStatsDeleteCommitMovesTimestamp exercises the case in which a value
// is written, later deleted via an intent and the deletion committed at an even
// higher timestamp. This exercises subtleties related to the implicit push of
// the intent (before resolution) and the accumulation of GCByteAge.
func TestMVCCStatsDeleteCommitMovesTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	ctx := context.Background()
	aggMS := &enginepb.MVCCStats{}

	assertEq(t, engine, "initially", aggMS, &enginepb.MVCCStats{})

	key := roachpb.Key("a")
	ts1 := hlc.Timestamp{WallTime: 1e9}
	// Put a value.
	value := roachpb.MakeValueFromString("value")
	if _, err := MVCCPut(ctx, engine, key, ts1, value, MVCCWriteOptions{Stats: aggMS}); err != nil {
		t.Fatal(err)
	}

	mKeySize := int64(mvccKey(key).EncodedSize()) // 2
	vKeySize := MVCCVersionTimestampSize          // 12
	vValSize := int64(len(value.RawBytes))        // 10
	if disableSimpleValueEncoding {
		vValSize += emptyMVCCValueHeaderSize // 17
	}

	expMS := enginepb.MVCCStats{
		LiveBytes:       mKeySize + vKeySize + vValSize, // 24[+7]
		LiveCount:       1,
		KeyBytes:        mKeySize + vKeySize, // 14
		KeyCount:        1,
		ValBytes:        vValSize, // 10[+7]
		ValCount:        1,
		LastUpdateNanos: 1e9,
	}
	assertEq(t, engine, "after put", aggMS, &expMS)

	// Delete the value at ts=3. We'll commit this at ts=4 later.
	ts3 := hlc.Timestamp{WallTime: 3 * 1e9}
	txn := &roachpb.Transaction{
		TxnMeta:       enginepb.TxnMeta{ID: uuid.MakeV4(), WriteTimestamp: ts3},
		ReadTimestamp: ts3,
	}
	if _, _, err := MVCCDelete(ctx, engine, key, txn.ReadTimestamp, MVCCWriteOptions{Txn: txn, Stats: aggMS}); err != nil {
		t.Fatal(err)
	}

	// Now commit the value, but with a timestamp gap (i.e. this is a
	// push-commit as it would happen for a SNAPSHOT txn)
	ts4 := hlc.Timestamp{WallTime: 4 * 1e9}
	txn.Status = roachpb.COMMITTED
	txn.WriteTimestamp.Forward(ts4)
	if _, _, _, _, err := MVCCResolveWriteIntent(ctx, engine, aggMS,
		roachpb.MakeLockUpdate(txn, roachpb.Span{Key: key}),
		MVCCResolveWriteIntentOptions{},
	); err != nil {
		t.Fatal(err)
	}

	// The initial write used the simple MVCCValue encoding. When resolved to
	// a higher timestamp, the MVCCValue retained its local timestamp, meaning
	// that it now uses the extended MVCCValue encoding.
	vValHeader := enginepb.MVCCValueHeader{LocalTimestamp: hlc.ClockTimestamp(ts1)}
	vValHeaderSize := extendedPreludeSize + int64(vValHeader.Size()) // 13
	vValSize += vValHeaderSize                                       // 23

	expAggMS := enginepb.MVCCStats{
		LastUpdateNanos: 4e9,
		LiveBytes:       0,
		LiveCount:       0,
		KeyCount:        1,
		ValCount:        2,
		// The implicit meta record (deletion tombstone) counts for len("a")+1=2.
		// Two versioned keys count for 2*vKeySize.
		KeyBytes: mKeySize + 2*vKeySize,
		ValBytes: vValSize,
		// No GCBytesAge has been accrued yet, as the value just got non-live at 4s.
		GCBytesAge: 0,
	}

	assertEq(t, engine, "after committing", aggMS, &expAggMS)
}

// TestMVCCStatsPutCommitMovesTimestamp is similar to
// TestMVCCStatsDeleteCommitMovesTimestamp, but is simpler: a first intent is
// written and then committed at a later timestamp.
func TestMVCCStatsPutCommitMovesTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	ctx := context.Background()
	aggMS := &enginepb.MVCCStats{}

	assertEq(t, engine, "initially", aggMS, &enginepb.MVCCStats{})

	key := roachpb.Key("a")
	ts1 := hlc.Timestamp{WallTime: 1e9}
	txn := &roachpb.Transaction{
		TxnMeta:       enginepb.TxnMeta{ID: uuid.MakeV4(), WriteTimestamp: ts1},
		ReadTimestamp: ts1,
	}
	// Write an intent at t=1s.
	value := roachpb.MakeValueFromString("value")
	if _, err := MVCCPut(ctx, engine, key, ts1, value, MVCCWriteOptions{Txn: txn, Stats: aggMS}); err != nil {
		t.Fatal(err)
	}

	mKeySize := int64(mvccKey(key).EncodedSize()) // 2
	mValSize := int64((&enginepb.MVCCMetadata{    // 46
		Timestamp: ts1.ToLegacyTimestamp(),
		Deleted:   false,
		Txn:       &txn.TxnMeta,
	}).Size())
	mValSize += 2
	vKeySize := MVCCVersionTimestampSize   // 12
	vValSize := int64(len(value.RawBytes)) // 10
	if disableSimpleValueEncoding {
		vValSize += emptyMVCCValueHeaderSize // 17
	}

	expMS := enginepb.MVCCStats{
		LastUpdateNanos: 1e9,
		LiveBytes:       mKeySize + mValSize + vKeySize + vValSize, // 2+(46[+2])+12+(10[+7]) = 68[+2][+7]
		LiveCount:       1,
		KeyBytes:        mKeySize + vKeySize, // 2+12 =14
		KeyCount:        1,
		ValBytes:        mValSize + vValSize, // (46[+2])+(10[+7]) = 54[+2][+7]
		ValCount:        1,
		IntentCount:     1,
		LockCount:       1,
		IntentBytes:     vKeySize + vValSize, // 12+(10[+7]) = 22[+7]
		GCBytesAge:      0,
	}
	assertEq(t, engine, "after put", aggMS, &expMS)

	// Now commit the intent, but with a timestamp gap (i.e. this is a
	// push-commit as it would happen for a SNAPSHOT txn)
	ts4 := hlc.Timestamp{WallTime: 4 * 1e9}
	txn.Status = roachpb.COMMITTED
	txn.WriteTimestamp.Forward(ts4)
	if _, _, _, _, err := MVCCResolveWriteIntent(ctx, engine, aggMS,
		roachpb.MakeLockUpdate(txn, roachpb.Span{Key: key}),
		MVCCResolveWriteIntentOptions{},
	); err != nil {
		t.Fatal(err)
	}

	// The initial write used the simple MVCCValue encoding. When resolved to
	// a higher timestamp, the MVCCValue retained its local timestamp, meaning
	// that it now uses the extended MVCCValue encoding.
	vValHeader := enginepb.MVCCValueHeader{LocalTimestamp: hlc.ClockTimestamp(ts1)}
	vValHeaderSize := extendedPreludeSize + int64(vValHeader.Size()) // 13
	vValSize = int64(len(value.RawBytes)) + vValHeaderSize           // 23

	expAggMS := enginepb.MVCCStats{
		LastUpdateNanos: 4e9,
		LiveBytes:       mKeySize + vKeySize + vValSize, // 2+12+23 = 37
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
}

// TestMVCCStatsPutPushMovesTimestamp is similar to TestMVCCStatsPutCommitMovesTimestamp:
// An intent is written and then re-written at a higher timestamp. This formerly messed up
// the LockAge computation.
func TestMVCCStatsPutPushMovesTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	ctx := context.Background()
	aggMS := &enginepb.MVCCStats{}

	assertEq(t, engine, "initially", aggMS, &enginepb.MVCCStats{})

	key := roachpb.Key("a")
	ts1 := hlc.Timestamp{WallTime: 1e9}
	txn := &roachpb.Transaction{
		TxnMeta:       enginepb.TxnMeta{ID: uuid.MakeV4(), WriteTimestamp: ts1},
		ReadTimestamp: ts1,
	}
	// Write an intent.
	value := roachpb.MakeValueFromString("value")
	if _, err := MVCCPut(
		ctx, engine, key, txn.ReadTimestamp, value, MVCCWriteOptions{Txn: txn, Stats: aggMS},
	); err != nil {
		t.Fatal(err)
	}

	mKeySize := int64(mvccKey(key).EncodedSize()) // 2
	mValSize := int64((&enginepb.MVCCMetadata{    // 46
		Timestamp: ts1.ToLegacyTimestamp(),
		Deleted:   false,
		Txn:       &txn.TxnMeta,
	}).Size())
	mValSize += 2
	vKeySize := MVCCVersionTimestampSize   // 12
	vValSize := int64(len(value.RawBytes)) // 10
	if disableSimpleValueEncoding {
		vValSize += emptyMVCCValueHeaderSize // 17
	}

	expMS := enginepb.MVCCStats{
		LastUpdateNanos: 1e9,
		LiveBytes:       mKeySize + mValSize + vKeySize + vValSize, // 2+(46[+2])+12+(10[+7]) = 70[+2][+7]
		LiveCount:       1,
		KeyBytes:        mKeySize + vKeySize, // 2+12 = 14
		KeyCount:        1,
		ValBytes:        mValSize + vValSize, // (46[+2])+(10[+7]) = 54[+2][+7]
		ValCount:        1,
		LockAge:         0,
		IntentCount:     1,
		LockCount:       1,
		IntentBytes:     vKeySize + vValSize, // 12+(10[+7]) = 22[+7]
	}
	assertEq(t, engine, "after put", aggMS, &expMS)

	// Now push the value, but with a timestamp gap (i.e. this is a
	// push as it would happen for a SNAPSHOT txn)
	ts4 := hlc.Timestamp{WallTime: 4 * 1e9}
	txn.WriteTimestamp.Forward(ts4)
	if _, _, _, _, err := MVCCResolveWriteIntent(ctx, engine, aggMS,
		roachpb.MakeLockUpdate(txn, roachpb.Span{Key: key}),
		MVCCResolveWriteIntentOptions{},
	); err != nil {
		t.Fatal(err)
	}
	// Account for removal of TxnDidNotUpdateMeta.
	mValSize -= 2

	// The initial write used the simple MVCCValue encoding. When resolved to
	// a higher timestamp, the MVCCValue retained its local timestamp, meaning
	// that it now uses the extended MVCCValue encoding.
	vValHeader := enginepb.MVCCValueHeader{LocalTimestamp: hlc.ClockTimestamp(ts1)}
	vValHeaderSize := extendedPreludeSize + int64(vValHeader.Size()) // 13
	vValSize = int64(len(value.RawBytes)) + vValHeaderSize           // 23

	expAggMS := enginepb.MVCCStats{
		LastUpdateNanos: 4e9,
		LiveBytes:       mKeySize + mValSize + vKeySize + vValSize, // 2+54+12+23 = 91
		LiveCount:       1,
		KeyCount:        1,
		ValCount:        1,
		// The explicit meta record counts for len("a")+1=2.
		// One versioned key counts for vKeySize.
		KeyBytes: mKeySize + vKeySize,
		// The intent is still there, so we see mValSize.
		ValBytes:    mValSize + vValSize, // 54+23 = 69
		LockAge:     0,                   // this was once erroneously positive
		IntentCount: 1,                   // still there
		LockCount:   1,
		IntentBytes: vKeySize + vValSize, // still there
	}

	assertEq(t, engine, "after pushing", aggMS, &expAggMS)
}

// TestMVCCStatsDeleteMovesTimestamp is similar to TestMVCCStatsPutCommitMovesTimestamp:
// An intent is written and then re-written at a higher timestamp. This formerly messed up
// the GCBytesAge computation.
func TestMVCCStatsDeleteMovesTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	ctx := context.Background()
	aggMS := &enginepb.MVCCStats{}

	assertEq(t, engine, "initially", aggMS, &enginepb.MVCCStats{})

	ts1 := hlc.Timestamp{WallTime: 1e9}
	ts2 := hlc.Timestamp{WallTime: 2 * 1e9}

	key := roachpb.Key("a")
	txn := &roachpb.Transaction{
		TxnMeta:       enginepb.TxnMeta{ID: uuid.MakeV4(), WriteTimestamp: ts1},
		ReadTimestamp: ts1,
	}

	// Write an intent.
	value := roachpb.MakeValueFromString("value")
	if _, err := MVCCPut(ctx, engine, key, txn.ReadTimestamp, value, MVCCWriteOptions{Txn: txn, Stats: aggMS}); err != nil {
		t.Fatal(err)
	}

	mKeySize := int64(mvccKey(key).EncodedSize())
	require.EqualValues(t, 2, mKeySize)

	mVal1Size := int64((&enginepb.MVCCMetadata{
		Timestamp: ts1.ToLegacyTimestamp(),
		Deleted:   false,
		Txn:       &txn.TxnMeta,
	}).Size())
	require.EqualValues(t, 46, mVal1Size)
	mVal1Size += 2

	// TODO(sumeer): this is the first put at ts1, so why are we using this m1ValSize
	// instead of mVal1Size being sufficient?
	m1ValSize := int64((&enginepb.MVCCMetadata{
		Timestamp: ts2.ToLegacyTimestamp(),
		Deleted:   false,
		Txn:       &txn.TxnMeta,
	}).Size())
	require.EqualValues(t, 46, m1ValSize)
	m1ValSize += 2

	vKeySize := MVCCVersionTimestampSize
	require.EqualValues(t, 12, vKeySize)

	vValSize := int64(len(value.RawBytes))
	require.EqualValues(t, 10, vValSize)
	if disableSimpleValueEncoding {
		vValSize += emptyMVCCValueHeaderSize
		require.EqualValues(t, 17, vValSize)
	}

	expMS := enginepb.MVCCStats{
		LastUpdateNanos: 1e9,
		LiveBytes:       mKeySize + m1ValSize + vKeySize + vValSize, // 2+(46[+2])+12+(10[+7]) = 70[+2][+7]
		LiveCount:       1,
		KeyBytes:        mKeySize + vKeySize, // 2+12 = 14
		KeyCount:        1,
		ValBytes:        mVal1Size + vValSize, // (46[+2])+(10[+7]) = 56[+2][+7]
		ValCount:        1,
		LockAge:         0,
		IntentCount:     1,
		LockCount:       1,
		IntentBytes:     vKeySize + vValSize, // 12+(10[+7]) = 22[+7]
	}
	assertEq(t, engine, "after put", aggMS, &expMS)

	// Now replace our intent with a deletion intent, but with a timestamp gap.
	// This could happen if a transaction got restarted with a higher timestamp
	// and ran logic different from that in the first attempt.
	txn.WriteTimestamp.Forward(ts2)

	txn.Sequence++

	// Annoyingly, the new meta value is actually a little larger thanks to the
	// sequence number. Also since there was a write previously on the same
	// transaction, the IntentHistory will add a few bytes to the metadata.
	encValue, err := EncodeMVCCValue(MVCCValue{Value: value})
	require.NoError(t, err)
	m2ValSize := int64((&enginepb.MVCCMetadata{
		Timestamp: ts2.ToLegacyTimestamp(),
		Txn:       &txn.TxnMeta,
		IntentHistory: []enginepb.MVCCMetadata_SequencedIntent{
			{Sequence: 0, Value: encValue},
		},
	}).Size())
	expM2ValSize := 64
	if disableSimpleValueEncoding {
		expM2ValSize = 71
	}
	require.EqualValues(t, expM2ValSize, m2ValSize)

	if _, _, err := MVCCDelete(ctx, engine, key, txn.ReadTimestamp, MVCCWriteOptions{Txn: txn, Stats: aggMS}); err != nil {
		t.Fatal(err)
	}

	vVal2Size := int64(0) // tombstone
	if disableSimpleValueEncoding {
		vVal2Size = emptyMVCCValueHeaderSize // 7
	}

	expAggMS := enginepb.MVCCStats{
		LastUpdateNanos: 2e9,
		LiveBytes:       0,
		LiveCount:       0,
		KeyCount:        1,
		ValCount:        1,
		// The explicit meta record counts for len("a")+1=2.
		// One versioned key counts for vKeySize.
		KeyBytes: mKeySize + vKeySize,
		// The intent is still there, but this time with mVal2Size, and a zero vValSize.
		ValBytes:    m2ValSize + vVal2Size,
		LockAge:     0,
		IntentCount: 1, // still there
		LockCount:   1,
		IntentBytes: vKeySize + vVal2Size, // still there, but now without vValSize
		GCBytesAge:  0,                    // this was once erroneously negative
	}

	assertEq(t, engine, "after deleting", aggMS, &expAggMS)
}

// TestMVCCStatsPutMovesDeletionTimestamp is similar to TestMVCCStatsPutCommitMovesTimestamp: A
// tombstone intent is written and then replaced by a value intent at a higher timestamp. This
// formerly messed up the GCBytesAge computation.
func TestMVCCStatsPutMovesDeletionTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	ctx := context.Background()
	aggMS := &enginepb.MVCCStats{}

	assertEq(t, engine, "initially", aggMS, &enginepb.MVCCStats{})

	ts1 := hlc.Timestamp{WallTime: 1e9}
	ts2 := hlc.Timestamp{WallTime: 2 * 1e9}

	key := roachpb.Key("a")
	txn := &roachpb.Transaction{
		TxnMeta:       enginepb.TxnMeta{ID: uuid.MakeV4(), WriteTimestamp: ts1},
		ReadTimestamp: ts1,
	}

	// Write a deletion tombstone intent.
	if _, _, err := MVCCDelete(ctx, engine, key, txn.ReadTimestamp, MVCCWriteOptions{Txn: txn, Stats: aggMS}); err != nil {
		t.Fatal(err)
	}

	mKeySize := int64(mvccKey(key).EncodedSize())
	require.EqualValues(t, mKeySize, 2)

	mVal1Size := int64((&enginepb.MVCCMetadata{
		Timestamp: ts1.ToLegacyTimestamp(),
		Deleted:   false,
		Txn:       &txn.TxnMeta,
	}).Size())
	require.EqualValues(t, mVal1Size, 46)
	mVal1Size += 2

	vKeySize := MVCCVersionTimestampSize
	require.EqualValues(t, vKeySize, 12)

	vVal1Size := int64(0) // tombstone
	if disableSimpleValueEncoding {
		vVal1Size = emptyMVCCValueHeaderSize // 7
	}

	expMS := enginepb.MVCCStats{
		LastUpdateNanos: 1e9,
		LiveBytes:       0,
		LiveCount:       0,
		KeyBytes:        mKeySize + vKeySize, // 2 + 12 = 24
		KeyCount:        1,
		ValBytes:        mVal1Size + vVal1Size, // 46[+2] [+7]
		ValCount:        1,
		LockAge:         0,
		IntentCount:     1,
		LockCount:       1,
		IntentBytes:     vKeySize + vVal1Size, // 12 [+7]
		GCBytesAge:      0,
	}
	assertEq(t, engine, "after delete", aggMS, &expMS)

	// Now replace our deletion with a value intent, but with a timestamp gap.
	// This could happen if a transaction got restarted with a higher timestamp
	// and ran logic different from that in the first attempt.
	txn.WriteTimestamp.Forward(ts2)

	txn.Sequence++

	// Annoyingly, the new meta value is actually a little larger thanks to the
	// sequence number. Also the value is larger because the previous intent on the
	// transaction is recorded in the IntentHistory.
	encVal1, err := EncodeMVCCValue(MVCCValue{Value: roachpb.Value{RawBytes: []byte{}}})
	require.NoError(t, err)
	mVal2Size := int64((&enginepb.MVCCMetadata{
		Timestamp: ts2.ToLegacyTimestamp(),
		Txn:       &txn.TxnMeta,
		IntentHistory: []enginepb.MVCCMetadata_SequencedIntent{
			{Sequence: 0, Value: encVal1},
		},
	}).Size())
	expMVal2Size := 54
	if disableSimpleValueEncoding {
		expMVal2Size = 61
	}
	require.EqualValues(t, expMVal2Size, mVal2Size)

	value := roachpb.MakeValueFromString("value")

	vVal2Size := int64(len(value.RawBytes))
	require.EqualValues(t, vVal2Size, 10)
	if disableSimpleValueEncoding {
		vVal2Size += emptyMVCCValueHeaderSize
		require.EqualValues(t, vVal2Size, 17)
	}

	if _, err := MVCCPut(
		ctx, engine, key, txn.ReadTimestamp, value, MVCCWriteOptions{Txn: txn, Stats: aggMS},
	); err != nil {
		t.Fatal(err)
	}

	expAggMS := enginepb.MVCCStats{
		LastUpdateNanos: 2e9,
		LiveBytes:       mKeySize + mVal2Size + vKeySize + vVal2Size, // 2+46+12+(10[+7]) = 70[+7]
		LiveCount:       1,
		KeyCount:        1,
		ValCount:        1,
		// The explicit meta record counts for len("a")+1=2.
		// One versioned key counts for vKeySize.
		KeyBytes: mKeySize + vKeySize,
		// The intent is still there, but this time with mVal2Size, and a zero vValSize.
		ValBytes:    vVal2Size + mVal2Size, // (10[+7])+46 = 56[+7]
		LockAge:     0,
		IntentCount: 1, // still there
		LockCount:   1,
		IntentBytes: vKeySize + vVal2Size, // still there, now bigger
		GCBytesAge:  0,                    // this was once erroneously negative
	}

	assertEq(t, engine, "after put", aggMS, &expAggMS)
}

// TestMVCCStatsDelDelCommit writes a non-transactional tombstone, and then adds an intent tombstone
// on top that is then committed or aborted at a higher timestamp. This random-looking sequence was
// the smallest failing example that highlighted a stats computation error once, and exercises a
// code path in which MVCCResolveIntent has to read the pre-intent value in order to arrive at the
// correct stats.
func TestMVCCStatsDelDelCommitMovesTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	ctx := context.Background()
	aggMS := &enginepb.MVCCStats{}

	assertEq(t, engine, "initially", aggMS, &enginepb.MVCCStats{})

	key := roachpb.Key("a")

	ts1 := hlc.Timestamp{WallTime: 1e9}
	ts2 := hlc.Timestamp{WallTime: 2e9}
	ts3 := hlc.Timestamp{WallTime: 3e9}

	// Write a non-transactional tombstone at t=1s.
	if _, _, err := MVCCDelete(ctx, engine, key, ts1, MVCCWriteOptions{Stats: aggMS}); err != nil {
		t.Fatal(err)
	}

	mKeySize := int64(mvccKey(key).EncodedSize())
	require.EqualValues(t, mKeySize, 2)
	vKeySize := MVCCVersionTimestampSize
	require.EqualValues(t, vKeySize, 12)

	vValSize := int64(0) // tombstone
	if disableSimpleValueEncoding {
		vValSize = emptyMVCCValueHeaderSize // 7
	}

	expMS := enginepb.MVCCStats{
		LastUpdateNanos: 1e9,
		KeyBytes:        mKeySize + vKeySize,
		KeyCount:        1,
		ValBytes:        vValSize,
		ValCount:        1,
	}

	assertEq(t, engine, "after non-transactional delete", aggMS, &expMS)

	// Write a tombstone intent at t=2s.
	txn := &roachpb.Transaction{
		TxnMeta:       enginepb.TxnMeta{ID: uuid.MakeV4(), WriteTimestamp: ts2},
		ReadTimestamp: ts2,
	}
	if _, _, err := MVCCDelete(ctx, engine, key, txn.ReadTimestamp, MVCCWriteOptions{Txn: txn, Stats: aggMS}); err != nil {
		t.Fatal(err)
	}

	mValSize := int64((&enginepb.MVCCMetadata{
		Timestamp: ts1.ToLegacyTimestamp(),
		Deleted:   true,
		Txn:       &txn.TxnMeta,
	}).Size())
	require.EqualValues(t, mValSize, 46)
	// Account for TxnDidNotUpdateMeta
	mValSize += 2

	expMS = enginepb.MVCCStats{
		LastUpdateNanos: 2e9,
		KeyBytes:        mKeySize + 2*vKeySize, // 2+2*12 = 26
		KeyCount:        1,
		ValBytes:        mValSize + 2*vValSize, // 46[+2] [+7]
		ValCount:        2,
		IntentCount:     1,
		LockCount:       1,
		IntentBytes:     vKeySize + vValSize,
		// The original non-transactional write (at 1s) has now aged one second.
		GCBytesAge: 1 * (vKeySize + vValSize),
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
		txnCommit.WriteTimestamp.Forward(ts3)
		if _, _, _, _, err := MVCCResolveWriteIntent(ctx, engine, &aggMS,
			roachpb.MakeLockUpdate(txnCommit, roachpb.Span{Key: key}),
			MVCCResolveWriteIntentOptions{},
		); err != nil {
			t.Fatal(err)
		}

		// The initial write used the simple MVCCValue encoding. When resolved to
		// a higher timestamp, the MVCCValue retained its local timestamp, meaning
		// that it now uses the extended MVCCValue encoding.
		vVal2Header := enginepb.MVCCValueHeader{LocalTimestamp: hlc.ClockTimestamp(ts2)}
		vVal2HeaderSize := extendedPreludeSize + int64(vVal2Header.Size()) // 13
		vVal2Size := vVal2HeaderSize + 0                                   // tombstone, so just a header

		expAggMS := enginepb.MVCCStats{
			LastUpdateNanos: 3e9,
			KeyBytes:        mKeySize + 2*vKeySize, // 2+2*12 = 26
			KeyCount:        1,
			ValBytes:        vValSize + vVal2Size,
			ValCount:        2,
			IntentCount:     0,
			IntentBytes:     0,
			// The very first write picks up another second of age. Before a bug fix,
			// this was failing to do so.
			GCBytesAge: 2 * (vKeySize + vValSize),
		}

		assertEq(t, engine, "after committing", &aggMS, &expAggMS)
	})
	t.Run("Abort", func(t *testing.T) {
		aggMS := *aggMS
		engine := engine.NewBatch()
		defer engine.Close()

		txnAbort := txn.Clone()
		txnAbort.Status = roachpb.ABORTED
		txnAbort.WriteTimestamp.Forward(ts3)
		if _, _, _, _, err := MVCCResolveWriteIntent(ctx, engine, &aggMS,
			roachpb.MakeLockUpdate(txnAbort, roachpb.Span{Key: key}),
			MVCCResolveWriteIntentOptions{},
		); err != nil {
			t.Fatal(err)
		}

		expAggMS := enginepb.MVCCStats{
			LastUpdateNanos: 3e9,
			KeyBytes:        mKeySize + vKeySize, // 2+12 = 14
			KeyCount:        1,
			ValBytes:        vValSize,
			ValCount:        1,
			IntentCount:     0,
			IntentBytes:     0,
			// We aborted our intent, but the value we first wrote was a tombstone, and
			// so it's expected to retain its age. Since it's now the only value, it
			// also contributes as a meta key.
			GCBytesAge: 2 * (mKeySize + vKeySize + vValSize),
		}

		assertEq(t, engine, "after aborting", &aggMS, &expAggMS)
	})
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
	defer log.Scope(t).Close(t)
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	ctx := context.Background()
	aggMS := &enginepb.MVCCStats{}

	assertEq(t, engine, "initially", aggMS, &enginepb.MVCCStats{})

	key := roachpb.Key("a")

	ts1 := hlc.Timestamp{WallTime: 1e9}
	ts2 := hlc.Timestamp{WallTime: 2e9}
	ts3 := hlc.Timestamp{WallTime: 3e9}

	// Write a non-transactional value at t=1s.
	value := roachpb.MakeValueFromString("value")
	if _, err := MVCCPut(ctx, engine, key, ts1, value, MVCCWriteOptions{Stats: aggMS}); err != nil {
		t.Fatal(err)
	}

	mKeySize := int64(mvccKey(key).EncodedSize())
	require.EqualValues(t, mKeySize, 2)

	vKeySize := MVCCVersionTimestampSize
	require.EqualValues(t, vKeySize, 12)

	vValSize := int64(len(value.RawBytes))
	require.EqualValues(t, vValSize, 10)
	if disableSimpleValueEncoding {
		vValSize += emptyMVCCValueHeaderSize // 17
	}

	expMS := enginepb.MVCCStats{
		LastUpdateNanos: 1e9,
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
		TxnMeta:       enginepb.TxnMeta{ID: uuid.MakeV4(), WriteTimestamp: ts2},
		ReadTimestamp: ts2,
	}
	if _, _, err := MVCCDelete(ctx, engine, key, txn.ReadTimestamp, MVCCWriteOptions{Txn: txn, Stats: aggMS}); err != nil {
		t.Fatal(err)
	}

	mValSize := int64((&enginepb.MVCCMetadata{
		Timestamp: ts1.ToLegacyTimestamp(),
		Deleted:   true,
		Txn:       &txn.TxnMeta,
	}).Size())
	require.EqualValues(t, mValSize, 46)
	mValSize += 2

	vDelSize := int64(0) // tombstone
	if disableSimpleValueEncoding {
		vDelSize = emptyMVCCValueHeaderSize // 7
	}

	expMS = enginepb.MVCCStats{
		LastUpdateNanos: 2e9,
		KeyBytes:        mKeySize + 2*vKeySize, // 2+2*12 = 26
		KeyCount:        1,
		ValBytes:        mValSize + vValSize + vDelSize, // 46[+2]+10[+7] = 56[+2][+7]
		ValCount:        2,
		IntentCount:     1,
		LockCount:       1,
		IntentBytes:     vKeySize + vDelSize, // 12[+7]
		// The original non-transactional write becomes non-live at 2s, so no age
		// is accrued yet.
		GCBytesAge: 0,
	}
	assertEq(t, engine, "after txn delete", aggMS, &expMS)

	// Now commit or abort the intent, but with a timestamp gap (i.e. this is a push-commit as it
	// would happen for a SNAPSHOT txn)

	txn.WriteTimestamp.Forward(ts3)
	txn.Sequence++

	// Annoyingly, the new meta value is actually a little larger thanks to the
	// sequence number.
	m2ValSize := int64((&enginepb.MVCCMetadata{
		Timestamp: ts3.ToLegacyTimestamp(),
		Txn:       &txn.TxnMeta,
	}).Size())

	require.EqualValues(t, m2ValSize, 48)

	t.Run("Abort", func(t *testing.T) {
		aggMS := *aggMS
		engine := engine.NewBatch()
		defer engine.Close()

		txnAbort := txn.Clone()
		txnAbort.Status = roachpb.ABORTED // doesn't change m2ValSize, fortunately
		if _, _, _, _, err := MVCCResolveWriteIntent(ctx, engine, &aggMS,
			roachpb.MakeLockUpdate(txnAbort, roachpb.Span{Key: key}),
			MVCCResolveWriteIntentOptions{},
		); err != nil {
			t.Fatal(err)
		}

		expAggMS := enginepb.MVCCStats{
			LastUpdateNanos: 3e9,
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
		if disableSimpleValueEncoding {
			vVal2Size += emptyMVCCValueHeaderSize
			require.EqualValues(t, 21, vVal2Size)
		}

		txn.WriteTimestamp.Forward(ts3)
		if _, err := MVCCPut(ctx, engine, key, txn.ReadTimestamp, val2, MVCCWriteOptions{Txn: txn, Stats: &aggMS}); err != nil {
			t.Fatal(err)
		}

		// Annoyingly, the new meta value is actually a little larger thanks to the
		// sequence number.
		encDel, err := EncodeMVCCValue(MVCCValue{Value: roachpb.Value{RawBytes: []byte{}}})
		require.NoError(t, err)
		mVal2SizeWithHistory := int64((&enginepb.MVCCMetadata{
			Timestamp: ts3.ToLegacyTimestamp(),
			Txn:       &txn.TxnMeta,
			IntentHistory: []enginepb.MVCCMetadata_SequencedIntent{
				{Sequence: 0, Value: encDel},
			},
		}).Size())
		expMVal2Size := 54
		if disableSimpleValueEncoding {
			expMVal2Size = 61
		}
		require.EqualValues(t, expMVal2Size, mVal2SizeWithHistory)

		expAggMS := enginepb.MVCCStats{
			LastUpdateNanos: 3e9,
			KeyBytes:        mKeySize + 2*vKeySize, // 2+2*12 = 26
			KeyCount:        1,
			ValBytes:        mVal2SizeWithHistory + vValSize + vVal2Size,
			ValCount:        2,
			LiveCount:       1,
			LiveBytes:       mKeySize + mVal2SizeWithHistory + vKeySize + vVal2Size,
			IntentCount:     1,
			LockCount:       1,
			IntentBytes:     vKeySize + vVal2Size,
			// The original write was previously non-live at 2s because that's where the
			// intent originally lived. But the intent has moved to 3s, and so has the
			// moment in time at which the shadowed put became non-live; it's now 3s as
			// well, so there's no contribution yet.
			GCBytesAge: 0,
		}
		assertEq(t, engine, "after txn put", &aggMS, &expAggMS)
	})
}

// TestMVCCStatsDelDelGC prevents regression of a bug in MVCCGarbageCollect
// that was exercised by running two deletions followed by a specific GC.
func TestMVCCStatsDelDelGC(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	ctx := context.Background()
	aggMS := &enginepb.MVCCStats{}

	assertEq(t, engine, "initially", aggMS, &enginepb.MVCCStats{})

	key := roachpb.Key("a")
	ts1 := hlc.Timestamp{WallTime: 1e9}
	ts2 := hlc.Timestamp{WallTime: 2e9}

	// Write tombstones at ts1 and ts2.
	if _, _, err := MVCCDelete(ctx, engine, key, ts1, MVCCWriteOptions{Stats: aggMS}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := MVCCDelete(ctx, engine, key, ts2, MVCCWriteOptions{Stats: aggMS}); err != nil {
		t.Fatal(err)
	}

	mKeySize := int64(mvccKey(key).EncodedSize()) // 2
	vKeySize := MVCCVersionTimestampSize          // 12
	vValSize := int64(0)                          // tombstone
	if disableSimpleValueEncoding {
		vValSize = emptyMVCCValueHeaderSize // 7
	}

	expMS := enginepb.MVCCStats{
		LastUpdateNanos: 2e9,
		KeyBytes:        mKeySize + 2*vKeySize, // 26
		ValBytes:        2 * vValSize,
		KeyCount:        1,
		ValCount:        2,
		GCBytesAge:      1 * (vKeySize + vValSize), // first tombstone, aged from ts1 to ts2
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
		[]kvpb.GCRequest_GCKey{{
			Key:       key,
			Timestamp: ts2,
		}},
		ts2,
	); err != nil {
		t.Fatal(err)
	}

	expAggMS := enginepb.MVCCStats{
		LastUpdateNanos: 2e9,
	}

	assertEq(t, engine, "after GC", aggMS, &expAggMS)
}

// TestMVCCStatsPutIntentTimestampNotPutTimestamp exercises a scenario in which
// an intent is rewritten to a lower timestamp. This formerly caused bugs
// because when computing the stats updates, there was an implicit assumption
// that the meta entries would always move forward in time.
// UPDATE: since there should be no way for a txn to write older intents,
//
//	mvccPutInternal now makes sure that writes are always done at the most
//	recent intent timestamp within the same txn. Note that this case occurs
//	when the txn timestamp is moved forward due to a write too old condition,
//	which writes the first intent at a higher timestamp. We don't allow the
//	second intent to then be written at a lower timestamp, because that breaks
//	the contract that the intent is always the newest version.
//	This test now merely verifies that even when we try to write an older
//	version, we're upgraded to write the MVCCMetadata.Timestamp.
func TestMVCCStatsPutIntentTimestampNotPutTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	ctx := context.Background()
	aggMS := &enginepb.MVCCStats{}

	assertEq(t, engine, "initially", aggMS, &enginepb.MVCCStats{})

	key := roachpb.Key("a")
	ts201 := hlc.Timestamp{WallTime: 2e9 + 1}
	ts099 := hlc.Timestamp{WallTime: 1e9 - 1}
	txn := &roachpb.Transaction{
		TxnMeta:       enginepb.TxnMeta{ID: uuid.MakeV4(), WriteTimestamp: ts201},
		ReadTimestamp: ts099,
	}
	// Write an intent at 2s+1.
	value := roachpb.MakeValueFromString("value")
	if _, err := MVCCPut(ctx, engine, key, txn.ReadTimestamp, value, MVCCWriteOptions{Txn: txn, Stats: aggMS}); err != nil {
		t.Fatal(err)
	}

	mKeySize := int64(mvccKey(key).EncodedSize()) // 2
	m1ValSize := int64((&enginepb.MVCCMetadata{   // 46
		Timestamp: ts201.ToLegacyTimestamp(),
		Txn:       &txn.TxnMeta,
	}).Size())
	m1ValSize += 2
	vKeySize := MVCCVersionTimestampSize   // 12
	vValSize := int64(len(value.RawBytes)) // 10
	if disableSimpleValueEncoding {
		vValSize += emptyMVCCValueHeaderSize // 17
	}

	expMS := enginepb.MVCCStats{
		LastUpdateNanos: 2e9 + 1,
		LiveBytes:       mKeySize + m1ValSize + vKeySize + vValSize, // 2+(46[+2])+12+(10[+7]) = 68[+2][+7]
		LiveCount:       1,
		KeyBytes:        mKeySize + vKeySize, // 14
		KeyCount:        1,
		ValBytes:        m1ValSize + vValSize, // (46[+2])+(10[+7]) = 54[+2][+7]
		ValCount:        1,
		IntentCount:     1,
		LockCount:       1,
		IntentBytes:     vKeySize + vValSize, // 12+(10[+7]) = 22[+7]
	}
	assertEq(t, engine, "after first put", aggMS, &expMS)

	// Replace the intent with an identical one, but we write it at 1s-1 now. If
	// you're confused, don't worry. There are two timestamps here: the one in
	// the txn (which is, perhaps surprisingly, only really used when
	// committing/aborting intents), and the timestamp passed directly to
	// MVCCPut (which is where the intent will actually end up being written at,
	// and which usually corresponds to txn.ReadTimestamp).
	txn.Sequence++
	txn.WriteTimestamp = ts099

	// Annoyingly, the new meta value is actually a little larger thanks to the
	// sequence number.
	encValue, err := EncodeMVCCValue(MVCCValue{Value: value})
	require.NoError(t, err)
	m2ValSize := int64((&enginepb.MVCCMetadata{ // 46
		Timestamp: ts201.ToLegacyTimestamp(),
		Txn:       &txn.TxnMeta,
		IntentHistory: []enginepb.MVCCMetadata_SequencedIntent{
			{Sequence: 0, Value: encValue},
		},
	}).Size())
	if _, err := MVCCPut(ctx, engine, key, txn.ReadTimestamp, value, MVCCWriteOptions{Txn: txn, Stats: aggMS}); err != nil {
		t.Fatal(err)
	}

	expAggMS := enginepb.MVCCStats{
		// Even though we tried to put a new intent at an older timestamp, it
		// will have been written at 2E9+1, so the age will be 0.
		LockAge: 0,

		LastUpdateNanos: 2e9 + 1,
		LiveBytes:       mKeySize + m2ValSize + vKeySize + vValSize, // 2+(46[+7])+12+(10[+7]) = 70[+14]
		LiveCount:       1,
		KeyBytes:        mKeySize + vKeySize, // 14
		KeyCount:        1,
		ValBytes:        m2ValSize + vValSize, // (46[+7])+(10[+7]) = 56[+14]
		ValCount:        1,
		IntentCount:     1,
		LockCount:       1,
		IntentBytes:     vKeySize + vValSize, // 12+(10[+7]) = 22[+7]
	}

	assertEq(t, engine, "after second put", aggMS, &expAggMS)
}

// TestMVCCStatsPutWaitDeleteGC puts a value, deletes it, and runs a GC that
// deletes the original write, but not the deletion tombstone.
func TestMVCCStatsPutWaitDeleteGC(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	ctx := context.Background()
	aggMS := &enginepb.MVCCStats{}

	assertEq(t, engine, "initially", aggMS, &enginepb.MVCCStats{})

	key := roachpb.Key("a")

	ts1 := hlc.Timestamp{WallTime: 1e9}
	ts2 := hlc.Timestamp{WallTime: 2e9}

	// Write a value at ts1.
	val1 := roachpb.MakeValueFromString("value")
	if _, err := MVCCPut(ctx, engine, key, ts1, val1, MVCCWriteOptions{Stats: aggMS}); err != nil {
		t.Fatal(err)
	}

	mKeySize := int64(mvccKey(key).EncodedSize())
	require.EqualValues(t, mKeySize, 2)

	vKeySize := MVCCVersionTimestampSize
	require.EqualValues(t, vKeySize, 12)

	vValSize := int64(len(val1.RawBytes))
	require.EqualValues(t, vValSize, 10)
	if disableSimpleValueEncoding {
		vValSize += emptyMVCCValueHeaderSize
		require.EqualValues(t, vValSize, 17)
	}

	expMS := enginepb.MVCCStats{
		LastUpdateNanos: 1e9,
		KeyCount:        1,
		KeyBytes:        mKeySize + vKeySize, // 2+12 = 14
		ValCount:        1,
		ValBytes:        vValSize, // 10[+7]
		LiveCount:       1,
		LiveBytes:       mKeySize + vKeySize + vValSize, // 2+12+(10[+7]) = 24[+7]
	}
	assertEq(t, engine, "after first put", aggMS, &expMS)

	// Delete the value at ts5.

	if _, _, err := MVCCDelete(ctx, engine, key, ts2, MVCCWriteOptions{Stats: aggMS}); err != nil {
		t.Fatal(err)
	}

	vVal2Size := int64(0) // tombstone
	if disableSimpleValueEncoding {
		vVal2Size = emptyMVCCValueHeaderSize // 7
	}

	expMS = enginepb.MVCCStats{
		LastUpdateNanos: 2e9,
		KeyCount:        1,
		KeyBytes:        mKeySize + 2*vKeySize, // 2+2*12 = 26
		ValBytes:        vValSize + vVal2Size,  // 10[+7]
		ValCount:        2,
		LiveBytes:       0,
		LiveCount:       0,
		GCBytesAge:      0, // before a fix, this was vKeySize + vValSize
	}

	assertEq(t, engine, "after delete", aggMS, &expMS)

	if err := MVCCGarbageCollect(ctx, engine, aggMS, []kvpb.GCRequest_GCKey{{
		Key:       key,
		Timestamp: ts1,
	}}, ts2); err != nil {
		t.Fatal(err)
	}

	expMS = enginepb.MVCCStats{
		LastUpdateNanos: 2e9,
		KeyCount:        1,
		KeyBytes:        mKeySize + vKeySize, // 2+12 = 14
		ValBytes:        vVal2Size,
		ValCount:        1,
		LiveBytes:       0,
		LiveCount:       0,
		GCBytesAge:      0, // before a fix, this was vKeySize + vValSize
	}

	assertEq(t, engine, "after GC", aggMS, &expMS)
}

// TestMVCCStatsSysTxnPutPut prevents regression of a bug that, when rewriting an intent
// on a sys key, would lead to overcounting `ms.SysBytes`.
func TestMVCCStatsTxnSysPutPut(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	ctx := context.Background()
	aggMS := &enginepb.MVCCStats{}

	assertEqLocal(t, engine, "initially", aggMS, &enginepb.MVCCStats{})

	key := keys.RangeDescriptorKey(roachpb.RKey("a"))

	ts1 := hlc.Timestamp{WallTime: 1e9}
	ts2 := hlc.Timestamp{WallTime: 2e9}

	txn := &roachpb.Transaction{
		TxnMeta:       enginepb.TxnMeta{ID: uuid.MakeV4(), WriteTimestamp: ts1},
		ReadTimestamp: ts1,
	}

	// Write an intent at ts1.
	val1 := roachpb.MakeValueFromString("value")
	if _, err := MVCCPut(
		ctx, engine, key, txn.ReadTimestamp, val1, MVCCWriteOptions{Txn: txn, Stats: aggMS},
	); err != nil {
		t.Fatal(err)
	}

	mKeySize := int64(mvccKey(key).EncodedSize())
	require.EqualValues(t, mKeySize, 11)

	mValSize := int64((&enginepb.MVCCMetadata{
		Timestamp: ts1.ToLegacyTimestamp(),
		Deleted:   false,
		Txn:       &txn.TxnMeta,
	}).Size())
	require.EqualValues(t, mValSize, 46)
	mValSize += 2

	vKeySize := MVCCVersionTimestampSize
	require.EqualValues(t, vKeySize, 12)

	vVal1Size := int64(len(val1.RawBytes))
	require.EqualValues(t, vVal1Size, 10)
	if disableSimpleValueEncoding {
		vVal1Size += emptyMVCCValueHeaderSize
		require.EqualValues(t, vVal1Size, 17)
	}

	val2 := roachpb.MakeValueFromString("longvalue")
	vVal2Size := int64(len(val2.RawBytes))
	require.EqualValues(t, vVal2Size, 14)
	if disableSimpleValueEncoding {
		vVal2Size += emptyMVCCValueHeaderSize
		require.EqualValues(t, 21, vVal2Size)
	}

	expMS := enginepb.MVCCStats{
		LastUpdateNanos: 1e9,
		SysBytes:        mKeySize + mValSize + vKeySize + vVal1Size, // 11+(46[+2])+12+(10[+7]) = 79[+2][+7]
		SysCount:        1,
	}
	assertEqLocal(t, engine, "after first put", aggMS, &expMS)

	// Rewrite the intent to ts2 with a different value.
	txn.WriteTimestamp.Forward(ts2)
	txn.Sequence++

	// The new meta value grows because we've bumped `txn.Sequence`.
	// The value also grows as the older value is part of the same
	// transaction and so contributes to the intent history.
	encVal1, err := EncodeMVCCValue(MVCCValue{Value: val1})
	require.NoError(t, err)
	mVal2Size := int64((&enginepb.MVCCMetadata{
		Timestamp: ts2.ToLegacyTimestamp(),
		Deleted:   false,
		Txn:       &txn.TxnMeta,
		IntentHistory: []enginepb.MVCCMetadata_SequencedIntent{
			{Sequence: 0, Value: encVal1},
		},
	}).Size())
	expMVal2Size := 64
	if disableSimpleValueEncoding {
		expMVal2Size = 71
	}
	require.EqualValues(t, expMVal2Size, mVal2Size)

	if _, err := MVCCPut(
		ctx, engine, key, txn.ReadTimestamp, val2, MVCCWriteOptions{Txn: txn, Stats: aggMS},
	); err != nil {
		t.Fatal(err)
	}

	expMS = enginepb.MVCCStats{
		LastUpdateNanos: 1e9,
		SysBytes:        mKeySize + mVal2Size + vKeySize + vVal2Size, // 11+(46[+7])+12+14 = 83[+7]
		SysCount:        1,
	}

	assertEqLocal(t, engine, "after intent rewrite", aggMS, &expMS)
}

// TestMVCCStatsTxnSysPutAbort prevents regression of a bug that, when aborting
// an intent on a sys key, would lead to undercounting `ms.IntentBytes` and
// `ms.IntentCount`.
func TestMVCCStatsTxnSysPutAbort(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	ctx := context.Background()
	aggMS := &enginepb.MVCCStats{}

	assertEqLocal(t, engine, "initially", aggMS, &enginepb.MVCCStats{})

	key := keys.RangeDescriptorKey(roachpb.RKey("a"))

	ts1 := hlc.Timestamp{WallTime: 1e9}
	txn := &roachpb.Transaction{
		TxnMeta:       enginepb.TxnMeta{ID: uuid.MakeV4(), WriteTimestamp: ts1},
		ReadTimestamp: ts1,
	}

	// Write a system intent at ts1.
	val1 := roachpb.MakeValueFromString("value")
	if _, err := MVCCPut(
		ctx, engine, key, txn.ReadTimestamp, val1, MVCCWriteOptions{Txn: txn, Stats: aggMS},
	); err != nil {
		t.Fatal(err)
	}

	mKeySize := int64(mvccKey(key).EncodedSize())
	require.EqualValues(t, mKeySize, 11)

	mValSize := int64((&enginepb.MVCCMetadata{
		Timestamp: ts1.ToLegacyTimestamp(),
		Deleted:   false,
		Txn:       &txn.TxnMeta,
	}).Size())
	require.EqualValues(t, mValSize, 46)
	mValSize += 2

	vKeySize := MVCCVersionTimestampSize
	require.EqualValues(t, vKeySize, 12)

	vVal1Size := int64(len(val1.RawBytes))
	require.EqualValues(t, vVal1Size, 10)
	if disableSimpleValueEncoding {
		vVal1Size += emptyMVCCValueHeaderSize
		require.EqualValues(t, vVal1Size, 17)
	}

	val2 := roachpb.MakeValueFromString("longvalue")
	vVal2Size := int64(len(val2.RawBytes))
	require.EqualValues(t, vVal2Size, 14)
	if disableSimpleValueEncoding {
		vVal2Size += emptyMVCCValueHeaderSize
		require.EqualValues(t, 21, vVal2Size)
	}

	expMS := enginepb.MVCCStats{
		LastUpdateNanos: 1e9,
		SysBytes:        mKeySize + mValSize + vKeySize + vVal1Size, // 11+(46[+2])+12+(10[+7]) = 79[+2][+7]
		SysCount:        1,
	}
	assertEqLocal(t, engine, "after first put", aggMS, &expMS)

	// Now abort the intent.
	txn.Status = roachpb.ABORTED
	if _, _, _, _, err := MVCCResolveWriteIntent(ctx, engine, aggMS,
		roachpb.MakeLockUpdate(txn, roachpb.Span{Key: key}),
		MVCCResolveWriteIntentOptions{},
	); err != nil {
		t.Fatal(err)
	}

	expMS = enginepb.MVCCStats{
		LastUpdateNanos: 1e9,
	}
	assertEqLocal(t, engine, "after aborting", aggMS, &expMS)
}

// TestMVCCStatsSysPutPut prevents regression of a bug that, when writing a new
// value on top of an existing system key, would undercount.
func TestMVCCStatsSysPutPut(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	ctx := context.Background()
	aggMS := &enginepb.MVCCStats{}

	assertEqLocal(t, engine, "initially", aggMS, &enginepb.MVCCStats{})

	key := keys.RangeDescriptorKey(roachpb.RKey("a"))

	ts1 := hlc.Timestamp{WallTime: 1e9}
	ts2 := hlc.Timestamp{WallTime: 2e9}

	// Write a value at ts1.
	val1 := roachpb.MakeValueFromString("value")
	if _, err := MVCCPut(ctx, engine, key, ts1, val1, MVCCWriteOptions{Stats: aggMS}); err != nil {
		t.Fatal(err)
	}

	mKeySize := int64(mvccKey(key).EncodedSize())
	require.EqualValues(t, mKeySize, 11)

	vKeySize := MVCCVersionTimestampSize
	require.EqualValues(t, vKeySize, 12)

	vVal1Size := int64(len(val1.RawBytes))
	require.EqualValues(t, vVal1Size, 10)
	if disableSimpleValueEncoding {
		vVal1Size += emptyMVCCValueHeaderSize
		require.EqualValues(t, vVal1Size, 17)
	}

	val2 := roachpb.MakeValueFromString("longvalue")
	vVal2Size := int64(len(val2.RawBytes))
	require.EqualValues(t, vVal2Size, 14)
	if disableSimpleValueEncoding {
		vVal2Size += emptyMVCCValueHeaderSize
		require.EqualValues(t, 21, vVal2Size)
	}

	expMS := enginepb.MVCCStats{
		LastUpdateNanos: 1e9,
		SysBytes:        mKeySize + vKeySize + vVal1Size, // 11+12+(10[+7]) = 33[+7]
		SysCount:        1,
	}
	assertEqLocal(t, engine, "after first put", aggMS, &expMS)

	// Put another value at ts2.

	if _, err := MVCCPut(ctx, engine, key, ts2, val2, MVCCWriteOptions{Stats: aggMS}); err != nil {
		t.Fatal(err)
	}

	expMS = enginepb.MVCCStats{
		LastUpdateNanos: 1e9,
		SysBytes:        mKeySize + 2*vKeySize + vVal1Size + vVal2Size,
		SysCount:        1,
	}

	assertEqLocal(t, engine, "after second put", aggMS, &expMS)
}

// TestMVCCStatsPutRollbackDelete exercises the case in which an intent is
// written, then re-written by a deletion tombstone, which is rolled back. We
// expect the stats to be adjusted correctly for the rolled back delete, and
// crucially, for the LiveBytes and LiveCount to be non-zero and corresponding
// to the original value.
func TestMVCCStatsPutRollbackDelete(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	ctx := context.Background()
	aggMS := &enginepb.MVCCStats{}
	assertEq(t, engine, "initially", aggMS, &enginepb.MVCCStats{})

	key := roachpb.Key("a")
	value := roachpb.MakeValueFromString("value")
	ts := hlc.Timestamp{WallTime: 1e9}
	txn := &roachpb.Transaction{
		TxnMeta:       enginepb.TxnMeta{ID: uuid.MakeV4(), WriteTimestamp: ts},
		ReadTimestamp: ts,
	}

	// Put a value.
	if _, err := MVCCPut(ctx, engine, key, txn.ReadTimestamp, value, MVCCWriteOptions{Txn: txn, Stats: aggMS}); err != nil {
		t.Fatal(err)
	}

	mKeySize := int64(mvccKey(key).EncodedSize()) // 2
	mValSize := int64((&enginepb.MVCCMetadata{    // 46
		Timestamp: ts.ToLegacyTimestamp(),
		Deleted:   false,
		Txn:       &txn.TxnMeta,
	}).Size())
	mValSize += 2
	vKeySize := MVCCVersionTimestampSize   // 12
	vValSize := int64(len(value.RawBytes)) // 10
	if disableSimpleValueEncoding {
		vValSize += emptyMVCCValueHeaderSize // 17
	}

	expMS := enginepb.MVCCStats{
		LastUpdateNanos: 1e9,
		LiveBytes:       mKeySize + mValSize + vKeySize + vValSize, // 2+(46[+2])+12+(10[+7]) = 68[+2][+7]
		LiveCount:       1,
		KeyBytes:        mKeySize + vKeySize, // 2+12 =14
		KeyCount:        1,
		ValBytes:        mValSize + vValSize, // (46[+2])+(10[+7]) = 54[+2][+7]
		ValCount:        1,
		IntentCount:     1,
		LockCount:       1,
		IntentBytes:     vKeySize + vValSize, // 12+(10[+7]) = 22[+7]
		GCBytesAge:      0,
	}
	assertEq(t, engine, "after put", aggMS, &expMS)

	txn.Sequence++

	// Annoyingly, the new meta value is actually a little larger thanks to the
	// sequence number. Also since there was a write previously on the same
	// transaction, the IntentHistory will add a few bytes to the metadata.
	encValue, err := EncodeMVCCValue(MVCCValue{Value: value})
	require.NoError(t, err)
	m2ValSize := int64((&enginepb.MVCCMetadata{
		Timestamp: ts.ToLegacyTimestamp(),
		Deleted:   true,
		Txn:       &txn.TxnMeta,
		IntentHistory: []enginepb.MVCCMetadata_SequencedIntent{
			{Sequence: 0, Value: encValue},
		},
	}).Size())
	expM2ValSize := 64
	if disableSimpleValueEncoding {
		expM2ValSize += int(emptyMVCCValueHeaderSize)
	}
	require.EqualValues(t, expM2ValSize, m2ValSize)

	// Delete the value.
	if _, _, err := MVCCDelete(ctx, engine, key, txn.ReadTimestamp, MVCCWriteOptions{Txn: txn, Stats: aggMS}); err != nil {
		t.Fatal(err)
	}

	v2ValSize := int64(0) // tombstone
	if disableSimpleValueEncoding {
		v2ValSize += emptyMVCCValueHeaderSize // 7
	}

	expAggMS := enginepb.MVCCStats{
		LastUpdateNanos: 1e9,
		LiveBytes:       0,
		LiveCount:       0,
		KeyCount:        1,
		ValCount:        1,
		KeyBytes:        mKeySize + vKeySize,
		ValBytes:        m2ValSize + v2ValSize,
		LockAge:         0,
		IntentCount:     1,
		LockCount:       1,
		IntentBytes:     vKeySize + v2ValSize,
		GCBytesAge:      0,
	}

	assertEq(t, engine, "after deleting", aggMS, &expAggMS)

	// Now commit the value and roll back the delete.
	txn.Status = roachpb.COMMITTED
	txn.AddIgnoredSeqNumRange(enginepb.IgnoredSeqNumRange{Start: 1, End: 1})
	if _, _, _, _, err := MVCCResolveWriteIntent(ctx, engine, aggMS,
		roachpb.MakeLockUpdate(txn, roachpb.Span{Key: key}),
		MVCCResolveWriteIntentOptions{},
	); err != nil {
		t.Fatal(err)
	}

	expAggMS = enginepb.MVCCStats{
		LastUpdateNanos: 1e9,
		LiveBytes:       mKeySize + vKeySize + vValSize,
		LiveCount:       1, // the key is live after the rollback
		KeyCount:        1,
		ValCount:        1,
		KeyBytes:        mKeySize + vKeySize,
		ValBytes:        vValSize,
		GCBytesAge:      0,
	}

	assertEq(t, engine, "after committing", aggMS, &expAggMS)
}

// TestMVCCStatsDeleteRollbackPut exercises the case in which a deletion
// tombstone is written, then re-written by an intent, which is rolled back. We
// expect the stats to be adjusted correctly for the rolled back intent, and
// crucially, for the LiveBytes and LiveCount to be zero.
func TestMVCCStatsDeleteRollbackPut(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	ctx := context.Background()
	aggMS := &enginepb.MVCCStats{}
	assertEq(t, engine, "initially", aggMS, &enginepb.MVCCStats{})

	key := roachpb.Key("a")
	value := roachpb.MakeValueFromString("value")
	ts := hlc.Timestamp{WallTime: 1e9}
	txn := &roachpb.Transaction{
		TxnMeta:       enginepb.TxnMeta{ID: uuid.MakeV4(), WriteTimestamp: ts},
		ReadTimestamp: ts,
	}

	// Delete the value.
	if _, _, err := MVCCDelete(ctx, engine, key, txn.ReadTimestamp, MVCCWriteOptions{Txn: txn, Stats: aggMS}); err != nil {
		t.Fatal(err)
	}

	mKeySize := int64(mvccKey(key).EncodedSize()) // 2
	mValSize := int64((&enginepb.MVCCMetadata{    // 46
		Timestamp: ts.ToLegacyTimestamp(),
		Deleted:   true,
		Txn:       &txn.TxnMeta,
	}).Size())
	mValSize += 2
	vKeySize := MVCCVersionTimestampSize // 12
	vValSize := int64(0)                 // tombstone
	if disableSimpleValueEncoding {
		vValSize += emptyMVCCValueHeaderSize // 7
	}

	expMS := enginepb.MVCCStats{
		LastUpdateNanos: 1e9,
		LiveBytes:       0,
		LiveCount:       0,
		KeyBytes:        mKeySize + vKeySize,
		KeyCount:        1,
		ValBytes:        mValSize + vValSize,
		ValCount:        1,
		IntentCount:     1,
		LockCount:       1,
		IntentBytes:     vKeySize + vValSize,
		GCBytesAge:      0,
	}
	assertEq(t, engine, "after delete", aggMS, &expMS)

	txn.Sequence++

	// Annoyingly, the new meta value is actually a little larger thanks to the
	// sequence number. Also since there was a write previously on the same
	// transaction, the IntentHistory will add a few bytes to the metadata.
	encValue, err := EncodeMVCCValue(MVCCValue{Value: roachpb.Value{RawBytes: []byte{}}})
	require.NoError(t, err)
	m2ValSize := int64((&enginepb.MVCCMetadata{
		Timestamp: ts.ToLegacyTimestamp(),
		Txn:       &txn.TxnMeta,
		Deleted:   false,
		IntentHistory: []enginepb.MVCCMetadata_SequencedIntent{
			{Sequence: 0, Value: encValue},
		},
	}).Size())
	expM2ValSize := 54
	if disableSimpleValueEncoding {
		expM2ValSize += int(emptyMVCCValueHeaderSize)
	}
	require.EqualValues(t, expM2ValSize, m2ValSize)

	// Put the value.
	if _, err := MVCCPut(ctx, engine, key, ts, value, MVCCWriteOptions{Txn: txn, Stats: aggMS}); err != nil {
		t.Fatal(err)
	}

	v2ValSize := int64(len(value.RawBytes))
	if disableSimpleValueEncoding {
		v2ValSize += emptyMVCCValueHeaderSize // 17
	}

	expAggMS := enginepb.MVCCStats{
		LastUpdateNanos: 1e9,
		LiveBytes:       mKeySize + m2ValSize + vKeySize + v2ValSize,
		LiveCount:       1,
		KeyCount:        1,
		ValCount:        1,
		KeyBytes:        mKeySize + vKeySize,
		ValBytes:        m2ValSize + v2ValSize,
		LockAge:         0,
		IntentCount:     1,
		LockCount:       1,
		IntentBytes:     vKeySize + v2ValSize,
		GCBytesAge:      0,
	}

	assertEq(t, engine, "after put", aggMS, &expAggMS)

	// Now commit the value and roll back the put.
	txn.Status = roachpb.COMMITTED
	txn.AddIgnoredSeqNumRange(enginepb.IgnoredSeqNumRange{Start: 1, End: 1})
	if _, _, _, _, err := MVCCResolveWriteIntent(ctx, engine, aggMS,
		roachpb.MakeLockUpdate(txn, roachpb.Span{Key: key}),
		MVCCResolveWriteIntentOptions{},
	); err != nil {
		t.Fatal(err)
	}

	expAggMS = enginepb.MVCCStats{
		LastUpdateNanos: 1e9,
		LiveBytes:       0,
		LiveCount:       0, // the key is not live after the rollback
		KeyCount:        1,
		ValCount:        1,
		KeyBytes:        mKeySize + vKeySize,
		ValBytes:        vValSize,
		GCBytesAge:      0,
	}

	assertEq(t, engine, "after committing", aggMS, &expAggMS)
}

var mvccStatsTests = []struct {
	name string
	fn   func(Reader, roachpb.Key, roachpb.Key, int64) (enginepb.MVCCStats, error)
}{
	{
		name: "ComputeStats",
		fn: func(r Reader, start, end roachpb.Key, nowNanos int64) (enginepb.MVCCStats, error) {
			return ComputeStats(context.Background(), r, start, end, nowNanos)
		},
	},
	{
		name: "ComputeStatsForIter",
		fn: func(r Reader, start, end roachpb.Key, nowNanos int64) (enginepb.MVCCStats, error) {
			iter, err := r.NewMVCCIterator(context.Background(), MVCCKeyAndIntentsIterKind, IterOptions{
				KeyTypes:   IterKeyTypePointsAndRanges,
				LowerBound: start,
				UpperBound: end,
			})
			if err != nil {
				return enginepb.MVCCStats{}, err
			}
			defer iter.Close()
			iter.SeekGE(MVCCKey{Key: start})
			return ComputeStatsForIter(iter, nowNanos)
		},
	},
}

type state struct {
	MSDelta *enginepb.MVCCStats
	TS      hlc.Timestamp
	Txn     *roachpb.Transaction

	batch      Batch
	rng        *rand.Rand
	key        roachpb.Key
	inline     bool
	isLocalKey bool
	internal   struct {
		ms  *enginepb.MVCCStats
		eng Engine
	}
}

func (s *state) intent(status roachpb.TransactionStatus) roachpb.LockUpdate {
	intent := roachpb.MakeLockUpdate(s.Txn, roachpb.Span{Key: s.key})
	intent.Status = status
	return intent
}

func (s *state) intentRange(status roachpb.TransactionStatus) roachpb.LockUpdate {
	keyMin := keys.LocalMax
	keyMax := roachpb.KeyMax
	if isLocal(s.key) {
		keyMin = roachpb.KeyMin
		keyMax = keys.LocalMax
	}
	intent := roachpb.MakeLockUpdate(s.Txn, roachpb.Span{Key: keyMin, EndKey: keyMax})
	intent.Status = status
	return intent
}

func (s *state) rngVal() roachpb.Value {
	return roachpb.MakeValueFromBytes(randutil.RandBytes(s.rng, int(s.rng.Int31n(128))))
}

type randomTest struct {
	state

	inline      bool
	actions     map[string]func(*state) (ok bool, info string)
	actionNames []string // auto-populated
	cycle       int
}

func (s *randomTest) step(t *testing.T) {
	if !s.inline {
		// Jump up to a few seconds into the future. In ~1% of cases, jump
		// backwards instead (this exercises intactness on WriteTooOld, etc).
		s.TS = hlc.Timestamp{
			WallTime: s.TS.WallTime + int64((s.state.rng.Float32()-0.01)*4e9),
			Logical:  int32(s.rng.Intn(10)),
		}
		if s.TS.WallTime < 0 {
			// See TestMVCCStatsDocumentNegativeWrites. Negative MVCC timestamps
			// aren't something we're interested in, and besides, they corrupt
			// everything.
			s.TS.WallTime = 0
		}
		// If this is a transactional request, ensure the request timestamp is the
		// same as the transaction's read timestamp. Otherwise, writes will return
		// an error from mvccPutInternal.
		if s.Txn != nil {
			s.TS = s.Txn.ReadTimestamp
		}
	} else {
		s.TS = hlc.Timestamp{}
		s.inline = true
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
	if s.Txn != nil {
		s.Txn.Sequence++
	}
	s.batch = s.internal.eng.NewBatch()
	*s.MSDelta = enginepb.MVCCStats{}
	ok, info := s.actions[actName](&s.state)
	if ok {
		s.internal.ms.Add(*s.MSDelta)
		require.NoError(t, s.batch.Commit(false /* sync */))
		if *s.MSDelta != (enginepb.MVCCStats{}) {
			delta := *s.MSDelta
			delta.AgeTo(0)
			abs := *s.internal.ms
			abs.AgeTo(0)
			info += fmt.Sprintf("\n\tstats delta: %+v\nabsolute: %+v", delta, abs)
		}
	}
	s.batch.Close()

	txnS := "<none>"
	if preTxn != nil {
		txnS = preTxn.WriteTimestamp.String()
	}

	if info != "" {
		info = "\n\t" + info
	}
	var noopS string
	if !ok {
		noopS = " [noop]"
	}
	t.Logf("%10s %s txn=%s%s%s", s.TS, actName, txnS, noopS, info)

	// Verify stats agree with recomputations.
	assertEqImpl(t, s.internal.eng, fmt.Sprintf("cycle %d", s.cycle), !s.isLocalKey, s.internal.ms, s.internal.ms)

	if t.Failed() {
		t.FailNow()
	}
}

func TestMVCCStatsRandomized(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// NB: no failure type ever required count five or more. When there is a result
	// found by this test, or any other MVCC code is changed, it's worth reducing
	// this first to two, three, ... and running the test for a minute to get a
	// good idea of minimally reproducing examples.
	const count = 200
	// Allows easily trimming down the set of actions to
	// schedule to more easily minimize repro for a known problem.
	//
	// When empty, enables all actions (that's what's checked in).
	enabledActions := map[string]struct{}{}

	actions := make(map[string]func(*state) (wrote bool, info string))

	currentKeySpan := func(s *state) roachpb.Span {
		keyMin := roachpb.KeyMin
		keyMax := roachpb.KeyMax
		if s.isLocalKey {
			keyMax = keys.LocalMax
		} else {
			keyMin = keys.LocalMax
		}
		return roachpb.Span{Key: keyMin, EndKey: keyMax}
	}

	actions["Put"] = func(s *state) (bool, string) {
		opts := MVCCWriteOptions{
			Txn:   s.Txn,
			Stats: s.MSDelta,
		}
		if _, err := MVCCPut(ctx, s.batch, s.key, s.TS, s.rngVal(), opts); err != nil {
			return false, err.Error()
		}
		return true, ""
	}
	actions["InitPut"] = func(s *state) (bool, string) {
		opts := MVCCWriteOptions{
			Txn:   s.Txn,
			Stats: s.MSDelta,
		}
		failOnTombstones := s.rng.Intn(2) == 0
		desc := fmt.Sprintf("failOnTombstones=%t", failOnTombstones)
		if _, err := MVCCInitPut(ctx, s.batch, s.key, s.TS, s.rngVal(), failOnTombstones, opts); err != nil {
			return false, desc + ": " + err.Error()
		}
		return true, desc
	}
	actions["Del"] = func(s *state) (bool, string) {
		opts := MVCCWriteOptions{
			Txn:   s.Txn,
			Stats: s.MSDelta,
		}
		if _, _, err := MVCCDelete(ctx, s.batch, s.key, s.TS, opts); err != nil {
			return false, err.Error()
		}
		return true, ""
	}
	actions["DelRange"] = func(s *state) (bool, string) {
		opts := MVCCWriteOptions{
			Txn:   s.Txn,
			Stats: s.MSDelta,
		}
		keySpan := currentKeySpan(s)

		mvccRangeDel := !s.isLocalKey && s.Txn == nil && s.rng.Intn(2) == 0
		var mvccRangeDelKey, mvccRangeDelEndKey roachpb.Key
		var predicates kvpb.DeleteRangePredicates
		if mvccRangeDel {
			mvccRangeDelKey = keys.LocalMax
			mvccRangeDelEndKey = roachpb.KeyMax
			n := s.rng.Intn(5)
			switch n {
			case 0: // leave as is
			case 1:
				mvccRangeDelKey = s.key
			case 2:
				mvccRangeDelEndKey = s.key.Next()
			case 3:
				mvccRangeDelKey = s.key.Next()
			case 4:
				mvccRangeDelEndKey = s.key.Prevish(10)
			}

			if !s.inline && s.rng.Intn(2) == 0 {
				predicates.StartTime.WallTime = s.rng.Int63n(s.TS.WallTime + 1)
			}
		}

		returnKeys := !mvccRangeDel && (s.rng.Intn(2) == 0)
		var max int64
		if !mvccRangeDel {
			max = s.rng.Int63n(5)
		}

		var desc string
		var err error
		if !mvccRangeDel {
			desc = fmt.Sprintf("mvccDeleteRange=%s, returnKeys=%t, max=%d", keySpan, returnKeys, max)
			_, _, _, _, err = MVCCDeleteRange(
				ctx, s.batch, keySpan.Key, keySpan.EndKey, max, s.TS, opts, returnKeys,
			)
		} else if predicates == (kvpb.DeleteRangePredicates{}) {
			desc = fmt.Sprintf("mvccDeleteRangeUsingTombstone=%s",
				roachpb.Span{Key: mvccRangeDelKey, EndKey: mvccRangeDelEndKey})
			const idempotent = false
			const maxLockConflicts = 0        // unlimited
			const targetLockConflictBytes = 0 // unlimited
			msCovered := (*enginepb.MVCCStats)(nil)
			err = MVCCDeleteRangeUsingTombstone(
				ctx, s.batch, s.MSDelta, mvccRangeDelKey, mvccRangeDelEndKey, s.TS, hlc.ClockTimestamp{}, nil, /* leftPeekBound */
				nil /* rightPeekBound */, idempotent, maxLockConflicts, targetLockConflictBytes, msCovered,
			)
		} else {
			rangeTombstoneThreshold := s.rng.Int63n(5)
			desc = fmt.Sprintf("mvccPredicateDeleteRange=%s, predicates=%#v rangeTombstoneThreshold=%d",
				roachpb.Span{Key: mvccRangeDelKey, EndKey: mvccRangeDelEndKey}, predicates, rangeTombstoneThreshold)
			const maxLockConflicts = 0        // unlimited
			const targetLockConflictBytes = 0 // unlimited
			_, err = MVCCPredicateDeleteRange(ctx, s.batch, s.MSDelta, mvccRangeDelKey, mvccRangeDelEndKey,
				s.TS, hlc.ClockTimestamp{}, nil /* leftPeekBound */, nil, /* rightPeekBound */
				predicates, 0, 0, rangeTombstoneThreshold, maxLockConflicts, targetLockConflictBytes)
		}
		if err != nil {
			return false, desc + ": " + err.Error()
		}
		return true, desc
	}

	actions["ClearTimeRange"] = func(s *state) (bool, string) {
		if s.inline {
			return false, ""
		}
		keySpan := currentKeySpan(s)

		// TODO(erikgrinaker): MVCCClearTimeRange has known stats bugs when data
		// exists above endTime. For now, use MaxTimestamp. See:
		// https://github.com/cockroachdb/cockroach/issues/88909
		startTime := hlc.Timestamp{WallTime: rand.Int63n(s.TS.WallTime + 1)}
		endTime := hlc.MaxTimestamp
		clearRangeThreshold := int(s.rng.Int63n(5))

		desc := fmt.Sprintf("mvccClearTimeRange=%s, startTime=%s, endTime=%s", keySpan, startTime, endTime)
		_, err := MVCCClearTimeRange(ctx, s.batch, s.MSDelta, keySpan.Key, keySpan.EndKey,
			startTime, endTime, nil /* leftPeekBound */, nil /* rightPeekBound */, clearRangeThreshold, 0, 0, 0)
		if err != nil {
			desc += " " + err.Error()
			return false, desc + ": " + err.Error()
		}

		return true, desc
	}
	actions["AcquireLock"] = func(s *state) (bool, string) {
		str := lock.Shared
		if s.rng.Intn(2) != 0 {
			str = lock.Exclusive
		}
		var txnMeta *enginepb.TxnMeta
		var ignoredSeq []enginepb.IgnoredSeqNumRange
		if s.Txn != nil {
			txnMeta = &s.Txn.TxnMeta
			ignoredSeq = s.Txn.IgnoredSeqNums
		}
		if err := MVCCAcquireLock(ctx, s.batch, txnMeta, ignoredSeq, str, s.key, s.MSDelta, 0, 0); err != nil {
			return false, err.Error()
		}
		return true, ""
	}
	actions["EnsureTxn"] = func(s *state) (bool, string) {
		if s.Txn == nil {
			txn := roachpb.MakeTransaction("test", nil, 0, 0, s.TS, 0, 1, 0, false /* omitInRangefeeds */)
			s.Txn = &txn
		}
		return true, ""
	}

	resolve := func(s *state, status roachpb.TransactionStatus) (bool, string) {
		ranged := s.rng.Intn(2) == 0
		desc := fmt.Sprintf("ranged=%t", ranged)
		if s.Txn != nil {
			if !ranged {
				if _, _, _, _, err := MVCCResolveWriteIntent(ctx, s.batch, s.MSDelta, s.intent(status), MVCCResolveWriteIntentOptions{}); err != nil {
					return false, desc + ": " + err.Error()
				}
			} else {
				max := s.rng.Int63n(5)
				desc += fmt.Sprintf(", max=%d", max)
				if _, _, _, _, _, err := MVCCResolveWriteIntentRange(
					ctx, s.batch, s.MSDelta, s.intentRange(status),
					MVCCResolveWriteIntentRangeOptions{}); err != nil {
					return false, desc + ": " + err.Error()
				}
			}
			if status != roachpb.PENDING {
				s.Txn = nil
			}
		}
		return true, desc
	}

	actions["Abort"] = func(s *state) (bool, string) {
		return resolve(s, roachpb.ABORTED)
	}
	actions["Commit"] = func(s *state) (bool, string) {
		return resolve(s, roachpb.COMMITTED)
	}
	actions["Rollback"] = func(s *state) (bool, string) {
		if s.Txn != nil {
			for i := 0; i < int(s.Txn.Sequence); i++ {
				if s.rng.Intn(2) == 0 {
					s.Txn.AddIgnoredSeqNumRange(enginepb.IgnoredSeqNumRange{Start: enginepb.TxnSeq(i), End: enginepb.TxnSeq(i)})
				}
			}
			desc := fmt.Sprintf("ignored=%v", s.Txn.IgnoredSeqNums)
			return true, desc
		}
		return false, ""
	}
	actions["Push"] = func(s *state) (bool, string) {
		return resolve(s, roachpb.PENDING)
	}
	actions["GC"] = func(s *state) (bool, string) {
		// Sometimes GC everything, sometimes only older versions.
		gcTS := hlc.Timestamp{
			WallTime: s.rng.Int63n(s.TS.WallTime + 1), // avoid zero
		}
		if err := MVCCGarbageCollect(
			ctx,
			s.batch,
			s.MSDelta,
			[]kvpb.GCRequest_GCKey{{
				Key:       s.key,
				Timestamp: gcTS,
			}},
			s.TS,
		); err != nil {
			return false, err.Error()
		}

		if s.isLocalKey {
			// Range keys are not used for local keyspace.
			return true, fmt.Sprint(gcTS)
		}

		keySpan := currentKeySpan(s)
		mvccGCRangeKey := keySpan.Key
		mvccGCRangeEndKey := keySpan.EndKey
		n := s.rng.Intn(5)
		switch n {
		case 0: // leave as is
		case 1:
			mvccGCRangeKey = s.key
		case 2:
			mvccGCRangeEndKey = s.key.Next()
		case 3:
			mvccGCRangeKey = s.key
			mvccGCRangeEndKey = s.key.Next()
		}

		if err := MVCCGarbageCollectRangeKeys(
			ctx,
			s.batch,
			s.MSDelta,
			[]CollectableGCRangeKey{
				{
					MVCCRangeKey: MVCCRangeKey{
						StartKey:  mvccGCRangeKey,
						EndKey:    mvccGCRangeEndKey,
						Timestamp: gcTS,
					},
					LatchSpan: roachpb.Span{
						Key:    keySpan.Key,
						EndKey: keySpan.EndKey,
					},
				},
			},
		); err != nil {
			return false, err.Error()
		}

		return true, fmt.Sprint(gcTS)
	}

	if len(enabledActions) == 0 {
		for name := range actions {
			enabledActions[name] = struct{}{}
		}
	}

	// Remove actions that are not enabled.
	for name := range actions {
		_, enabled := enabledActions[name]
		if !enabled {
			delete(actions, name)
		}
	}

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
				t.Logf("seed: %d", test.seed)
				eng := NewDefaultInMemForTesting()
				defer eng.Close()

				s := &randomTest{
					actions: actions,
					inline:  inline,
					state: state{
						rng:        rand.New(rand.NewSource(test.seed)),
						key:        test.key,
						isLocalKey: keys.IsLocal(test.key),
						MSDelta:    &enginepb.MVCCStats{},
					},
				}
				s.internal.ms = &enginepb.MVCCStats{}
				s.internal.eng = eng

				for i := 0; i < count; i++ {
					s.step(t)
				}
			})
		})
	}
}

func TestMVCCComputeStatsError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	engine := NewDefaultInMemForTesting()
	defer engine.Close()

	// Write a MVCC metadata key where the value is not an encoded MVCCMetadata
	// protobuf.
	if err := engine.PutUnversioned(roachpb.Key("garbage"), []byte("garbage")); err != nil {
		t.Fatal(err)
	}

	for _, mvccStatsTest := range mvccStatsTests {
		t.Run(mvccStatsTest.name, func(t *testing.T) {
			_, err := mvccStatsTest.fn(engine, keys.LocalMax, roachpb.KeyMax, 100)
			if e := "unable to decode MVCCMetadata"; !testutils.IsError(err, e) {
				t.Fatalf("expected %s, got %v", e, err)
			}
		})
	}
}
