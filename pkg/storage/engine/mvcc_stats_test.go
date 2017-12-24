// Copyright 2014 The Cockroach Authors.
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
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"unsafe"

	"github.com/kr/pretty"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// assertEq compares the given ms and expMS and errors when they don't match. It
// also recomputes the stats over the whole engine with all known
// implementations and errors on mismatch with any of them.
func assertEq(t *testing.T, engine Engine, debug string, ms, expMS *enginepb.MVCCStats) {
	t.Helper()

	msCpy := *ms // shallow copy
	ms = &msCpy
	ms.AgeTo(expMS.LastUpdateNanos)
	if !ms.Equal(expMS) {
		t.Errorf("%s: diff(ms, expMS) = %s", debug, pretty.Diff(ms, expMS))
	}

	it := engine.NewIterator(false)
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
	engine := createTestEngine()
	defer engine.Close()

	ctx := context.Background()
	aggMS := &enginepb.MVCCStats{}

	assertEq(t, engine, "initially", aggMS, &enginepb.MVCCStats{})

	key := roachpb.Key("a")
	ts1 := hlc.Timestamp{WallTime: 1E9}
	// Put a value.
	value := roachpb.MakeValueFromString("value")
	if err := MVCCPut(context.Background(), engine, aggMS, key, ts1, value, nil); err != nil {
		t.Fatal(err)
	}

	mKeySize := int64(mvccKey(key).EncodedSize()) // 2
	vKeySize := mvccVersionTimestampSize          // 12
	vValSize := int64(len(value.RawBytes))        // 10

	log.Infof(ctx, "mKeySize=%d vKeySize=%d vValSize=%d", mKeySize, vKeySize, vValSize)

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
	txn := &roachpb.Transaction{TxnMeta: enginepb.TxnMeta{ID: uuid.MakeV4(), Timestamp: ts3}}
	txn.Timestamp.Forward(ts3)
	if err := MVCCDelete(context.Background(), engine, aggMS, key, ts3, txn); err != nil {
		t.Fatal(err)
	}

	// Now commit the value, but with a timestamp gap (i.e. this is a
	// push-commit as it would happen for a SNAPSHOT txn)
	ts4 := hlc.Timestamp{WallTime: 4 * 1E9}
	txn.Status = roachpb.COMMITTED
	txn.Timestamp.Forward(ts4)
	if err := MVCCResolveWriteIntent(context.Background(), engine, aggMS, roachpb.Intent{Span: roachpb.Span{Key: key}, Status: txn.Status, Txn: txn.TxnMeta}); err != nil {
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
		KeyBytes:   mKeySize + 2*vKeySize,
		ValBytes:   vValSize,                  // the initial write (10)...
		GCBytesAge: (vValSize + vKeySize) * 3, // ... along with the value (12) aged over 3s, a total of 66
	}

	assertEq(t, engine, "after committing", aggMS, &expAggMS)
}

// TestMVCCStatsPutCommitMovesTimestamp is similar to
// TestMVCCStatsDeleteCommitMovesTimestamp, but is simpler: a first intent is
// written and then committed at a later timestamp.
func TestMVCCStatsPutCommitMovesTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
	defer engine.Close()

	ctx := context.Background()
	aggMS := &enginepb.MVCCStats{}

	assertEq(t, engine, "initially", aggMS, &enginepb.MVCCStats{})

	key := roachpb.Key("a")
	ts1 := hlc.Timestamp{WallTime: 1E9}
	txn := &roachpb.Transaction{TxnMeta: enginepb.TxnMeta{ID: uuid.MakeV4(), Timestamp: ts1}}
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
	vKeySize := mvccVersionTimestampSize   // 12
	vValSize := int64(len(value.RawBytes)) // 10

	expMS := enginepb.MVCCStats{
		LastUpdateNanos: 1E9,
		LiveBytes:       mKeySize + mValSize + vKeySize + vValSize, // 2+44+12+10 = 68
		LiveCount:       1,
		KeyBytes:        mKeySize + vKeySize, // 14
		KeyCount:        1,
		ValBytes:        mValSize + vValSize, // 44+10 = 54
		ValCount:        1,
		IntentCount:     1,
		IntentBytes:     vKeySize + vValSize, // 12+10 = 22
	}
	assertEq(t, engine, "after put", aggMS, &expMS)

	// Now commit the intent, but with a timestamp gap (i.e. this is a
	// push-commit as it would happen for a SNAPSHOT txn)
	ts4 := hlc.Timestamp{WallTime: 4 * 1E9}
	txn.Status = roachpb.COMMITTED
	txn.Timestamp.Forward(ts4)
	if err := MVCCResolveWriteIntent(ctx, engine, aggMS, roachpb.Intent{Span: roachpb.Span{Key: key}, Status: txn.Status, Txn: txn.TxnMeta}); err != nil {
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
}

// TestMVCCStatsPutPushMovesTimestamp is similar to TestMVCCStatsPutCommitMovesTimestamp:
// An intent is written and then re-written at a higher timestamp. This formerly messed up
// the IntentAge computation.
func TestMVCCStatsPutPushMovesTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
	defer engine.Close()

	ctx := context.Background()
	aggMS := &enginepb.MVCCStats{}

	assertEq(t, engine, "initially", aggMS, &enginepb.MVCCStats{})

	key := roachpb.Key("a")
	ts1 := hlc.Timestamp{WallTime: 1E9}
	txn := &roachpb.Transaction{TxnMeta: enginepb.TxnMeta{ID: uuid.MakeV4(), Timestamp: ts1}}
	// Write an intent.
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
	vKeySize := mvccVersionTimestampSize   // 12
	vValSize := int64(len(value.RawBytes)) // 10

	expMS := enginepb.MVCCStats{
		LastUpdateNanos: 1E9,
		LiveBytes:       mKeySize + mValSize + vKeySize + vValSize, // 2+44+12+10 = 68
		LiveCount:       1,
		KeyBytes:        mKeySize + vKeySize, // 14
		KeyCount:        1,
		ValBytes:        mValSize + vValSize, // 44+10 = 54
		ValCount:        1,
		IntentCount:     1,
		IntentBytes:     vKeySize + vValSize, // 12+10 = 22
	}
	assertEq(t, engine, "after put", aggMS, &expMS)

	// Now push the value, but with a timestamp gap (i.e. this is a
	// push as it would happen for a SNAPSHOT txn)
	ts4 := hlc.Timestamp{WallTime: 4 * 1E9}
	txn.Timestamp.Forward(ts4)
	if err := MVCCResolveWriteIntent(ctx, engine, aggMS, roachpb.Intent{Span: roachpb.Span{Key: key}, Status: txn.Status, Txn: txn.TxnMeta}); err != nil {
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
}

func TestMVCCStatsDelDelGC(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
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
	vKeySize := mvccVersionTimestampSize          // 12

	expMS := enginepb.MVCCStats{
		LastUpdateNanos: 2E9,
		KeyBytes:        mKeySize + 2*vKeySize, // 26
		KeyCount:        1,
		ValCount:        2,
		GCBytesAge:      vKeySize, // first tombstone, aged from ts1 to ts2
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
}

// TestMVCCStatsPutIntentTimestampNotPutTimestamp exercises a scenario in which
// an intent is rewritten to a lower timestamp. This formerly caused bugs
// because when computing the stats updates, there was an implicit assumption
// that the meta entries would always move forward in time.
func TestMVCCStatsPutIntentTimestampNotPutTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
	defer engine.Close()

	ctx := context.Background()
	aggMS := &enginepb.MVCCStats{}

	assertEq(t, engine, "initially", aggMS, &enginepb.MVCCStats{})

	key := roachpb.Key("a")
	ts201 := hlc.Timestamp{WallTime: 2E9 + 1}
	ts099 := hlc.Timestamp{WallTime: 1E9 - 1}
	txn := &roachpb.Transaction{TxnMeta: enginepb.TxnMeta{ID: uuid.MakeV4(), Timestamp: ts201}}
	// Write an intent at 2s+1.
	value := roachpb.MakeValueFromString("value")
	if err := MVCCPut(ctx, engine, aggMS, key, ts201, value, txn); err != nil {
		t.Fatal(err)
	}

	mKeySize := int64(mvccKey(key).EncodedSize()) // 2
	m1ValSize := int64((&enginepb.MVCCMetadata{   // 44
		Timestamp: hlc.LegacyTimestamp(ts201),
		Txn:       &txn.TxnMeta,
	}).Size())
	vKeySize := mvccVersionTimestampSize   // 12
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

	// Annoyingly, the new meta value is actually a little larger thanks to the
	// sequence number.
	m2ValSize := int64((&enginepb.MVCCMetadata{ // 46
		Timestamp: hlc.LegacyTimestamp(ts201),
		Txn:       &txn.TxnMeta,
	}).Size())
	if err := MVCCPut(ctx, engine, aggMS, key, ts099, value, txn); err != nil {
		t.Fatal(err)
	}

	expAggMS := enginepb.MVCCStats{
		// Surprise: the new intent actually lives at 1E9-1, and it's now
		// 2E9+1, so it has accumulated an age of two. This formerly failed
		// to register.
		IntentAge: 2,

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
}

// TestMVCCStatsDocumentNegativeWrites documents that things go wrong when you
// write at a negative timestamp. We shouldn't do that in practice and perhaps
// we should have it error outright.
func TestMVCCStatsDocumentNegativeWrites(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
	defer engine.Close()

	ctx := context.Background()
	aggMS := &enginepb.MVCCStats{}

	assertEq(t, engine, "initially", aggMS, &enginepb.MVCCStats{})

	key := roachpb.Key("a")

	// Do something funky: write a key at a negative WallTime. This must never
	// happen in practice but it did in `TestMVCCStatsRandomized` (no more).
	tsNegative := hlc.Timestamp{WallTime: -1}

	// Put a deletion tombstone. We just need something at a negative timestamp
	// that generates GCByteAge and this is the simplest we can do.
	if err := MVCCDelete(ctx, engine, aggMS, key, tsNegative, nil); err != nil {
		t.Fatal(err)
	}

	mKeySize := int64(mvccKey(key).EncodedSize()) // 2
	vKeySize := mvccVersionTimestampSize          // 12

	expMS := enginepb.MVCCStats{
		LastUpdateNanos: 0,
		KeyBytes:        mKeySize + vKeySize, // 14
		KeyCount:        1,
		ValCount:        1,
	}
	assertEq(t, engine, "after deletion", aggMS, &expMS)

	// Do it again at higher timestamp to expose that we've corrupted things.
	ts1 := hlc.Timestamp{WallTime: 1E9}
	if err := MVCCDelete(ctx, engine, aggMS, key, ts1, nil); err != nil {
		t.Fatal(err)
	}

	expMS = enginepb.MVCCStats{
		LastUpdateNanos: 1E9,
		KeyBytes:        mKeySize + 2*vKeySize, // 2 + 24 = 26
		KeyCount:        1,
		ValCount:        2,
		// vKeySize is what you'd kinda expect. Really you would hope to also
		// see the transition through zero as adding to the factor (picking up a
		// 2x), but this isn't true (we compute the number of steps via
		// now/1E9-ts/1E9, which doesn't handle this case). What we get is even
		// more surprising though, and if you dig into it, you'll see that the
		// negative-timestamp value has become the first value (i.e. it has
		// inverted with the one written at ts1). We're screwed.
		GCBytesAge: vKeySize + mKeySize, // 14
	}
	// Make the test pass with what comes out of recomputing from the engine:
	// The value at -1 is now the first value, so it picks up one second GCBytesAge
	// but gets to claim mKeySize as part of itself (which it wouldn't if it were
	// in its proper place).
	aggMS.GCBytesAge += mKeySize
	assertEq(t, engine, "after second deletion", aggMS, &expMS)
}

// TestMVCCStatsBasic writes a value, then deletes it as an intent via a
// transaction, then resolves the intent, manually verifying the mvcc stats at
// each step.
func TestMVCCStatsBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
	defer engine.Close()

	ms := &enginepb.MVCCStats{}

	assertEq(t, engine, "initially", ms, &enginepb.MVCCStats{})

	// Verify size of mvccVersionTimestampSize.
	ts := hlc.Timestamp{WallTime: 1 * 1E9}
	key := roachpb.Key("a")
	keySize := int64(mvccVersionKey(key, ts).EncodedSize() - mvccKey(key).EncodedSize())
	if keySize != mvccVersionTimestampSize {
		t.Errorf("expected version timestamp size %d; got %d", mvccVersionTimestampSize, keySize)
	}

	// Put a value.
	value := roachpb.MakeValueFromString("value")
	if err := MVCCPut(context.Background(), engine, ms, key, ts, value, nil); err != nil {
		t.Fatal(err)
	}
	mKeySize := int64(mvccKey(key).EncodedSize())
	vKeySize := mvccVersionTimestampSize
	vValSize := int64(len(value.RawBytes))

	expMS := enginepb.MVCCStats{
		LiveBytes:       mKeySize + vKeySize + vValSize,
		LiveCount:       1,
		KeyBytes:        mKeySize + vKeySize,
		KeyCount:        1,
		ValBytes:        vValSize,
		ValCount:        1,
		LastUpdateNanos: 1E9,
	}
	assertEq(t, engine, "after put", ms, &expMS)
	if e, a := int64(0), ms.GCBytes(); e != a {
		t.Fatalf("GCBytes: expected %d, got %d", e, a)
	}

	// Delete the value using a transaction.
	// TODO(tschottdorf): this case is interesting: we write at ts2, bt the timestamp is ts1.
	// Need to check whether that's reasonable, and if so, test it more.
	txn := &roachpb.Transaction{TxnMeta: enginepb.TxnMeta{ID: uuid.MakeV4(), Timestamp: hlc.Timestamp{WallTime: 1 * 1E9}}}
	ts2 := hlc.Timestamp{WallTime: 2 * 1E9}
	if err := MVCCDelete(context.Background(), engine, ms, key, ts2, txn); err != nil {
		t.Fatal(err)
	}
	m2ValSize := int64((&enginepb.MVCCMetadata{
		Timestamp: hlc.LegacyTimestamp(ts2),
		Deleted:   true,
		Txn:       &txn.TxnMeta,
	}).Size())
	v2KeySize := mvccVersionTimestampSize
	v2ValSize := int64(0)

	expMS2 := enginepb.MVCCStats{
		KeyBytes:        mKeySize + vKeySize + v2KeySize,
		KeyCount:        1,
		ValBytes:        m2ValSize + vValSize + v2ValSize,
		ValCount:        2,
		IntentBytes:     v2KeySize + v2ValSize,
		IntentCount:     1,
		IntentAge:       0,
		GCBytesAge:      vValSize + vKeySize, // immediately recognizes GC'able bytes from old value at 1E9
		LastUpdateNanos: 2E9,
	}
	assertEq(t, engine, "after delete", ms, &expMS2)
	// This is expMS2.KeyBytes + expMS2.ValBytes - expMS2.LiveBytes
	expGC2 := mKeySize + vKeySize + v2KeySize + m2ValSize + vValSize + v2ValSize
	if a := ms.GCBytes(); expGC2 != a {
		t.Fatalf("GCBytes: expected %d, got %d", expGC2, a)
	}

	// Resolve the deletion by aborting it.
	txn.Status = roachpb.ABORTED
	txn.Timestamp.Forward(ts2)
	if err := MVCCResolveWriteIntent(context.Background(), engine, ms, roachpb.Intent{Span: roachpb.Span{Key: key}, Status: txn.Status, Txn: txn.TxnMeta}); err != nil {
		t.Fatal(err)
	}
	// Stats should equal same as before the deletion after aborting the intent.
	expMS.LastUpdateNanos = 2E9
	assertEq(t, engine, "after abort", ms, &expMS)

	// Re-delete, but this time, we're going to commit it.
	txn.Status = roachpb.PENDING
	ts3 := hlc.Timestamp{WallTime: 3 * 1E9}
	txn.Timestamp.Forward(ts3)
	if err := MVCCDelete(context.Background(), engine, ms, key, ts3, txn); err != nil {
		t.Fatal(err)
	}
	// GCBytesAge will now count the deleted value from ts=1E9 to ts=3E9.
	expMS2.GCBytesAge = (vValSize + vKeySize) * 2
	expMS2.LastUpdateNanos = 3E9
	assertEq(t, engine, "after 2nd delete", ms, &expMS2) // should be same as before.
	if a := ms.GCBytes(); expGC2 != a {
		t.Fatalf("GCBytes: expected %d, got %d", expGC2, a)
	}

	// Write a second transactional value (i.e. an intent).
	ts4 := hlc.Timestamp{WallTime: 4 * 1E9}
	txn.Timestamp = ts4
	key2 := roachpb.Key("b")
	value2 := roachpb.MakeValueFromString("value")
	if err := MVCCPut(context.Background(), engine, ms, key2, ts4, value2, txn); err != nil {
		t.Fatal(err)
	}
	mKey2Size := int64(mvccKey(key2).EncodedSize())
	mVal2Size := int64((&enginepb.MVCCMetadata{
		Timestamp: hlc.LegacyTimestamp(ts4),
		Txn:       &txn.TxnMeta,
	}).Size())
	vKey2Size := mvccVersionTimestampSize
	vVal2Size := int64(len(value2.RawBytes))
	expMS3 := enginepb.MVCCStats{
		KeyBytes:    mKeySize + vKeySize + v2KeySize + mKey2Size + vKey2Size,
		KeyCount:    2,
		ValBytes:    m2ValSize + vValSize + v2ValSize + mVal2Size + vVal2Size,
		ValCount:    3,
		LiveBytes:   mKey2Size + vKey2Size + mVal2Size + vVal2Size,
		LiveCount:   1,
		IntentBytes: v2KeySize + v2ValSize + vKey2Size + vVal2Size,
		IntentCount: 2,
		IntentAge:   1,
		// It gets interesting: The first term is the contribution from the
		// deletion of the first put (written at 1s, deleted at 3s). From 3s
		// to 4s, on top of that we age the intent's meta entry plus deletion
		// tombstone on top of that (expGC2).
		GCBytesAge:      (vValSize+vKeySize)*2 + expGC2,
		LastUpdateNanos: 4E9,
	}

	expGC3 := expGC2 // no change, didn't delete anything
	assertEq(t, engine, "after 2nd put", ms, &expMS3)
	if a := ms.GCBytes(); expGC3 != a {
		t.Fatalf("GCBytes: expected %d, got %d", expGC3, a)
	}

	// Now commit both values.
	txn.Status = roachpb.COMMITTED
	if err := MVCCResolveWriteIntent(context.Background(), engine, ms, roachpb.Intent{Span: roachpb.Span{Key: key}, Status: txn.Status, Txn: txn.TxnMeta}); err != nil {
		t.Fatal(err)
	}
	expMS4 := enginepb.MVCCStats{
		KeyBytes:    mKeySize + vKeySize + v2KeySize + mKey2Size + vKey2Size,
		KeyCount:    2,
		ValBytes:    vValSize + v2ValSize + mVal2Size + vVal2Size,
		ValCount:    3,
		LiveBytes:   mKey2Size + vKey2Size + mVal2Size + vVal2Size,
		LiveCount:   1,
		IntentBytes: vKey2Size + vVal2Size,
		IntentCount: 1,
		// The commit turned the explicit deletion intent meta back into an
		// implicit one, so we see the originally written value which is now 3s
		// old. There is no contribution from the deletion tombstone yet as it
		// moved from 3s (inside the meta) to 4s (implicit meta), the current
		// time.
		GCBytesAge:      (vValSize + vKeySize) * 3,
		LastUpdateNanos: 4E9,
	}
	assertEq(t, engine, "after first commit", ms, &expMS4)

	// With commit of the deletion intent, what really happens is that the
	// explicit meta (carrying the intent) becomes implicit (so its key
	// gets counted in the same way by convention, but its value is now empty).
	expGC4 := expGC3 - m2ValSize
	if a := ms.GCBytes(); expGC4 != a {
		t.Fatalf("GCBytes: expected %d, got %d", expGC4, a)
	}

	if err := MVCCResolveWriteIntent(context.Background(), engine, ms, roachpb.Intent{Span: roachpb.Span{Key: key2}, Status: txn.Status, Txn: txn.TxnMeta}); err != nil {
		t.Fatal(err)
	}
	expMS4 = enginepb.MVCCStats{
		KeyBytes:        mKeySize + vKeySize + v2KeySize + mKey2Size + vKey2Size,
		KeyCount:        2,
		ValBytes:        vValSize + v2ValSize + vVal2Size,
		ValCount:        3,
		LiveBytes:       mKey2Size + vKey2Size + vVal2Size,
		LiveCount:       1,
		IntentAge:       0,
		GCBytesAge:      (vValSize + vKeySize) * 3, // unchanged; still at 4s
		LastUpdateNanos: 4E9,
	}
	assertEq(t, engine, "after second commit", ms, &expMS4)
	if a := ms.GCBytes(); expGC4 != a { // no change here
		t.Fatalf("GCBytes: expected %d, got %d", expGC4, a)
	}

	// Write over existing value to create GC'able bytes.
	ts5 := hlc.Timestamp{WallTime: 10 * 1E9} // skip ahead 6s
	if err := MVCCPut(context.Background(), engine, ms, key2, ts5, value2, nil); err != nil {
		t.Fatal(err)
	}
	expMS5 := expMS4
	expMS5.KeyBytes += vKey2Size
	expMS5.ValBytes += vVal2Size
	expMS5.ValCount = 4
	// The age increases: 6 seconds for each key2 and key.
	expMS5.GCBytesAge += (vKey2Size+vVal2Size)*6 + expGC4*6
	expMS5.LastUpdateNanos = 10E9
	assertEq(t, engine, "after overwrite", ms, &expMS5)

	// Write a transaction record which is a system-local key.
	txnKey := keys.TransactionKey(txn.Key, txn.ID)
	txnVal := roachpb.MakeValueFromString("txn-data")
	if err := MVCCPut(context.Background(), engine, ms, txnKey, hlc.Timestamp{}, txnVal, nil); err != nil {
		t.Fatal(err)
	}
	txnKeySize := int64(mvccKey(txnKey).EncodedSize())
	txnValSize := int64((&enginepb.MVCCMetadata{RawBytes: txnVal.RawBytes}).Size())
	expMS6 := expMS5
	expMS6.SysBytes += txnKeySize + txnValSize
	expMS6.SysCount++
	assertEq(t, engine, "after sys-local key", ms, &expMS6)
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

type randomTest struct {
	state

	actions     map[string]func(*state) string
	actionNames []string // auto-populated
	cycle       int
}

func (s *randomTest) step(t *testing.T) {
	// Jump up to a few seconds into the future. In ~1% of cases, jump
	// backwards instead (this exercises intactness on WriteTooOld, etc).
	s.TS = hlc.Timestamp{
		WallTime: s.TS.WallTime + int64((s.state.rng.Float32()-0.01)*4E9),
		Logical:  int32(s.rng.Intn(10)),
	}
	if s.TS.WallTime <= 0 {
		// See TestMVCCStatsDocumentNegativeWrites. Negative MVCC timestamps
		// aren't something we're interested in, and besides, they corrupt
		// everything.
		//
		// As a convenience, we also avoid zero itself (because that doesn't
		// play well with rand.Int63n).
		s.TS.WallTime = 1
	}
	if s.Txn != nil {
		// TODO(tschottdorf): experiment with s.TS != s.Txn.TS. Which of those
		// cases are reasonable and which should we catch and error out?
		//
		// Note that we already exercise txn.Timestamp > s.TS since we call
		// Forward() here (while s.TS may move backwards).
		s.Txn.Timestamp.Forward(s.TS)
		s.Txn.Sequence++
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
	seed := randutil.NewPseudoSeed()
	log.Infof(context.Background(), "using pseudo random number generator with seed %d", seed)

	eng := createTestEngine()
	defer eng.Close()

	const count = 200

	s := &randomTest{
		actions: make(map[string]func(*state) string),
	}
	s.state.rng = rand.New(rand.NewSource(seed))
	s.state.eng = eng
	s.state.key = roachpb.Key("asd")
	s.state.MS = &enginepb.MVCCStats{}

	rngVal := func() roachpb.Value {
		return roachpb.MakeValueFromBytes(randutil.RandBytes(s.rng, int(s.rng.Int31n(128))))
	}

	s.actions["Put"] = func(s *state) string {
		if err := MVCCPut(ctx, s.eng, s.MS, s.key, s.TS, rngVal(), s.Txn); err != nil {
			return err.Error()
		}
		return ""
	}
	s.actions["Del"] = func(s *state) string {
		if err := MVCCDelete(ctx, s.eng, s.MS, s.key, s.TS, s.Txn); err != nil {
			return err.Error()
		}
		return ""
	}
	s.actions["EnsureTxn"] = func(s *state) string {
		if s.Txn == nil {
			s.Txn = &roachpb.Transaction{TxnMeta: enginepb.TxnMeta{ID: uuid.MakeV4(), Timestamp: s.TS}}
		}
		return ""
	}
	s.actions["Abort"] = func(s *state) string {
		if s.Txn != nil {
			if err := MVCCResolveWriteIntent(ctx, s.eng, s.MS, s.intent(roachpb.ABORTED)); err != nil {
				return err.Error()
			}
			s.Txn = nil
		}
		return ""
	}
	s.actions["Commit"] = func(s *state) string {
		if s.Txn != nil {
			if err := MVCCResolveWriteIntent(ctx, s.eng, s.MS, s.intent(roachpb.COMMITTED)); err != nil {
				return err.Error()
			}
			s.Txn = nil
		}
		return ""
	}
	s.actions["Push"] = func(s *state) string {
		if s.Txn != nil {
			if err := MVCCResolveWriteIntent(ctx, s.eng, s.MS, s.intent(roachpb.PENDING)); err != nil {
				return err.Error()
			}
		}
		return ""
	}
	s.actions["GC"] = func(s *state) string {
		// Sometimes GC everything, sometimes only older versions.
		gcTS := hlc.Timestamp{
			WallTime: s.rng.Int63n(s.TS.WallTime),
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
		return fmt.Sprintf("%s (count %d)", gcTS, count)
	}

	for i := 0; i < count; i++ {
		s.step(t)
	}
}

func TestMVCCComputeStatsError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
	defer engine.Close()

	// Write a MVCC metadata key where the value is not an encoded MVCCMetadata
	// protobuf.
	if err := engine.Put(mvccKey(roachpb.Key("garbage")), []byte("garbage")); err != nil {
		t.Fatal(err)
	}

	iter := engine.NewIterator(false)
	defer iter.Close()
	for _, mvccStatsTest := range mvccStatsTests {
		t.Run(mvccStatsTest.name, func(t *testing.T) {
			_, err := mvccStatsTest.fn(iter, mvccKey(roachpb.KeyMin), mvccKey(roachpb.KeyMax), 100)
			if e := "unable to decode MVCCMetadata"; !testutils.IsError(err, e) {
				t.Fatalf("expected %s, got %v", e, err)
			}
		})
	}
}

// BenchmarkMVCCStats set MVCCStats values.
func BenchmarkMVCCStats(b *testing.B) {
	rocksdb := NewInMem(roachpb.Attributes{Attrs: []string{"ssd"}}, testCacheSize)
	defer rocksdb.Close()

	ms := enginepb.MVCCStats{
		LiveBytes:       1,
		KeyBytes:        1,
		ValBytes:        1,
		IntentBytes:     1,
		LiveCount:       1,
		KeyCount:        1,
		ValCount:        1,
		IntentCount:     1,
		IntentAge:       1,
		GCBytesAge:      1,
		SysBytes:        1,
		SysCount:        1,
		LastUpdateNanos: 1,
	}
	b.SetBytes(int64(unsafe.Sizeof(ms)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := MVCCSetRangeStats(context.Background(), rocksdb, 1, &ms); err != nil {
			b.Fatal(err)
		}
	}

	b.StopTimer()
}
