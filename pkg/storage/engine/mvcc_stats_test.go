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

	if t.Failed() {
		t.FailNow()
	}
}

// TestMVCCStatsResolveMovesTimestamp exercises the case in which an intent has its timestamp changed
// and commits.
func TestMVCCStatsResolveMovesTimestamp(t *testing.T) {
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

// TestMVCCStatsBasic writes a value, then deletes it as an intent via
// a transaction, then resolves the intent, manually verifying the
// mvcc stats at each step.
func TestMVCCStatsBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	engine := createTestEngine()
	defer engine.Close()

	ctx := context.Background()
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

// TestMVCCStatsWithRandomRuns creates a random sequence of puts,
// deletes and delete ranges and at each step verifies that the mvcc
// stats match a manual computation of range stats via a scan of the
// underlying engine.
func TestMVCCStatsWithRandomRuns(t *testing.T) {
	defer leaktest.AfterTest(t)()
	seed := randutil.NewPseudoSeed()
	log.Infof(context.Background(), "using pseudo random number generator with seed %d", seed)

	rng := rand.New(rand.NewSource(seed))

	engine := createTestEngine()
	defer engine.Close()

	ms := &enginepb.MVCCStats{}

	// Now, generate a random sequence of puts, deletes and resolves.
	// Each put and delete may or may not involve a txn. Resolves may
	// either commit or abort.
	keys := map[int32][]byte{}
	var lastWT int64
	for i := int32(0); i < int32(1000); i++ {
		// Create random future timestamp, up to a few seconds ahead.
		ts := hlc.Timestamp{WallTime: lastWT + int64(rng.Float32()*4E9), Logical: int32(rng.Int())}
		lastWT = ts.WallTime

		if log.V(1) {
			log.Infof(context.Background(), "*** cycle %d @ %s", i, ts)
		}
		// Manually advance aggregate intent age based on one extra second of simulation.
		// Same for aggregate gc'able bytes age.
		key := []byte(fmt.Sprintf("%s-%d", randutil.RandBytes(rng, int(rng.Int31n(32))), i))
		keys[i] = key

		var txn *roachpb.Transaction
		if rng.Int31n(2) == 0 { // create a txn with 50% prob
			txn = &roachpb.Transaction{TxnMeta: enginepb.TxnMeta{ID: uuid.MakeV4(), Timestamp: ts}}
		}
		// With 25% probability, put a new value; otherwise, delete an earlier
		// key. Because an earlier step in this process may have itself been
		// a delete, we could end up deleting a non-existent key, which is good;
		// we don't mind testing that case as well.
		isDelete := rng.Int31n(4) == 0
		if i > 0 && isDelete {
			idx := rng.Int31n(i)
			if log.V(1) {
				log.Infof(context.Background(), "*** DELETE index %d", idx)
			}
			if err := MVCCDelete(context.Background(), engine, ms, keys[idx], ts, txn); err != nil {
				// Abort any write intent on an earlier, unresolved txn.
				if wiErr, ok := err.(*roachpb.WriteIntentError); ok {
					wiErr.Intents[0].Status = roachpb.ABORTED
					if log.V(1) {
						log.Infof(context.Background(), "*** ABORT index %d", idx)
					}
					// Note that this already incorporates committing an intent
					// at a later time (since we use a potentially later ts here
					// for the resolution).
					if err := MVCCResolveWriteIntent(context.Background(), engine, ms, wiErr.Intents[0]); err != nil {
						t.Fatal(err)
					}
					// Now, re-delete.
					if log.V(1) {
						log.Infof(context.Background(), "*** RE-DELETE index %d", idx)
					}
					if err := MVCCDelete(context.Background(), engine, ms, keys[idx], ts, txn); err != nil {
						t.Fatal(err)
					}
				} else {
					t.Fatal(err)
				}
			}
		} else {
			rngVal := roachpb.MakeValueFromBytes(randutil.RandBytes(rng, int(rng.Int31n(128))))
			if log.V(1) {
				log.Infof(context.Background(), "*** PUT index %d; TXN=%t", i, txn != nil)
			}
			if err := MVCCPut(context.Background(), engine, ms, key, ts, rngVal, txn); err != nil {
				t.Fatal(err)
			}
		}
		if !isDelete && txn != nil && rng.Int31n(2) == 0 { // resolve txn with 50% prob
			// TODO(tschottdorf): need to simulate resolving at a pushed timestamp.
			txn.Status = roachpb.COMMITTED
			if rng.Int31n(10) == 0 { // abort txn with 10% prob
				txn.Status = roachpb.ABORTED
			}
			if log.V(1) {
				log.Infof(context.Background(), "*** RESOLVE index %d; COMMIT=%t", i, txn.Status == roachpb.COMMITTED)
			}
			if err := MVCCResolveWriteIntent(context.Background(), engine, ms, roachpb.Intent{Span: roachpb.Span{Key: key}, Status: txn.Status, Txn: txn.TxnMeta}); err != nil {
				t.Fatal(err)
			}
		}

		ms.AgeTo(ts.WallTime) // a noop may not have updated the stats
		// Every 10th step, verify the stats via manual engine scan.
		if i%10 == 0 {
			// Recompute the stats and compare.
			assertEq(t, engine, fmt.Sprintf("cycle %d", i), ms, ms)
			if t.Failed() {
				t.Fatal("giving up")
			}
		}
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
