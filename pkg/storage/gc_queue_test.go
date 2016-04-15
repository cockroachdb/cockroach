// Copyright 2015 The Cockroach Authors.
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
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import (
	"fmt"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// makeTS creates a new hybrid logical timestamp.
func makeTS(nanos int64, logical int32) hlc.Timestamp {
	return hlc.Timestamp{
		WallTime: nanos,
		Logical:  logical,
	}
}

// TestGCQueueShouldQueue verifies conditions which inform priority
// and whether or not the range should be queued into the GC queue.
// Ranges are queued for GC based on two conditions. The age of bytes
// available to be GC'd, and the age of unresolved intents.
func TestGCQueueShouldQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	cfg, ok := tc.gossip.GetSystemConfig()
	if !ok {
		t.Fatal("config not set")
	}
	desc := tc.repl.Desc()
	zone, err := cfg.GetZoneConfigForKey(desc.StartKey)
	if err != nil {
		log.Errorf(context.Background(), "could not find GC policy for range %s: %s, got zone %+v",
			tc.repl, err, zone)
		return
	}
	policy := zone.GC

	iaN := intentAgeNormalization.Nanoseconds()
	ia := iaN / 1E9
	bc := int64(gcByteCountNormalization)
	ttl := int64(policy.TTLSeconds)

	now := makeTS(considerThreshold*iaN, 0) // at time of stats object

	testCases := []struct {
		gcBytes     int64
		gcBytesAge  int64
		intentCount int64
		intentAge   int64
		now         hlc.Timestamp // at time of shouldQueue
		shouldQ     bool
		priority    float64
	}{
		// First, test cases where shouldQueue is called with the same
		// timestamp that the stats are at.

		// No GC'able bytes, no time elapsed.
		{0, 0, 0, 0, now, false, 0},
		// No GC'able bytes, with intent age, 1/2 intent normalization period elapsed.
		{0, 0, 1, ia / 2, now, false, 0},
		// No GC'able bytes, with intent age=1/2 period, and other 1/2 period elapsed.
		{0, 0, 1, ia / 2, now, false, 0},
		// No GC'able bytes, with (abs and avg) intent age=1.5*normalization.
		{0, 0, 1, 3 * ia / 2, now, true, 1.5},
		// No GC'able bytes, 2 intents, with avg intent age=3.5*normalization.
		{0, 0, 2, 7 * ia, now, true, 3.5},
		// GC'able bytes, no time elapsed.
		{bc, 0, 0, 0, now, false, 0},
		// GC'able bytes, avg age = just below TTLSeconds.
		{bc, bc*ttl - 1, 0, 0, now, false, 0},
		// GC'able bytes, avg age = 2*TTLSeconds.
		{bc, 2 * bc * ttl, 0, 0, now, true, 2},
		// x2 GC'able bytes, avg age = TTLSeconds.
		{2 * bc, 2 * bc * ttl, 0, 0, now, true, 2},
		// GC'able bytes, intent bytes, and intent normalization * 2 elapsed.
		// Queues solely because of gc'able bytes.
		{bc, 5 * bc * ttl, 10 * ia, 0, now, true, 5},
		// A contribution of 1 from gc, 10/5 from intents.
		{bc, bc * ttl, 5, 10 * ia, now, true, (1 + 2)},

		// Some tests where the ages increase since we call shouldNow with
		// a later timestamp.

		// One normalized unit of unaged gc'able bytes at time zero.
		{ttl * bc, 0, 0, 0, hlc.Timestamp{}, true, float64(now.WallTime) / (1E9 * considerThreshold)},

		// 2 intents aging from zero to now (which is exactly the intent age
		// normalization).
		{0, 0, 2, 0, hlc.Timestamp{}, true, 1},
	}

	gcQ := newGCQueue(tc.store, tc.gossip)

	for i, test := range testCases {
		// Write gc'able bytes as key bytes; since "live" bytes will be
		// zero, this will translate into non live bytes.  Also write
		// intent count. Note: the actual accounting on bytes is fictional
		// in this test.
		ms := enginepb.MVCCStats{
			KeyBytes:        test.gcBytes,
			IntentCount:     test.intentCount,
			IntentAge:       test.intentAge * considerThreshold,
			GCBytesAge:      test.gcBytesAge * considerThreshold,
			LastUpdateNanos: test.now.WallTime,
		}
		func() {
			// Hold lock throughout to reduce chance of random commands
			// leading to inconsistent state.
			tc.repl.mu.Lock()
			defer tc.repl.mu.Unlock()
			if err := tc.repl.stateLoader.setMVCCStats(context.Background(), tc.repl.store.Engine(), &ms); err != nil {
				t.Fatal(err)
			}
			tc.repl.mu.state.Stats = ms
		}()
		shouldQ, priority := gcQ.shouldQueue(context.TODO(), now, tc.repl, cfg)
		if shouldQ != test.shouldQ {
			t.Errorf("%d: should queue expected %t; got %t", i, test.shouldQ, shouldQ)
		}
		if scaledExpPri := test.priority * considerThreshold; math.Abs(priority-scaledExpPri) > 0.00001 {
			t.Errorf("%d: priority expected %f; got %f", i, scaledExpPri, priority)
		}
	}
}

// TestGCQueueProcess creates test data in the range over various time
// scales and verifies that scan queue process properly GCs test data.
func TestGCQueueProcess(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	tc.manualClock.Increment(48 * 60 * 60 * 1E9) // 2d past the epoch
	now := tc.Clock().Now().WallTime

	ts1 := makeTS(now-2*24*60*60*1E9+1, 0)                     // 2d old (add one nanosecond so we're not using zero timestamp)
	ts2 := makeTS(now-25*60*60*1E9, 0)                         // GC will occur at time=25 hours
	ts2m1 := ts2.Prev()                                        // ts2 - 1 so we have something not right at the GC time
	ts3 := makeTS(now-intentAgeThreshold.Nanoseconds(), 0)     // 2h old
	ts4 := makeTS(now-(intentAgeThreshold.Nanoseconds()-1), 0) // 2h-1ns old
	ts5 := makeTS(now-1E9, 0)                                  // 1s old
	key1 := roachpb.Key("a")
	key2 := roachpb.Key("b")
	key3 := roachpb.Key("c")
	key4 := roachpb.Key("d")
	key5 := roachpb.Key("e")
	key6 := roachpb.Key("f")
	key7 := roachpb.Key("g")
	key8 := roachpb.Key("h")
	key9 := roachpb.Key("i")
	key10 := roachpb.Key("j")
	key11 := roachpb.Key("k")

	data := []struct {
		key roachpb.Key
		ts  hlc.Timestamp
		del bool
		txn bool
	}{
		// For key1, we expect first value to GC.
		{key1, ts1, false, false},
		{key1, ts2, false, false},
		{key1, ts5, false, false},
		// For key2, we expect values to GC, even though most recent is deletion.
		{key2, ts1, false, false},
		{key2, ts2m1, false, false}, // use a value < the GC time to verify it's kept
		{key2, ts5, true, false},
		// For key3, we expect just ts1 to GC, because most recent deletion is intent.
		{key3, ts1, false, false},
		{key3, ts2, false, false},
		{key3, ts5, true, true},
		// For key4, expect oldest value to GC.
		{key4, ts1, false, false},
		{key4, ts2, false, false},
		// For key5, expect all values to GC (most recent value deleted).
		{key5, ts1, false, false},
		{key5, ts2, true, false}, // deleted, so GC
		// For key6, expect no values to GC because most recent value is intent.
		{key6, ts1, false, false},
		{key6, ts5, false, true},
		// For key7, expect no values to GC because intent is exactly 2h old.
		{key7, ts2, false, false},
		{key7, ts4, false, true},
		// For key8, expect most recent value to resolve by aborting, which will clean it up.
		{key8, ts2, false, false},
		{key8, ts3, true, true},
		// For key9, resolve naked intent with no remaining values.
		{key9, ts3, false, true},
		// For key10, GC ts1 because it's a delete but not ts3 because it's above the threshold.
		{key10, ts1, true, false},
		{key10, ts3, true, false},
		{key10, ts4, false, false},
		{key10, ts5, false, false},
		// For key11, we can't GC anything because ts1 isn't a delete.
		{key11, ts1, false, false},
		{key11, ts3, true, false},
		{key11, ts4, true, false},
		{key11, ts5, true, false},
	}

	for i, datum := range data {
		if datum.del {
			dArgs := deleteArgs(datum.key)
			var txn *roachpb.Transaction
			if datum.txn {
				txn = newTransaction("test", datum.key, 1, enginepb.SERIALIZABLE, tc.Clock())
				txn.OrigTimestamp = datum.ts
				txn.Timestamp = datum.ts
			}
			if _, err := tc.SendWrappedWith(roachpb.Header{
				Timestamp: datum.ts,
				Txn:       txn,
			}, &dArgs); err != nil {
				t.Fatalf("%d: could not delete data: %s", i, err)
			}
		} else {
			pArgs := putArgs(datum.key, []byte("value"))
			var txn *roachpb.Transaction
			if datum.txn {
				txn = newTransaction("test", datum.key, 1, enginepb.SERIALIZABLE, tc.Clock())
				txn.OrigTimestamp = datum.ts
				txn.Timestamp = datum.ts
			}
			if _, err := tc.SendWrappedWith(roachpb.Header{
				Timestamp: datum.ts,
				Txn:       txn,
			}, &pArgs); err != nil {
				t.Fatalf("%d: could not put data: %s", i, err)
			}
		}
	}

	cfg, ok := tc.gossip.GetSystemConfig()
	if !ok {
		t.Fatal("config not set")
	}

	// Process through a scan queue.
	gcQ := newGCQueue(tc.store, tc.gossip)
	if err := gcQ.process(context.Background(), tc.repl, cfg); err != nil {
		t.Fatal(err)
	}

	expKVs := []struct {
		key roachpb.Key
		ts  hlc.Timestamp
	}{
		{key1, ts5},
		{key1, ts2},
		{key2, ts5},
		{key2, ts2m1},
		{key3, hlc.Timestamp{}},
		{key3, ts5},
		{key3, ts2},
		{key4, ts2},
		{key6, hlc.Timestamp{}},
		{key6, ts5},
		{key6, ts1},
		{key7, hlc.Timestamp{}},
		{key7, ts4},
		{key7, ts2},
		{key8, ts2},
		{key10, ts5},
		{key10, ts4},
		{key10, ts3},
		{key11, ts5},
		{key11, ts4},
		{key11, ts3},
		{key11, ts1},
	}
	// Read data directly from engine to avoid intent errors from MVCC.
	kvs, err := engine.Scan(tc.store.Engine(), engine.MakeMVCCMetadataKey(key1),
		engine.MakeMVCCMetadataKey(keys.MaxKey), 0)
	if err != nil {
		t.Fatal(err)
	}
	for i, kv := range kvs {
		if log.V(1) {
			log.Infof(context.Background(), "%d: %s", i, kv.Key)
		}
	}
	if len(kvs) != len(expKVs) {
		t.Fatalf("expected length %d; got %d", len(expKVs), len(kvs))
	}
	for i, kv := range kvs {
		if !kv.Key.Key.Equal(expKVs[i].key) {
			t.Errorf("%d: expected key %q; got %q", i, expKVs[i].key, kv.Key.Key)
		}
		if kv.Key.Timestamp != expKVs[i].ts {
			t.Errorf("%d: expected ts=%s; got %s", i, expKVs[i].ts, kv.Key.Timestamp)
		}
		if log.V(1) {
			log.Infof(context.Background(), "%d: %s", i, kv.Key)
		}
	}
}

func TestGCQueueTransactionTable(t *testing.T) {
	defer leaktest.AfterTest(t)()

	manual := hlc.NewManualClock(123)
	tsc := TestStoreConfig(hlc.NewClock(manual.UnixNano, time.Nanosecond))
	manual.Set(3 * 24 * time.Hour.Nanoseconds())

	now := manual.UnixNano()

	gcTxnAndAC := now - txnCleanupThreshold.Nanoseconds()
	gcACOnly := now - abortCacheAgeThreshold.Nanoseconds()
	if gcTxnAndAC >= gcACOnly {
		t.Fatalf("test assumption violated due to changing constants; needs adjustment")
	}

	type spec struct {
		status      roachpb.TransactionStatus
		orig        int64
		hb          int64                     // last heartbeat (none if Timestamp{})
		newStatus   roachpb.TransactionStatus // -1 for GCed
		failResolve bool                      // do we want to fail resolves in this trial?
		expResolve  bool                      // expect attempt at removing txn-persisted intents?
		expAbortGC  bool                      // expect abort cache entries removed?
	}
	// Describes the state of the Txn table before the test.
	// Many of the abort cache entries deleted wouldn't even be there, so don't
	// be confused by that.
	testCases := map[string]spec{
		// Too young, should not touch.
		"aa": {
			status:    roachpb.PENDING,
			orig:      gcACOnly + 1,
			newStatus: roachpb.PENDING,
		},
		// A little older, so the AbortCache gets cleaned up.
		"ab": {
			status:     roachpb.PENDING,
			orig:       gcTxnAndAC + 1,
			newStatus:  roachpb.PENDING,
			expAbortGC: true,
		},
		// Old and pending, but still heartbeat (so no Push attempted; it would succeed).
		// It's old enough to delete the abort cache entry though.
		"ba": {
			status:     roachpb.PENDING,
			orig:       1, // immaterial
			hb:         gcTxnAndAC + 1,
			newStatus:  roachpb.PENDING,
			expAbortGC: true,
		},
		// Not old enough for Txn GC, but old enough to remove the abort cache entry.
		"bb": {
			status:     roachpb.ABORTED,
			orig:       gcACOnly - 1,
			newStatus:  roachpb.ABORTED,
			expAbortGC: true,
		},
		// Old, pending and abandoned. Should push and abort it successfully,
		// but not GC it just yet (this is an artifact of the implementation).
		// The abort cache gets cleaned up though.
		"c": {
			status:     roachpb.PENDING,
			orig:       gcTxnAndAC - 1,
			newStatus:  roachpb.ABORTED,
			expAbortGC: true,
		},
		// Old and aborted, should delete.
		"d": {
			status:     roachpb.ABORTED,
			orig:       gcTxnAndAC - 1,
			newStatus:  -1,
			expResolve: true,
			expAbortGC: true,
		},
		// Committed and fresh, so no action. But the abort cache entry is old
		// enough to be discarded.
		"e": {
			status:     roachpb.COMMITTED,
			orig:       gcTxnAndAC + 1,
			newStatus:  roachpb.COMMITTED,
			expAbortGC: true,
		},
		// Committed and old. It has an intent (like all tests here), which is
		// resolvable and hence we can GC.
		"f": {
			status:     roachpb.COMMITTED,
			orig:       gcTxnAndAC - 1,
			newStatus:  -1,
			expResolve: true,
			expAbortGC: true,
		},
		// Same as the previous one, but we've rigged things so that the intent
		// resolution here will fail and consequently no GC is expected.
		"g": {
			status:      roachpb.COMMITTED,
			orig:        gcTxnAndAC - 1,
			newStatus:   roachpb.COMMITTED,
			failResolve: true,
			expResolve:  true,
			expAbortGC:  true,
		},
	}

	resolved := map[string][]roachpb.Span{}

	tsc.TestingKnobs.TestingEvalFilter =
		func(filterArgs storagebase.FilterArgs) *roachpb.Error {
			if resArgs, ok := filterArgs.Req.(*roachpb.ResolveIntentRequest); ok {
				id := string(resArgs.IntentTxn.Key)
				resolved[id] = append(resolved[id], roachpb.Span{
					Key:    resArgs.Key,
					EndKey: resArgs.EndKey,
				})
				// We've special cased one test case. Note that the intent is still
				// counted in `resolved`.
				if testCases[id].failResolve {
					return roachpb.NewErrorWithTxn(errors.Errorf("boom"), filterArgs.Hdr.Txn)
				}
			}
			return nil
		}
	tc := testContext{manualClock: manual}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.StartWithStoreConfig(t, stopper, tsc)

	outsideKey := tc.repl.Desc().EndKey.Next().AsRawKey()
	testIntents := []roachpb.Span{{Key: roachpb.Key("intent")}}

	txns := map[string]roachpb.Transaction{}
	for strKey, test := range testCases {
		baseKey := roachpb.Key(strKey)
		txnClock := hlc.NewClock(hlc.NewManualClock(test.orig).UnixNano, time.Nanosecond)
		txn := newTransaction("txn1", baseKey, 1, enginepb.SERIALIZABLE, txnClock)
		txn.Status = test.status
		txn.Intents = testIntents
		if test.hb > 0 {
			txn.LastHeartbeat = hlc.Timestamp{WallTime: test.hb}
		}
		// Set a high Timestamp to make sure it does not matter. Only
		// OrigTimestamp (and heartbeat) are used for GC decisions.
		txn.Timestamp.Forward(hlc.MaxTimestamp)
		txns[strKey] = *txn
		for _, addrKey := range []roachpb.Key{baseKey, outsideKey} {
			key := keys.TransactionKey(addrKey, *txn.ID)
			if err := engine.MVCCPutProto(context.Background(), tc.engine, nil, key, hlc.Timestamp{}, nil, txn); err != nil {
				t.Fatal(err)
			}
		}
		entry := roachpb.AbortCacheEntry{Key: txn.Key, Timestamp: txn.LastActive()}
		if err := tc.repl.abortCache.Put(context.Background(), tc.engine, nil, *txn.ID, &entry); err != nil {
			t.Fatal(err)
		}
	}

	// Run GC.
	gcQ := newGCQueue(tc.store, tc.gossip)
	cfg, ok := tc.gossip.GetSystemConfig()
	if !ok {
		t.Fatal("config not set")
	}

	if err := gcQ.process(context.Background(), tc.repl, cfg); err != nil {
		t.Fatal(err)
	}

	testutils.SucceedsSoon(t, func() error {
		for strKey, sp := range testCases {
			txn := &roachpb.Transaction{}
			key := keys.TransactionKey(roachpb.Key(strKey), *txns[strKey].ID)
			ok, err := engine.MVCCGetProto(context.Background(), tc.engine, key, hlc.Timestamp{}, true, nil, txn)
			if err != nil {
				return err
			}
			if expGC := (sp.newStatus == -1); expGC {
				if expGC != !ok {
					return fmt.Errorf("%s: expected gc: %t, but found %s\n%s", strKey, expGC, txn, roachpb.Key(strKey))
				}
			} else if sp.newStatus != txn.Status {
				return fmt.Errorf("%s: expected status %s, but found %s", strKey, sp.newStatus, txn.Status)
			}
			var expIntents []roachpb.Span
			if sp.expResolve {
				expIntents = testIntents
			}
			if !reflect.DeepEqual(resolved[strKey], expIntents) {
				return fmt.Errorf("%s: unexpected intent resolutions:\nexpected: %s\nobserved: %s",
					strKey, expIntents, resolved[strKey])
			}
			entry := &roachpb.AbortCacheEntry{}
			abortExists, err := tc.repl.abortCache.Get(context.Background(), tc.store.Engine(), *txns[strKey].ID, entry)
			if err != nil {
				t.Fatal(err)
			}
			if abortExists == sp.expAbortGC {
				return fmt.Errorf("%s: expected abort cache gc: %t, found %+v", strKey, sp.expAbortGC, entry)
			}
		}
		return nil
	})

	outsideTxnPrefix := keys.TransactionKey(outsideKey, uuid.UUID{})
	outsideTxnPrefixEnd := keys.TransactionKey(outsideKey.Next(), uuid.UUID{})
	var count int
	if _, err := engine.MVCCIterate(context.Background(), tc.store.Engine(), outsideTxnPrefix, outsideTxnPrefixEnd, hlc.Timestamp{},
		true, nil, false, func(roachpb.KeyValue) (bool, error) {
			count++
			return false, nil
		}); err != nil {
		t.Fatal(err)
	}
	if exp := len(testCases); exp != count {
		t.Fatalf("expected the %d external transaction entries to remain untouched, "+
			"but only %d are left", exp, count)
	}

	batch := tc.engine.NewSnapshot()
	defer batch.Close()
	tc.repl.assertState(batch) // check that in-mem and on-disk state were updated

	tc.repl.mu.Lock()
	txnSpanThreshold := tc.repl.mu.state.TxnSpanGCThreshold
	tc.repl.mu.Unlock()

	// Verify that the new TxnSpanGCThreshold has reached the Replica.
	if expWT := int64(gcTxnAndAC); txnSpanThreshold.WallTime != expWT {
		t.Fatalf("expected TxnSpanGCThreshold.Walltime %d, got timestamp %s",
			expWT, txnSpanThreshold)
	}
}

// TestGCQueueIntentResolution verifies intent resolution with many
// intents spanning just two transactions.
func TestGCQueueIntentResolution(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	tc.manualClock.Set(48 * 60 * 60 * 1E9) // 2d past the epoch
	now := tc.Clock().Now().WallTime

	txns := []*roachpb.Transaction{
		newTransaction("txn1", roachpb.Key("0-00000"), 1, enginepb.SERIALIZABLE, tc.Clock()),
		newTransaction("txn2", roachpb.Key("1-00000"), 1, enginepb.SERIALIZABLE, tc.Clock()),
	}
	intentResolveTS := makeTS(now-intentAgeThreshold.Nanoseconds(), 0)
	txns[0].OrigTimestamp = intentResolveTS
	txns[0].Timestamp = intentResolveTS
	txns[1].OrigTimestamp = intentResolveTS
	txns[1].Timestamp = intentResolveTS

	// Two transactions.
	for i := 0; i < 2; i++ {
		// 5 puts per transaction.
		// TODO(spencerkimball): benchmark with ~50k.
		for j := 0; j < 5; j++ {
			pArgs := putArgs(roachpb.Key(fmt.Sprintf("%d-%05d", i, j)), []byte("value"))
			if _, err := tc.SendWrappedWith(roachpb.Header{
				Txn: txns[i],
			}, &pArgs); err != nil {
				t.Fatalf("%d: could not put data: %s", i, err)
			}
			txns[i].Sequence++
		}
	}

	cfg, ok := tc.gossip.GetSystemConfig()
	if !ok {
		t.Fatal("config not set")
	}

	// Process through a scan queue.
	gcQ := newGCQueue(tc.store, tc.gossip)
	if err := gcQ.process(context.Background(), tc.repl, cfg); err != nil {
		t.Fatal(err)
	}

	// Iterate through all values to ensure intents have been fully resolved.
	meta := &enginepb.MVCCMetadata{}
	err := tc.store.Engine().Iterate(engine.MakeMVCCMetadataKey(roachpb.KeyMin),
		engine.MakeMVCCMetadataKey(roachpb.KeyMax), func(kv engine.MVCCKeyValue) (bool, error) {
			if !kv.Key.IsValue() {
				if err := proto.Unmarshal(kv.Value, meta); err != nil {
					return false, err
				}
				if meta.Txn != nil {
					return false, errors.Errorf("non-nil Txn after GC for key %s", kv.Key)
				}
			}
			return false, nil
		})
	if err != nil {
		t.Fatal(err)
	}
}

func TestGCQueueLastProcessedTimestamps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	// Create two last processed times both at the range start key and
	// also at some mid-point key in order to simulate a merge.
	// Two transactions.
	lastProcessedVals := []struct {
		key   roachpb.Key
		expGC bool
	}{
		{keys.QueueLastProcessedKey(roachpb.RKeyMin, "timeSeriesMaintenance"), false},
		{keys.QueueLastProcessedKey(roachpb.RKeyMin, "replica consistency checker"), false},
		{keys.QueueLastProcessedKey(roachpb.RKey("a"), "timeSeriesMaintenance"), true},
		{keys.QueueLastProcessedKey(roachpb.RKey("b"), "replica consistency checker"), true},
	}

	ts := tc.Clock().Now()
	for _, lpv := range lastProcessedVals {
		if err := engine.MVCCPutProto(context.Background(), tc.engine, nil, lpv.key, hlc.Timestamp{}, nil, &ts); err != nil {
			t.Fatal(err)
		}
	}

	cfg, ok := tc.gossip.GetSystemConfig()
	if !ok {
		t.Fatal("config not set")
	}

	// Process through a scan queue.
	gcQ := newGCQueue(tc.store, tc.gossip)
	if err := gcQ.process(context.Background(), tc.repl, cfg); err != nil {
		t.Fatal(err)
	}

	// Verify GC.
	testutils.SucceedsSoon(t, func() error {
		for _, lpv := range lastProcessedVals {
			ok, err := engine.MVCCGetProto(context.Background(), tc.engine, lpv.key, hlc.Timestamp{}, true, nil, &ts)
			if err != nil {
				return err
			}
			if ok == lpv.expGC {
				return errors.Errorf("expected GC of %s: %t; got %t", lpv.key, lpv.expGC, ok)
			}
		}
		return nil
	})
}
