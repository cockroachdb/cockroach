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

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/testutils/storageutils"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/uuid"
	"github.com/gogo/protobuf/proto"
)

// makeTS creates a new hybrid logical timestamp.
func makeTS(nanos int64, logical int32) roachpb.Timestamp {
	return roachpb.Timestamp{
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
	tc.Start(t)
	defer tc.Stop()

	cfg, ok := tc.gossip.GetSystemConfig()
	if !ok {
		t.Fatal("config not set")
	}
	desc := tc.rng.Desc()
	zone, err := cfg.GetZoneConfigForKey(desc.StartKey)
	if err != nil {
		log.Errorf("could not find GC policy for range %s: %s, got zone %+v",
			tc.rng, err, zone)
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
		now         roachpb.Timestamp // at time of shouldQueue
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
		{ttl * bc, 0, 0, 0, roachpb.ZeroTimestamp, true, float64(now.WallTime) / (1E9 * considerThreshold)},

		// 2 intents aging from zero to now (which is exactly the intent age
		// normalization).
		{0, 0, 2, 0, roachpb.ZeroTimestamp, true, 1},
	}

	gcQ := newGCQueue(tc.gossip)

	for i, test := range testCases {
		// Write gc'able bytes as key bytes; since "live" bytes will be
		// zero, this will translate into non live bytes.  Also write
		// intent count. Note: the actual accounting on bytes is fictional
		// in this test.
		stats := engine.MVCCStats{
			KeyBytes:        test.gcBytes,
			IntentCount:     test.intentCount,
			IntentAge:       test.intentAge * considerThreshold,
			GCBytesAge:      test.gcBytesAge * considerThreshold,
			LastUpdateNanos: test.now.WallTime,
		}
		if err := tc.rng.stats.SetMVCCStats(tc.rng.store.Engine(), stats); err != nil {
			t.Fatal(err)
		}
		shouldQ, priority := gcQ.shouldQueue(now, tc.rng, cfg)
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
	tc.Start(t)
	defer tc.Stop()

	const now int64 = 48 * 60 * 60 * 1E9 // 2d past the epoch
	tc.manualClock.Set(now)

	ts1 := makeTS(now-2*24*60*60*1E9+1, 0)                     // 2d old (add one nanosecond so we're not using zero timestamp)
	ts2 := makeTS(now-25*60*60*1E9, 0)                         // GC will occur at time=25 hours
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

	data := []struct {
		key roachpb.Key
		ts  roachpb.Timestamp
		del bool
		txn bool
	}{
		// For key1, we expect first two values to GC.
		{key1, ts1, false, false},
		{key1, ts2, false, false},
		{key1, ts5, false, false},
		// For key2, we expect all values to GC, because most recent is deletion.
		{key2, ts1, false, false},
		{key2, ts2, false, false},
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
		{key5, ts2, true, false},
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
		{key9, ts3, true, false},
	}

	for i, datum := range data {
		if datum.del {
			dArgs := deleteArgs(datum.key)
			var txn *roachpb.Transaction
			if datum.txn {
				txn = newTransaction("test", datum.key, 1, roachpb.SERIALIZABLE, tc.clock)
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
				txn = newTransaction("test", datum.key, 1, roachpb.SERIALIZABLE, tc.clock)
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
	gcQ := newGCQueue(tc.gossip)
	if err := gcQ.process(tc.clock.Now(), tc.rng, cfg); err != nil {
		t.Fatal(err)
	}

	expKVs := []struct {
		key roachpb.Key
		ts  roachpb.Timestamp
	}{
		{key1, ts5},
		{key3, roachpb.ZeroTimestamp},
		{key3, ts5},
		{key3, ts2},
		{key4, ts2},
		{key6, roachpb.ZeroTimestamp},
		{key6, ts5},
		{key6, ts1},
		{key7, roachpb.ZeroTimestamp},
		{key7, ts4},
		{key7, ts2},
		{key8, ts2},
	}
	// Read data directly from engine to avoid intent errors from MVCC.
	kvs, err := engine.Scan(tc.store.Engine(), engine.MakeMVCCMetadataKey(key1),
		engine.MakeMVCCMetadataKey(keys.MaxKey), 0)
	if err != nil {
		t.Fatal(err)
	}
	for i, kv := range kvs {
		if log.V(1) {
			log.Infof("%d: %s", i, kv.Key)
		}
	}
	if len(kvs) != len(expKVs) {
		t.Fatalf("expected length %d; got %d", len(expKVs), len(kvs))
	}
	for i, kv := range kvs {
		if !kv.Key.Key.Equal(expKVs[i].key) {
			t.Errorf("%d: expected key %q; got %q", i, expKVs[i].key, kv.Key.Key)
		}
		if !kv.Key.Timestamp.Equal(expKVs[i].ts) {
			t.Errorf("%d: expected ts=%s; got %s", i, expKVs[i].ts, kv.Key.Timestamp)
		}
		if log.V(1) {
			log.Infof("%d: %s", i, kv.Key)
		}
	}

	// Verify that the last verification timestamp was updated as whole range was scanned.
	if _, err := tc.rng.getLastVerificationTimestamp(); err != nil {
		t.Fatal(err)
	}
}

func TestGCQueueTransactionTable(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const now time.Duration = 3 * 24 * time.Hour

	const gcTxnAndAC = now - txnCleanupThreshold
	const gcACOnly = now - abortCacheAgeThreshold
	if gcTxnAndAC >= gcACOnly {
		t.Fatalf("test assumption violated due to changing constants; needs adjustment")
	}

	type spec struct {
		status      roachpb.TransactionStatus
		orig        time.Duration
		hb          time.Duration             // last heartbeat (none if ZeroTimestamp)
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

	tc := testContext{}
	tsc := TestStoreContext()
	tsc.TestingKnobs.TestingCommandFilter =
		func(filterArgs storageutils.FilterArgs) *roachpb.Error {
			if resArgs, ok := filterArgs.Req.(*roachpb.ResolveIntentRequest); ok {
				id := string(resArgs.IntentTxn.Key)
				resolved[id] = append(resolved[id], roachpb.Span{
					Key:    resArgs.Key,
					EndKey: resArgs.EndKey,
				})
				// We've special cased one test case. Note that the intent is still
				// counted in `resolved`.
				if testCases[id].failResolve {
					return roachpb.NewErrorWithTxn(util.Errorf("boom"), filterArgs.Hdr.Txn)
				}
			}
			return nil
		}
	tc.StartWithStoreContext(t, tsc)
	defer tc.Stop()
	tc.manualClock.Set(int64(now))

	outsideKey := tc.rng.Desc().EndKey.Next().AsRawKey()
	testIntents := []roachpb.Span{{Key: roachpb.Key("intent")}}

	txns := map[string]roachpb.Transaction{}
	for strKey, test := range testCases {
		baseKey := roachpb.Key(strKey)
		txnClock := hlc.NewClock(hlc.NewManualClock(int64(test.orig)).UnixNano)
		txn := newTransaction("txn1", baseKey, 1, roachpb.SERIALIZABLE, txnClock)
		txn.Status = test.status
		txn.Intents = testIntents
		if test.hb > 0 {
			txn.LastHeartbeat = &roachpb.Timestamp{WallTime: int64(test.hb)}
		}
		// Set a high Timestamp to make sure it does not matter. Only
		// OrigTimestamp (and heartbeat) are used for GC decisions.
		txn.Timestamp.Forward(roachpb.MaxTimestamp)
		txns[strKey] = *txn
		for _, addrKey := range []roachpb.Key{baseKey, outsideKey} {
			key := keys.TransactionKey(addrKey, txn.ID)
			if err := engine.MVCCPutProto(context.Background(), tc.engine, nil, key, roachpb.ZeroTimestamp, nil, txn); err != nil {
				t.Fatal(err)
			}
		}
		entry := roachpb.AbortCacheEntry{Key: txn.Key, Timestamp: txn.LastActive()}
		if err := tc.rng.abortCache.Put(context.Background(), tc.engine, nil, txn.ID, &entry); err != nil {
			t.Fatal(err)
		}
	}

	// Run GC.
	gcQ := newGCQueue(tc.gossip)
	cfg, ok := tc.gossip.GetSystemConfig()
	if !ok {
		t.Fatal("config not set")
	}

	if err := gcQ.process(tc.clock.Now(), tc.rng, cfg); err != nil {
		t.Fatal(err)
	}

	util.SucceedsSoon(t, func() error {
		for strKey, sp := range testCases {
			txn := &roachpb.Transaction{}
			key := keys.TransactionKey(roachpb.Key(strKey), txns[strKey].ID)
			ok, err := engine.MVCCGetProto(context.Background(), tc.engine, key, roachpb.ZeroTimestamp, true, nil, txn)
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
			abortExists, err := tc.rng.abortCache.Get(context.Background(), tc.store.Engine(), txns[strKey].ID, entry)
			if err != nil {
				t.Fatal(err)
			}
			if (abortExists == false) != sp.expAbortGC {
				return fmt.Errorf("%s: expected abort cache gc: %t, found %+v", strKey, sp.expAbortGC, entry)
			}
		}
		return nil
	})

	outsideTxnPrefix := keys.TransactionKey(outsideKey, uuid.EmptyUUID)
	outsideTxnPrefixEnd := keys.TransactionKey(outsideKey.Next(), uuid.EmptyUUID)
	var count int
	if _, err := engine.MVCCIterate(context.Background(), tc.store.Engine(), outsideTxnPrefix, outsideTxnPrefixEnd, roachpb.ZeroTimestamp,
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
}

// TestGCQueueIntentResolution verifies intent resolution with many
// intents spanning just two transactions.
func TestGCQueueIntentResolution(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	const now int64 = 48 * 60 * 60 * 1E9 // 2d past the epoch
	tc.manualClock.Set(now)

	txns := []*roachpb.Transaction{
		newTransaction("txn1", roachpb.Key("0-00000"), 1, roachpb.SERIALIZABLE, tc.clock),
		newTransaction("txn2", roachpb.Key("1-00000"), 1, roachpb.SERIALIZABLE, tc.clock),
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
	gcQ := newGCQueue(tc.gossip)
	if err := gcQ.process(tc.clock.Now(), tc.rng, cfg); err != nil {
		t.Fatal(err)
	}

	// Iterate through all values to ensure intents have been fully resolved.
	meta := &engine.MVCCMetadata{}
	err := tc.store.Engine().Iterate(engine.MakeMVCCMetadataKey(roachpb.KeyMin),
		engine.MakeMVCCMetadataKey(roachpb.KeyMax), func(kv engine.MVCCKeyValue) (bool, error) {
			if !kv.Key.IsValue() {
				if err := proto.Unmarshal(kv.Value, meta); err != nil {
					return false, err
				}
				if meta.Txn != nil {
					return false, util.Errorf("non-nil Txn after GC for key %s", kv.Key)
				}
			}
			return false, nil
		})
	if err != nil {
		t.Fatal(err)
	}
}
