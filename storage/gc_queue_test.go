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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import (
	"math"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/log"
)

// makeTS creates a new hybrid logical timestamp.
func makeTS(nanos int64, logical int32) proto.Timestamp {
	return proto.Timestamp{
		WallTime: nanos,
		Logical:  logical,
	}
}

// TestGCQueueShouldQueue verifies conditions which inform priority
// and whether or not the range should be queued into the GC queue.
// Ranges are queued for GC based on two conditions. The age of bytes
// available to be GC'd, and the age of unresolved intents.
func TestGCQueueShouldQueue(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	// Put an empty GC metadata; all that's read from it is last scan nanos.
	key := engine.RangeGCMetadataKey(tc.rng.Desc().RaftID)
	if err := engine.MVCCPutProto(tc.rng.rm.Engine(), nil, key, proto.ZeroTimestamp, nil, &proto.GCMetadata{}); err != nil {
		t.Fatal(err)
	}

	iaN := intentAgeNormalization.Nanoseconds()
	ia := iaN / 1E9
	bc := int64(gcByteCountNormalization)
	ttl := int64(24 * 60 * 60)

	testCases := []struct {
		gcBytes     int64
		gcBytesAge  int64
		intentCount int64
		intentAge   int64
		now         proto.Timestamp
		shouldQ     bool
		priority    float64
	}{
		// No GC'able bytes, no time elapsed.
		{0, 0, 0, 0, makeTS(0, 0), false, 0},
		// No GC'able bytes, with intent age, 1/2 intent normalization period elapsed.
		{0, 0, 1, ia / 2, makeTS(0, 0), false, 0},
		// No GC'able bytes, with intent age=1/2 period, and other 1/2 period elapsed.
		{0, 0, 1, ia / 2, makeTS(iaN/2, 0), false, 0},
		// No GC'able bytes, with intent age=2*intent normalization.
		{0, 0, 1, 3 * ia / 2, makeTS(iaN/2, 0), true, 2},
		// No GC'able bytes, 2 intents, with avg intent age=4x intent normalization.
		{0, 0, 2, 7 * ia, makeTS(iaN, 0), true, 4.5},
		// GC'able bytes, no time elapsed.
		{bc, 0, 0, 0, makeTS(0, 0), false, 0},
		// GC'able bytes, avg age = TTLSeconds.
		{bc, bc * ttl, 0, 0, makeTS(0, 0), false, 0},
		// GC'able bytes, avg age = 2*TTLSeconds.
		{bc, 2 * bc * ttl, 0, 0, makeTS(0, 0), true, 2},
		// x2 GC'able bytes, avg age = TTLSeconds.
		{2 * bc, 2 * bc * ttl, 0, 0, makeTS(0, 0), true, 2},
		// GC'able bytes, intent bytes, and intent normalization * 2 elapsed.
		{bc, bc * ttl, 1, 0, makeTS(iaN*2, 0), true, 5},
	}

	gcQ := newGCQueue()

	for i, test := range testCases {
		// Write gc'able bytes as key bytes; since "live" bytes will be
		// zero, this will translate into non live bytes.  Also write
		// intent count. Note: the actual accounting on bytes is fictional
		// in this test.
		stats := proto.MVCCStats{
			KeyBytes:    test.gcBytes,
			IntentCount: test.intentCount,
			IntentAge:   test.intentAge,
			GCBytesAge:  test.gcBytesAge,
		}
		if err := tc.rng.stats.SetMVCCStats(tc.rng.rm.Engine(), stats); err != nil {
			t.Fatal(err)
		}
		shouldQ, priority := gcQ.shouldQueue(test.now, tc.rng)
		if shouldQ != test.shouldQ {
			t.Errorf("%d: should queue expected %t; got %t", i, test.shouldQ, shouldQ)
		}
		if math.Abs(priority-test.priority) > 0.00001 {
			t.Errorf("%d: priority expected %f; got %f", i, test.priority, priority)
		}
	}
}

// TestGCQueueProcess creates test data in the range over various time
// scales and verifies that scan queue process properly GCs test data.
func TestGCQueueProcess(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	const now int64 = 48 * 60 * 60 * 1E9 // 2d past the epoch
	tc.manualClock.Set(now)

	ts1 := makeTS(now-2*24*60*60*1E9+1, 0)                     // 2d old (add one nanosecond so we're not using zero timestamp)
	ts2 := makeTS(now-25*60*60*1E9, 0)                         // GC will occur at time=25 hours
	ts3 := makeTS(now-(intentAgeThreshold.Nanoseconds()+1), 0) // 2h+1ns old
	ts4 := makeTS(now-(intentAgeThreshold.Nanoseconds()-1), 0) // 2h-ns old
	ts5 := makeTS(now-1E9, 0)                                  // 1s old
	key1 := proto.Key("a")
	key2 := proto.Key("b")
	key3 := proto.Key("c")
	key4 := proto.Key("d")
	key5 := proto.Key("e")
	key6 := proto.Key("f")
	key7 := proto.Key("g")
	key8 := proto.Key("h")
	key9 := proto.Key("i")

	data := []struct {
		key proto.Key
		ts  proto.Timestamp
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
		{key6, ts5, true, true},
		// For key7, expect no values to GC because intent is exactly 2h old.
		{key7, ts2, false, false},
		{key7, ts4, true, true},
		// For key8, expect most recent value to resolve by aborting, which will clean it up.
		{key8, ts2, false, false},
		{key8, ts3, true, true},
		// /For key9, resolve naked intent with no remaining values.
		{key9, ts3, true, false},
	}

	for i, datum := range data {
		if datum.del {
			dArgs, dReply := deleteArgs(datum.key, tc.rng.Desc().RaftID, tc.store.StoreID())
			dArgs.Timestamp = datum.ts
			if datum.txn {
				dArgs.Txn = newTransaction("test", datum.key, 1, proto.SERIALIZABLE, tc.clock)
				dArgs.Txn.Timestamp = datum.ts
			}
			if err := tc.rng.AddCmd(client.Call{Args: dArgs, Reply: dReply}, true); err != nil {
				t.Fatalf("%d: could not delete data: %s", i, err)
			}
		} else {
			pArgs, pReply := putArgs(datum.key, []byte("value"), tc.rng.Desc().RaftID, tc.store.StoreID())
			pArgs.Timestamp = datum.ts
			if datum.txn {
				pArgs.Txn = newTransaction("test", datum.key, 1, proto.SERIALIZABLE, tc.clock)
				pArgs.Txn.Timestamp = datum.ts
			}
			if err := tc.rng.AddCmd(client.Call{Args: pArgs, Reply: pReply}, true); err != nil {
				t.Fatalf("%d: could not put data: %s", i, err)
			}
		}
	}

	// Process through a scan queue.
	gcQ := newGCQueue()
	if err := gcQ.process(tc.clock.Now(), tc.rng); err != nil {
		t.Error(err)
	}

	expKVs := []struct {
		key proto.Key
		ts  proto.Timestamp
	}{
		{key1, proto.ZeroTimestamp},
		{key1, ts5},
		{key3, proto.ZeroTimestamp},
		{key3, ts5},
		{key3, ts2},
		{key4, proto.ZeroTimestamp},
		{key4, ts2},
		{key6, proto.ZeroTimestamp},
		{key6, ts5},
		{key6, ts1},
		{key7, proto.ZeroTimestamp},
		{key7, ts4},
		{key7, ts2},
		{key8, proto.ZeroTimestamp},
		{key8, ts2},
	}
	// Read data directly from engine to avoid intent errors from MVCC.
	kvs, err := engine.Scan(tc.store.Engine(), engine.MVCCEncodeKey(key1), engine.MVCCEncodeKey(engine.KeyMax), 0)
	if err != nil {
		t.Fatal(err)
	}
	for i, kv := range kvs {
		if key, ts, isValue := engine.MVCCDecodeKey(kv.Key); isValue {
			if log.V(1) {
				log.Infof("%d: %q, ts=%s", i, key, ts)
			}
		} else {
			if log.V(1) {
				log.Infof("%d: %q meta", i, key)
			}
		}
	}
	if len(kvs) != len(expKVs) {
		t.Fatalf("expected length %d; got %d", len(expKVs), len(kvs))
	}
	for i, kv := range kvs {
		key, ts, isValue := engine.MVCCDecodeKey(kv.Key)
		if !key.Equal(expKVs[i].key) {
			t.Errorf("%d: expected key %q; got %q", i, expKVs[i].key, key)
		}
		if !ts.Equal(expKVs[i].ts) {
			t.Errorf("%d: expected ts=%s; got %s", i, expKVs[i].ts, ts)
		}
		if isValue {
			if log.V(1) {
				log.Infof("%d: %q, ts=%s", i, key, ts)
			}
		} else {
			if log.V(1) {
				log.Infof("%d: %q meta", i, key)
			}
		}
	}

	// Verify the oldest extant intent age.
	gcMeta, err := tc.rng.GetGCMetadata()
	if err != nil {
		t.Fatal(err)
	}
	if gcMeta.LastScanNanos != now {
		t.Errorf("expected last scan nanos=%d; got %d", now, gcMeta.LastScanNanos)
	}
	if *gcMeta.OldestIntentNanos != ts4.WallTime {
		t.Errorf("expected oldest intent nanos=%d; got %d", ts4.WallTime, gcMeta.OldestIntentNanos)
	}

	// Verify that the last verification timestamp was updated as whole range was scanned.
	ts, err := tc.rng.GetLastVerificationTimestamp()
	if err != nil {
		t.Fatal(err)
	}
	if gcMeta.LastScanNanos != ts.WallTime {
		t.Errorf("expected walltime nanos %d; got %d", gcMeta.LastScanNanos, ts.WallTime)
	}
}

// TestGCQueueLookupGCPolicy verifies the hierarchical lookup of GC
// policy in the event that the longest matching key prefix does not
// have a zone configured.
func TestGCQueueLookupGCPolicy(t *testing.T) {
	defer leaktest.AfterTest(t)
	zoneConfig1 := proto.ZoneConfig{
		ReplicaAttrs:  []proto.Attributes{},
		RangeMinBytes: 1 << 10,
		RangeMaxBytes: 1 << 18,
		GC: &proto.GCPolicy{
			TTLSeconds: 60 * 60, // 1 hour only
		},
	}
	zoneConfig2 := proto.ZoneConfig{
		ReplicaAttrs:  []proto.Attributes{},
		RangeMinBytes: 1 << 10,
		RangeMaxBytes: 1 << 18,
		// Note thtere is no GC set here, so we should select the
		// hierarchical parent's GC policy; in this case, zoneConfig1.
	}
	configs := []*PrefixConfig{
		{engine.KeyMin, nil, &zoneConfig1},
		{proto.Key("/db1"), nil, &zoneConfig2},
	}
	pcc, err := NewPrefixConfigMap(configs)
	if err != nil {
		t.Fatal(err)
	}

	// Setup test context and add new zone config map. This would normally
	// start a split, but splits are disabled in this testing configuration
	// because the mock DB does not support splits.
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()
	if err := tc.rng.rm.Gossip().AddInfo(gossip.KeyConfigZone, pcc, 0*time.Second); err != nil {
		t.Fatal(err)
	}

	// Create a new range within "/db1" and verify that lookup of
	// zone config results in the
	rng2 := createRange(tc.store, 2, proto.Key("/db1/a"), proto.Key("/db1/b"))
	if err := tc.store.AddRange(rng2); err != nil {
		t.Fatal(err)
	}

	gcQ := newGCQueue()
	gcPolicy, err := gcQ.lookupGCPolicy(rng2)
	if err != nil {
		t.Fatal(err)
	}
	if ttl := gcPolicy.TTLSeconds; ttl != 60*60 {
		t.Errorf("expected TTL=%d; got %d", 60*60, ttl)
	}
}
