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
	"math/rand"
	"reflect"
	"testing"
	"testing/quick"
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
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/kr/pretty"
)

// makeTS creates a new hybrid logical timestamp.
func makeTS(nanos int64, logical int32) hlc.Timestamp {
	return hlc.Timestamp{
		WallTime: nanos,
		Logical:  logical,
	}
}

func TestGCQueueScoreString(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for i, c := range []struct {
		r   gcQueueScore
		exp string
	}{
		{gcQueueScore{}, "(empty)"},
		{gcQueueScore{
			ShouldQueue:              true,
			FuzzFactor:               1.25,
			FinalScore:               3.45 * 1.25,
			ValuesScalableScore:      4,
			DeadFraction:             0.25,
			IntentScore:              0.45,
			ExpMinGCByteAgeReduction: 256 * 1024,
			GCByteAge:                512 * 1024,
			GCBytes:                  1024 * 3,
			LikelyLastGC:             5 * time.Second,
		},
			`queue=true with 4.31/fuzz(1.25)=3.45=valScaleScore(4.00)*deadFrac(0.25)+intentScore(0.45)
likely last GC: 5s ago, 3.0 KiB non-live, curr. age 512 KiB*s, min exp. reduction: 256 KiB*s`},
	} {
		if act := c.r.String(); act != c.exp {
			t.Errorf("%d: wanted:\n'%s'\ngot:\n'%s'", i, c.exp, act)
		}
	}
}

func TestGCQueueMakeGCScoreInvariantQuick(t *testing.T) {
	defer leaktest.AfterTest(t)()

	rnd, seed := randutil.NewPseudoRand()
	cfg := quick.Config{
		MaxCount: 50000,
		Rand:     rnd,
	}
	initialNow := hlc.Timestamp{}.Add(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano(), 0)
	ctx := context.Background()

	// Verify that the basic assumption always holds: We won't queue based on
	// GCByte{s,Age} unless TTL-based deletion would actually delete something.
	if err := quick.Check(func(
		seed uint16, uTTLSec uint32, uTimePassed time.Duration, uGCBytes uint32, uGCByteAge uint32,
	) bool {
		ttlSec, timePassed := int32(uTTLSec), time.Duration(uTimePassed)
		gcBytes, gcByteAge := int64(uGCBytes), int64(uGCByteAge)

		ms := enginepb.MVCCStats{
			LastUpdateNanos: initialNow.WallTime,
			ValBytes:        gcBytes,
			GCBytesAge:      gcByteAge,
		}
		now := initialNow.Add(timePassed.Nanoseconds(), 0)
		r := makeGCQueueScoreImpl(ctx, int64(seed), now, ms, ttlSec)
		wouldHaveToDeleteSomething := gcBytes*int64(ttlSec) < ms.GCByteAge(now.WallTime)
		result := !r.ShouldQueue || wouldHaveToDeleteSomething
		if !result {
			log.Warningf(ctx, "failing on ttl=%d, timePassed=%s, gcBytes=%d, gcByteAge=%d:\n%s",
				ttlSec, timePassed, gcBytes, gcByteAge, r)
		}
		return result
	}, &cfg); err != nil {
		t.Fatal(errors.Wrapf(err, "with seed %d", seed))
	}
}

func TestGCQueueMakeGCScoreAnomalousStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if err := quick.Check(func(keyBytes, valBytes, liveBytes int32, containsEstimates bool) bool {
		r := makeGCQueueScoreImpl(context.Background(), 0, hlc.Timestamp{}, enginepb.MVCCStats{
			ContainsEstimates: containsEstimates,
			LiveBytes:         int64(liveBytes),
			ValBytes:          int64(valBytes),
			KeyBytes:          int64(keyBytes),
		}, 60)
		return r.DeadFraction >= 0 && r.DeadFraction <= 1
	}, &quick.Config{MaxCount: 1000}); err != nil {
		t.Fatal(err)
	}
}

const cacheFirstLen = 3

type gcTestCacheKey struct {
	enginepb.MVCCStats
	string
}
type gcTestCacheVal struct {
	first [cacheFirstLen]enginepb.MVCCStats
	last  enginepb.MVCCStats
}

type cachedWriteSimulator struct {
	cache map[gcTestCacheKey]gcTestCacheVal
	t     *testing.T
}

func newCachedWriteSimulator(t *testing.T) *cachedWriteSimulator {
	var cws cachedWriteSimulator
	cws.t = t
	cws.cache = map[gcTestCacheKey]gcTestCacheVal{
		{enginepb.MVCCStats{LastUpdateNanos: 946684800000000000}, "1-1m0s-1.0 MiB"}: {
			first: [cacheFirstLen]enginepb.MVCCStats{
				{ContainsEstimates: false, LastUpdateNanos: 946684800000000000, IntentAge: 0, GCBytesAge: 0, LiveBytes: 1048604, LiveCount: 1, KeyBytes: 23, KeyCount: 1, ValBytes: 1048581, ValCount: 1, IntentBytes: 0, IntentCount: 0, SysBytes: 0, SysCount: 0},
				{ContainsEstimates: false, LastUpdateNanos: 946684801000000000, IntentAge: 0, GCBytesAge: 1048593, LiveBytes: 1048604, LiveCount: 1, KeyBytes: 35, KeyCount: 1, ValBytes: 2097162, ValCount: 2, IntentBytes: 0, IntentCount: 0, SysBytes: 0, SysCount: 0},
				{ContainsEstimates: false, LastUpdateNanos: 946684802000000000, IntentAge: 0, GCBytesAge: 3145779, LiveBytes: 1048604, LiveCount: 1, KeyBytes: 47, KeyCount: 1, ValBytes: 3145743, ValCount: 3, IntentBytes: 0, IntentCount: 0, SysBytes: 0, SysCount: 0},
			},
			last: enginepb.MVCCStats{ContainsEstimates: false, LastUpdateNanos: 946684860000000000, IntentAge: 0, GCBytesAge: 1918925190, LiveBytes: 1048604, LiveCount: 1, KeyBytes: 743, KeyCount: 1, ValBytes: 63963441, ValCount: 61, IntentBytes: 0, IntentCount: 0, SysBytes: 0, SysCount: 0},
		},
	}
	return &cws
}

func (cws *cachedWriteSimulator) value(size int) roachpb.Value {
	var value roachpb.Value
	kb := make([]byte, size)
	if n, err := rand.New(rand.NewSource(int64(size))).Read(kb); err != nil {
		cws.t.Fatal(err)
	} else if n != size {
		cws.t.Fatalf("wrote only %d < %d", n, size)
	}
	value.SetBytes(kb)
	return value
}

func (cws *cachedWriteSimulator) multiKey(
	numOps int, size int, txn *roachpb.Transaction, ms *enginepb.MVCCStats,
) {
	eng := engine.NewInMem(roachpb.Attributes{}, 1<<20)
	defer eng.Close()
	t, ctx := cws.t, context.Background()

	ts := hlc.Timestamp{}.Add(ms.LastUpdateNanos, 0)
	key, value := []byte("multikey"), cws.value(size)
	var eachMS enginepb.MVCCStats
	if err := engine.MVCCPut(ctx, eng, &eachMS, key, ts, value, txn); err != nil {
		t.Fatal(err)
	}
	for i := 1; i < numOps; i++ {
		ms.Add(eachMS)
	}
}

func (cws *cachedWriteSimulator) singleKeySteady(
	qps int, duration time.Duration, size int, ms *enginepb.MVCCStats,
) {
	eng := engine.NewInMem(roachpb.Attributes{}, 1<<20)
	defer eng.Close()
	t, ctx := cws.t, context.Background()
	cacheKey := fmt.Sprintf("%d-%s-%s", qps, duration, humanizeutil.IBytes(int64(size)))
	cached, ok := cws.cache[gcTestCacheKey{*ms, cacheKey}]
	if !ok && duration > 0 {
		t.Fatalf("cache key missing: %s with %s", cacheKey, pretty.Sprint(*ms))
	}
	firstSl := make([]enginepb.MVCCStats, 0, cacheFirstLen)
	// Pick random bytes once; they don't matter for stats but we want reproducability.
	key, value := []byte("0123456789"), cws.value(size)

	initialNow := hlc.Timestamp{}.Add(ms.LastUpdateNanos, 0)

	for elapsed := time.Duration(0); elapsed <= duration; elapsed += time.Second {
		for i := 0; i < qps; i++ {
			now := initialNow.Add(elapsed.Nanoseconds(), int32(i))

			if err := engine.MVCCPut(ctx, eng, ms, key, now, value, nil /* txn */); err != nil {
				t.Fatal(err)
			}
			if len(firstSl) < cacheFirstLen {
				firstSl = append(firstSl, *ms)
			} else if len(firstSl) == cacheFirstLen {
				if ok && reflect.DeepEqual(firstSl, cached.first[:cacheFirstLen]) {
					*ms = cached.last
					// Exit both loops.
					elapsed += duration
					break
				} else {
					ok = false
				}
			}
		}
	}

	if !ok && duration > 0 {
		copy(cached.first[:3], firstSl)
		cached.last = *ms
		t.Fatalf("missing or incorrect cache entry for %s, should be \n%s", cacheKey, pretty.Sprint(cached))
	}
}

func (cws *cachedWriteSimulator) shouldQueue(
	b bool, prio float64, after time.Duration, ttl time.Duration, ms enginepb.MVCCStats,
) {
	ts := hlc.Timestamp{}.Add(ms.LastUpdateNanos+after.Nanoseconds(), 0)
	r := makeGCQueueScoreImpl(context.Background(), 0 /* seed */, ts, ms, int32(ttl.Seconds()))
	if fmt.Sprintf("%.2f", r.FinalScore) != fmt.Sprintf("%.2f", prio) || b != r.ShouldQueue {
		cws.t.Errorf("expected queued=%t (is %t), prio=%.2f, got %.2f: after=%s, ttl=%s:\nms: %+v\nscore: %s",
			b, r.ShouldQueue, prio, r.FinalScore, after, ttl, ms, r)
	}
}

// TestGCQueueMakeGCScoreRealistic verifies conditions which inform priority and
// whether or not the range should be queued into the GC queue. Ranges are
// queued for GC based on two conditions. The age of bytes available to be GC'd,
// and the age of unresolved intents.
func TestGCQueueMakeGCScoreRealistic(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cws := newCachedWriteSimulator(t)

	initialMS := func() enginepb.MVCCStats {
		var zeroMS enginepb.MVCCStats
		zeroMS.AgeTo(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano())
		return zeroMS
	}

	minuteTTL, hourTTL := time.Minute, time.Hour

	{
		// Hammer a key with 1MB blobs for a (simulated) minute. Logically, at
		// the end of that you have around 60MB that are non-live, but cannot
		// immediately be deleted. Deletion is only possible for keys that are
		// (more or less) past TTL, and shouldQueue should respect that.
		ms := initialMS()
		cws.singleKeySteady(1, time.Minute, 1<<20, &ms)

		// First, check that a zero second TTL behaves like a one second one.
		// This is a special case without which the priority would be +Inf.
		//
		// Since at the time of this check the data is already 30s old on
		// average (i.e. ~30x the TTL), we expect to *really* want GC.
		cws.shouldQueue(true, 36.68, time.Duration(0), 0, ms)
		cws.shouldQueue(true, 36.68, time.Duration(0), 0, ms)

		// Right after we finished writing, we don't want to GC yet with a one-minute TTL.
		cws.shouldQueue(false, 0.61, time.Duration(0), minuteTTL, ms)

		// Neither after a minute. The first values are about to become GC'able, though.
		cws.shouldQueue(false, 1.81, time.Minute, minuteTTL, ms)
		// 90 seconds in it's really close, but still just shy of GC. Half of the
		// values could be deleted now (remember that we wrote them over a one
		// minute period).
		cws.shouldQueue(true, 2.42, 3*time.Minute/2, minuteTTL, ms)
		// Advancing another 1/4 minute does the trick.
		cws.shouldQueue(true, 2.72, 7*time.Minute/4, minuteTTL, ms)
		// After an hour, that's (of course) still true with a very high priority.
		cws.shouldQueue(true, 72.76, time.Hour, minuteTTL, ms)

		// Let's see what the same would look like with a 1h TTL.
		// Can't delete anything until 59min have passed, and indeed the score is low.
		cws.shouldQueue(false, 0.01, time.Duration(0), hourTTL, ms)
		cws.shouldQueue(false, 0.03, time.Minute, hourTTL, ms)
		cws.shouldQueue(false, 1.21, time.Hour, hourTTL, ms)
		// After 90 minutes, we're getting closer. After two hours, definitely ripe for
		// GC (and this would delete all the values).
		cws.shouldQueue(false, 1.81, 3*time.Hour/2, hourTTL, ms)
		cws.shouldQueue(true, 2.42, 2*time.Hour, hourTTL, ms)
	}

	{
		ms, valSize := initialMS(), 1<<10

		// Write 999 distinct 1kb keys at the initial timestamp without transaction.
		cws.multiKey(999, valSize, nil /* no txn */, &ms)

		// GC shouldn't move at all, even after a long time and short TTL.
		cws.shouldQueue(false, 0, 24*time.Hour, minuteTTL, ms)

		// Write a single key twice.
		cws.singleKeySteady(2, 0, valSize, &ms)

		// Now we have the situation in which the replica is pretty large
		// compared to the amount of (ever) GC'able data it has. Consequently,
		// the GC score should rise very slowly.

		// If the fact that 99.9% of the replica is live were not taken into
		// account, this would get us at least close to GC.
		cws.shouldQueue(false, 0.01, 5*time.Minute, minuteTTL, ms)

		// 12 hours in the score becomes relevant, but not yet large enough.
		// The key is of course GC'able and has been for a long time, but
		// to find it we'd have to scan all the other kv pairs as well.
		cws.shouldQueue(false, 0.87, 12*time.Hour, minuteTTL, ms)

		// Two days in we're more than ready to go, would queue for GC, and
		// delete.
		cws.shouldQueue(true, 3.49, 48*time.Hour, minuteTTL, ms)
	}

	{
		irrelevantTTL := 24 * time.Hour * 365
		ms, valSize := initialMS(), 1<<10
		txn := newTransaction(
			"txn", roachpb.Key("key"), roachpb.NormalUserPriority, enginepb.SERIALIZABLE,
			hlc.NewClock(func() int64 { return ms.LastUpdateNanos }, time.Millisecond))

		// Write 1000 distinct 1kb intents at the initial timestamp. This means that
		// the average intent age is just the time elapsed from now, and this is roughly
		// normalized by one day at the time of writing. Note that the size of the writes
		// doesn't matter. In reality, the value-based GC score will often strike first.
		cws.multiKey(100, valSize, txn, &ms)

		cws.shouldQueue(false, 1.22, 24*time.Hour, irrelevantTTL, ms)
		cws.shouldQueue(false, 2.45, 2*24*time.Hour, irrelevantTTL, ms)
		cws.shouldQueue(false, 4.89, 4*24*time.Hour, irrelevantTTL, ms)
		cws.shouldQueue(false, 8.56, 7*24*time.Hour, irrelevantTTL, ms)
		cws.shouldQueue(true, 11, 9*24*time.Hour, irrelevantTTL, ms)
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
