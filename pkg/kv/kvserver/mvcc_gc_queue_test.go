// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"sync/atomic"
	"testing"
	"testing/quick"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/gc"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/syncmap"
)

// makeTS creates a new hybrid logical timestamp.
func makeTS(nanos int64, logical int32) hlc.Timestamp {
	return hlc.Timestamp{
		WallTime: nanos,
		Logical:  logical,
	}
}

func TestMVCCGCQueueScoreString(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	for i, c := range []struct {
		r   mvccGCQueueScore
		exp string
	}{
		{mvccGCQueueScore{}, "(empty)"},
		{mvccGCQueueScore{
			ShouldQueue:              true,
			FuzzFactor:               1.25,
			FinalScore:               3.45 * 1.25,
			ValuesScalableScore:      4,
			DeadFraction:             0.25,
			IntentScore:              0.45,
			ExpMinGCByteAgeReduction: 256 * 1024,
			GCByteAge:                512 * 1024,
			GCBytes:                  1024 * 3,
			LastGC:                   5 * time.Second,
		},
			`queue=true with 4.31/fuzz(1.25)=3.45=valScaleScore(4.00)*deadFrac(0.25)+intentScore(0.45)
likely last GC: 5s ago, 3.0 KiB non-live, curr. age 512 KiB*s, min exp. reduction: 256 KiB*s`},
		// Check case of empty Threshold.
		{mvccGCQueueScore{ShouldQueue: true}, `queue=true with 0.00/fuzz(0.00)=NaN=valScaleScore(0.00)*deadFrac(0.00)+intentScore(0.00)
likely last GC: never, 0 B non-live, curr. age 0 B*s, min exp. reduction: 0 B*s`},
	} {
		if act := c.r.String(); act != c.exp {
			t.Errorf("%d: wanted:\n'%s'\ngot:\n'%s'", i, c.exp, act)
		}
	}
}

func TestMVCCGCQueueMakeGCScoreInvariantQuick(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	rnd, seed := randutil.NewTestRand()
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
		ttlSec, timePassed := int32(uTTLSec), uTimePassed
		gcBytes, gcByteAge := int64(uGCBytes), int64(uGCByteAge)

		ms := enginepb.MVCCStats{
			LastUpdateNanos: initialNow.WallTime,
			ValBytes:        gcBytes,
			GCBytesAge:      gcByteAge,
		}
		now := initialNow.Add(timePassed.Nanoseconds(), 0)
		r := makeMVCCGCQueueScoreImpl(
			ctx, int64(seed), now, ms, time.Duration(ttlSec)*time.Second, hlc.Timestamp{},
			true,                        /* canAdvanceGCThreshold */
			roachpb.GCHint{}, time.Hour, /* txnCleanupThreshold */
		)
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

func TestMVCCGCQueueMakeGCScoreAnomalousStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	if err := quick.Check(func(keyBytes, valBytes, liveBytes int32, containsEstimates int64) bool {
		r := makeMVCCGCQueueScoreImpl(context.Background(), 0, hlc.Timestamp{}, enginepb.MVCCStats{
			ContainsEstimates: containsEstimates,
			LiveBytes:         int64(liveBytes),
			ValBytes:          int64(valBytes),
			KeyBytes:          int64(keyBytes),
		}, 60*time.Second, hlc.Timestamp{}, true, /* canAdvanceGCThreshold */
			roachpb.GCHint{}, time.Hour, /* txnCleanupThreshold */
		)
		return r.DeadFraction >= 0 && r.DeadFraction <= 1
	}, &quick.Config{MaxCount: 1000}); err != nil {
		t.Fatal(err)
	}
}

func TestMVCCGCQueueMakeGCScoreLargeAbortSpan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	const seed = 1
	var ms enginepb.MVCCStats
	ms.AbortSpanBytes += largeAbortSpanBytesThreshold
	ms.SysBytes += largeAbortSpanBytesThreshold
	ms.SysCount++

	expiration := gc.TxnCleanupThreshold.Default().Nanoseconds() + 1

	// GC triggered if abort span should all be gc'able and it's large.
	{
		r := makeMVCCGCQueueScoreImpl(
			context.Background(), seed,
			hlc.Timestamp{WallTime: expiration + 1},
			ms, 10000*time.Second,
			hlc.Timestamp{},
			true, /* canAdvanceGCThreshold */
			roachpb.GCHint{},
			time.Hour, /* txnCleanupThreshold */
		)
		require.True(t, r.ShouldQueue)
		require.NotZero(t, r.FinalScore)
	}

	// GC triggered if abort span bytes count is 0, but the sys bytes
	// and sys count exceed a certain threshold (likely large abort span).
	ms.AbortSpanBytes = 0
	ms.SysCount = probablyLargeAbortSpanSysCountThreshold
	{
		r := makeMVCCGCQueueScoreImpl(
			context.Background(), seed,
			hlc.Timestamp{WallTime: expiration + 1},
			ms, 10000*time.Second,
			hlc.Timestamp{},
			true, /* canAdvanceGCThreshold */
			roachpb.GCHint{},
			time.Hour, /* txnCleanupThreshold */
		)
		require.True(t, r.ShouldQueue)
		require.NotZero(t, r.FinalScore)
	}

	// Heuristic doesn't fire if last GC within TxnCleanupThreshold.
	{
		r := makeMVCCGCQueueScoreImpl(context.Background(), seed,
			hlc.Timestamp{WallTime: expiration},
			ms, 10000*time.Second,
			hlc.Timestamp{WallTime: expiration - 100},
			true, /* canAdvanceGCThreshold */
			roachpb.GCHint{},
			time.Hour, /* txnCleanupThreshold */
		)
		require.False(t, r.ShouldQueue)
		require.Zero(t, r.FinalScore)
	}
}

func TestMVCCGCQueueMakeGCScoreIntentCooldown(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const seed = 1
	ctx := context.Background()
	now := hlc.Timestamp{WallTime: 1e6 * 1e9}
	gcTTL := time.Second

	testcases := map[string]struct {
		lastGC   hlc.Timestamp
		mvccGC   bool
		expectGC bool
	}{
		"never GCed":               {lastGC: hlc.Timestamp{}, expectGC: true},
		"just GCed":                {lastGC: now.Add(-1, 0), expectGC: false},
		"future GC":                {lastGC: now.Add(1, 0), expectGC: false},
		"MVCC GC ignores cooldown": {lastGC: now.Add(-1, 0), mvccGC: true, expectGC: true},
		"before GC cooldown":       {lastGC: now.Add(-mvccGCQueueIntentCooldownDuration.Nanoseconds()+1, 0), expectGC: false},
		"at GC cooldown":           {lastGC: now.Add(-mvccGCQueueIntentCooldownDuration.Nanoseconds(), 0), expectGC: true},
		"after GC cooldown":        {lastGC: now.Add(-mvccGCQueueIntentCooldownDuration.Nanoseconds()-1, 0), expectGC: true},
	}
	for name, tc := range testcases {
		tc := tc
		t.Run(name, func(t *testing.T) {
			ms := enginepb.MVCCStats{
				IntentCount: 1e9,
			}
			if tc.mvccGC {
				ms.ValBytes = 1e9
			}

			r := makeMVCCGCQueueScoreImpl(
				ctx, seed, now, ms, gcTTL, tc.lastGC, true, /* canAdvanceGCThreshold */
				roachpb.GCHint{}, time.Hour, /* txnCleanupThreshold */
			)
			require.Equal(t, tc.expectGC, r.ShouldQueue)
		})
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
				{ContainsEstimates: 0, LastUpdateNanos: 946684800000000000, IntentAge: 0, GCBytesAge: 0, LiveBytes: 1048604, LiveCount: 1, KeyBytes: 23, KeyCount: 1, ValBytes: 1048581, ValCount: 1, IntentBytes: 0, IntentCount: 0, SysBytes: 0, SysCount: 0, AbortSpanBytes: 0},
				{ContainsEstimates: 0, LastUpdateNanos: 946684801000000000, IntentAge: 0, GCBytesAge: 0, LiveBytes: 1048604, LiveCount: 1, KeyBytes: 35, KeyCount: 1, ValBytes: 2097162, ValCount: 2, IntentBytes: 0, IntentCount: 0, SysBytes: 0, SysCount: 0, AbortSpanBytes: 0},
				{ContainsEstimates: 0, LastUpdateNanos: 946684802000000000, IntentAge: 0, GCBytesAge: 1048593, LiveBytes: 1048604, LiveCount: 1, KeyBytes: 47, KeyCount: 1, ValBytes: 3145743, ValCount: 3, IntentBytes: 0, IntentCount: 0, SysBytes: 0, SysCount: 0, AbortSpanBytes: 0},
			},
			last: enginepb.MVCCStats{ContainsEstimates: 0, LastUpdateNanos: 946684860000000000, IntentAge: 0, GCBytesAge: 1856009610, LiveBytes: 1048604, LiveCount: 1, KeyBytes: 743, KeyCount: 1, ValBytes: 63963441, ValCount: 61, IntentBytes: 0, IntentCount: 0, SysBytes: 0, SysCount: 0, AbortSpanBytes: 0},
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
	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()
	t, ctx := cws.t, context.Background()

	ts := hlc.Timestamp{}.Add(ms.LastUpdateNanos, 0)
	key, value := []byte("multikey"), cws.value(size)
	var eachMS enginepb.MVCCStats
	opts := storage.MVCCWriteOptions{
		Txn:   txn,
		Stats: &eachMS,
	}
	if err := storage.MVCCPut(ctx, eng, key, ts, value, opts); err != nil {
		t.Fatal(err)
	}
	for i := 1; i < numOps; i++ {
		ms.Add(eachMS)
	}
}

func (cws *cachedWriteSimulator) singleKeySteady(
	qps int, duration time.Duration, size int, ms *enginepb.MVCCStats,
) {
	eng := storage.NewDefaultInMemForTesting()
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

			if err := storage.MVCCPut(ctx, eng, key, now, value, storage.MVCCWriteOptions{Stats: ms}); err != nil {
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
	b bool,
	prio float64,
	after time.Duration,
	ttl time.Duration,
	ms enginepb.MVCCStats,
	timeSinceGC time.Duration,
) {
	cws.t.Helper()
	ts := hlc.Timestamp{}.Add(ms.LastUpdateNanos+after.Nanoseconds(), 0)
	prevGCTs := hlc.Timestamp{}
	if timeSinceGC > 0 {
		prevGCTs = prevGCTs.Add(ms.LastUpdateNanos+(after-timeSinceGC).Nanoseconds(), 0)
	}
	r := makeMVCCGCQueueScoreImpl(context.Background(), 0 /* seed */, ts, ms, ttl,
		prevGCTs, true, /* canAdvanceGCThreshold */
		roachpb.GCHint{}, time.Hour, /* txnCleanupThreshold */
	)
	if fmt.Sprintf("%.2f", r.FinalScore) != fmt.Sprintf("%.2f", prio) || b != r.ShouldQueue {
		cws.t.Errorf("expected queued=%t (is %t), prio=%.2f, got %.2f: after=%s, ttl=%s:\nms: %+v\nscore: %s",
			b, r.ShouldQueue, prio, r.FinalScore, after, ttl, ms, r)
	}
}

// TestMVCCGCQueueMakeGCScoreRealistic verifies conditions which inform priority
// and whether or not the range should be queued into the MVCC GC queue. Ranges
// are queued for MVCC GC based on two conditions. The age of bytes available to
// be GC'd, and the age of unresolved intents.
func TestMVCCGCQueueMakeGCScoreRealistic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	storage.DisableMetamorphicSimpleValueEncoding(t)

	cws := newCachedWriteSimulator(t)

	initialMS := func() enginepb.MVCCStats {
		var zeroMS enginepb.MVCCStats
		zeroMS.AgeTo(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano())
		return zeroMS
	}

	minuteTTL, hourTTL := time.Minute, time.Hour
	const never = time.Duration(0)

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
		cws.shouldQueue(true, 28.94, time.Duration(0), 0, ms, never)
		cws.shouldQueue(true, 28.94, time.Duration(0), 0, ms, never)

		// Right after we finished writing, we don't want to GC yet with a one-minute TTL.
		cws.shouldQueue(false, 0.48, time.Duration(0), minuteTTL, ms, never)

		// After a minute score is already above minimum threshold and some data is
		// GC able so it triggers shouldQueue if there were no GC runs recently
		// enough, but not queueable if GC run recently.
		cws.shouldQueue(true, 1.46, time.Minute, minuteTTL, ms, never)
		cws.shouldQueue(false, 1.46, time.Minute, minuteTTL, ms, 30*time.Second)
		// 90 seconds in it's really close, but still just shy of GC. Half of the
		// values could be deleted now (remember that we wrote them over a one
		// minute period).
		// We are close to high threshold, so cooldown time shrunk to just under 6
		// minutes.
		cws.shouldQueue(true, 1.95, 3*time.Minute/2, minuteTTL, ms, 6*time.Minute)
		cws.shouldQueue(false, 1.95, 3*time.Minute/2, minuteTTL, ms, 5*time.Minute)
		// Advancing another 1/4 minute does the trick.
		cws.shouldQueue(true, 2.20, 7*time.Minute/4, minuteTTL, ms, never)
		// After an hour, that's (of course) still true with a very high priority.
		cws.shouldQueue(true, 59.34, time.Hour, minuteTTL, ms, never)

		// Let's see what the same would look like with a 1h TTL.
		// Can't delete anything until 59min have passed, and indeed the score is low.
		cws.shouldQueue(false, 0.01, time.Duration(0), hourTTL, ms, never)
		cws.shouldQueue(false, 0.02, time.Minute, hourTTL, ms, never)
		cws.shouldQueue(false, 0.99, time.Hour, hourTTL, ms, never)
		// After 90 minutes, we're getting closer. After just over two hours,
		// definitely ripe for GC (and this would delete all the values).
		cws.shouldQueue(true, 1.48, 90*time.Minute, hourTTL, ms, never)
		cws.shouldQueue(false, 1.48, 90*time.Minute, hourTTL, ms, 5*time.Minute)
		cws.shouldQueue(true, 2.05, 125*time.Minute, hourTTL, ms, never)
	}

	{
		ms, valSize := initialMS(), 1<<10

		// Write 999 distinct 1kb keys at the initial timestamp without transaction.
		cws.multiKey(999, valSize, nil /* no txn */, &ms)

		// GC shouldn't move at all, even after a long time and short TTL.
		cws.shouldQueue(false, 0, 24*time.Hour, minuteTTL, ms, never)

		// Write a single key twice.
		cws.singleKeySteady(2, 0, valSize, &ms)

		// Now we have the situation in which the replica is pretty large
		// compared to the amount of (ever) GC'able data it has. Consequently,
		// the GC score should rise very slowly.

		// If the fact that 99.9% of the replica is live were not taken into
		// account, this would get us at least close to GC.
		cws.shouldQueue(false, 0.00, 5*time.Minute, minuteTTL, ms, never)

		// 12 hours in the score becomes relevant, but not yet large enough.
		// The key is of course GC'able and has been for a long time, but
		// to find it we'd have to scan all the other kv pairs as well.
		cws.shouldQueue(false, 0.71, 12*time.Hour, minuteTTL, ms, never)

		// Two days in we're more than ready to go, would queue for GC, and
		// delete.
		cws.shouldQueue(true, 2.85, 48*time.Hour, minuteTTL, ms, never)
	}

	{
		irrelevantTTL := 24 * time.Hour * 365
		ms, valSize := initialMS(), 1<<10
		mc := timeutil.NewManualTime(timeutil.Unix(0, ms.LastUpdateNanos))
		txn := newTransaction(
			"txn", roachpb.Key("key"), roachpb.NormalUserPriority,
			hlc.NewClockForTesting(mc))

		// Write 1000 distinct 1kb intents at the initial timestamp. This means that
		// the average intent age is just the time elapsed from now, and this is roughly
		// normalized by one hour at the time of writing. Note that the size of the writes
		// doesn't matter. In reality, the value-based GC score will often strike first.
		cws.multiKey(100, valSize, txn, &ms)

		cws.shouldQueue(false, 0.12, 1*time.Hour, irrelevantTTL, ms, never)
		cws.shouldQueue(false, 0.87, 7*time.Hour, irrelevantTTL, ms, never)
		cws.shouldQueue(true, 1.12, 9*time.Hour, irrelevantTTL, ms, never)
		// Check if cooldown policy protects us from too frequent runs based on
		// intents alone.
		cws.shouldQueue(false, 1.12, 9*time.Hour, irrelevantTTL, ms, time.Hour)
	}
}

// TestFullRangeDeleteHeuristic verifies that deleting all data with range
// tombstone will lower time threshold needed to enable GC of data.
func TestFullRangeDeleteHeuristic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()

	rng, _ := randutil.NewTestRand()
	localMax, maxKey := keys.LocalMax, keys.MaxKey

	var keys []roachpb.Key
	fillDataForStats := func(rw storage.ReadWriter, keyCount, valCount int) (enginepb.MVCCStats, hlc.Timestamp) {
		for i := 0; i < keyCount; i++ {
			stringKey := randutil.RandString(rng, 20, randutil.PrintableKeyAlphabet)
			keys = append(keys, roachpb.Key(stringKey))
		}
		var ms enginepb.MVCCStats
		for i := 1; i < valCount; i++ {
			key := keys[rng.Intn(keyCount)]
			var value roachpb.Value
			if rng.Float32() > 0.5 {
				value.SetBytes(make([]byte, 20))
			}
			require.NoError(t, storage.MVCCPut(ctx, rw, key,
				hlc.Timestamp{WallTime: time.Millisecond.Nanoseconds() * int64(i)},
				value, storage.MVCCWriteOptions{Stats: &ms}))
		}
		return ms, hlc.Timestamp{WallTime: time.Millisecond.Nanoseconds() * int64(valCount)}
	}

	deleteWithTombstone := func(rw storage.ReadWriter, delTime hlc.Timestamp, ms *enginepb.MVCCStats) {
		require.NoError(t, storage.MVCCDeleteRangeUsingTombstone(
			ctx, rw, ms, localMax, maxKey, delTime, hlc.ClockTimestamp{}, nil, nil, false, 1, nil))
	}
	deleteWithPoints := func(rw storage.ReadWriter, delTime hlc.Timestamp, ms *enginepb.MVCCStats) {
		for _, key := range keys {
			require.NoError(t, storage.MVCCPut(ctx, rw, key, delTime, roachpb.Value{}, storage.MVCCWriteOptions{Stats: ms}))
		}
	}

	ms, deletionTime := fillDataForStats(eng, 100, 10000)

	shouldQueueAfter := func(ms enginepb.MVCCStats, delay time.Duration, gcTTL time.Duration) bool {
		now := deletionTime.Add(delay.Nanoseconds(), 0)
		r := makeMVCCGCQueueScoreImpl(ctx, 0 /* seed */, now, ms, gcTTL, now.Add(-time.Hour.Nanoseconds(), 0), true,
			roachpb.GCHint{}, time.Hour)
		return r.ShouldQueue
	}

	rangeMs := ms
	pointMs := ms
	withBatch(eng, func(b storage.Batch) { deleteWithTombstone(b, deletionTime, &rangeMs) })
	withBatch(eng, func(b storage.Batch) { deleteWithPoints(b, deletionTime, &pointMs) })

	gcTTL := time.Minute * 30
	for _, d := range []struct {
		delayMin           int
		pointDel, rangeDel bool
	}{
		{delayMin: 20, pointDel: false, rangeDel: false},
		{delayMin: 30, pointDel: false, rangeDel: true},
		{delayMin: 61, pointDel: true, rangeDel: true},
	} {
		t.Run(fmt.Sprintf("delay=%d", d.delayMin), func(t *testing.T) {
			delay := time.Minute * time.Duration(d.delayMin)
			dr := shouldQueueAfter(rangeMs, delay, gcTTL)
			dp := shouldQueueAfter(pointMs, delay, gcTTL)
			fmt.Printf("Delay min: %f, Queue del points: %t Queue del range: %t\n", delay.Minutes(), dp,
				dr)
			require.Equal(t, d.pointDel, dp, "expect enqueue metric for point deletion")
			require.Equal(t, d.rangeDel, dr, "expect enqueue metric for range deletion")
		})
	}
}

func withBatch(eng storage.Engine, fn func(b storage.Batch)) {
	b := eng.NewBatch()
	defer b.Close()
	fn(b)
}

func TestGCScoreWithHint(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	now := hlc.Timestamp{WallTime: time.Hour.Nanoseconds()}
	ttl := time.Minute

	for _, d := range []struct {
		name string

		rangeKeys int
		liveData  int
		garbage   int
		estimates int
		hintTs    hlc.Timestamp

		shouldGC bool
		priority float64
	}{
		{
			name:      "hint_not_reached",
			liveData:  0,
			garbage:   1000,
			rangeKeys: 1,
			hintTs:    now.Add(-ttl.Nanoseconds()+1000, 0),
			shouldGC:  true,
			priority:  60.441670451764324,
		},
		{
			name:      "hint_reached",
			liveData:  0,
			garbage:   100,
			rangeKeys: 1,
			hintTs:    now.Add(-ttl.Nanoseconds()-1, 0),
			shouldGC:  true,
			priority:  deleteRangePriority,
		},
		{
			name:      "hint_with_estimates",
			estimates: 1,
			hintTs:    now.Add(-ttl.Nanoseconds()-1, 0),
			shouldGC:  true,
			priority:  deleteRangePriority,
		},
		{
			name:     "hint_with_live_data",
			liveData: 1000,
			garbage:  10,
			hintTs:   now.Add(-ttl.Nanoseconds()-1, 0),
			shouldGC: false,
		},
	} {
		t.Run(d.name, func(t *testing.T) {
			ms := enginepb.MVCCStats{
				KeyCount:          int64(d.liveData + d.garbage),
				KeyBytes:          int64((d.liveData + d.garbage) * 10),
				RangeKeyCount:     int64(d.rangeKeys),
				RangeKeyBytes:     int64(d.rangeKeys * 20),
				LiveCount:         int64(d.liveData),
				LiveBytes:         int64(d.liveData * 10),
				ContainsEstimates: int64(d.estimates),
				GCBytesAge:        int64((d.garbage + d.rangeKeys*2) * 10 * 100),
			}
			score := makeMVCCGCQueueScoreImpl(ctx, 1, now, ms, time.Minute, hlc.Timestamp{}, true,
				roachpb.GCHint{LatestRangeDeleteTimestamp: d.hintTs}, time.Hour)
			require.Equal(t, d.shouldGC, score.ShouldQueue)
			if d.shouldGC {
				require.Equal(t, d.priority, score.FinalScore)
			}
		})
	}
}

// TestMVCCGCQueueProcess creates test data in the range over various time
// scales and verifies that scan queue process properly GCs test data.
func TestMVCCGCQueueProcess(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	storage.DisableMetamorphicSimpleValueEncoding(t)
	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

	const intentAgeThreshold = 2 * time.Hour
	const txnCleanupThreshold = time.Hour

	tc.manualClock.Advance(48 * 60 * 60 * 1e9) // 2d past the epoch
	now := tc.Clock().Now().WallTime

	ts1 := makeTS(now-2*24*60*60*1e9+1, 0)                     // 2d old (add one nanosecond so we're not using zero timestamp)
	ts2 := makeTS(now-25*60*60*1e9, 0)                         // GC will occur at time=25 hours
	ts2m1 := ts2.Prev()                                        // ts2 - 1 so we have something not right at the GC time
	ts3 := makeTS(now-intentAgeThreshold.Nanoseconds(), 0)     // 2h old
	ts4 := makeTS(now-(intentAgeThreshold.Nanoseconds()-1), 0) // 2h-1ns old
	ts5 := makeTS(now-1e9, 0)                                  // 1s old
	mkKey := func(suff string) roachpb.Key {
		var k roachpb.Key
		k = append(k, keys.ScratchRangeMin...)
		k = append(k, suff...)
		return k
	}
	key1 := mkKey("a")
	key2 := mkKey("b")
	key3 := mkKey("c")
	key4 := mkKey("d")
	key5 := mkKey("e")
	key6 := mkKey("f")
	key7 := mkKey("g")
	key8 := mkKey("h")
	key9 := mkKey("i")
	key10 := mkKey("j")
	key11 := mkKey("k")
	key12 := mkKey("l")
	key13 := mkKey("m")
	key14 := mkKey("n")
	key15 := mkKey("o")

	type kvData struct {
		key    roachpb.Key
		endKey roachpb.Key
		ts     hlc.Timestamp
		del    bool
		txn    bool
	}

	mkVal := func(key roachpb.Key, ts hlc.Timestamp) kvData {
		return kvData{key: key, ts: ts}
	}
	mkDel := func(key roachpb.Key, ts hlc.Timestamp) kvData {
		return kvData{key: key, ts: ts, del: true}
	}
	mkTxn := func(data kvData) kvData {
		data.txn = true
		return data
	}
	mkRng := func(key, endKey roachpb.Key, ts hlc.Timestamp) kvData {
		return kvData{key: key, endKey: endKey, ts: ts, del: true}
	}

	data := []kvData{
		// For key1, we expect first value to GC.
		mkVal(key1, ts1),
		mkVal(key1, ts2),
		mkVal(key1, ts5),
		// For key2, we expect values to GC, even though most recent is deletion.
		mkVal(key2, ts1),
		mkVal(key2, ts2m1), // use a value < the GC time to verify it's kept
		mkDel(key2, ts5),
		// For key3, we expect just ts1 to GC, because most recent deletion is intent.
		mkVal(key3, ts1),
		mkVal(key3, ts2),
		mkTxn(mkDel(key3, ts5)),
		// For key4, expect oldest value to GC.
		mkVal(key4, ts1),
		mkVal(key4, ts2),
		// For key5, expect all values to GC (most recent value deleted).
		mkVal(key5, ts1),
		mkDel(key5, ts2), // deleted, so GC
		// For key6, expect no values to GC because most recent value is intent.
		mkVal(key6, ts1),
		mkTxn(mkVal(key6, ts5)),
		// For key7, expect no values to GC because intent is exactly 2h old.
		mkVal(key7, ts2),
		mkTxn(mkVal(key7, ts4)),
		// For key8, expect most recent value to resolve by aborting, which will clean it up.
		mkVal(key8, ts2),
		mkTxn(mkDel(key8, ts3)),
		// For key9, resolve naked intent with no remaining values.
		mkTxn(mkVal(key9, ts3)),
		// For key10, GC ts1 because it's a delete but not ts3 because it's above the threshold.
		mkDel(key10, ts1),
		mkDel(key10, ts3),
		mkVal(key10, ts4),
		mkVal(key10, ts5),
		// For key11, we can't GC anything because ts1 isn't a delete.
		mkVal(key11, ts1),
		mkDel(key11, ts3),
		mkDel(key11, ts4),
		mkDel(key11, ts5),
		// key12 has its older version covered by range tombstone and should be GCd
		mkVal(key12, ts1),
		mkVal(key12, ts5),
		// key13 has all versions covered by range tombstone
		mkVal(key13, ts1),
		// This is old range tombstone below gc threshold
		mkRng(key12, key14, ts2),
		// This is newer range tombstone above gc threshold
		mkRng(key13, key15, ts3),
	}

	sort.Slice(data, func(i, j int) bool {
		return data[i].ts.Less(data[j].ts)
	})

	for i, datum := range data {
		if len(datum.endKey) > 0 {
			drArgs := deleteRangeArgs(datum.key, datum.endKey)
			drArgs.UseRangeTombstone = true
			if _, err := tc.SendWrappedWith(kvpb.Header{
				Timestamp: datum.ts,
			}, &drArgs); err != nil {
				t.Fatalf("%d: could not delete data: %+v", i, err)
			}
			continue
		}
		if datum.del {
			dArgs := deleteArgs(datum.key)
			var txn *roachpb.Transaction
			if datum.txn {
				txn = newTransaction("test", datum.key, 1, tc.Clock())
				// Overwrite the timestamps set by newTransaction().
				txn.ReadTimestamp = datum.ts
				txn.WriteTimestamp = datum.ts
				txn.MinTimestamp = datum.ts
				assignSeqNumsForReqs(txn, &dArgs)
			}
			if _, err := tc.SendWrappedWith(kvpb.Header{
				Timestamp: datum.ts,
				Txn:       txn,
			}, &dArgs); err != nil {
				t.Fatalf("%d: could not delete data: %+v", i, err)
			}
			continue
		}
		pArgs := putArgs(datum.key, []byte("value"))
		var txn *roachpb.Transaction
		if datum.txn {
			txn = newTransaction("test", datum.key, 1, tc.Clock())
			// Overwrite the timestamps set by newTransaction().
			txn.ReadTimestamp = datum.ts
			txn.WriteTimestamp = datum.ts
			txn.MinTimestamp = datum.ts
			assignSeqNumsForReqs(txn, &pArgs)
		}
		if _, err := tc.SendWrappedWith(kvpb.Header{
			Timestamp: datum.ts,
			Txn:       txn,
		}, &pArgs); err != nil {
			t.Fatalf("%d: could not put data: %+v", i, err)
		}
	}

	cfg, err := tc.store.GetConfReader(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// TODO: following computations should take care of new local timestamp
	// for tombstones as they can be non-zero in size.
	var (
		// The total size of the GC'able versions of the keys and values in Info.
		// Key size: len(scratch+"a") + 1 + MVCCVersionTimestampSize (12 bytes) = 15 bytes.
		// Value size: len("value") + headerSize (5 bytes) = 10 bytes.
		// key1 at ts1  (15 bytes) => "value" (10 bytes)
		// key2 at ts1  (15 bytes) => "value" (10 bytes)
		// key3 at ts1  (15 bytes) => "value" (10 bytes)
		// key4 at ts1  (15 bytes) => "value" (10 bytes)
		// key5 at ts1  (15 bytes) => "value" (10 bytes)
		// key5 at ts2  (15 bytes) => delete (0 bytes)
		// key10 at ts1 (15 bytes) => delete (0 bytes)
		// key12 at ts1 (15 bytes) => "value" (10 bytes)
		// key13 at ts1 (15 bytes) => "value" (10 bytes)
		expectedVersionsKeyBytes int64 = 9 * 15
		expectedVersionsValBytes int64 = 7 * 10
		// Range Key size: len(scratch + "x") * 2 + timestamp (12)
		// Range Value size: 0 for deletion
		// key13, key14 at ts1 (12) => delete (0 bytes)
		// key14, key15 at ts1 (16) => delete (0 bytes)
		expectedVersionsRangeKeyBytes int64 = 12 + 16
		expectedVersionsRangeValBytes int64 = 0
	)

	// Call Run with dummy functions to get current Info.
	gcInfo, err := func() (gc.Info, error) {
		snap := tc.repl.store.TODOEngine().NewSnapshot()
		desc := tc.repl.Desc()
		defer snap.Close()

		conf, err := cfg.GetSpanConfigForKey(ctx, desc.StartKey)
		if err != nil {
			t.Fatalf("could not find zone config for range %s: %+v", tc.repl, err)
		}

		now := tc.Clock().Now()
		newThreshold := gc.CalculateThreshold(now, conf.TTL())
		return gc.Run(ctx, desc, snap, now, newThreshold, gc.RunOptions{
			IntentAgeThreshold:  intentAgeThreshold,
			TxnCleanupThreshold: txnCleanupThreshold,
		},
			conf.TTL(), gc.NoopGCer{},
			func(ctx context.Context, intents []roachpb.Intent) error {
				return nil
			},
			func(ctx context.Context, txn *roachpb.Transaction) error {
				return nil
			})
	}()
	if err != nil {
		t.Fatal(err)
	}
	if gcInfo.AffectedVersionsKeyBytes != expectedVersionsKeyBytes {
		t.Errorf("expected total keys size: %d bytes; got %d bytes", expectedVersionsKeyBytes,
			gcInfo.AffectedVersionsKeyBytes)
	}
	if gcInfo.AffectedVersionsValBytes != expectedVersionsValBytes {
		t.Errorf("expected total values size: %d bytes; got %d bytes", expectedVersionsValBytes,
			gcInfo.AffectedVersionsValBytes)
	}
	if gcInfo.AffectedVersionsRangeKeyBytes != expectedVersionsRangeKeyBytes {
		t.Errorf("expected total range key size: %d bytes; got %d bytes", expectedVersionsRangeKeyBytes,
			gcInfo.AffectedVersionsRangeKeyBytes)
	}
	if gcInfo.AffectedVersionsRangeValBytes != expectedVersionsRangeValBytes {
		t.Errorf("expected total range value size: %d bytes; got %d bytes", expectedVersionsRangeValBytes,
			gcInfo.AffectedVersionsRangeValBytes)
	}

	// Process through a scan queue.
	mgcq := newMVCCGCQueue(tc.store)
	processed, err := mgcq.process(ctx, tc.repl)
	if err != nil {
		t.Fatal(err)
	}
	assert.True(t, processed, "queue not processed")

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
		{key12, ts5},
	}
	// Read data directly from engine to avoid intent errors from MVCC.
	// However, because the GC processing pushes transactions and
	// resolves intents asynchronously, we use a SucceedsSoon loop.
	testutils.SucceedsSoon(t, func() error {
		kvs, err := storage.Scan(tc.store.TODOEngine(), key1, keys.MaxKey, 0)
		if err != nil {
			return err
		}
		for i, kv := range kvs {
			t.Logf("%d: %s", i, kv.Key)
		}
		if len(kvs) != len(expKVs) {
			return fmt.Errorf("expected length %d; got %d", len(expKVs), len(kvs))
		}
		for i, kv := range kvs {
			if !kv.Key.Key.Equal(expKVs[i].key) {
				return fmt.Errorf("%d: expected key %q; got %q", i, expKVs[i].key, kv.Key.Key)
			}
			if kv.Key.Timestamp != expKVs[i].ts {
				return fmt.Errorf("%d: expected ts=%s; got %s", i, expKVs[i].ts, kv.Key.Timestamp)
			}
			log.VEventf(ctx, 2, "%d: %s", i, kv.Key)
		}
		t.Log("success")
		return nil
	})
}

func TestMVCCGCQueueTransactionTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	manual := timeutil.NewManualTime(timeutil.Unix(0, 123))
	tsc := TestStoreConfig(hlc.NewClockForTesting(manual))
	manual.MustAdvanceTo(timeutil.Unix(0, 3*24*time.Hour.Nanoseconds()))

	testTime := manual.Now().Add(2 * time.Hour)
	gcExpiration := testTime.Add(-gc.TxnCleanupThreshold.Default())

	type spec struct {
		status      roachpb.TransactionStatus
		orig        time.Time
		hb          time.Time                 // last heartbeat (zero if Timestamp{})
		newStatus   roachpb.TransactionStatus // -1 for GCed
		failResolve bool                      // do we want to fail resolves in this trial?
		expResolve  bool                      // expect attempt at removing txn-persisted intents?
		expAbortGC  bool                      // expect AbortSpan entries removed?
	}
	// Describes the state of the Txn table before the test.
	// Many of the AbortSpan entries deleted wouldn't even be there, so don't
	// be confused by that.
	testCases := map[string]spec{
		// Too young, should not touch.
		"a": {
			status:    roachpb.PENDING,
			orig:      gcExpiration.Add(1),
			newStatus: roachpb.PENDING,
		},
		// Old and pending, but still heartbeat (so no Push attempted; it
		// would not succeed).
		"b": {
			status:    roachpb.PENDING,
			orig:      timeutil.Unix(0, 1), // immaterial
			hb:        gcExpiration.Add(1),
			newStatus: roachpb.PENDING,
		},
		// Old, pending, and abandoned. Should push and abort it
		// successfully, and GC it, along with resolving the intent. The
		// AbortSpan is also cleaned up.
		"c": {
			status:     roachpb.PENDING,
			orig:       gcExpiration.Add(-1),
			newStatus:  -1,
			expResolve: true,
			expAbortGC: true,
		},
		// Staging and fresh, so no action.
		"d": {
			status:    roachpb.STAGING,
			orig:      gcExpiration.Add(1),
			newStatus: roachpb.STAGING,
		},
		// Old and staging, but still heartbeat (so no Push attempted; it
		// would not succeed).
		"e": {
			status:    roachpb.STAGING,
			orig:      timeutil.Unix(0, 1), // immaterial
			hb:        gcExpiration.Add(1),
			newStatus: roachpb.STAGING,
		},
		// Old, staging, and abandoned. Should push it and hit an indeterminate
		// commit error. Should successfully recover the transaction and GC it,
		// along with resolving the intent.
		"f": {
			status:     roachpb.STAGING,
			orig:       gcExpiration.Add(-1),
			newStatus:  -1,
			expResolve: true,
			expAbortGC: true,
		},
		// Old and aborted, should delete.
		"g": {
			status:     roachpb.ABORTED,
			orig:       gcExpiration.Add(-1),
			newStatus:  -1,
			expResolve: true,
			expAbortGC: true,
		},
		// Committed and fresh, so no action.
		"h": {
			status:    roachpb.COMMITTED,
			orig:      gcExpiration.Add(1),
			newStatus: roachpb.COMMITTED,
		},
		// Committed and old. It has an intent (like all tests here), which is
		// resolvable and hence we can GC.
		"i": {
			status:     roachpb.COMMITTED,
			orig:       gcExpiration.Add(-1),
			newStatus:  -1,
			expResolve: true,
			expAbortGC: true,
		},
		// Same as the previous one, but we've rigged things so that the intent
		// resolution here will fail and consequently no GC is expected.
		"j": {
			status:      roachpb.COMMITTED,
			orig:        gcExpiration.Add(-1),
			newStatus:   roachpb.COMMITTED,
			failResolve: true,
			expResolve:  true,
			expAbortGC:  true,
		},
	}

	var resolved syncmap.Map
	// Set the MaxIntentResolutionBatchSize to 1 so that the injected error for
	// resolution of "g" does not lead to the failure of resolution of "f".
	// The need to set this highlights the "fate sharing" aspect of batching
	// intent resolution.
	tsc.TestingKnobs.IntentResolverKnobs.MaxIntentResolutionBatchSize = 1
	tsc.TestingKnobs.EvalKnobs.TestingEvalFilter =
		func(filterArgs kvserverbase.FilterArgs) *kvpb.Error {
			if resArgs, ok := filterArgs.Req.(*kvpb.ResolveIntentRequest); ok {
				id := string(resArgs.IntentTxn.Key)
				// Only count finalizing intent resolution attempts in `resolved`.
				if resArgs.Status != roachpb.PENDING {
					var spans []roachpb.Span
					val, ok := resolved.Load(id)
					if ok {
						spans = val.([]roachpb.Span)
					}
					spans = append(spans, roachpb.Span{
						Key:    resArgs.Key,
						EndKey: resArgs.EndKey,
					})
					resolved.Store(id, spans)
				}
				// We've special cased one test case. Note that the intent is still
				// counted in `resolved`.
				if testCases[id].failResolve {
					return kvpb.NewErrorWithTxn(errors.Errorf("boom"), filterArgs.Hdr.Txn)
				}
			}
			return nil
		}
	tc := testContext{manualClock: manual}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.StartWithStoreConfig(ctx, t, stopper, tsc)
	manual.MustAdvanceTo(testTime)

	outsideKey := tc.repl.Desc().EndKey.Next().AsRawKey()
	testIntents := []roachpb.Span{{Key: roachpb.Key("intent")}}

	txns := map[string]roachpb.Transaction{}
	for strKey, test := range testCases {
		baseKey := roachpb.Key(strKey)
		txnClock := hlc.NewClockForTesting(timeutil.NewManualTime(test.orig))
		txn := newTransaction("txn1", baseKey, 1, txnClock)
		txn.Status = test.status
		txn.LockSpans = testIntents
		if !test.hb.IsZero() {
			txn.LastHeartbeat = hlc.Timestamp{WallTime: test.hb.UnixNano()}
		}
		txns[strKey] = *txn
		for _, addrKey := range []roachpb.Key{baseKey, outsideKey} {
			key := keys.TransactionKey(addrKey, txn.ID)
			if err := storage.MVCCPutProto(ctx, tc.engine, key, hlc.Timestamp{}, txn, storage.MVCCWriteOptions{}); err != nil {
				t.Fatal(err)
			}
		}
		entry := roachpb.AbortSpanEntry{Key: txn.Key, Timestamp: txn.LastActive()}
		if err := tc.repl.abortSpan.Put(ctx, tc.engine, nil, txn.ID, &entry); err != nil {
			t.Fatal(err)
		}
	}

	// Run GC.
	mgcq := newMVCCGCQueue(tc.store)
	processed, err := mgcq.process(ctx, tc.repl)
	if err != nil {
		t.Fatal(err)
	}
	assert.True(t, processed, "queue not processed")

	testutils.SucceedsSoon(t, func() error {
		for strKey, sp := range testCases {
			txn := &roachpb.Transaction{}
			txnKey := keys.TransactionKey(roachpb.Key(strKey), txns[strKey].ID)
			txnTombstoneTSCacheKey := transactionTombstoneMarker(
				roachpb.Key(strKey), txns[strKey].ID)
			ok, err := storage.MVCCGetProto(ctx, tc.engine, txnKey, hlc.Timestamp{}, txn,
				storage.MVCCGetOptions{})
			if err != nil {
				return err
			}
			if expGC := (sp.newStatus == -1); expGC {
				if expGC != !ok {
					return fmt.Errorf("%s: expected gc: %t, but found %s\n%s", strKey, expGC, txn, roachpb.Key(strKey))
				}
				// If the transaction record was GCed and didn't begin the test
				// as finalized, verify that the timestamp cache was
				// updated (by the corresponding PushTxn that marked the record
				// as ABORTED) to prevent it from being created again in the
				// future.
				if !sp.status.IsFinalized() {
					tombstoneTimestamp, _ := tc.store.tsCache.GetMax(
						txnTombstoneTSCacheKey, nil /* end */)
					if min := (hlc.Timestamp{WallTime: sp.orig.UnixNano()}); tombstoneTimestamp.Less(min) {
						return fmt.Errorf("%s: expected tscache entry for tombstone key to be >= %s, "+
							"but found %s", strKey, min, tombstoneTimestamp)
					}
				}
			} else if sp.newStatus != txn.Status {
				return fmt.Errorf("%s: expected status %s, but found %s", strKey, sp.newStatus, txn.Status)
			}
			var expIntents []roachpb.Span
			if sp.expResolve {
				expIntents = testIntents
			}
			var spans []roachpb.Span
			val, ok := resolved.Load(strKey)
			if ok {
				spans = val.([]roachpb.Span)
			}
			if !reflect.DeepEqual(spans, expIntents) {
				return fmt.Errorf("%s: unexpected intent resolutions:\nexpected: %s\nobserved: %s", strKey, expIntents, spans)
			}
			entry := &roachpb.AbortSpanEntry{}
			abortExists, err := tc.repl.abortSpan.Get(ctx, tc.store.TODOEngine(), txns[strKey].ID, entry)
			if err != nil {
				t.Fatal(err)
			}
			if abortExists == sp.expAbortGC {
				return fmt.Errorf("%s: expected AbortSpan gc: %t, found %+v", strKey, sp.expAbortGC, entry)
			}
		}
		return nil
	})

	outsideTxnPrefix := keys.TransactionKey(outsideKey, uuid.UUID{})
	outsideTxnPrefixEnd := keys.TransactionKey(outsideKey.Next(), uuid.UUID{})
	var count int
	if _, err := storage.MVCCIterate(ctx, tc.store.TODOEngine(), outsideTxnPrefix, outsideTxnPrefixEnd, hlc.Timestamp{},
		storage.MVCCScanOptions{}, func(roachpb.KeyValue) error {
			count++
			return nil
		}); err != nil {
		t.Fatal(err)
	}
	if exp := len(testCases); exp != count {
		t.Fatalf("expected the %d external transaction entries to remain untouched, "+
			"but only %d are left", exp, count)
	}

	batch := tc.engine.NewSnapshot()
	defer batch.Close()
	tc.repl.raftMu.Lock()
	tc.repl.mu.RLock()
	tc.repl.assertStateRaftMuLockedReplicaMuRLocked(ctx, batch) // check that in-mem and on-disk state were updated
	tc.repl.mu.RUnlock()
	tc.repl.raftMu.Unlock()
}

// TestMVCCGCQueueIntentResolution verifies intent resolution with many intents
// spanning just two transactions.
func TestMVCCGCQueueIntentResolution(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

	tc.manualClock.MustAdvanceTo(timeutil.Unix(48*60*60, 0)) // 2d past the epoch
	now := tc.Clock().Now().WallTime

	txns := []*roachpb.Transaction{
		newTransaction("txn1", roachpb.Key("0-0"), 1, tc.Clock()),
		newTransaction("txn2", roachpb.Key("1-0"), 1, tc.Clock()),
	}
	intentAgeThreshold := gc.IntentAgeThreshold.Get(&tc.repl.store.ClusterSettings().SV)
	intentResolveTS := makeTS(now-intentAgeThreshold.Nanoseconds(), 0)
	txns[0].ReadTimestamp = intentResolveTS
	txns[0].WriteTimestamp = intentResolveTS
	// The MinTimestamp is used by pushers that don't find a transaction record to
	// infer when the coordinator was alive.
	txns[0].MinTimestamp = intentResolveTS
	txns[1].ReadTimestamp = intentResolveTS
	txns[1].WriteTimestamp = intentResolveTS
	txns[1].MinTimestamp = intentResolveTS

	// Two transactions.
	for i := 0; i < 2; i++ {
		// 5 puts per transaction.
		for j := 0; j < 5; j++ {
			pArgs := putArgs(roachpb.Key(fmt.Sprintf("%d-%d", i, j)), []byte("value"))
			assignSeqNumsForReqs(txns[i], &pArgs)
			if _, err := tc.SendWrappedWith(kvpb.Header{
				Txn: txns[i],
			}, &pArgs); err != nil {
				t.Fatalf("%d: could not put data: %+v", i, err)
			}
		}
	}

	// Process through GC queue.
	mgcq := newMVCCGCQueue(tc.store)
	processed, err := mgcq.process(ctx, tc.repl)
	if err != nil {
		t.Fatal(err)
	}
	assert.True(t, processed, "queue not processed")

	// MVCCIterate through all values to ensure intents have been fully resolved.
	// This must be done in a SucceedsSoon loop because intent resolution
	// is initiated asynchronously from the GC queue.
	testutils.SucceedsSoon(t, func() error {
		meta := &enginepb.MVCCMetadata{}
		// The range is specified using only global keys, since the implementation
		// may use an intentInterleavingIter.
		return tc.store.TODOEngine().MVCCIterate(
			keys.LocalMax, roachpb.KeyMax, storage.MVCCKeyAndIntentsIterKind, storage.IterKeyTypePointsOnly,
			func(kv storage.MVCCKeyValue, _ storage.MVCCRangeKeyStack) error {
				if !kv.Key.IsValue() {
					if err := protoutil.Unmarshal(kv.Value, meta); err != nil {
						return err
					}
					if meta.Txn != nil {
						return errors.Errorf("non-nil Txn after GC for key %s", kv.Key)
					}
				}
				return nil
			})
	})
}

func TestMVCCGCQueueLastProcessedTimestamps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

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
		if err := storage.MVCCPutProto(ctx, tc.engine, lpv.key, hlc.Timestamp{}, &ts, storage.MVCCWriteOptions{}); err != nil {
			t.Fatal(err)
		}
	}

	// Process through a scan queue.
	mgcq := newMVCCGCQueue(tc.store)
	processed, err := mgcq.process(ctx, tc.repl)
	if err != nil {
		t.Fatal(err)
	}
	assert.True(t, processed, "queue not processed")

	// Verify GC.
	testutils.SucceedsSoon(t, func() error {
		for _, lpv := range lastProcessedVals {
			ok, err := storage.MVCCGetProto(ctx, tc.engine, lpv.key, hlc.Timestamp{}, &ts,
				storage.MVCCGetOptions{})
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

// TestMVCCGCQueueChunkRequests verifies that many intents are chunked into
// separate batches. This is verified both for many different keys and also for
// many different versions of keys.
func TestMVCCGCQueueChunkRequests(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	var gcRequests int32
	manual := timeutil.NewManualTime(timeutil.Unix(0, 123))
	tsc := TestStoreConfig(hlc.NewClockForTesting(manual))
	tsc.TestingKnobs.EvalKnobs.TestingEvalFilter =
		func(filterArgs kvserverbase.FilterArgs) *kvpb.Error {
			if _, ok := filterArgs.Req.(*kvpb.GCRequest); ok {
				atomic.AddInt32(&gcRequests, 1)
				return nil
			}
			return nil
		}
	tc := testContext{manualClock: manual}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.StartWithStoreConfig(ctx, t, stopper, tsc)

	const keyCount = 100
	if gc.KeyVersionChunkBytes%keyCount != 0 {
		t.Fatalf("expected gcKeyVersionChunkBytes to be a multiple of %d", keyCount)
	}
	// Reduce the key size by MVCCVersionTimestampSize (13 bytes) to prevent batch overflow.
	// This is due to MVCCKey.EncodedSize(), which returns the full size of the encoded key.
	const keySize = (gc.KeyVersionChunkBytes / keyCount) - 13
	// Create a format string for creating version keys of exactly
	// length keySize.
	fmtStr := fmt.Sprintf("%%0%dd", keySize)

	// First write 2 * gcKeyVersionChunkBytes different keys (each with two versions).
	ba1, ba2 := &kvpb.BatchRequest{}, &kvpb.BatchRequest{}
	for i := 0; i < 2*keyCount; i++ {
		// Create keys which are
		key := roachpb.Key(fmt.Sprintf(fmtStr, i))
		pArgs := putArgs(key, []byte("value1"))
		ba1.Add(&pArgs)
		pArgs = putArgs(key, []byte("value2"))
		ba2.Add(&pArgs)
	}
	ba1.Header = kvpb.Header{Timestamp: tc.Clock().Now()}
	if _, pErr := tc.Sender().Send(ctx, ba1); pErr != nil {
		t.Fatal(pErr)
	}
	ba2.Header = kvpb.Header{Timestamp: tc.Clock().Now()}
	if _, pErr := tc.Sender().Send(ctx, ba2); pErr != nil {
		t.Fatal(pErr)
	}

	// Next write 2 keys, the first with keyCount different GC'able
	// versions, and the second with 2*keyCount GC'able versions.
	key1 := roachpb.Key(fmt.Sprintf(fmtStr, 2*keyCount))
	key2 := roachpb.Key(fmt.Sprintf(fmtStr, 2*keyCount+1))
	for i := 0; i < 2*keyCount+1; i++ {
		ba := &kvpb.BatchRequest{}
		// Only write keyCount+1 versions of key1.
		if i < keyCount+1 {
			pArgs1 := putArgs(key1, []byte(fmt.Sprintf("value%04d", i)))
			ba.Add(&pArgs1)
		}
		// Write all 2*keyCount+1 versions of key2 to verify we'll
		// tackle key2 in two separate batches.
		pArgs2 := putArgs(key2, []byte(fmt.Sprintf("value%04d", i)))
		ba.Add(&pArgs2)
		ba.Header = kvpb.Header{Timestamp: tc.Clock().Now()}
		if _, pErr := tc.Sender().Send(ctx, ba); pErr != nil {
			t.Fatal(pErr)
		}
	}

	// Forward the clock past the default GC time.
	conf, err := tc.store.GetSpanConfigForKey(ctx, roachpb.RKey("key"))
	if err != nil {
		t.Fatalf("could not find span config for range %s", err)
	}
	tc.manualClock.Advance(conf.TTL() + 1)
	mgcq := newMVCCGCQueue(tc.store)
	processed, err := mgcq.process(ctx, tc.repl)
	if err != nil {
		t.Fatal(err)
	}
	assert.True(t, processed, "queue not processed")
	// We wrote two batches worth of keys spread out, and two keys that
	// each have enough old versions to fill a whole batch each in the
	// first case, and two whole batches in the second, adding up to
	// five batches. There is also a first batch which sets the GC
	// thresholds, making six total batches.
	if a, e := atomic.LoadInt32(&gcRequests), int32(6); a != e {
		t.Errorf("expected %d gc requests; got %d", e, a)
	}
}

func TestMVCCGCQueueGroupsRangeDeletions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	key := func(k string) roachpb.RKey {
		return testutils.MakeKey(keys.ScratchRangeMin, []byte(k))
	}

	var leaseError = true

	// Create store and prepare by removing default range.
	clock := timeutil.NewManualTime(timeutil.Unix(0, 123))
	cfg := TestStoreConfig(hlc.NewClockForTesting(clock))
	cfg.TestingKnobs.MVCCGCQueueLeaseCheckInterceptor = func(ctx context.Context, replica *Replica, now hlc.ClockTimestamp,
	) bool {
		return leaseError
	}
	store := createTestStoreWithConfig(ctx, t, stopper, testStoreOpts{createSystemRanges: false}, &cfg)
	r, err := store.GetReplica(roachpb.RangeID(1))
	require.NoError(t, err)
	require.NoError(t, store.RemoveReplica(ctx, r, r.Desc().NextReplicaID, RemoveOptions{DestroyData: true}))
	// Add replica without hint.
	r1 := createReplica(store, roachpb.RangeID(100), key("a"), key("b"))
	require.NoError(t, store.AddReplica(r1))
	// Add replica with hint and GC ttl.
	r2 := createReplica(store, roachpb.RangeID(101), key("b"), key("c"))
	require.NoError(t, store.AddReplica(r2))
	r2.RaftStatus()
	r2.handleGCHintResult(ctx, &roachpb.GCHint{LatestRangeDeleteTimestamp: hlc.Timestamp{WallTime: 1}})
	// FIXME
	//	r2.SetSpanConfig(roachpb.SpanConfig{GCPolicy: roachpb.GCPolicy{TTLSeconds: 100}})

	gcQueue := newMVCCGCQueue(store)

	// Check that low priority replicas doesn't cause hint scan.
	gcQueue.postProcessScheduled(ctx, r1, 1)
	qr, _ := gcQueue.pop()
	require.Nil(t, qr, "unexpected enqueued replica")

	// Check that high priority replica is not added if GC hint time didn't exceed
	// ttl.
	gcQueue.postProcessScheduled(ctx, r1, deleteRangePriority)
	qr, _ = gcQueue.pop()
	require.Nil(t, qr, "unexpected enqueued replica")

	// Check that replica is not queued if sequence of hi-pri replicas are being
	// processed.
	clock.Advance(200 * time.Second)
	gcQueue.postProcessScheduled(ctx, r1, deleteRangePriority)
	qr, _ = gcQueue.pop()
	require.Nil(t, qr, "unexpected enqueued replica")

	// Reset sequence by running a lo pri replica.
	gcQueue.postProcessScheduled(ctx, r1, 1)
	qr, _ = gcQueue.pop()
	require.Nil(t, qr, "unexpected enqueued replica")

	// Check that replica is processed when hint criteria is met.
	gcQueue.postProcessScheduled(ctx, r1, deleteRangePriority)
	qr, prio := gcQueue.pop()
	require.NotNil(t, qr, "unexpected nil replica")
	require.Equal(t, deleteRangePriority, prio, "expected high priority")

	// Check that non leaseholder replicas are ignored.
	leaseError = false
	gcQueue.postProcessScheduled(ctx, r1, deleteRangePriority)
	qr, _ = gcQueue.pop()
	require.Nil(t, qr, "unexpected nil replica")
}
