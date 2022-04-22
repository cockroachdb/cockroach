// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvmvccgcqueue

import (
	"context"
	"testing"
	"testing/quick"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestMVCCGCQueueScoreString(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	for i, c := range []struct {
		r   MvccGCQueueScore
		exp string
	}{
		{MvccGCQueueScore{}, "(empty)"},
		{MvccGCQueueScore{
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
		{MvccGCQueueScore{ShouldQueue: true}, `queue=true with 0.00/fuzz(0.00)=NaN=valScaleScore(0.00)*deadFrac(0.00)+intentScore(0.00)
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
		r := MakeMVCCGCQueueScoreImpl(
			ctx, int64(seed), now, ms, time.Duration(ttlSec)*time.Second, hlc.Timestamp{},
			true /* canAdvanceGCThreshold */)
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
		r := MakeMVCCGCQueueScoreImpl(context.Background(), 0, hlc.Timestamp{}, enginepb.MVCCStats{
			ContainsEstimates: containsEstimates,
			LiveBytes:         int64(liveBytes),
			ValBytes:          int64(valBytes),
			KeyBytes:          int64(keyBytes),
		}, 60*time.Second, hlc.Timestamp{}, true /* canAdvanceGCThreshold */)
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

	expiration := kvserverbase.TxnCleanupThreshold.Nanoseconds() + 1

	// GC triggered if abort span should all be gc'able and it's large.
	{
		r := MakeMVCCGCQueueScoreImpl(
			context.Background(), seed,
			hlc.Timestamp{WallTime: expiration + 1},
			ms, 10000*time.Second,
			hlc.Timestamp{}, true, /* canAdvanceGCThreshold */
		)
		require.True(t, r.ShouldQueue)
		require.NotZero(t, r.FinalScore)
	}

	// GC triggered if abort span bytes count is 0, but the sys bytes
	// and sys count exceed a certain threshold (likely large abort span).
	ms.AbortSpanBytes = 0
	ms.SysCount = probablyLargeAbortSpanSysCountThreshold
	{
		r := MakeMVCCGCQueueScoreImpl(
			context.Background(), seed,
			hlc.Timestamp{WallTime: expiration + 1},
			ms, 10000*time.Second,
			hlc.Timestamp{}, true, /* canAdvanceGCThreshold */
		)
		require.True(t, r.ShouldQueue)
		require.NotZero(t, r.FinalScore)
	}

	// Heuristic doesn't fire if last GC within TxnCleanupThreshold.
	{
		r := MakeMVCCGCQueueScoreImpl(context.Background(), seed,
			hlc.Timestamp{WallTime: expiration},
			ms, 10000*time.Second,
			hlc.Timestamp{WallTime: expiration - 100}, true, /* canAdvanceGCThreshold */
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

			r := MakeMVCCGCQueueScoreImpl(
				ctx, seed, now, ms, gcTTL, tc.lastGC, true /* canAdvanceGCThreshold */)
			require.Equal(t, tc.expectGC, r.ShouldQueue)
		})
	}
}
