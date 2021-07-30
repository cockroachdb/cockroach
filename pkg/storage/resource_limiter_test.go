// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestResourceLimiter(t *testing.T) {
	defer leaktest.AfterTest(t)()

	clock := timeutil.NewManualTime(timeutil.Now())
	l := NewResourceLimiter(ResourceLimiterOptions{MaxRunTime: time.Minute}, clock)

	require.Equal(t, ResourceLimitNotReached, l.IsExhausted(), "Exhausted when time didn't move")
	clock.Advance(time.Second * 59)
	require.Equal(t, ResourceLimitReachedSoft, l.IsExhausted(), "Soft limit not reached")
	clock.Advance(time.Minute)
	require.Equal(t, ResourceLimitReachedHard, l.IsExhausted(), "Hard limit not reached")
}

var iterations = 0
var sumTime int64 = 0

func BenchmarkIteratorSlowdown(b *testing.B) {
	engine := createTestPebbleEngine()
	defer engine.Close()

	startTime := hlc.Timestamp{WallTime: 1000000}
	endTime := hlc.Timestamp{WallTime: 1000000 + 10000}
	rng := rand.New(rand.NewSource(timeutil.Now().Unix()))
	for i := 0; i < 100000; i++ {
		key := []byte(fmt.Sprintf("key-%d", rand.Int63n(int64(10000))))
		timestamp := hlc.Timestamp{WallTime: 1000000 + rand.Int63n(int64(1000))}
		kv := makeKVT(key, randutil.RandBytes(rng, 256), timestamp)
		if err := engine.PutMVCC(kv.Key, kv.Value); err != nil {
			b.Logf("Can't write value %v", err)
		}
	}
	require.NoError(b, engine.Flush())

	b.ResetTimer()
	for benchIter := 0; benchIter < b.N; benchIter++ {
		it := NewMVCCIncrementalIterator(engine, MVCCIncrementalIterOptions{
			EnableTimeBoundIteratorOptimization: true,
			EndKey:                              []byte("key-999999999999999999"),
			StartTime:                           startTime,
			EndTime:                             endTime,
			IntentPolicy:                        0,
			InlinePolicy:                        0,
		})

		startClock := timeutil.Now()
		_ = startClock
		for it.SeekGE(MVCCKey{Key: []byte("key-")}); ; {
			ok, _ := it.Valid()
			if !ok {
				break
			}
			it.Next()
			sumTime += timeutil.Since(startClock).Nanoseconds()
			iterations++
		}
	}
	b.Logf("Total iterations %d", iterations)
	b.Logf("Sum time %d", sumTime)
}
