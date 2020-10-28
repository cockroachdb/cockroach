// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package gc

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCalculateThreshold(t *testing.T) {
	for _, c := range []struct {
		ttlSeconds int32
		ts         hlc.Timestamp
	}{
		{
			ts:         hlc.Timestamp{WallTime: time.Hour.Nanoseconds(), Logical: 0},
			ttlSeconds: 1,
		},
	} {
		policy := zonepb.GCPolicy{TTLSeconds: c.ttlSeconds}
		require.Equal(t, c.ts, TimestampForThreshold(CalculateThreshold(c.ts, policy), policy))
	}
}

type collectingGCer struct {
	keys [][]roachpb.GCRequest_GCKey
}

func (c *collectingGCer) SetGCThreshold(context.Context, Threshold) error {
	return nil
}

func (c *collectingGCer) GC(_ context.Context, keys []roachpb.GCRequest_GCKey) error {
	c.keys = append(c.keys, keys)
	return nil
}

func TestBatchingInlineGCer(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	c := &collectingGCer{}
	m := makeBatchingInlineGCer(c, func(err error) { t.Error(err) })
	if m.max == 0 {
		t.Fatal("did not init max")
	}
	m.max = 10 // something reasonable for this unit test

	long := roachpb.GCRequest_GCKey{
		Key: bytes.Repeat([]byte("x"), m.max-1),
	}
	short := roachpb.GCRequest_GCKey{
		Key: roachpb.Key("q"),
	}

	m.FlushingAdd(ctx, long.Key)
	require.Nil(t, c.keys) // no flush

	m.FlushingAdd(ctx, short.Key)
	// Flushed long and short.
	require.Len(t, c.keys, 1)
	require.Len(t, c.keys[0], 2)
	require.Equal(t, long, c.keys[0][0])
	require.Equal(t, short, c.keys[0][1])
	// Reset itself properly.
	require.Nil(t, m.gcKeys)
	require.Zero(t, m.size)

	m.FlushingAdd(ctx, short.Key)
	require.Len(t, c.keys, 1) // no flush

	m.Flush(ctx)
	require.Len(t, c.keys, 2) // flushed
	require.Len(t, c.keys[1], 1)
	require.Equal(t, short, c.keys[1][0])
	// Reset itself properly.
	require.Nil(t, m.gcKeys)
	require.Zero(t, m.size)
}

// TestUseClearRange tests that the GC logic properly issues request keys
// with UseClearRange set when Run is called with useClearRange and the
// appropriate situations occur.
func TestUseClearRange(t *testing.T) {
	secondsToNanos := func(seconds int) (nanos int64) {
		return (time.Second * time.Duration(seconds)).Nanoseconds()
	}
	mkTs := func(seconds int) hlc.Timestamp {
		return hlc.Timestamp{WallTime: secondsToNanos(seconds)}
	}
	type versionsSpec struct {
		start, end hlc.Timestamp
		step       time.Duration
	}
	type keyVersions struct {
		key              roachpb.Key
		versions         versionsSpec
		expectClearRange bool
		expectBatches    int
	}
	type testCase struct {
		name          string
		useClearRange bool
		now           hlc.Timestamp
		ttl           int
		keys          []keyVersions
	}
	oneHundredVersions := versionsSpec{start: mkTs(1), end: mkTs(100), step: time.Second}
	tenThousandVersions := versionsSpec{start: mkTs(1), end: mkTs(10000), step: time.Second}
	value := []byte("foo")
	run := func(t *testing.T, tc testCase) {
		eng := storage.NewDefaultInMem()
		defer eng.Close()
		for _, k := range tc.keys {
			ts := k.versions.start
			for ts.LessEq(k.versions.end) {
				require.NoError(t, eng.Put(storage.MVCCKey{
					Key:       k.key,
					Timestamp: ts,
				}, value))
				ts = ts.Add(k.versions.step.Nanoseconds(), 0)
			}
		}
		desc := roachpb.RangeDescriptor{
			RangeID:       1,
			StartKey:      roachpb.RKey(keys.MinKey),
			EndKey:        roachpb.RKey(keys.MaxKey),
			NextReplicaID: 1,
		}
		snap := eng.NewSnapshot()
		defer snap.Close()
		var gcer collectingGCer
		newThreshold := tc.now.Add(secondsToNanos(tc.ttl), 0)
		_, err := Run(context.Background(), &desc, snap, tc.now, newThreshold,
			zonepb.GCPolicy{TTLSeconds: int32(tc.ttl)}, &gcer,
			noopCleanupIntentsFunc, noopCleanupIntentsAsyncFunc,
			tc.useClearRange)
		require.NoError(t, err)
		keyBatchesSeen := make(map[string]int)
		keyClearRangesSeen := make(map[string]int)
		for _, b := range gcer.keys {
			for _, k := range b {
				keyStr := string(k.Key)
				keyBatchesSeen[keyStr]++
				if k.UseClearRange {
					keyClearRangesSeen[keyStr]++
				}
			}
		}
		for _, k := range tc.keys {
			keyStr := string(k.key)
			assert.Equal(t, k.expectBatches, keyBatchesSeen[keyStr], keyStr)
			var expectedClearRanges int
			if k.expectClearRange {
				expectedClearRanges = 1
			}
			assert.Equal(t, expectedClearRanges, keyClearRangesSeen[keyStr], keyStr)
		}
	}
	tests := []testCase{
		{
			name:          "basic clear range enabled, one clear range batch",
			useClearRange: true,
			now:           mkTs(1000000),
			ttl:           1,
			keys: []keyVersions{
				{
					key:           roachpb.Key("a"),
					versions:      oneHundredVersions,
					expectBatches: 1,
				},
				{
					key:           roachpb.Key(strings.Repeat("a", 256)),
					versions:      oneHundredVersions,
					expectBatches: 1,
				},
				{
					key:              roachpb.Key(strings.Repeat("b", 256)),
					versions:         tenThousandVersions,
					expectClearRange: true,
					expectBatches:    1,
				},
			},
		},
		{
			name:          "no clear range because the keys are too large",
			useClearRange: true,
			now:           mkTs(1000000),
			ttl:           1,
			keys: []keyVersions{
				{
					key:           roachpb.Key(strings.Repeat("a", 32<<10)),
					versions:      oneHundredVersions,
					expectBatches: (((32 << 10) * 100) / (256 << 10)) + 1, // 12
				},
			},
		},
		{
			name:          "basic clear range disabled",
			useClearRange: false,
			now:           mkTs(1000000),
			ttl:           1,
			keys: []keyVersions{
				{
					key:           roachpb.Key("a"),
					versions:      oneHundredVersions,
					expectBatches: 1,
				},
				{
					key:           roachpb.Key(strings.Repeat("a", 256)),
					versions:      oneHundredVersions,
					expectBatches: 1,
				},
				{
					key:           roachpb.Key(strings.Repeat("b", 256)),
					versions:      tenThousandVersions,
					expectBatches: 11,
				},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			run(t, tc)
		})
	}
}
