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
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
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

// TestIntentAgeThresholdSetting verifies that the GC intent resolution threshold can be
// adjusted. It uses short and long threshold to verify that intents inserted between two
// thresholds are not considered for resolution when threshold is high (1st attempt) and
// considered when threshold is low (2nd attempt).
func TestIntentAgeThresholdSetting(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()

	// Test event timeline.
	now := 3 * time.Hour
	intentShortThreshold := 5 * time.Minute
	intentLongThreshold := 2 * time.Hour
	intentTs := now - (intentShortThreshold+intentLongThreshold)/2

	// Prepare test intents in MVCC.
	key := []byte("a")
	value := roachpb.Value{RawBytes: []byte("0123456789")}
	intentHlc := hlc.Timestamp{
		WallTime: intentTs.Nanoseconds(),
	}
	txn := roachpb.MakeTransaction("txn", key, roachpb.NormalUserPriority, intentHlc, 1000)
	require.NoError(t, storage.MVCCPut(ctx, eng, nil, key, intentHlc, value, &txn))
	require.NoError(t, eng.Flush())

	// Prepare test fixtures for GC run.
	desc := roachpb.RangeDescriptor{
		StartKey: roachpb.RKey(key),
		EndKey:   roachpb.RKey("b"),
	}
	policy := zonepb.GCPolicy{TTLSeconds: 1}
	snap := eng.NewSnapshot()
	nowTs := hlc.Timestamp{
		WallTime: now.Nanoseconds(),
	}
	fakeGCer := makeFakeGCer()

	// Test GC desired behavior.
	info, err := Run(ctx, &desc, snap, nowTs, nowTs, RunOptions{IntentAgeThreshold: intentAgeThreshold}, policy, &fakeGCer, fakeGCer.resolveIntents,
		fakeGCer.resolveIntentsAsync)
	require.NoError(t, err, "GC Run shouldn't fail")
	assert.Zero(t, info.IntentsConsidered,
		"Expected no intents considered by GC with default threshold")

	info, err = Run(ctx, &desc, snap, nowTs, nowTs, RunOptions{IntentAgeThreshold: intentShortThreshold}, policy, &fakeGCer, fakeGCer.resolveIntents,
		fakeGCer.resolveIntentsAsync)
	require.NoError(t, err, "GC Run shouldn't fail")
	assert.Equal(t, 1, info.IntentsConsidered,
		"Expected 1 intents considered by GC with short threshold")
}

func TestIntentCleanupBatching(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()

	intentThreshold := 2 * time.Hour
	now := 3 * intentThreshold
	intentTs := now - intentThreshold*2

	// Prepare test intents in MVCC.
	txnPrefixes := []byte{'a', 'b', 'c'}
	objectKeys := []byte{'a', 'b', 'c', 'd', 'e'}
	value := roachpb.Value{RawBytes: []byte("0123456789")}
	intentHlc := hlc.Timestamp{
		WallTime: intentTs.Nanoseconds(),
	}
	for _, prefix := range txnPrefixes {
		key := []byte{prefix, objectKeys[0]}
		txn := roachpb.MakeTransaction("txn", key, roachpb.NormalUserPriority, intentHlc, 1000)
		for _, suffix := range objectKeys {
			key := []byte{prefix, suffix}
			require.NoError(t, storage.MVCCPut(ctx, eng, nil, key, intentHlc, value, &txn))
		}
		require.NoError(t, eng.Flush())
	}
	desc := roachpb.RangeDescriptor{
		StartKey: roachpb.RKey([]byte{txnPrefixes[0], objectKeys[0]}),
		EndKey:   roachpb.RKey("z"),
	}
	policy := zonepb.GCPolicy{TTLSeconds: 1}
	snap := eng.NewSnapshot()
	nowTs := hlc.Timestamp{
		WallTime: now.Nanoseconds(),
	}

	// Base GCr will cleanup all intents in one go and its result is used as a baseline
	// to compare batched runs for checking completeness.
	baseGCer := makeFakeGCer()
	_, err := Run(ctx, &desc, snap, nowTs, nowTs, RunOptions{IntentAgeThreshold: intentAgeThreshold}, policy, &baseGCer, baseGCer.resolveIntents,
		baseGCer.resolveIntentsAsync)
	if err != nil {
		t.Fatal("Can't prepare test fixture. Non batched GC run fails.")
	}
	baseGCer.normalize()

	for _, spec := range []struct {
		name      string
		batchSize int64
	}{
		{"transaction split", 7},
		{"exactly fits in batch", 15},
		{"exactly on txn boundary", 10},
		{"exactly on txn boundary", 5},
	} {
		t.Run(spec.name, func(t *testing.T) {
			fakeGCer := makeFakeGCer()
			info, err := Run(ctx, &desc, snap, nowTs, nowTs, RunOptions{IntentAgeThreshold: intentAgeThreshold, MaxIntentCleanupBatch: spec.batchSize}, policy, &fakeGCer, fakeGCer.resolveIntents,
				fakeGCer.resolveIntentsAsync)
			require.NoError(t, err, "GC Run shouldn't fail")
			for _, size := range fakeGCer.batchSizes {
				require.LessOrEqual(t, int64(size), spec.batchSize, "Batch size")
			}
			require.Equal(t, 15, info.ResolveTotal)
			fakeGCer.normalize()
			require.EqualValues(t, baseGCer, fakeGCer, "GC result with batching")
		})
	}
}
