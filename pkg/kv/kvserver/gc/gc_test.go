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
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
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

// TestIntentAgeThresholdSetting test verifies that resolution threshold could be adjusted
// it uses short and long threshold to verify that intents inserted between two thresholds
// are not considered for resolution when threshold is high (1st attempt) and considered
// when threshold is low (2nd attempt)
func TestIntentAgeThresholdSetting(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rng := rand.New(rand.NewSource(1))

	ctx := context.Background()
	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()

	tablePrefix := keys.SystemSQLCodec.TablePrefix(42)
	desc := roachpb.RangeDescriptor{
		StartKey: roachpb.RKey(tablePrefix),
		EndKey:   roachpb.RKey(tablePrefix.PrefixEnd()),
	}

	now := 3 * time.Hour
	intentShortThreshold := 5 * time.Minute
	intentLongThreshold := 2 * time.Hour
	nowTs := hlc.Timestamp{
		WallTime: now.Nanoseconds(),
	}

	intentTs := now - (intentShortThreshold+intentLongThreshold)/2
	distSpec := uniformDistSpec{
		tsFrom:          int64(intentTs.Seconds()),
		tsTo:            int64(intentTs.Seconds()) + 1000,
		keysPerValueMin: 1,
		keysPerValueMax: 1,
		keySuffixMin:    1,
		keySuffixMax:    10,
		intentFrac:      1,
	}
	dist := distSpec.dist(10, rng)

	dist.setupTest(t, eng, desc)

	policy := zonepb.GCPolicy{TTLSeconds: 1}
	snap := eng.NewSnapshot()
	fakeGCer := makeFakeGCer()

	info, err := Run(ctx, &desc, snap, nowTs, nowTs, intentLongThreshold, policy, &fakeGCer, fakeGCer.resolveIntents,
		fakeGCer.resolveIntentsAsync)
	if err != nil {
		t.Errorf("GC failed with %v", err)
	}
	if info.IntentsConsidered > 0 {
		t.Errorf("Expected no intents considered with default threshold, got %d", info.IntentsConsidered)
	}

	info, err = Run(ctx, &desc, snap, nowTs, nowTs, intentShortThreshold, policy, &fakeGCer, fakeGCer.resolveIntents,
		fakeGCer.resolveIntentsAsync)
	if err != nil {
		t.Errorf("GC failed with %v", err)
	}
	if info.IntentsConsidered != 10 {
		t.Errorf("Expected all 10 intents considered for resolution with default threshold, got %d",
			info.IntentsConsidered)
	}
}
