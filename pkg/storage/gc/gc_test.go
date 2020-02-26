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
