// Copyright 2024 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package logical

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func skv(k string) streampb.StreamEvent_KV {
	return streampb.StreamEvent_KV{KeyValue: roachpb.KeyValue{Key: roachpb.Key(k)}}
}

func ts(i int64) []jobspb.ResolvedSpan {
	return []jobspb.ResolvedSpan{{Timestamp: hlc.Timestamp{WallTime: i}}}
}

func TestPurgatory(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	p := &purgatory{
		levelLimit: 5,
	}

	var flushed int
	noopFlush := func(
		_ context.Context, ev []streampb.StreamEvent_KV, _ bool,
	) ([]streampb.StreamEvent_KV, error) {
		for i := range ev {
			if i%2 == 0 {
				flushed++
				t.Logf("flush %s", ev[i].KeyValue.Key)
				ev[i] = streampb.StreamEvent_KV{}
			}
		}
		return filterRemaining(ev), nil
	}

	var resolved int
	checkpoint := func(_ context.Context, sp []jobspb.ResolvedSpan) error {
		resolved = int(sp[0].Timestamp.WallTime)
		return nil
	}

	// Adding events makes it non-empty.
	require.True(t, p.Empty())
	require.NoError(t, p.Store(ctx, []streampb.StreamEvent_KV{skv("a"), skv("b")}, nil, nil))
	require.NoError(t, p.Store(ctx, []streampb.StreamEvent_KV{skv("c"), skv("d")}, nil, nil))
	p.Checkpoint(ctx, ts(1))

	require.NoError(t, p.Store(ctx, []streampb.StreamEvent_KV{skv("e"), skv("f"), skv("g"), skv("h")}, nil, nil))
	p.Checkpoint(ctx, ts(2))

	require.NoError(t, p.Store(ctx, []streampb.StreamEvent_KV{skv("x")}, nil, nil))
	require.False(t, p.Empty())

	// Nothing has drained yet, so no movement in flushed or resolved.
	require.Equal(t, 0, flushed)
	require.Equal(t, 0, resolved)
	require.Equal(t, 4, len(p.levels))

	// Draining drains half the events, but no checkpoints yet.
	require.NoError(t, p.Drain(ctx, noopFlush, checkpoint))
	require.Equal(t, 5, flushed)
	require.Equal(t, 0, resolved)

	// Draining drains half the events, now checkpointing after lvl 2.
	require.NoError(t, p.Drain(ctx, noopFlush, checkpoint))
	require.Equal(t, 8, flushed)
	require.Equal(t, 1, resolved)
	require.False(t, p.Empty())

	require.NoError(t, p.Drain(ctx, noopFlush, checkpoint))
	require.Equal(t, 9, flushed)
	require.Equal(t, 2, resolved)
	require.True(t, p.Empty())
}
