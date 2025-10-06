// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logical

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
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

	var flushed, resolved int
	p := &purgatory{
		byteLimit:   func() int64 { return 5 << 20 },
		delay:       func() time.Duration { return 0 },
		deadline:    func() time.Duration { return 0 },
		bytesGauge:  metric.NewGauge(metric.Metadata{}),
		eventsGauge: metric.NewGauge(metric.Metadata{}),
		flush: func(
			_ context.Context, ev []streampb.StreamEvent_KV, _ bool, _ retryEligibility,
		) ([]streampb.StreamEvent_KV, int64, error) {
			var unappliedBytes int64
			for i := range ev {
				if i%2 == 0 {
					flushed++
					t.Logf("flush %s", ev[i].KeyValue.Key)
					ev[i] = streampb.StreamEvent_KV{}
				} else {
					unappliedBytes += int64(ev[i].Size())
				}
			}
			return filterRemaining(ev), unappliedBytes, nil
		},
		checkpoint: func(_ context.Context, sp []jobspb.ResolvedSpan) error {
			resolved = int(sp[0].Timestamp.WallTime)
			return nil
		},
		debug: &streampb.DebugLogicalConsumerStatus{},
	}

	sz := int64((&streampb.StreamEvent_KV{KeyValue: roachpb.KeyValue{Key: roachpb.Key("a")}}).Size())
	t.Logf("size of one kv: %d", sz)
	// Adding events makes it non-empty.
	require.True(t, p.Empty())
	require.NoError(t, p.Store(ctx, []streampb.StreamEvent_KV{skv("a"), skv("b")}, sz*2))
	require.Equal(t, sz*2, p.bytes)
	require.NoError(t, p.Store(ctx, []streampb.StreamEvent_KV{skv("c"), skv("d")}, sz*2))
	require.Equal(t, sz*4, p.bytes)
	p.Checkpoint(ctx, ts(1))

	require.NoError(t, p.Store(ctx, []streampb.StreamEvent_KV{skv("e"), skv("f"), skv("g"), skv("h")}, sz*4))
	require.Equal(t, int64(8), p.eventsGauge.Value())
	require.Equal(t, sz*8, p.bytes)
	p.Checkpoint(ctx, ts(2))

	require.NoError(t, p.Store(ctx, []streampb.StreamEvent_KV{skv("x")}, sz*1))
	require.False(t, p.Empty())
	require.Equal(t, 4, len(p.levels))
	require.Equal(t, sz*9, p.bytes)
	require.Equal(t, sz*9, p.bytesGauge.Value())

	// Nothing has drained yet, so no movement in flushed or resolved.
	require.Equal(t, 0, flushed)
	require.Equal(t, 0, resolved)

	// Draining drains half the events, but no checkpoints yet.
	require.NoError(t, p.Drain(ctx))
	require.Equal(t, 5, flushed)
	require.Equal(t, int64(4), p.eventsGauge.Value())
	require.Equal(t, 0, resolved)
	require.Equal(t, sz*4, p.bytes)
	require.Equal(t, sz*4, p.bytesGauge.Value())

	// Draining drains half the events, now checkpointing after lvl 2.
	require.NoError(t, p.Drain(ctx))
	require.Equal(t, 8, flushed)
	require.Equal(t, 1, resolved)
	require.Equal(t, sz*1, p.bytes)
	require.False(t, p.Empty())

	require.NoError(t, p.Drain(ctx))
	require.Equal(t, 9, flushed)
	require.Equal(t, 2, resolved)
	require.Equal(t, sz*0, p.bytes)
	require.True(t, p.Empty())
	require.Equal(t, int64(0), p.eventsGauge.Value())
	require.Equal(t, sz*0, p.bytesGauge.Value())
}
