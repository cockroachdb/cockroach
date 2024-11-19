// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeedbuffer_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed/rangefeedbuffer"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestBuffer ensures that buffer addition, flushing and limit-setting semantics
// work as expected.
func TestBuffer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ts := func(nanos int) hlc.Timestamp {
		return hlc.Timestamp{WallTime: int64(nanos)}
	}

	ctx := context.Background()
	const limit = 25
	buffer := rangefeedbuffer.New[*testEvent](limit)

	{ // Sanity check the newly initialized rangefeed buffer.
		events := buffer.Flush(ctx, ts(0))
		require.True(t, len(events) == 0)
	}

	{ // Ensure that checkpoint events are discarded.
		events := buffer.Flush(ctx, ts(5))
		require.True(t, len(events) == 0)
	}

	{ // Bumping the frontier timestamp without existing events should behave expectedly.
		events := buffer.Flush(ctx, ts(10))
		require.True(t, len(events) == 0)
	}

	{ // Flushing at a timestamp lower than buffered events should return nothing.
		require.NoError(t, buffer.Add(makeEvent("a", ts(13)))) // a@13
		require.NoError(t, buffer.Add(makeEvent("b", ts(11)))) // b@11
		require.NoError(t, buffer.Add(makeEvent("c", ts(15)))) // c@15
		require.NoError(t, buffer.Add(makeEvent("d", ts(12)))) // d@12
		require.NoError(t, buffer.Add(makeEvent("e", ts(18)))) // e@18

		events := buffer.Flush(ctx, ts(10))
		require.True(t, len(events) == 0)
	}

	{ // Flushing should capture only entries with timestamps <= the frontier, in sorted order.
		events := buffer.Flush(ctx, ts(14))

		require.True(t, len(events) == 3)
		require.Equal(t, events[0].data, "b") // b@11
		require.Equal(t, events[1].data, "d") // d@12
		require.Equal(t, events[2].data, "a") // a@13
	}

	{ // Incremental advances should only surface the events until the given timestamp.
		events := buffer.Flush(ctx, ts(15))

		require.True(t, len(events) == 1)
		require.Equal(t, events[0].data, "c") // c@15
	}

	{ // Adding events with timestamps <= the last flush are discarded.
		require.NoError(t, buffer.Add(makeEvent("a", ts(13)))) // a@13
		require.NoError(t, buffer.Add(makeEvent("b", ts(11)))) // b@11
		require.NoError(t, buffer.Add(makeEvent("c", ts(15)))) // c@15
		require.NoError(t, buffer.Add(makeEvent("d", ts(12)))) // d@12

		events := buffer.Flush(ctx, ts(15))
		require.True(t, len(events) == 0)
	}

	{ // Additional events are flushed out at appropriate points.
		require.NoError(t, buffer.Add(makeEvent("f", ts(19)))) // f@19
		require.NoError(t, buffer.Add(makeEvent("g", ts(21)))) // g@21

		events := buffer.Flush(ctx, ts(20))
		require.True(t, len(events) == 2)
		require.Equal(t, events[0].data, "e") // e@18
		require.Equal(t, events[1].data, "f") // f@19
	}

	{ // Ensure that a timestamp greater than any previous event flushes everything.
		events := buffer.Flush(ctx, ts(100))
		require.True(t, len(events) == 1)
		require.Equal(t, events[0].data, "g") // g@21
	}

	{ // Sanity check that there are no events left over.
		events := buffer.Flush(ctx, ts(100))
		require.True(t, len(events) == 0)
	}

	{ // Ensure that buffer limits are respected.
		for i := 0; i < limit; i++ {
			require.NoError(t, buffer.Add(makeEvent("x", ts(101)))) // x@101
		}

		err := buffer.Add(makeEvent("x", ts(101)))
		require.ErrorIs(t, err, rangefeedbuffer.ErrBufferLimitExceeded)
	}

	{ // Ensure that limit changes behave as expected.
		buffer.SetLimit(limit + 1)
		require.NoError(t, buffer.Add(makeEvent("x", ts(101)))) // x@101

		buffer.SetLimit(limit - 1)
		err := buffer.Add(makeEvent("y", ts(102))) // y@102
		require.ErrorIs(t, err, rangefeedbuffer.ErrBufferLimitExceeded)

		events := buffer.Flush(ctx, ts(102))
		require.True(t, len(events) == limit+1)
	}
}

type testEvent struct {
	data string
	ts   hlc.Timestamp
}

func (t *testEvent) Timestamp() hlc.Timestamp {
	return t.ts
}

var _ rangefeedbuffer.Event = &testEvent{}

func makeEvent(data string, ts hlc.Timestamp) *testEvent {
	return &testEvent{data: data, ts: ts}
}
