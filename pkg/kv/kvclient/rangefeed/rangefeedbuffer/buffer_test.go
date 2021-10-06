// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rangefeedbuffer_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed/rangefeedbuffer"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestBuffer ensures that buffer addition, checkpoints, and flushing semantics
// work as expected.
//
// XXX: Make this datadriven?
func TestBuffer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ts := func(nanos int) hlc.Timestamp {
		return hlc.Timestamp{WallTime: int64(nanos)}
	}
	span := func(start, end string) roachpb.Span {
		return roachpb.Span{Key: roachpb.Key(start), EndKey: roachpb.Key(end)}
	}
	key := func(k string) roachpb.Key {
		return roachpb.Key(k)
	}
	value := func(v string, ts hlc.Timestamp) roachpb.Value {
		return roachpb.Value{
			RawBytes:  []byte(v),
			Timestamp: ts,
		}
	}

	ctx := context.Background()
	buffer := rangefeedbuffer.New()

	{ // Sanity check the newly initialized rangefeed buffer.
		events, checkpointTS := buffer.Flush()
		require.True(t, checkpointTS.IsEmpty())
		require.True(t, len(events) == 0)
	}

	// Check that adding rangefeed error events error out.
	require.Error(t, buffer.Add(ctx, MakeRangeFeedError(t, roachpb.Error{})))

	{ // Checkpointing without any events being added should have nothing to flush out.
		require.NoError(t, buffer.Add(ctx, MakeRangeFeedCheckpoint(t, span("a", "z"), ts(10))))
		events, checkpointTS := buffer.Flush()
		require.Equal(t, ts(10), checkpointTS)
		require.True(t, len(events) == 0)
	}

	{ // Adding a bunch of non-checkpoint events should not produce a flush.
		require.NoError(t, buffer.Add(ctx, MakeRangeFeedValue(t, key("a"), value("a", ts(13))))) // a@13
		require.NoError(t, buffer.Add(ctx, MakeRangeFeedValue(t, key("b"), value("b", ts(11))))) // b@11
		require.NoError(t, buffer.Add(ctx, MakeRangeFeedValue(t, key("c"), value("c", ts(15))))) // c@15
		require.NoError(t, buffer.Add(ctx, MakeRangeFeedValue(t, key("d"), value("d", ts(12))))) // d@12
		require.NoError(t, buffer.Add(ctx, MakeRangeFeedValue(t, key("d"), value("e", ts(18))))) // e@18

		events, checkpointTS := buffer.Flush()
		require.Equal(t, ts(10), checkpointTS) // checkpoint timestamp should remain unchanged
		require.True(t, len(events) == 0)
	}

	{ // A checkpoint entry should flush out entries with timestamps <= the checkpoint in sorted order
		require.NoError(t, buffer.Add(ctx, MakeRangeFeedCheckpoint(t, span("a", "z"), ts(14))))
		events, checkpointTS := buffer.Flush()
		require.Equal(t, ts(14), checkpointTS) // expect a higher checkpoint timestamp

		require.True(t, len(events) == 3)
		require.Equal(t, events[0].Val.Value, value("b", ts(11))) // b@11
		require.Equal(t, events[1].Val.Value, value("d", ts(12))) // d@12
		require.Equal(t, events[2].Val.Value, value("a", ts(13))) // a@13
	}

	{ // Incremental checkpoints should only surface the events until the given timestamp.
		require.NoError(t, buffer.Add(ctx, MakeRangeFeedCheckpoint(t, span("a", "z"), ts(15))))
		events, checkpointTS := buffer.Flush()
		require.Equal(t, ts(15), checkpointTS) // expect a higher checkpoint timestamp

		require.True(t, len(events) == 1)
		require.Equal(t, events[0].Val.Value, value("c", ts(15))) // c@15
	}

	{ // Adding events with timestamps <= the last checkpoint are dropped.
		require.NoError(t, buffer.Add(ctx, MakeRangeFeedValue(t, key("a"), value("a", ts(13))))) // a@13
		require.NoError(t, buffer.Add(ctx, MakeRangeFeedValue(t, key("b"), value("b", ts(11))))) // b@11
		require.NoError(t, buffer.Add(ctx, MakeRangeFeedValue(t, key("c"), value("c", ts(15))))) // c@15
		require.NoError(t, buffer.Add(ctx, MakeRangeFeedValue(t, key("d"), value("d", ts(12))))) // d@12

		events, checkpointTS := buffer.Flush()
		require.Equal(t, ts(15), checkpointTS)
		require.True(t, len(events) == 0)
	}

	{ // Additional events are flushed out at appropriate points.
		require.NoError(t, buffer.Add(ctx, MakeRangeFeedValue(t, key("f"), value("f", ts(19))))) // f@19
		require.NoError(t, buffer.Add(ctx, MakeRangeFeedValue(t, key("g"), value("g", ts(21))))) // g@21

		require.NoError(t, buffer.Add(ctx, MakeRangeFeedCheckpoint(t, span("a", "z"), ts(20))))
		events, checkpointTS := buffer.Flush()
		require.Equal(t, ts(20), checkpointTS)
		require.True(t, len(events) == 2)
		require.Equal(t, events[0].Val.Value, value("e", ts(18))) // e@18
		require.Equal(t, events[1].Val.Value, value("f", ts(19))) // f@19
	}

	{ // Ensure that a timestamp greater than any previous event flushes everything.
		require.NoError(t, buffer.Add(ctx, MakeRangeFeedCheckpoint(t, span("a", "z"), ts(100))))
		events, checkpointTS := buffer.Flush()
		require.Equal(t, ts(100), checkpointTS)
		require.True(t, len(events) == 1)
		require.Equal(t, events[0].Val.Value, value("g", ts(21))) // g@21
	}
}

func MakeRangeFeedCheckpoint(
	t *testing.T, span roachpb.Span, ts hlc.Timestamp,
) *roachpb.RangeFeedEvent {
	return MakeRangeFeedEvent(t, &roachpb.RangeFeedCheckpoint{Span: span, ResolvedTS: ts})
}

func MakeRangeFeedError(t *testing.T, err roachpb.Error) *roachpb.RangeFeedEvent {
	return MakeRangeFeedEvent(t, &roachpb.RangeFeedError{Error: err})
}

func MakeRangeFeedValue(t *testing.T, key roachpb.Key, val roachpb.Value) *roachpb.RangeFeedEvent {
	return MakeRangeFeedValueWithPrev(t, key, val, roachpb.Value{})
}

func MakeRangeFeedValueWithPrev(
	t *testing.T, key roachpb.Key, val, prev roachpb.Value,
) *roachpb.RangeFeedEvent {
	return MakeRangeFeedEvent(t, &roachpb.RangeFeedValue{Key: key, Value: val, PrevValue: prev})
}

func MakeRangeFeedEvent(t *testing.T, val interface{}) *roachpb.RangeFeedEvent {
	var ev roachpb.RangeFeedEvent
	if !ev.SetValue(val) {
		t.Fatalf("unable to set value: %v", val)
	}
	return &ev
}
