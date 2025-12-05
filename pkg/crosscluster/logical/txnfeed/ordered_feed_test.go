// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnfeed

import (
	"context"
	"testing"
	"testing/synctest"

	"github.com/cockroachdb/cockroach/pkg/crosscluster"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/stretchr/testify/require"
)

type kvEvent struct {
	key  string
	time int
}

type checkpoint struct {
	start string
	end   string
	time  int
}

type writeSet struct {
	kvs []kvEvent
	ts  int
}

// testSubscription is a fake subscription that emits events from a predefined list.
type testSubscription struct {
	events []any
	output chan crosscluster.Event
}

// Subscribe implements the Subscription interface.
func (s *testSubscription) Subscribe(ctx context.Context) error {
	for _, ev := range s.events {
		switch e := ev.(type) {
		case kvEvent:
			// Convert a single kvEvent to a KV event.
			s.output <- crosscluster.MakeKVEvent([]streampb.StreamEvent_KV{
				{
					KeyValue: roachpb.KeyValue{
						Key: roachpb.Key(e.key),
						Value: roachpb.Value{
							Timestamp: hlc.Timestamp{WallTime: int64(e.time)},
						},
					},
				},
			})
		case []kvEvent:
			// Convert multiple kvEvents to a single KV event with multiple rows.
			kvs := make([]streampb.StreamEvent_KV, len(e))
			for i, kv := range e {
				kvs[i] = streampb.StreamEvent_KV{
					KeyValue: roachpb.KeyValue{
						Key: roachpb.Key(kv.key),
						Value: roachpb.Value{
							Timestamp: hlc.Timestamp{WallTime: int64(kv.time)},
						},
					},
				}
			}
			s.output <- crosscluster.MakeKVEvent(kvs)
		case checkpoint:
			// Convert checkpoint to a checkpoint event.
			s.output <- crosscluster.MakeCheckpointEvent(&streampb.StreamEvent_StreamCheckpoint{
				ResolvedSpans: []jobspb.ResolvedSpan{
					{
						Span: roachpb.Span{
							Key:    roachpb.Key(e.start),
							EndKey: roachpb.Key(e.end),
						},
						Timestamp: hlc.Timestamp{WallTime: int64(e.time)},
					},
				},
			})
		}
	}
	return nil
}

// Events implements the Subscription interface.
func (s *testSubscription) Events() <-chan crosscluster.Event {
	return s.output
}

// Err implements the Subscription interface.
func (s *testSubscription) Err() error {
	return nil
}

// makeTestSubscription creates a test subscription from a list of simple events.
// It converts kvEvent and checkpoint types to actual crosscluster.Event types.
func makeTestSubscription(events []any) *testSubscription {
	ch := make(chan crosscluster.Event, len(events))
	return &testSubscription{events: events, output: ch}
}

// toSimpleWriteSet converts a WriteSet to the simple writeSet type for comparison.
func toSimpleWriteSet(ws WriteSet) writeSet {
	simple := writeSet{
		ts:  int(ws.Timestamp.WallTime),
		kvs: make([]kvEvent, len(ws.Rows)),
	}
	for i, kv := range ws.Rows {
		simple.kvs[i] = kvEvent{
			key:  string(kv.KeyValue.Key),
			time: int(kv.KeyValue.Value.Timestamp.WallTime),
		}
	}
	return simple
}

func TestOrderedFeed(t *testing.T) {
	// TODO test what happens if txns are delivered out of order
	// TODO test what happens when txn is delivered after the checkpoint
	// TODO test what happens when there are duplicate events
	type testCase struct {
		name     string
		events   []any
		expected []writeSet
	}

	testCases := []testCase{
		{
			name: "single transaction",
			events: []any{
				[]kvEvent{
					{key: "a", time: 100},
					{key: "b", time: 100},
				},
				checkpoint{start: "a", end: "z", time: 101},
			},
			expected: []writeSet{
				{
					ts: 100,
					kvs: []kvEvent{
						{key: "a", time: 100},
						{key: "b", time: 100},
					},
				},
			},
		},
		{
			name: "multiple transactions",
			events: []any{
				[]kvEvent{
					{key: "a", time: 100},
					{key: "b", time: 200},
				},
				checkpoint{start: "a", end: "z", time: 201},
			},
			expected: []writeSet{
				{
					ts: 100,
					kvs: []kvEvent{
						{key: "a", time: 100},
					},
				},
				{
					ts: 200,
					kvs: []kvEvent{
						{key: "b", time: 200},
					},
				},
			},
		},
		{
			name: "row redelivery after checkpoint",
			events: []any{
				[]kvEvent{
					{key: "a", time: 100},
				},
				checkpoint{start: "a", end: "z", time: 150},
				[]kvEvent{
					{key: "a", time: 100},
				},
				checkpoint{start: "a", end: "z", time: 200},
			},
			expected: []writeSet{
				{
					ts: 100,
					kvs: []kvEvent{
						{key: "a", time: 100},
					},
				},
			},
		},
		{
			name: "duplicate delivery",
			events: []any{
				[]kvEvent{
					{key: "a", time: 100},
					{key: "b", time: 100},
				},
				[]kvEvent{
					{key: "a", time: 100},
				},
				[]kvEvent{
					{key: "b", time: 100},
				},
				checkpoint{start: "a", end: "z", time: 200},
			},
			expected: []writeSet{
				{
					ts: 100,
					kvs: []kvEvent{
						{key: "a", time: 100},
						{key: "b", time: 100},
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {

			var received []writeSet
			synctest.Test(t, func(t *testing.T) {
				sub := makeTestSubscription(tc.events)
				frontier, err := span.MakeFrontier(roachpb.Span{
					Key:    roachpb.Key("a"),
					EndKey: roachpb.Key("z"),
				})
				require.NoError(t, err)

				feed := &OrderedFeed{
					rawSubscription: sub,
					frontier:        frontier,
				}

				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				go func() {
					err := sub.Subscribe(context.Background())
					require.NoError(t, err)
				}()
				go func() {
					for {
						ws, err := feed.Next(ctx)
						if err != nil {
							require.ErrorIs(t, err, context.Canceled)
							return
						}
						received = append(received, toSimpleWriteSet(ws))
					}
				}()
				synctest.Wait()
				cancel()
			})

			// Verify the received write sets match the expected ones.
			require.Equal(t, tc.expected, received)
		})
	}
}
