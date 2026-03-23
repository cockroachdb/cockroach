// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnfeed

import (
	"cmp"
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

func (a kvEvent) Compare(b kvEvent) int {
	if c := cmp.Compare(a.time, b.time); c != 0 {
		return c
	}
	return cmp.Compare(a.key, b.key)
}

type checkpoint struct {
	start string
	end   string
	time  int
}

// closeEvent is a sentinel that causes testSubscription to close its output
// channel, signaling stream completion.
type closeEvent struct{}

type writeSet struct {
	kvs []kvEvent
	ts  int
}

type testSubscription struct {
	events []any
	output chan crosscluster.Event
}

func (s *testSubscription) Subscribe(ctx context.Context) error {
	for _, ev := range s.events {
		switch e := ev.(type) {
		case kvEvent:
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
		case closeEvent:
			close(s.output)
			return nil
		}
	}
	return nil
}

func (s *testSubscription) Events() <-chan crosscluster.Event {
	return s.output
}

func (s *testSubscription) Err() error {
	return nil
}

func makeTestSubscription(events []any) *testSubscription {
	ch := make(chan crosscluster.Event, len(events))
	return &testSubscription{events: events, output: ch}
}

func eventToWriteSet(ev crosscluster.Event) writeSet {
	kvs := ev.GetKVs()
	ws := writeSet{
		ts:  int(kvs[0].KeyValue.Value.Timestamp.WallTime),
		kvs: make([]kvEvent, len(kvs)),
	}
	for i, kv := range kvs {
		ws.kvs[i] = kvEvent{
			key:  string(kv.KeyValue.Key),
			time: int(kv.KeyValue.Value.Timestamp.WallTime),
		}
	}
	return ws
}

func TestOrderedFeed(t *testing.T) {
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
				checkpoint{start: "a", end: "z", time: 100},
				closeEvent{},
			},
			expected: []writeSet{
				{
					ts:  100,
					kvs: []kvEvent{{key: "a", time: 100}, {key: "b", time: 100}},
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
				checkpoint{start: "a", end: "z", time: 200},
				closeEvent{},
			},
			expected: []writeSet{
				{
					ts: 100,
					kvs: []kvEvent{
						{key: "a", time: 100},
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
				closeEvent{},
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
				checkpoint{start: "a", end: "z", time: 100},
				closeEvent{},
			},
			expected: []writeSet{
				{
					ts:  100,
					kvs: []kvEvent{{key: "a", time: 100}, {key: "b", time: 100}},
				},
			},
		},
		{
			name: "out of order delivery",
			events: []any{
				kvEvent{key: "b", time: 200},
				kvEvent{key: "a", time: 100},
				kvEvent{key: "c", time: 200},
				checkpoint{start: "a", end: "z", time: 200},
				closeEvent{},
			},
			expected: []writeSet{
				{
					ts: 100,
					kvs: []kvEvent{
						{key: "a", time: 100},
						{key: "b", time: 200},
						{key: "c", time: 200},
					},
				},
			},
		},
		{
			// The frontier covers {a, z}. The first two checkpoints
			// advance sub-spans independently: {a,m}@250 does not
			// advance the overall frontier (m-z is still at 0), but
			// {m,z}@350 brings the minimum to 250, flushing KVs <=
			// 250 as one batch. The final checkpoint advances
			// everything to 400, flushing the remaining KV.
			name: "partial frontier advance",
			events: []any{
				kvEvent{key: "a", time: 100},
				kvEvent{key: "b", time: 200},
				kvEvent{key: "c", time: 200},
				kvEvent{key: "d", time: 300},
				checkpoint{start: "a", end: "m", time: 250},
				checkpoint{start: "m", end: "z", time: 350},
				checkpoint{start: "a", end: "z", time: 400},
				closeEvent{},
			},
			expected: []writeSet{
				{
					ts: 100,
					kvs: []kvEvent{
						{key: "a", time: 100},
						{key: "b", time: 200},
						{key: "c", time: 200},
					},
				},
				{
					ts:  300,
					kvs: []kvEvent{{key: "d", time: 300}},
				},
			},
		},
		{
			// Two full-span checkpoints cause two separate flushes.
			name: "multiple full checkpoints",
			events: []any{
				kvEvent{key: "a", time: 100},
				kvEvent{key: "b", time: 200},
				checkpoint{start: "a", end: "z", time: 250},
				kvEvent{key: "c", time: 300},
				kvEvent{key: "d", time: 400},
				checkpoint{start: "a", end: "z", time: 400},
				closeEvent{},
			},
			expected: []writeSet{
				{
					ts: 100,
					kvs: []kvEvent{
						{key: "a", time: 100},
						{key: "b", time: 200},
					},
				},
				{
					ts: 300,
					kvs: []kvEvent{
						{key: "c", time: 300},
						{key: "d", time: 400},
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

				feed, err := NewOrderedFeed(sub, frontier)
				require.NoError(t, err)

				go func() {
					err := feed.Subscribe(context.Background())
					require.NoError(t, err)
				}()
				go func() {
					for ev := range feed.Events() {
						if ev.Type() == crosscluster.KVEvent {
							received = append(received, eventToWriteSet(ev))
						}
					}
				}()
				synctest.Wait()
			})

			require.Equal(t, tc.expected, received)
		})
	}
}
