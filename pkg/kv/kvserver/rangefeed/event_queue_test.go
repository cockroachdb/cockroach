// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func checkInvariants(t *testing.T, q *eventQueue) {
	if q.first == nil && q.last == nil {
		require.True(t, q.empty())
	} else if q.first != nil && q.last == nil {
		t.Fatal("head is nil but tail is non-nil")
	} else if q.first == nil && q.last != nil {
		t.Fatal("tail is nil but head is non-nil")
	} else {
		// The queue maintains an invariant that it contains no finished chunks.
		if q.first == q.last {
			require.Nil(t, q.first.nextChunk)
		}
		if q.empty() {
			require.Equal(t, q.first, q.last)
			require.Equal(t, q.read, q.write)
		} else {
			require.NotNil(t, q.first)
			require.NotNil(t, q.last)
		}
	}
}

func TestEventQueue(t *testing.T) {
	eventCount := 10000
	q := newEventQueue()
	t.Run("basic operation: add one event and remove it", func(t *testing.T) {
		require.True(t, q.empty())
		q.pushBack(sharedMuxEvent{})
		require.False(t, q.empty())
		_, ok := q.popFront()
		require.True(t, ok)
		require.True(t, q.empty())
	})

	t.Run("repeatedly popping empty queue should be fine", func(t *testing.T) {
		_, ok := q.popFront()
		require.False(t, ok)
		require.True(t, q.empty())
	})

	t.Run("fill and empty queue", func(t *testing.T) {
		for i := 0; i < eventCount; i++ {
			require.Equal(t, i, q.size)
			q.pushBack(sharedMuxEvent{})
			checkInvariants(t, q)
		}

		for eventCount != 0 {
			require.False(t, q.empty())
			checkInvariants(t, q)
			_, ok := q.popFront()
			require.True(t, ok)
			eventCount--
		}

		require.True(t, q.empty())
		checkInvariants(t, q)
		_, ok := q.popFront()
		require.False(t, ok)
	})

	t.Run("removeAll sets queue to nil", func(t *testing.T) {
		q.free()
		require.Nil(t, q.first)
		require.Nil(t, q.last)
		require.True(t, q.empty())
	})

	t.Run("test queue order", func(t *testing.T) {
		// Add events and assert they are consumed in fifo order.
		var lastPop int64 = -1
		var lastPush int64 = -1
		for eventCount > 0 {
			rng, _ := randutil.NewTestRand()
			op := rng.Intn(5)
			if op < 3 {
				v := lastPush + 1
				q.pushBack(sharedMuxEvent{
					ev: &kvpb.MuxRangeFeedEvent{
						StreamID: v,
					},
				})
				lastPush++
			} else {
				e, ok := q.popFront()
				if !ok {
					require.Equal(t, lastPop, lastPush)
					require.True(t, q.empty())
				} else {
					require.Equal(t, lastPop+1, e.ev.StreamID)
					lastPop++
					eventCount--
				}
			}
		}
	})
	// TODO(wenyihu6): figure out a way to test removeAll memory release.
	// We can pass in callback but not sure performance implication on that.
}

func runBenchmarkEventQueue(b *testing.B) {
	b.ReportAllocs()
	rng, _ := randutil.NewTestRand()
	b.Run("mixed of alloc, pushBack, r././emoveAll, empty", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			q := newEventQueue()
			for i := 0; i < b.N; i++ {
				q.pushBack(sharedMuxEvent{})
			}
			require.Equal(b, b.N, q.size)
			q.free()
			_ = q.empty()
		}
	})

	b.Run("mixed of pushBack, popFront", func(b *testing.B) {
		q := newEventQueue()
		for i := 0; i < b.N; i++ {
			if rng.Intn(2) == 0 {
				q.pushBack(sharedMuxEvent{})
			} else {
				q.popFront()
			}
		}
	})
}

func BenchmarkQueueWithFixedChunkSize(b *testing.B) {
	runBenchmarkEventQueue(b)
}
