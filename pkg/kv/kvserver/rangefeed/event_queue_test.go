// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
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
	pushEmptyEvents := func(t *testing.T, q *eventQueue, count int) {
		for i := 0; i < count; i++ {
			require.Equal(t, i, q.size)
			q.pushBack(sharedMuxEvent{})
			checkInvariants(t, q)
		}
	}

	t.Run("basic operation: add one event and remove it", func(t *testing.T) {
		q := newEventQueue()
		defer q.free()

		require.True(t, q.empty())
		q.pushBack(sharedMuxEvent{})
		require.False(t, q.empty())
		_, ok := q.popFront()
		require.True(t, ok)
		require.True(t, q.empty())
	})

	t.Run("repeatedly popping empty queue should be fine", func(t *testing.T) {
		q := newEventQueue()
		defer q.free()
		_, ok := q.popFront()
		require.False(t, ok)
		_, ok = q.popFront()
		require.False(t, ok)
		require.True(t, q.empty())
	})

	t.Run("fill and empty queue", func(t *testing.T) {
		q := newEventQueue()
		eventCount := 10000
		pushEmptyEvents(t, q, eventCount)
		require.Equal(t, int64(eventCount), q.len())
		for eventCount != 0 {
			require.False(t, q.empty())
			checkInvariants(t, q)
			_, ok := q.popFront()
			require.True(t, ok)
			eventCount--
		}
		require.Equal(t, int64(0), q.len())
		require.True(t, q.empty())
		checkInvariants(t, q)
		_, ok := q.popFront()
		require.False(t, ok)
	})

	t.Run("free sets queue to nil", func(t *testing.T) {
		q := newEventQueue()
		pushEmptyEvents(t, q, eventQueueChunkSize*2)
		q.free()
		require.Nil(t, q.first)
		require.Nil(t, q.last)
		require.True(t, q.empty())
	})

	// Add events and assert they are consumed in fifo order.
	t.Run("queue is FIFO ordered", func(t *testing.T) {
		rng, _ := randutil.NewTestRand()
		q := newEventQueue()
		var lastPop int64 = -1
		var lastPush int64 = -1
		eventCount := 10000
		for eventCount > 0 {
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

	t.Run("drain releases allocations", func(t *testing.T) {
		ctx := context.Background()
		s := cluster.MakeTestingClusterSettings()
		m := mon.NewMonitor(mon.Options{
			Name:      mon.MakeMonitorName("rangefeed"),
			Increment: 1,
			Settings:  s,
		})
		eventCount := eventQueueChunkSize + 100

		m.Start(ctx, nil, mon.NewStandaloneBudget(int64(eventCount)))
		b := m.MakeBoundAccount()
		f := NewFeedBudget(&b, int64(eventCount), &s.SV)
		require.Equal(t, int64(0), b.Used())
		q := newEventQueue()

		for range eventCount {
			a, err := f.TryGet(ctx, 1)
			require.NoError(t, err)
			q.pushBack(sharedMuxEvent{
				alloc: a,
			})
		}
		q.drain(ctx)
		require.Equal(t, int64(0), b.Used())
	})
}

func BenchmarkEventQueue(b *testing.B) {
	b.ReportAllocs()
	events := eventQueueChunkSize * 2
	b.Run(fmt.Sprintf("pushBack/events=%d", events), func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			q := newEventQueue()
			for range events {
				q.pushBack(sharedMuxEvent{})
			}
			q.free()
		}
	})

	b.Run(fmt.Sprintf("popFront/events=%d", events), func(b *testing.B) {
		q := newEventQueue()
		defer q.free()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			for range events {
				q.pushBack(sharedMuxEvent{})
			}
			b.StartTimer()

			for {
				_, ok := q.popFront()
				if !ok {
					break
				}

			}
		}
	})
}

func BenchmarkEventQueueChunkCreation(b *testing.B) {
	q := newEventQueue()
	defer q.free()

	evt := sharedMuxEvent{
		ev: &kvpb.MuxRangeFeedEvent{},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Create 10 pages of events
		for range eventQueueChunkSize * 10 {
			q.pushBack(evt)
		}
		// Drain
		for {
			_, ok := q.popFront()
			if !ok {
				break
			}
		}
		// Create 10 more
		for range eventQueueChunkSize * 10 {
			q.pushBack(evt)
		}
	}
}

// lockedQueue is how we expect callers may implement concurrent use
// of eventQueue.
type lockedQueue struct {
	syncutil.Mutex
	q       *eventQueue
	notifyC chan struct{}
}

func (lq *lockedQueue) pushBack(e sharedMuxEvent) {
	lq.Lock()
	lq.q.pushBack(e)
	lq.Unlock()
	select {
	case lq.notifyC <- struct{}{}:
	default:
	}
}

func (lq *lockedQueue) pop() (sharedMuxEvent, bool) {
	lq.Lock()
	defer lq.Unlock()
	return lq.q.popFront()
}

func (lq *lockedQueue) popFront() (sharedMuxEvent, bool) {
	e, ok := lq.pop()
	if ok {
		return e, ok
	}
	<-lq.notifyC
	return lq.pop()
}

// chanQueue is a queue implementation using simple channels for
// comparison purposes. Note that as it uses a fixed channel buffer,
// it may block if the benchmark adds too much data before draining.
type chanQueue struct {
	c chan sharedMuxEvent
}

func (c *chanQueue) pushBack(e sharedMuxEvent) {
	c.c <- e
}

func (c *chanQueue) popFront() (sharedMuxEvent, bool) {
	e, ok := <-c.c
	return e, ok
}

// BenchmarkEventQueueMPSC tries to compare this queue to a simple
// channel for the MPSC use case.
func BenchmarkEventQueueMPSC(b *testing.B) {
	ctx := context.Background()
	b.ReportAllocs()

	type queue interface {
		pushBack(e sharedMuxEvent)
		popFront() (sharedMuxEvent, bool)
	}

	eventsPerWorker := 10 * eventQueueChunkSize
	producerCount := 10
	runBench := func(b *testing.B, q queue) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			g := ctxgroup.WithContext(ctx)
			g.GoCtx(func(ctx context.Context) error {
				expectedEventCount := eventsPerWorker * producerCount
				eventCount := 0
				for {
					_, t := q.popFront()
					if t {
						eventCount++
					}
					if eventCount >= expectedEventCount {
						return nil
					}
				}
			})

			for range producerCount {
				g.GoCtx(func(ctx context.Context) error {
					for range eventsPerWorker {
						q.pushBack(sharedMuxEvent{})
					}
					return nil
				})
			}

			require.NoError(b, g.Wait())
		}
	}
	b.Run("eventQueue", func(b *testing.B) {
		q := &lockedQueue{
			q:       newEventQueue(),
			notifyC: make(chan struct{}, 1),
		}
		defer q.q.free()
		runBench(b, q)
	})
	b.Run("chanQueue", func(b *testing.B) {
		q := &chanQueue{
			c: make(chan sharedMuxEvent, eventQueueChunkSize),
		}
		runBench(b, q)
	})
}
