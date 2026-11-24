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

	t.Run("add and remove", func(t *testing.T) {
		q := newEventQueue()
		defer q.free()

		require.True(t, q.empty())
		q.pushBack(sharedMuxEvent{})
		require.False(t, q.empty())
		dest := make([]sharedMuxEvent, 0, 4)
		dest = q.popFrontInto(dest[:0], 1)
		require.Equal(t, 1, len(dest))
		require.True(t, q.empty())
	})

	t.Run("empty queue", func(t *testing.T) {
		q := newEventQueue()
		defer q.free()
		dest := make([]sharedMuxEvent, 0, 1)
		require.Equal(t, 0, len(q.popFrontInto(dest, 4)))
	})

	t.Run("multiple events", func(t *testing.T) {
		q := newEventQueue()
		defer q.free()
		dest := make([]sharedMuxEvent, 0, 4)
		q.pushBack(sharedMuxEvent{})
		q.pushBack(sharedMuxEvent{})
		q.pushBack(sharedMuxEvent{})
		q.pushBack(sharedMuxEvent{})
		q.pushBack(sharedMuxEvent{})
		require.Equal(t, int64(5), q.len())
		require.Equal(t, 4, len(q.popFrontInto(dest[:0], 4)))
		require.Equal(t, int64(1), q.len())
		require.Equal(t, 1, len(q.popFrontInto(dest[:0], 4)))
		require.Equal(t, int64(0), q.len())
	})

	t.Run("pops up to page boundary", func(t *testing.T) {
		q := newEventQueue()
		defer q.free()
		bufSize := eventQueueChunkSize + 100
		dest := make([]sharedMuxEvent, 0, bufSize)
		// Push 2 pages of events
		for range eventQueueChunkSize * 2 {
			q.pushBack(sharedMuxEvent{})
		}

		require.Equal(t, int64(eventQueueChunkSize*2), q.len())
		require.Equal(t, eventQueueChunkSize, len(q.popFrontInto(dest[:0], bufSize)))
		require.Equal(t, int64(eventQueueChunkSize), q.len())
		require.Equal(t, eventQueueChunkSize, len(q.popFrontInto(dest[:0], bufSize)))
		require.Equal(t, int64(0), q.len())
	})

	t.Run("repeatedly popping empty queue should be fine", func(t *testing.T) {
		q := newEventQueue()
		defer q.free()
		dest := make([]sharedMuxEvent, 0, 4)

		dest = q.popFrontInto(dest[:0], 4)
		require.Equal(t, 0, len(dest))
		dest = q.popFrontInto(dest[:0], 4)
		require.Equal(t, 0, len(dest))
		require.True(t, q.empty())
	})

	t.Run("fill and empty queue", func(t *testing.T) {
		q := newEventQueue()
		eventCount := 4 * 2500
		pushEmptyEvents(t, q, eventCount)
		require.Equal(t, int64(eventCount), q.len())
		dest := make([]sharedMuxEvent, 0, 4)

		for eventCount != 0 {
			require.False(t, q.empty())
			checkInvariants(t, q)
			dest = q.popFrontInto(dest[:0], 4)
			require.Equal(t, 4, len(dest))
			eventCount = eventCount - 4
		}
		require.Equal(t, int64(0), q.len())
		require.True(t, q.empty())
		checkInvariants(t, q)
		dest = q.popFrontInto(dest[:0], 4)
		require.Equal(t, 0, len(dest))
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
		eventCount := 4 * 2500
		dest := make([]sharedMuxEvent, 0, 4)
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
				dest = q.popFrontInto(dest[:0], 4)
				if len(dest) == 0 {
					require.Equal(t, lastPop, lastPush)
					require.True(t, q.empty())
				} else {
					for _, e := range dest {
						require.Equal(t, lastPop+1, e.ev.StreamID)
						lastPop++
						eventCount--
					}
				}
			}
		}
	})

	t.Run("drain releases allocations", func(t *testing.T) {
		ctx := context.Background()
		s := cluster.MakeTestingClusterSettings()
		m := mon.NewMonitor(mon.Options{
			Name:      mon.MakeName("rangefeed"),
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
	bufSize := 64
	dest := make([]sharedMuxEvent, 0, bufSize)
	b.Run(fmt.Sprintf("pushBack/events=%d", events), func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			q := newEventQueue()
			for range events {
				q.pushBack(sharedMuxEvent{})
			}
			q.free()
		}
	})

	b.Run(fmt.Sprintf("popFrontInto/events=%d/bufSize=%d", events, bufSize), func(b *testing.B) {
		q := newEventQueue()
		defer q.free()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			for range events {
				q.pushBack(sharedMuxEvent{})
			}
			b.StartTimer()

			for {
				dest = q.popFrontInto(dest[:0], bufSize)
				if len(dest) == 0 {
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
		bufSize := 64
		dest := make([]sharedMuxEvent, 0, bufSize)
		// Create 10 pages of events
		for range eventQueueChunkSize * 10 {
			q.pushBack(evt)
		}
		// Drain
		for {
			dest = q.popFrontInto(dest[:0], bufSize)
			if len(dest) == 0 {
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

func (lq *lockedQueue) popFrontIntoInner(dest []sharedMuxEvent, maxToPop int) []sharedMuxEvent {
	lq.Lock()
	defer lq.Unlock()
	return lq.q.popFrontInto(dest, maxToPop)
}

func (lq *lockedQueue) popFrontInto(dest []sharedMuxEvent, maxToPop int) []sharedMuxEvent {
	ret := lq.popFrontIntoInner(dest, maxToPop)
	if len(ret) > 0 {
		return ret
	}
	<-lq.notifyC
	return lq.popFrontInto(dest, maxToPop)
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

func (c *chanQueue) popFrontInto(dest []sharedMuxEvent, maxToPop int) []sharedMuxEvent {
	e, ok := <-c.c
	if !ok {
		return dest
	}
	popped := 1
	dest = append(dest, e)
	for popped < maxToPop {
		select {
		case e, ok := <-c.c:
			if !ok {
				return dest
			}
			dest = append(dest, e)
			popped++
		default:
			return dest
		}
	}
	return dest
}

// BenchmarkEventQueueMPSC tries to compare this queue to a simple
// channel for the MPSC use case.
func BenchmarkEventQueueMPSC(b *testing.B) {
	ctx := context.Background()
	b.ReportAllocs()

	type queue interface {
		pushBack(sharedMuxEvent)
		popFrontInto([]sharedMuxEvent, int) []sharedMuxEvent
	}

	eventsPerWorker := 10 * eventQueueChunkSize
	producerCount := 10
	bufSize := 64
	runBench := func(b *testing.B, q queue) {
		// Pre-allocate dest outside the benchmark loop to avoid polluting the
		// allocation profile. This is safe because g.Wait() ensures the consumer
		// goroutine completes before the next iteration starts.
		dest := make([]sharedMuxEvent, 0, bufSize)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			g := ctxgroup.WithContext(ctx)
			g.GoCtx(func(ctx context.Context) error {
				expectedEventCount := eventsPerWorker * producerCount
				eventCount := 0
				for {
					dest = q.popFrontInto(dest[:0], bufSize)
					if len(dest) > 0 {
						eventCount += len(dest)
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
