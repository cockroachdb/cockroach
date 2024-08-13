// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package queue

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

type testQueueItem struct {
	i int64
}

// testQueueInterface is an interface that defines the operations that can be
// done on a queue for testing.
type testQueueInterface interface {
	Enqueue(*testQueueItem)
	Empty() bool
	Dequeue() (*testQueueItem, bool)
	removeAll()
	checkInvariants(*testing.T)
	checkEventCount(*testing.T, int)
	checkNil(*testing.T)
}

var _ testQueueInterface = &Queue[*testQueueItem]{}
var _ testQueueInterface = &QueueWithFixedChunkSize[*testQueueItem]{}

func runQueueTest(t *testing.T, q testQueueInterface) {
	eventCount := 1000000
	t.Run("basic operation: add one event and remove it", func(t *testing.T) {
		require.True(t, q.Empty())
		q.Enqueue(&testQueueItem{})
		require.False(t, q.Empty())
		_, ok := q.Dequeue()
		require.True(t, ok)
		require.True(t, q.Empty())
		q.checkInvariants(t)
		q.checkEventCount(t, 0)
	})

	t.Run("fill and empty queue", func(t *testing.T) {
		for i := 0; i < eventCount; i++ {
			q.checkInvariants(t)
			q.checkEventCount(t, i)
			q.Enqueue(&testQueueItem{})
		}
		for eventCount != 0 {
			require.Equal(t, eventCount <= 0, q.Empty())
			_, ok := q.Dequeue()
			require.True(t, ok)
			eventCount--
			q.checkInvariants(t)
			q.checkEventCount(t, eventCount)
		}
		require.True(t, q.Empty())
		q.checkInvariants(t)
		q.checkEventCount(t, 0)
		_, ok := q.Dequeue()
		require.False(t, ok)
		require.True(t, q.Empty())
	})

	t.Run("removeAll is noop for an empty queue", func(t *testing.T) {
		q.removeAll()
		q.checkNil(t)
		q.checkInvariants(t)
		q.checkEventCount(t, 0)
		require.True(t, q.Empty())
	})

	t.Run("empty queue", func(t *testing.T) {
		q.Enqueue(&testQueueItem{})
		require.False(t, q.Empty())
		q.Dequeue()
		require.True(t, q.Empty())
		q.checkInvariants(t)
		q.checkEventCount(t, 0)
	})

	t.Run("fill and empty queue with random operations", func(t *testing.T) {
		// Add events and assert they are consumed in fifo order.
		var lastPop int64 = -1
		var lastPush int64 = -1
		for eventCount > 0 {
			rng, _ := randutil.NewTestRand()
			op := rng.Intn(5)
			if op < 3 {
				v := lastPush + 1
				q.Enqueue(&testQueueItem{i: v})
				lastPush++
			} else {
				e, ok := q.Dequeue()
				if !ok {
					require.Equal(t, lastPop, lastPush)
					require.True(t, q.Empty())
				} else {
					require.Equal(t, lastPop+1, e.i)
					lastPop++
					eventCount--
				}
			}
		}

		t.Run("test removeAll", func(t *testing.T) {
			for eventCount > 0 {
				q.checkInvariants(t)
				q.checkEventCount(t, eventCount)
				rng, _ := randutil.NewTestRand()
				op := rng.Intn(15)
				sum := int64(0)
				if op < 10 {
					v := int64(rng.Intn(20000))
					q.Enqueue(&testQueueItem{i: v})
					sum += v
				} else if op < 12 {
					e, ok := q.Dequeue()
					if !ok {
						require.True(t, q.Empty())
					} else {
						sum -= e.i
						eventCount--
					}
				} else {
					fixedQ, ok := q.(*QueueWithFixedChunkSize[*testQueueItem])
					if op < 13 && !ok {
						q.removeAll()
						eventCount = 0
						q.checkNil(t)
						q.checkInvariants(t)
						q.checkEventCount(t, 0)
					} else {
						actualSum := int64(0)
						fixedQ.RemoveAll(func(e *testQueueItem) {
							actualSum += e.i
							eventCount--
						})
						fixedQ.checkNil(t)
						q.checkEventCount(t, 0)
						require.Equal(t, sum, actualSum)
					}
				}
			}
		})
	})
}

func TestQueue(t *testing.T) {
	rng, _ := randutil.NewTestRand()
	chunkSize := rng.Intn(255) + 1
	testutils.RunTrueAndFalse(t, "queue with fixed chunk size", func(t *testing.T, fixedChunkSize bool) {
		if fixedChunkSize {
			q := NewQueueWithFixedChunkSize[*testQueueItem]()
			runQueueTest(t, q)
		} else {
			q, err := NewQueue[*testQueueItem](WithChunkSize[*testQueueItem](chunkSize))
			require.NoError(t, err)
			runQueueTest(t, q)
		}
	})
}

func TestChunkSize(t *testing.T) {
	q, err := NewQueue[*testQueueItem](WithChunkSize[*testQueueItem](0))
	require.Error(t, err)
	require.Nil(t, q)

	q, err = NewQueue[*testQueueItem](WithChunkSize[*testQueueItem](1))
	require.Equal(t, 1, q.chunkSize)
	require.NoError(t, err)

	q, err = NewQueue[*testQueueItem]()
	require.Equal(t, defaultChunkSize, q.chunkSize)
	require.NoError(t, err)
}

type testQueueInterfaceWithInt interface {
	Enqueue(int)
	Empty() bool
	Dequeue() (int, bool)
	removeAll()
}

func runBenchmarkRangefeed(b *testing.B, q testQueueInterfaceWithInt) {
	rng, _ := randutil.NewTestRand()
	// Run a mixed workload of Enqueue, removeAll, and Empty.
	for i := 0; i < b.N; i++ {
		for i := 0; i < b.N; i++ {
			q.Enqueue(1)
		}
		q.removeAll()
		_ = q.Empty()
	}

	// Run a mixed workload of Enqueue and Dequeue.
	for i := 0; i < b.N; i++ {
		if rng.Intn(2) == 0 {
			q.Enqueue(1)
		} else {
			q.Dequeue()
		}
	}
}

func BenchmarkQueueWithFixedChunkSize(b *testing.B) {
	b.ReportAllocs()
	q := NewQueueWithFixedChunkSize[int]()
	runBenchmarkRangefeed(b, q)
}

func BenchmarkQueue(b *testing.B) {
	b.ReportAllocs()
	q, _ := NewQueue[int]()
	runBenchmarkRangefeed(b, q)
}
