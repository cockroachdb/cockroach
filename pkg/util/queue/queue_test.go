// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package queue

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func checkInvariants[T any](t *testing.T, q *Queue[T]) {
	if q.head == nil && q.tail == nil {
		require.True(t, q.Empty())
	} else if q.head != nil && q.tail == nil {
		t.Fatal("head is nil but tail is non-nil")
	} else if q.head == nil && q.tail != nil {
		t.Fatal("tail is nil but head is non-nil")
	} else {
		// The queue maintains an invariant that it contains no finished chunks.
		require.False(t, q.head.finished())
		require.False(t, q.tail.finished())

		if q.head == q.tail {
			require.Nil(t, q.head.next)
		} else {
			// q.tail is non-nil and not equal to q.head. There must be a non-empty
			// chunk after q.head.
			require.False(t, q.Empty())
			if q.head.empty() {
				require.False(t, q.head.next.empty())
			}
			// The q.tail is never empty in this case. The tail can only be empty
			// when it is equal to q.head and q.head is empty.
			require.False(t, q.tail.empty())
		}
	}
}

type testQueueItem struct {
	i int64
}

func TestQueue(t *testing.T) {
	rng, _ := randutil.NewTestRand()

	eventCount := 1000
	chunkSize := rng.Intn(255) + 1

	q, err := NewQueue[*testQueueItem](WithChunkSize[*testQueueItem](chunkSize))
	require.NoError(t, err)
	// Add one event and remove it
	assert.True(t, q.Empty())
	q.Enqueue(&testQueueItem{})
	assert.False(t, q.Empty())
	_, ok := q.Dequeue()
	assert.True(t, ok)
	assert.True(t, q.Empty())

	// Fill 5 chunks and then pop each one, ensuring empty() returns the correct
	// value each time.
	checkInvariants(t, q)
	for i := 0; i < eventCount; i++ {
		q.Enqueue(&testQueueItem{})
	}
	checkInvariants(t, q)
	for {
		assert.Equal(t, eventCount <= 0, q.Empty())
		_, ok = q.Dequeue()
		if !ok {
			assert.True(t, q.Empty())
			break
		} else {
			eventCount--
		}
		checkInvariants(t, q)
	}
	assert.Equal(t, 0, eventCount)
	q.Enqueue(&testQueueItem{})
	assert.False(t, q.Empty())
	q.Dequeue()
	assert.True(t, q.Empty())

	// Add events to fill 5 chunks and assert they are consumed in fifo order.
	var lastPop int64 = -1
	var lastPush int64 = -1
	checkInvariants(t, q)
	for eventCount > 0 {
		op := rng.Intn(5)
		if op < 3 {
			q.Enqueue(&testQueueItem{i: lastPush + 1})
			lastPush++
		} else {
			e, ok := q.Dequeue()
			if !ok {
				assert.Equal(t, lastPop, lastPush)
				assert.True(t, q.Empty())
			} else {
				assert.Equal(t, lastPop+1, e.i)
				lastPop++
				eventCount--
			}
		}
		checkInvariants(t, q)
	}

	q.purge()
	require.Nil(t, q.head)
	require.Nil(t, q.tail)
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
