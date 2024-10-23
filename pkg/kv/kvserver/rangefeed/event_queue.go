// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
)

const eventQueueChunkSize = 4096

type sharedMuxEvent struct {
	ev    *kvpb.MuxRangeFeedEvent
	alloc *SharedBudgetAllocation
}

// queueChunk is a queue chunk of a fixed size which eventQueue uses to extend
// its storage. Chunks are kept in the pool to reduce allocations.
type queueChunk struct {
	data      [eventQueueChunkSize]sharedMuxEvent
	nextChunk *queueChunk
}

var sharedQueueChunkSyncPool = sync.Pool{
	New: func() interface{} {
		return new(queueChunk)
	},
}

func getPooledQueueChunk() *queueChunk {
	return sharedQueueChunkSyncPool.Get().(*queueChunk)
}

func putPooledQueueChunk(e *queueChunk) {
	*e = queueChunk{}
	sharedQueueChunkSyncPool.Put(e)
}

// eventQueue stores sharedMuxEvents. Internally events are stored in
// eventQueueChunkSize sized arrays that are added as needed and discarded once
// reader and writers finish working with it.
//
// chunks are pooled in a sync.Pool to reduce the number of allocations.
//
// pushBack, popFront, len, and free run in constant time. drain runs in linear
// time with respect to the number of elements in the queue. This structure is
// not safe for concurrent use.
//
// TODO(ssd): Consider replacing with queue.Queue once that implementation can
// use a pool.
type eventQueue struct {
	first, last *queueChunk
	read, write int
	size        int
}

func newEventQueue() *eventQueue {
	chunk := getPooledQueueChunk()
	return &eventQueue{
		first: chunk,
		last:  chunk,
	}
}

func (q *eventQueue) pushBack(e sharedMuxEvent) {
	if q.write == eventQueueChunkSize {
		nextChunk := getPooledQueueChunk()
		q.last.nextChunk = nextChunk
		q.last = nextChunk
		q.write = 0
	}
	q.last.data[q.write] = e
	q.write++
	q.size++
}

func (q *eventQueue) popFront() (sharedMuxEvent, bool) {
	if q.size == 0 {
		return sharedMuxEvent{}, false
	}
	if q.read == eventQueueChunkSize {
		removed := q.first
		q.first = q.first.nextChunk
		putPooledQueueChunk(removed)
		q.read = 0
	}
	res := q.first.data[q.read]
	q.read++
	q.size--
	return res, true
}

// free drops references held by the queue.
func (q *eventQueue) free() {
	q.first = nil
	q.last = nil
	q.read = 0
	q.write = 0
	q.size = 0
}

// drain releases all allocations held by the queue and then frees it.
func (q *eventQueue) drain(ctx context.Context) {
	start := q.read
	for chunk := q.first; chunk != nil; {
		max := eventQueueChunkSize
		if chunk.nextChunk == nil {
			max = q.write
		}
		for i := start; i < max; i++ {
			chunk.data[i].alloc.Release(ctx)
		}
		next := chunk.nextChunk
		putPooledQueueChunk(chunk)
		chunk = next
		start = 0
	}
	q.free()
}

func (q *eventQueue) len() int64 {
	return int64(q.size)
}

func (q *eventQueue) empty() bool {
	return q.size == 0
}
