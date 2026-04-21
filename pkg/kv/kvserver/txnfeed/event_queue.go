// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnfeed

import (
	"sync"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
)

const eventQueueChunkSize = 4096

// queueChunk is a queue chunk of a fixed size which eventQueue uses to extend
// its storage. Chunks are kept in the pool to reduce allocations.
type queueChunk struct {
	data      [eventQueueChunkSize]*kvpb.MuxTxnFeedEvent
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

// eventQueue stores *kvpb.MuxTxnFeedEvent values. Internally events are stored
// in eventQueueChunkSize sized arrays that are added as needed and discarded
// once reader and writers finish working with it.
//
// Chunks are pooled in a sync.Pool to reduce the number of allocations.
//
// pushBack, popFront, len, and free run in constant time. drain runs in linear
// time with respect to the number of elements in the queue. This structure is
// not safe for concurrent use.
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

func (q *eventQueue) pushBack(e *kvpb.MuxTxnFeedEvent) {
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

// popFrontInto appends up to eventsToPop events into dest.
func (q *eventQueue) popFrontInto(
	dest []*kvpb.MuxTxnFeedEvent, eventsToPop int,
) []*kvpb.MuxTxnFeedEvent {
	if eventsToPop == 0 || q.size == 0 {
		return dest
	}

	if q.read == eventQueueChunkSize {
		removed := q.first
		q.first = q.first.nextChunk
		putPooledQueueChunk(removed)
		q.read = 0
	}

	// We only read out of the current chunk. We could loop until we've reached
	// eventsToPop.
	availableInChunk := eventQueueChunkSize - q.read
	if q.first == q.last {
		// Last chunk — only up to write position.
		availableInChunk = q.write - q.read
	}

	if eventsToPop > availableInChunk {
		eventsToPop = availableInChunk
	}

	dest = append(dest, q.first.data[q.read:q.read+eventsToPop]...)
	clear(q.first.data[q.read : q.read+eventsToPop])

	q.read += eventsToPop
	q.size -= eventsToPop
	return dest
}

// free drops references held by the queue.
func (q *eventQueue) free() {
	q.first = nil
	q.last = nil
	q.read = 0
	q.write = 0
	q.size = 0
}

// drain returns all chunks to the pool and frees the queue.
func (q *eventQueue) drain() {
	start := q.read
	for chunk := q.first; chunk != nil; {
		max := eventQueueChunkSize
		if chunk.nextChunk == nil {
			max = q.write
		}
		for i := start; i < max; i++ {
			chunk.data[i] = nil
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
