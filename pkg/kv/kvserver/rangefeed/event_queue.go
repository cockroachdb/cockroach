// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rangefeed

import (
	"context"
	"sync"
)

const eventQueueChunkSize = 4096

// idQueueChunk is a queue chunk of a fixed size which idQueue uses to extend
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

// eventQueue stores buffered events. Internally events are stored in
// eventQueueChunkSize sized arrays that are added as needed and discarded once
// reader and writers finish working with it.
//
// eventQueue is like a queue but uses a fixed size for the chunked
// linked list. Each chunk has a fixed size of 4096 elements. This
// implementation uses sync.Pool to reduce the number of allocations. It should
// be used when the queue is used to store a large number of elements.
//
// pushBack, popFront, len run in constant time. removeAll runs in linear time
// with respect to the number of elements in the queue. This structure is not
// safe for concurrent use.
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
		nexChunk := getPooledQueueChunk()
		q.last.nextChunk = nexChunk
		q.last = nexChunk
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

func (q *eventQueue) removeAll() {
	start := q.read
	for chunk := q.first; chunk != nil; {
		max := eventQueueChunkSize
		if chunk.nextChunk == nil {
			max = q.write
		}
		for i := start; i < max; i++ {
			chunk.data[i].alloc.Release(context.Background())
			chunk.data[i] = sharedMuxEvent{}
		}
		next := chunk.nextChunk
		putPooledQueueChunk(chunk)
		chunk = next
		start = 0
	}
	q.first = q.last
	q.read = 0
	q.write = 0
	q.size = 0
}

func (q *eventQueue) Len() int64 {
	return int64(q.size)
}
