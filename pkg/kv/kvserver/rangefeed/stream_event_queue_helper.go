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
	"sync"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
)

// Number of queue elements allocated at once to amortize queue allocations.
const eventQueueChunkSize = 4000

// idQueueChunk is a queue chunk of a fixed size which idQueue uses to extend
// its storage. Chunks are kept in the pool to reduce allocations.
type eventQueueChunk struct {
	data      [eventQueueChunkSize]*kvpb.MuxRangeFeedEvent
	nextChunk *eventQueueChunk
}

var sharedEventQueueChunkSyncPool = sync.Pool{
	New: func() interface{} {
		return new(eventQueueChunk)
	},
}

func getPooledEventQueueChunk() *eventQueueChunk {
	return sharedEventQueueChunkSyncPool.Get().(*eventQueueChunk)
}

func putPooledEventQueueChunk(e *eventQueueChunk) {
	*e = eventQueueChunk{}
	sharedEventQueueChunkSyncPool.Put(e)
}

// muxEventQueue stores pending processor ID's. Internally data is stored in
// eventQueueChunk sized arrays that are added as needed and discarded once
// reader and writers finish working with it. Since we only have a single
// scheduler per store, we don't use a pool as only reuse could happen within
// the same queue and in that case we can just increase chunk size.
type muxEventQueue struct {
	first, last *eventQueueChunk
	read, write int
	size        int
}

func newMuxEventQueue() muxEventQueue {
	chunk := getPooledEventQueueChunk()
	return muxEventQueue{
		first: chunk,
		last:  chunk,
	}
}

func (q *muxEventQueue) pushBack(event *kvpb.MuxRangeFeedEvent) {
	if q.write == eventQueueChunkSize {
		nexChunk := getPooledEventQueueChunk()
		q.last.nextChunk = nexChunk
		q.last = nexChunk
		q.write = 0
	}
	q.last.data[q.write] = event
	q.write++
	q.size++
}

func (q *muxEventQueue) popFront() (*kvpb.MuxRangeFeedEvent, bool) {
	//if q.first == q.last && q.read == q.write {
	//	return sharedMuxEvent{}, false
	//}
	if q.size == 0 {
		return &kvpb.MuxRangeFeedEvent{}, false
	}
	if q.read == eventQueueChunkSize {
		//removed := q.first
		q.first = q.first.nextChunk
		//putPooledEventQueueChunk(removed)
		q.read = 0
	}
	res := q.first.data[q.read]
	q.first.data[q.read] = &kvpb.MuxRangeFeedEvent{}
	q.read++
	q.size--
	return res, true
}

// removeAll removes aren releases all entries in the queue.
func (q *muxEventQueue) removeAll() {
	start := q.read
	for chunk := q.first; chunk != nil; {
		max := eventQueueChunkSize
		if chunk.nextChunk == nil {
			max = q.write
		}

		// (TODO wenyihu6): add memory accounting back
		for i := start; i < max; i++ {
			//	chunk.data[i].alloc.Release(ctx)
			chunk.data[i] = nil
		}

		next := chunk.nextChunk
		putPooledEventQueueChunk(chunk)
		chunk = next
		//start = 0
	}
	q.first = q.last
	q.read = 0
	q.write = 0
	q.size = 0
}

func (q *muxEventQueue) len() int64 {
	return int64(q.size)
}
