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

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

type sharedMuxEvent struct {
	streamID int64
	rangeID  roachpb.RangeID
	event    *kvpb.RangeFeedEvent
	alloc    *SharedBudgetAllocation
}

// Number of queue elements allocated at once to amortize queue allocations.
const eventQueueChunkSize = 4000

// idQueueChunk is a queue chunk of a fixed size which idQueue uses to extend
// its storage. Chunks are kept in the pool to reduce allocations.
type eventQueueChunk struct {
	data      [eventQueueChunkSize]sharedMuxEvent
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

func (q *muxEventQueue) pushBack(event sharedMuxEvent) {
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

func (q *muxEventQueue) popFront() (sharedMuxEvent, bool) {
	//if q.first == q.last && q.read == q.write {
	//	return sharedMuxEvent{}, false
	//}
	if q.size == 0 {
		return sharedMuxEvent{}, false
	}
	if q.read == eventQueueChunkSize {
		removed := q.first
		q.first = q.first.nextChunk
		putPooledEventQueueChunk(removed)
		q.read = 0
	}
	res := q.first.data[q.read]
	q.first.data[q.read] = sharedMuxEvent{}
	q.read++
	q.size--
	return res, true
}

// remove releases allocations and zero out entries that belong to particular
// streamID. Queue size is reduced by amount of removed entries, but empty
// entries stay until their chunks are removed. Removed entries are not counted
// against capacity when determining overflow.
func (q *muxEventQueue) remove(ctx context.Context, streamID int64) {
	start := q.read
	for chunk := q.first; chunk != nil; chunk = chunk.nextChunk {
		max := eventQueueChunkSize
		if chunk.nextChunk == nil {
			max = q.write
		}
		for i := start; i < max; i++ {
			if chunk.data[i].streamID == streamID {
				chunk.data[i].alloc.Release(ctx)
				chunk.data[i] = sharedMuxEvent{}
				q.size--
			}
		}
		start = 0
	}
}

// removeAll removes aren releases all entries in the queue.
func (q *muxEventQueue) removeAll(ctx context.Context) {
	start := q.read
	for chunk := q.first; chunk != nil; {
		max := eventQueueChunkSize
		if chunk.nextChunk == nil {
			max = q.write
		}
		for i := start; i < max; i++ {
			chunk.data[i].alloc.Release(ctx)
			chunk.data[i] = sharedMuxEvent{}
		}
		next := chunk.nextChunk
		putPooledEventQueueChunk(chunk)
		chunk = next
		start = 0
	}
	q.first = q.last
	q.read = 0
	q.write = 0
	q.size = 0
}

func (q *muxEventQueue) len() int {
	return q.size
}
