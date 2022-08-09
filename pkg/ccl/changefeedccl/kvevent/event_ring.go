package kvevent

import (
	"github.com/cockroachdb/cockroach/pkg/util/ring"
)

// A ring buffer implementation for kvevents.

const eventsPerBlockExp = 8
const blockMask = 0xFF
const reclaimBlocksThreshold = 8

type eventChunk struct {
	chunk [1 << eventsPerBlockExp]Event
}

type eventRing struct {
	chunks        ring.Buffer
	len           int
	reclaimTarget int
}

func (r *eventRing) enqueue(e Event) {
	var ec *eventChunk
	if r.chunks.Len()<<eventsPerBlockExp < r.len+1 {
		ec = &eventChunk{}
		r.chunks.AddLast(ec)
	} else {
		ec = r.chunks.GetLast().(*eventChunk)
	}
	ec.chunk[r.len&blockMask] = e
	r.len++
	if r.len > reclaimBlocksThreshold<<eventsPerBlockExp {
		r.reclaimTarget = reclaimBlocksThreshold >> 1
	}
}

func (r *eventRing) dequeue() (Event, bool) {
	if r.len == 0 {
		return Event{}, false
	}
	r.len--
	e := r.chunks.GetLast().(*eventChunk).chunk[r.len&blockMask]
	if r.len&blockMask == 0 {
		r.chunks.RemoveLast()
	}
	if r.reclaimTarget > 0 && r.len < r.reclaimTarget<<eventsPerBlockExp {
		r.chunks.Resize(r.reclaimTarget)
		r.reclaimTarget >>= 1
	}
	return e, true
}

func (r *eventRing) empty() bool {
	return r.len == 0
}

func (r *eventRing) purge() {}
