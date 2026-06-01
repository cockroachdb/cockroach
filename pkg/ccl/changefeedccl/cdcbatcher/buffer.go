// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package cdcbatcher is responsible for creating conflict-free batches
// for the no-linger sink.
package cdcbatcher

import (
	"context"
	"hash/maphash"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/util/container/heap"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

var keySeed = maphash.MakeSeed()

// pendingEvent wraps an event with the arrival sequence assigned at
// Add time. We do this because when the Buffer gives batches
// to the workers in the no-linger sink it prioritizes the keys whose
// pending events were added to the buffer first (not necessarily the
// events with the oldest MVCC timestamp).
type pendingEvent[E any] struct {
	seq uint64
	ev  E
}

// keyHeapEntry tracks a key by its hash to avoid a []byte -> string
// allocation on every Add. Hash collisions are harmless: colliding
// keys share a FIFO and inflight slot, slightly over-serializing them.
type keyHeapEntry struct {
	hash uint64
	seq  uint64
}

// keyHeap contains the non-inflight keys which have pending events.
// The top of the heap will be those keys whose pending events were
// added first.
type keyHeap []keyHeapEntry

func (h keyHeap) Len() int             { return len(h) }
func (h keyHeap) Less(i, j int) bool   { return h[i].seq < h[j].seq }
func (h keyHeap) Swap(i, j int)        { h[i], h[j] = h[j], h[i] }
func (h *keyHeap) Push(x keyHeapEntry) { *h = append(*h, x) }
func (h *keyHeap) Pop() keyHeapEntry {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// ErrClosed is returned by Add and GetBatch after Close.
var ErrClosed = errors.New("cdcbatcher.Buffer is closed")

// Config configures a Buffer.
type Config[E any] struct {
	// MaxMessages is the limit for how big an individual batch
	// can be.
	MaxMessages int
	// Key extracts the row key from an event.
	Key func(E) []byte
	// HashKey hashes a row key to the value used for FIFO and inflight
	// indexing. If nil, defaults to maphash.Bytes. Override in tests.
	HashKey func([]byte) uint64
}

// Batch is the struct representing the events we have given
// to a worker to send to the sink.
type Batch[E any] struct {
	// Events are the events in the batch, in the order they were added.
	Events []E
}

// Buffer is responsible for creating conflict-free batches for
// the no-linger sink.
//
// Buffer stores all pending events by their key (hash).
// - Before any events come in for a key OR after they're all
// processed, the key will be MISSING. Not in the events map,
// not in the prioritization heap and not in the inflight map.
// - Once events come in for a key, it will be QUEUED. At that
// point, it will have a heap entry and one or more events,
// but will not be in the inflight map. (see Add)
// - Once the key makes its way into a batch, it will be INFLIGHT.
// (see GetBatch) It will be in the inflight map, but will not
// be in the heap. It may still have associated pending events if
//   - not all of its events fit into the pending batch or
//   - new events for that key came in while it was in flight.
//
// Once the batch is completed (see completeBatch), the associated
// keys will go from INFLIGHT back to QUEUED or MISSING.
//
// TODO(#170211): support multiple topics. The buffer currently assumes
// a single topic; multi-topic support will need a heap of per-topic
// FIFOs (or per-topic buffers).
type Buffer[E any] struct {
	cfg Config[E]

	mu struct {
		syncutil.Mutex
		cond *sync.Cond
		// nextSeq is the arrival sequence assigned to the next row
		// to be added by Add.
		nextSeq uint64
		// events holds the pending events indexed by key hash. Each
		// slice of pending events will be in the order they were added.
		events map[uint64][]pendingEvent[E]
		// heap is in charge of telling us which keys to prioritize when
		// giving a batch to a worker. Therefore, it does NOT contain
		// inflight keys, since including those in a batch would cause
		// ordering conflicts. The top of the heap is the non-inflight
		// key with the oldest pending message (i.e. the arrival seq
		// of the head of their FIFO, not MVCC timestamp).
		heap keyHeap
		// inflight is the set of key hashes currently held by a worker
		// between GetBatch and completeBatch.
		inflight map[uint64]struct{}
		// closed is set by Close. Once true, Add and GetBatch return
		// ErrClosed.
		closed bool
	}
}

// New returns a new Buffer.
func New[E any](cfg Config[E]) *Buffer[E] {
	if cfg.MaxMessages <= 0 {
		panic(errors.AssertionFailedf("MaxMessages must be > 0, got %d", cfg.MaxMessages))
	}
	if cfg.Key == nil {
		panic(errors.AssertionFailedf("Key must be non-nil"))
	}
	if cfg.HashKey == nil {
		cfg.HashKey = func(k []byte) uint64 { return maphash.Bytes(keySeed, k) }
	}
	b := &Buffer[E]{cfg: cfg}
	b.mu.events = make(map[uint64][]pendingEvent[E])
	b.mu.inflight = make(map[uint64]struct{})
	b.mu.cond = sync.NewCond(&b.mu)
	return b
}

// Add enqueues ev for eventual delivery in some future batch. The
// caller transfers ownership of ev to the buffer; ownership is
// returned to the consumer when the batch containing ev is completed
// (#170203).
func (b *Buffer[E]) Add(ctx context.Context, ev E) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.mu.closed {
		return ErrClosed
	}

	newEvent := pendingEvent[E]{seq: b.mu.nextSeq, ev: ev}
	b.mu.nextSeq++

	k := b.cfg.HashKey(b.cfg.Key(ev))
	if pendingEvents, ok := b.mu.events[k]; ok {
		b.mu.events[k] = append(pendingEvents, newEvent)
		// We don't need to touch the heap or signal: either k is inflight (and
		// must stay out of the heap), or k is already in the heap at an
		// earlier seq (and is already eligible for a worker to pick up).
		return nil
	}
	b.mu.events[k] = []pendingEvent[E]{newEvent}

	// Only push if no worker currently holds this key. Pushing an inflight
	// key would let another worker grab it concurrently and produce a batch
	// that conflicts with the in-flight one.
	if _, inflight := b.mu.inflight[k]; !inflight {
		heap.Push[keyHeapEntry](&b.mu.heap, keyHeapEntry{hash: k, seq: newEvent.seq})
	}

	b.mu.cond.Signal()
	return nil
}

// GetBatch returns the next batch of work, blocking until at least one
// non-inflight event is available. The caller owns the returned batch
// until it is completed via the consumer side (#170203).
func (b *Buffer[E]) GetBatch(ctx context.Context) (*Batch[E], error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.mu.closed {
		return nil, ErrClosed
	}
	defer func() {
		// Hand off any remaining work to another waiter.
		if b.mu.heap.Len() > 0 {
			b.mu.cond.Signal()
		}
	}()
	// TODO(#170203): respect ctx cancellation in the wait loop.
	for b.mu.heap.Len() == 0 && !b.mu.closed {
		b.mu.cond.Wait()
	}
	// NB: This buffer could have closed while we were in this wait
	// loop. In that case, we would like to return an error.
	if b.mu.closed {
		return nil, ErrClosed
	}
	batch := &Batch[E]{Events: make([]E, 0, b.cfg.MaxMessages)}
	for b.mu.heap.Len() > 0 {
		// GetBatch prevents conflicts between batches by creating batches
		// one key at a time. Those keys, though, are chosen in order of
		// their oldest pending event (not necessarily MVCC order).
		entry := heap.Pop[keyHeapEntry](&b.mu.heap)
		b.mu.inflight[entry.hash] = struct{}{}

		pendingEvents := b.mu.events[entry.hash]
		batchCapacity := b.cfg.MaxMessages - len(batch.Events)
		numToBatch := min(len(pendingEvents), batchCapacity)

		for _, pending := range pendingEvents[:numToBatch] {
			batch.Events = append(batch.Events, pending.ev)
		}

		b.mu.events[entry.hash] = pendingEvents[numToBatch:]
		if len(b.mu.events[entry.hash]) == 0 {
			delete(b.mu.events, entry.hash)
		}

		if len(batch.Events) == b.cfg.MaxMessages {
			return batch, nil
		}
	}
	return batch, nil
}

// TODO(#170203): add the two consumer-side entry points:
//   - completeBatch(batch *Batch): releases the inflight keys
//     held by batch, returns its events to the rowEvent pool and
//     releases each event's kvevent.Alloc, and re-pushes any released
//     keys that still have pending events back onto the heap at their
//     original arrival slot.
//   - drain() error: blocks until every event accepted by Add has
//     been pulled by a worker via GetBatch AND released via
//     completeBatch. Used by Sink.Flush. Returns ErrClosed
//     if Close races the drain.

// Close marks the buffer closed and wakes all waiters. Subsequent
// Add calls return ErrClosed immediately. Close is
// idempotent.
func (b *Buffer[E]) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.mu.closed = true
	b.mu.cond.Broadcast()
}
