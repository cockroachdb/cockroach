// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/util/container/heap"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// pendingEvent wraps a rowEvent with the arrival sequence assigned at
// addRow time. We do this because when the pending buffer gives batches
// to the workers in the no-linger sink it prioritizes the keys whose
// pending events were added to the buffer first (not necessarily the
// events with the oldest MVCC timestamp).
type pendingEvent struct {
	seq uint64
	ev  *rowEvent
}

type keyHeapEntry struct {
	key string
	seq uint64
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
func (h *keyHeap) Peek() keyHeapEntry {
	return (*h)[0]
}

var errPendingBufferClosed = errors.New("pendingBuffer is closed")

type pendingBufferConfig struct {
	// maxMessages is the limit for how big an individual batch
	// can be.
	maxMessages int
}

// pendingBatch is the struct representing the events we have given
// to a worker to send to the sink.
type pendingBatch struct {
	events []*rowEvent
	// completed is guarded by pendingBuffer.mu; flipped to true by the
	// first completeBatch call. Used to detect double-completion.
	completed bool
}

// pendingBuffer is responsible for creating conflict-free batches for
// the no-linger sink.
type pendingBuffer struct {
	cfg pendingBufferConfig
	// TODO(#170211): support multiple topics.
	topic TopicDescriptor

	mu struct {
		syncutil.Mutex
		cond *sync.Cond
		// nextSeq is the arrival sequence assigned to the next row
		// to be added by AddRow.
		nextSeq uint64
		// events holds the pending events indexed by key. Each keys
		// slice of pending events will be in the order they were added.
		events map[string][]pendingEvent
		// heap orders non-inflight keys by the arrival seq of the head
		// of their FIFO.
		heap keyHeap
		// inflight is the set of keys currently held by a worker between
		// getBatch and completeBatch. Each inflight key records its
		// oldest inflight events seq so drain can track its progress.
		inflight map[string]uint64
		// closed is set by close. Once true, addRow and getBatch return
		// errPendingBufferClosed.
		closed bool
	}
}

func newPendingBuffer(cfg pendingBufferConfig, topic TopicDescriptor) *pendingBuffer {
	b := &pendingBuffer{cfg: cfg, topic: topic}
	b.mu.events = make(map[string][]pendingEvent)
	b.mu.inflight = make(map[string]uint64)
	b.mu.cond = sync.NewCond(&b.mu)
	return b
}

// addRow enqueues ev for eventual delivery in some future batch. The
// caller transfers ownership of ev (and its kvevent.Alloc) to the
// buffer; completeBatch will release the alloc and recycle the rowEvent
// once the batch containing ev is finished.
func (b *pendingBuffer) addRow(ctx context.Context, ev *rowEvent) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.mu.closed {
		return errPendingBufferClosed
	}

	pending := pendingEvent{seq: b.mu.nextSeq, ev: ev}
	b.mu.nextSeq++

	k := string(ev.key)
	if fifo, ok := b.mu.events[k]; ok {
		b.mu.events[k] = append(fifo, pending)
		return nil
	}
	b.mu.events[k] = []pendingEvent{pending}

	// This maintains the heap invariant that it contains no inflight keys.
	if _, inflight := b.mu.inflight[k]; !inflight {
		heap.Push[keyHeapEntry](&b.mu.heap, keyHeapEntry{key: k, seq: pending.seq})
	}

	b.mu.cond.Signal()
	return nil
}

// getBatch returns the next batch of work, blocking until at least one
// non-inflight event is available. The caller owns the returned batch
// until it calls completeBatch.
func (b *pendingBuffer) getBatch(ctx context.Context) (*pendingBatch, error) {
	batch := &pendingBatch{}
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.waitForCondOrCancel(ctx, func() bool {
		return b.mu.heap.Len() > 0 || b.mu.closed
	}); err != nil {
		return nil, err
	}
	if b.mu.closed {
		return nil, errPendingBufferClosed
	}
	for b.mu.heap.Len() > 0 {
		// getBatch prevents conflicts between batches by creating batches
		// one key at a time. Those keys, though, are chosen in order of
		// their oldest pending event (not necessarily MVCC order).
		entry := heap.Pop[keyHeapEntry](&b.mu.heap)
		b.mu.inflight[entry.key] = entry.seq

		fifo := b.mu.events[entry.key]
		remaining := b.cfg.maxMessages - len(batch.events)
		if len(fifo) > remaining {
			// There are too many messages for this key to fit them all
			// into this batch. Fit the ones we can into this batch.
			// The rest will continue living in fifo but will not be
			// put into batches until this batch completes.
			for _, pending := range fifo[:remaining] {
				batch.events = append(batch.events, pending.ev)
			}
			b.mu.events[entry.key] = fifo[remaining:]
			return batch, nil
		}

		for _, pending := range fifo {
			batch.events = append(batch.events, pending.ev)
		}
		delete(b.mu.events, entry.key)
		if len(batch.events) == b.cfg.maxMessages {
			return batch, nil
		}
	}
	return batch, nil
}

// completeBatch releases the inflight keys held by batch and returns
// its events to the rowEvent pool, releasing each event's
// kvevent.Alloc. It also adds any inflight keys back to the heap if
// they have pending events.
func (b *pendingBuffer) completeBatch(batch *pendingBatch) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if batch.completed {
		panic(errors.AssertionFailedf("completeBatch called twice on same batch"))
	}
	batch.completed = true

	for _, ev := range batch.events {
		inflightKey := string(ev.key)
		if _, inflight := b.mu.inflight[inflightKey]; !inflight {
			// If we've already seen an event for this key in this
			// batch, we will have already removed it from inflight.
			continue
		}

		delete(b.mu.inflight, inflightKey)
		if pendingEvents, ok := b.mu.events[inflightKey]; ok {
			if len(pendingEvents) == 0 {
				panic(errors.AssertionFailedf("events map has empty FIFO for inflight key instead of nil"))
			}
			// The events in pendingEvents are sorted by the order we
			// recieved them. The seq value of the first event is the
			// one we use for the heap.
			firstPendingEvent := pendingEvents[0]
			heapEntry := keyHeapEntry{key: inflightKey, seq: firstPendingEvent.seq}
			heap.Push[keyHeapEntry](&b.mu.heap, heapEntry)
		}
	}
	b.mu.cond.Broadcast()
}

// drain blocks until every event accepted by addRow has been pulled
// by a worker via getBatch AND released via completeBatch. Used by
// Sink.Flush. Returns errPendingBufferClosed if close races with the
// drain — a closed buffer cannot guarantee the pre-drain events were
// actually flushed.
func (b *pendingBuffer) drain(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	seqToReach := b.mu.nextSeq
	hasPreDrainWork := func() bool {
		// By the heap invariant, its min entry has the seq of the
		// oldest pending event for all non-inflight keys.
		if b.mu.heap.Len() > 0 && b.mu.heap.Peek().seq < seqToReach {
			return true
		}
		for _, keyProgress := range b.mu.inflight {
			if keyProgress < seqToReach {
				return true
			}
		}
		return false
	}
	if err := b.waitForCondOrCancel(ctx, func() bool {
		return !hasPreDrainWork() || b.mu.closed
	}); err != nil {
		return err
	}
	if b.mu.closed {
		return errPendingBufferClosed
	}
	return nil
}

// waitForCondOrCancel waits on b.mu.cond until pred returns true or
// ctx is cancelled. Must be called with b.mu held. Returns ctx.Err()
// on cancellation, nil otherwise.
func (b *pendingBuffer) waitForCondOrCancel(ctx context.Context, pred func() bool) error {
	if pred() {
		return nil
	}

	stop := make(chan struct{})
	defer close(stop)
	go func() {
		// Check for context cancellation in a separate goroutine.
		// Broadcast when the context is cancelled so others can
		// return as well.
		select {
		case <-ctx.Done():
			b.mu.Lock()
			defer b.mu.Unlock()
			b.mu.cond.Broadcast()
		case <-stop:
		}
	}()

	for !pred() && ctx.Err() == nil {
		b.mu.cond.Wait()
	}
	return ctx.Err()
}

// close marks the buffer closed and wakes all waiters. Subsequent
// addRow calls return errPendingBufferClosed immediately. close is
// idempotent.
func (b *pendingBuffer) close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.mu.closed = true
	b.mu.cond.Broadcast()
}
