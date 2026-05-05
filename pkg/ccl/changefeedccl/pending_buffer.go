// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// pendingBufferConfig configures a pendingBuffer.
type pendingBufferConfig struct {
	// maxMessages caps the number of rows in any one batch returned from
	// getBatch. Reflects the downstream SinkClient.Flush limit.
	maxMessages int
	// maxBytes caps the total byte size of any one batch returned from
	// getBatch. Reflects the downstream SinkClient.Flush limit.
	maxBytes int
	// bufferLimit caps the number of buffered events the pendingBuffer
	// will hold before addRow blocks. Bounds the queue's memory footprint
	// independently of the per-batch sink limits above.
	bufferLimit int
}

// pendingBatch is the unit of work returned by pendingBuffer.getBatch and
// consumed by pendingBuffer.completeBatch. The events slice is owned by
// the worker until completeBatch is called, at which point the buffer
// releases the inflight-key entries it stamped on getBatch.
type pendingBatch struct {
	events       []*rowEvent
	numBytes     int
	inflightKeys [][]byte
}

// pendingBuffer is the queue between the changefeed's EmitRow producer
// and the noLingerSink's worker pool. Producers call addRow to enqueue
// rows; workers call getBatch to pull a batch and completeBatch when the
// downstream SinkClient.Flush returns. Workers never see a batch that
// shares a key with an in-progress batch (conflict-free by construction).
//
// M2 implementation: events are held in a single FIFO slice and inflight
// tracking is keyed on the raw row key. Subsequent milestones replace
// this with a two-level heap keyed by (topic, key) for per-topic batching
// and oldest-first fairness across topics.
type pendingBuffer struct {
	cfg pendingBufferConfig

	// mu protects all fields below. cond is signaled (or broadcast) by
	// addRow, completeBatch, and close to wake waiters in getBatch and
	// addRow.
	mu   sync.Mutex
	cond *sync.Cond

	// events is the FIFO of pending rows. addRow appends; getBatch
	// removes in order, skipping rows whose key is in inflight.
	events []*rowEvent
	// inflight is the set of keys currently held by some worker (between
	// getBatch and completeBatch). Map keys are string([]byte). Excluded
	// from new batches by getBatch.
	inflight map[string]struct{}
	// closed is set by close. When true, getBatch and addRow waiters wake
	// and return a sentinel error.
	closed bool
}

// newPendingBuffer returns a pendingBuffer with the given configuration.
func newPendingBuffer(cfg pendingBufferConfig) *pendingBuffer {
	b := &pendingBuffer{
		cfg:      cfg,
		inflight: make(map[string]struct{}),
	}
	b.cond = sync.NewCond(&b.mu)
	return b
}

// addRow enqueues ev for eventual delivery in some future batch. The
// caller transfers ownership of ev (and its kvevent.Alloc) to the buffer;
// completeBatch will release the alloc and recycle the rowEvent once the
// batch containing ev is finished.
//
// In M2 addRow does not block on the buffer being full. Backpressure is
// added in a subsequent commit; for now ctx is checked only as a
// best-effort cancellation surface before taking the lock.
func (b *pendingBuffer) addRow(ctx context.Context, ev *rowEvent) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.events = append(b.events, ev)
	if log.V(2) {
		log.Changefeed.Infof(ctx, "pendingBuffer addRow key=%x (depth=%d)", ev.key, len(b.events))
	}
	// Signal one waiter: a new event can be claimed by at most one worker.
	b.cond.Signal()
	return nil
}

// completeBatch releases the inflight keys held by batch and returns its
// events to the rowEvent pool, releasing each event's kvevent.Alloc.
// After this returns, the keys in batch.inflightKeys may appear in
// subsequent batches.
func (b *pendingBuffer) completeBatch(ctx context.Context, batch *pendingBatch) {
	b.mu.Lock()
	for _, k := range batch.inflightKeys {
		delete(b.inflight, string(k))
	}
	for _, ev := range batch.events {
		ev.alloc.Release(ctx)
		freeRowEvent(ev)
	}
	// Broadcast: releasing inflight keys may newly unblock multiple
	// workers (events that were skipped because their key was inflight),
	// and once backpressure lands, addRow waiters too.
	b.cond.Broadcast()
	b.mu.Unlock()
}
