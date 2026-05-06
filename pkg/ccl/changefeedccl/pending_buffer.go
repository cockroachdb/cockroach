// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// pendingBufferConfig configures a pendingBuffer.
type pendingBufferConfig struct {
	// maxMessages caps the number of rows in any one batch returned from
	// getBatch (not yet implemented). Reflects the downstream
	// SinkClient.Flush limit.
	maxMessages int
	// maxBytes caps the total byte size of any one batch returned from
	// getBatch (not yet implemented). Reflects the downstream
	// SinkClient.Flush limit.
	maxBytes int
	// bufferLimit caps the number of buffered events the pendingBuffer
	// will hold before addRow blocks. Bounds the queue's memory footprint
	// independently of the per-batch sink limits above. Honored once
	// backpressure lands in a later commit.
	bufferLimit int
}

// pendingBatch is the unit of work that getBatch (not yet implemented)
// will return and that completeBatch consumes. The events slice is owned
// by the worker until completeBatch is called, at which point the buffer
// releases the inflight-key entries getBatch stamped on it.
type pendingBatch struct {
	events   []*rowEvent
	numBytes int
	// inflightKeys are released by completeBatch from the buffer's
	// inflight set. In the M2 FIFO they would be redundant with
	// events[i].key, but M4's two-level heap will key inflight tracking
	// on (topic, key) rather than the raw key, so this slice carries the
	// canonical inflight identifiers chosen at getBatch time.
	inflightKeys [][]byte
}

// pendingBuffer is the queue between the changefeed's EmitRow producer
// and the noLingerSink's worker pool (the worker pool is not yet
// implemented). Producers call addRow to enqueue rows; workers will call
// getBatch (not yet implemented) to pull a batch and completeBatch when
// the downstream SinkClient.Flush returns. Workers will never see a batch
// that shares a key with an in-progress batch (conflict-free by
// construction).
//
// M2 implementation: events are held in a single FIFO slice and inflight
// tracking is keyed on the raw row key. Subsequent milestones replace
// this with a two-level heap keyed by (topic, key) for per-topic
// batching and oldest-first fairness across topics.
type pendingBuffer struct {
	cfg pendingBufferConfig

	// mu groups the buffer's mutable state. cond is signaled (or
	// broadcast) by addRow, completeBatch, and close (close is not yet
	// implemented) to wake waiters in getBatch and addRow.
	mu struct {
		syncutil.Mutex
		cond *sync.Cond
		// events is the FIFO of pending rows. addRow appends; getBatch
		// (not yet implemented) will remove in order, skipping rows
		// whose key is in inflight.
		events []*rowEvent
		// inflight is the set of keys currently held by some worker
		// (between getBatch and completeBatch). Map keys are
		// string([]byte). Will be excluded from new batches by getBatch
		// (not yet implemented).
		inflight map[string]struct{}
		// closed will be set by close (not yet implemented). Once true,
		// getBatch and addRow waiters wake and return a sentinel error.
		closed bool
	}
}

// newPendingBuffer returns a pendingBuffer with the given configuration.
func newPendingBuffer(cfg pendingBufferConfig) *pendingBuffer {
	b := &pendingBuffer{cfg: cfg}
	b.mu.inflight = make(map[string]struct{})
	b.mu.cond = sync.NewCond(&b.mu)
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
	b.mu.events = append(b.mu.events, ev)
	if log.V(2) {
		log.Changefeed.Infof(ctx, "pendingBuffer addRow key=%x (depth=%d)", ev.key, len(b.mu.events))
	}
	// Signal one waiter: a new event can be claimed by at most one worker.
	b.mu.cond.Signal()
	return nil
}

// completeBatch releases the inflight keys held by batch and returns its
// events to the rowEvent pool, releasing each event's kvevent.Alloc. The
// per-event releases happen after the lock is dropped so that N event
// releases do not serialize concurrent addRow / future getBatch calls.
//
// After this returns, the keys in batch.inflightKeys may appear in
// subsequent batches. ctx is forwarded to Alloc.Release; cancellation
// does not skip the release.
func (b *pendingBuffer) completeBatch(ctx context.Context, batch *pendingBatch) {
	b.releaseInflight(batch.inflightKeys)
	for _, ev := range batch.events {
		ev.alloc.Release(ctx)
		freeRowEvent(ev)
	}
}

// releaseInflight removes keys from the inflight set under the lock and
// broadcasts so any worker waiting because its candidate key was inflight
// (or, once backpressure lands, any addRow waiting on a full buffer) can
// make progress.
func (b *pendingBuffer) releaseInflight(keys [][]byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, k := range keys {
		delete(b.mu.inflight, string(k))
	}
	b.mu.cond.Broadcast()
}
