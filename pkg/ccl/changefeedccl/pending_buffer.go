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
	"github.com/cockroachdb/errors"
)

// errPendingBufferClosed is returned by addRow and getBatch after close
// has been called.
var errPendingBufferClosed = errors.New("pendingBuffer is closed")

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
	// broadcast) by addRow, completeBatch, and close to wake waiters in
	// getBatch and addRow.
	mu struct {
		syncutil.Mutex
		cond *sync.Cond
		// events is the FIFO of pending rows. addRow appends; getBatch
		// removes in order, skipping rows whose key is in inflight.
		events []*rowEvent
		// inflight is the set of keys currently held by some worker
		// (between getBatch and completeBatch). Map keys are
		// string([]byte). Excluded from new batches by getBatch.
		inflight map[string]struct{}
		// closed is set by close. Once true, addRow returns
		// errPendingBufferClosed; getBatch returns it once the FIFO
		// drains.
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
// addRow blocks while the buffer holds bufferLimit events, returning
// only once a worker drains it (or close is called). ctx is checked
// once before the lock and not re-checked across cond.Wait iterations;
// close is the supported wakeup path on cancellation.
func (b *pendingBuffer) addRow(ctx context.Context, ev *rowEvent) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for !b.mu.closed && len(b.mu.events) >= b.cfg.bufferLimit {
		b.mu.cond.Wait()
	}
	if b.mu.closed {
		return errPendingBufferClosed
	}
	b.mu.events = append(b.mu.events, ev)
	if log.V(2) {
		log.Changefeed.Infof(ctx, "pendingBuffer addRow key=%x (depth=%d)", ev.key, len(b.mu.events))
	}
	// Signal one waiter: a new event can be claimed by at most one worker.
	b.mu.cond.Signal()
	return nil
}

// getBatch returns the next batch of work, blocking until at least one
// non-inflight event is available. The caller owns the returned batch
// until it calls completeBatch, which releases the inflight keys back
// to the buffer.
//
// ctx is checked once, before the first lock acquisition, and not
// re-checked across cond.Wait iterations. A goroutine blocked in
// cond.Wait will not wake on ctx.Done; close is the supported wakeup
// path on cancellation.
func (b *pendingBuffer) getBatch(ctx context.Context) (*pendingBatch, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for {
		// Drain has precedence over closed: a closed buffer with
		// remaining non-inflight events still serves them.
		if batch := b.buildBatchLocked(); batch != nil {
			return batch, nil
		}
		if b.mu.closed {
			return nil, errPendingBufferClosed
		}
		b.mu.cond.Wait()
	}
}

// buildBatchLocked walks the FIFO and picks events whose key is not
// inflight, up to maxMessages and maxBytes. Returns nil if no event can
// be selected. On success, marks the batch's keys as inflight and
// removes the chosen events from the FIFO. b.mu must be held.
//
// A single event whose own size already exceeds maxBytes is admitted
// (otherwise it would never be sent); the limit is then respected as a
// "no further events" rule for that batch.
func (b *pendingBuffer) buildBatchLocked() *pendingBatch {
	batch := &pendingBatch{}
	batchHasKey := make(map[string]struct{})
	keep := make([]*rowEvent, 0, len(b.mu.events))
	for _, ev := range b.mu.events {
		if len(batch.events) >= b.cfg.maxMessages {
			keep = append(keep, ev)
			continue
		}
		keyStr := string(ev.key)
		if _, conflict := b.mu.inflight[keyStr]; conflict {
			keep = append(keep, ev)
			continue
		}
		sz := len(ev.key) + len(ev.val)
		if len(batch.events) > 0 && batch.numBytes+sz > b.cfg.maxBytes {
			keep = append(keep, ev)
			continue
		}
		batch.events = append(batch.events, ev)
		batch.numBytes += sz
		if _, dup := batchHasKey[keyStr]; !dup {
			batchHasKey[keyStr] = struct{}{}
			batch.inflightKeys = append(batch.inflightKeys, ev.key)
			b.mu.inflight[keyStr] = struct{}{}
		}
	}
	if len(batch.events) == 0 {
		return nil
	}
	b.mu.events = keep
	// Broadcast: a batch can free many slots at once, potentially
	// admitting multiple addRow waiters.
	b.mu.cond.Broadcast()
	return batch
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
// (or any addRow waiting on a full buffer) can make progress.
func (b *pendingBuffer) releaseInflight(keys [][]byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, k := range keys {
		delete(b.mu.inflight, string(k))
	}
	b.mu.cond.Broadcast()
}

// close marks the buffer closed and wakes all waiters. Subsequent
// addRow calls return errPendingBufferClosed immediately. Pending
// events still drain through getBatch until the FIFO is empty, after
// which getBatch returns errPendingBufferClosed. In-flight batches
// already returned by getBatch can still be completed (completeBatch
// does not consult closed).
//
// Events buffered at close time but not pulled by any getBatch are
// dropped without releasing their kvevent.Allocs. This matches the
// existing batchingSink.Close semantics — the changefeed restarts from
// its last resolved checkpoint after a sink shutdown — but should be
// revisited once the noLingerSink worker pool lands and we can reason
// about who owns cleanup on shutdown.
//
// close is idempotent.
func (b *pendingBuffer) close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.mu.closed = true
	b.mu.cond.Broadcast()
}
