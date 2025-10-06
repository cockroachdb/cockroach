// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logstore

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/pebble/record"
)

// syncWaiter is capable of waiting for a disk write to be durably committed.
type syncWaiter interface {
	// SyncWait waits for the write to be durable.
	SyncWait() error
	// Close closes the syncWaiter and releases associated resources.
	// Must be called after SyncWait returns.
	Close()
}

var _ syncWaiter = storage.Batch(nil)

// syncWaiterCallback is a callback provided to a SyncWaiterLoop.
// The callback is structured as an interface instead of a closure to allow
// users to batch the callback and its inputs into a single heap object, and
// then pool the allocation of that object.
type syncWaiterCallback interface {
	// run executes the callback.
	run()
}

// SyncWaiterLoop waits on a sequence of in-progress disk writes, notifying
// callbacks when their corresponding disk writes have completed.
// Invariant: The callbacks are notified in the order that they were enqueued
// and without concurrency.
type SyncWaiterLoop struct {
	q       chan syncBatch
	stopped chan struct{}

	logEveryEnqueueBlocked log.EveryN
}

type syncBatch struct {
	wg syncWaiter
	cb syncWaiterCallback
}

// NewSyncWaiterLoop constructs a SyncWaiterLoop. It must be Started before use.
func NewSyncWaiterLoop() *SyncWaiterLoop {
	return &SyncWaiterLoop{
		// We size the waiter loop's queue to twice the size of Pebble's sync
		// concurrency, which is the maximum number of pending syncWaiter's that
		// pebble allows. Doubling the size gives us headroom to prevent the sync
		// waiter loop from blocking on calls to enqueue, even if consumption from
		// the queue is delayed. If the pipeline is going to block, we'd prefer for
		// it to do so during the call to batch.CommitNoSyncWait.
		q:                      make(chan syncBatch, 2*record.SyncConcurrency),
		stopped:                make(chan struct{}),
		logEveryEnqueueBlocked: log.Every(1 * time.Second),
	}
}

// Start launches the loop.
func (w *SyncWaiterLoop) Start(ctx context.Context, stopper *stop.Stopper) {
	_ = stopper.RunAsyncTaskEx(ctx,
		stop.TaskOpts{
			TaskName: "raft-logstore-sync-waiter-loop",
			// This task doesn't reference a parent because it runs for the server's
			// lifetime.
			SpanOpt: stop.SterileRootSpan,
		},
		func(ctx context.Context) {
			w.waitLoop(ctx, stopper)
		})
}

// waitLoop pulls off the SyncWaiterLoop's queue. For each syncWaiter, it waits
// for the sync to complete and then calls the associated callback.
func (w *SyncWaiterLoop) waitLoop(ctx context.Context, stopper *stop.Stopper) {
	defer close(w.stopped)
	for {
		select {
		case w := <-w.q:
			if err := w.wg.SyncWait(); err != nil {
				log.Fatalf(ctx, "SyncWait error: %+v", err)
			}
			w.cb.run()
			w.wg.Close()
		case <-stopper.ShouldQuiesce():
			return
		}
	}
}

// enqueue registers the syncWaiter with the SyncWaiterLoop. The provided
// callback will be called once the syncWaiter's associated disk write has been
// durably committed. It may never be called in case the stopper stops.
//
// The syncWaiter will be Closed after its SyncWait method completes. It must
// not be Closed by the caller. The cb is called before the syncWaiter is
// closed, in case the cb implementation needs to extract something form the
// syncWaiter.
//
// If the SyncWaiterLoop has already been stopped, the callback will never be
// called.
func (w *SyncWaiterLoop) enqueue(ctx context.Context, wg syncWaiter, cb syncWaiterCallback) {
	b := syncBatch{wg, cb}
	select {
	case w.q <- b:
	case <-w.stopped:
	default:
		if w.logEveryEnqueueBlocked.ShouldLog() {
			// NOTE: we don't expect to hit this because we size the enqueue channel
			// with enough capacity to hold more in-progress sync operations than
			// Pebble allows (pebble/record.SyncConcurrency). However, we can still
			// see this in cases where consumption from the queue is delayed.
			log.VWarningf(ctx, 1, "SyncWaiterLoop.enqueue blocking due to insufficient channel capacity")
		}
		select {
		case w.q <- b:
		case <-w.stopped:
		}
	}
}
