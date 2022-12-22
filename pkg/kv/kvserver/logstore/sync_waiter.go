// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package logstore

import (
	"context"

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

// SyncWaiterLoop waits on a sequence of in-progress disk writes, notifying
// callbacks when their corresponding disk writes have completed.
type SyncWaiterLoop struct {
	q       chan syncBatch
	stopped chan struct{}
}

type syncBatch struct {
	wg syncWaiter
	cb func()
}

// NewSyncWaiterLoop constructs a SyncWaiterLoop. It must be Started before use.
func NewSyncWaiterLoop() *SyncWaiterLoop {
	return &SyncWaiterLoop{
		// We size the waiter loop's queue to the same size as Pebble's sync
		// concurrency. This is the maximum number of pending syncWaiter's that
		// pebble allows.
		q:       make(chan syncBatch, record.SyncConcurrency),
		stopped: make(chan struct{}),
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
			w.wg.Close()
			w.cb()
		case <-stopper.ShouldQuiesce():
			return
		}
	}
}

// enqueue registers the syncWaiter with the SyncWaiterLoop. The provided
// callback will be called once the syncWaiter's associated disk write has been
// durably committed.
//
// The syncWaiter will be Closed after its SyncWait method completes. It must
// not be Closed by the caller.
//
// If the SyncWaiterLoop has already been stopped, the callback will never be
// called.
func (w *SyncWaiterLoop) enqueue(ctx context.Context, wg syncWaiter, cb func()) {
	b := syncBatch{wg, cb}
	select {
	case w.q <- b:
	case <-w.stopped:
	default:
		log.Warningf(ctx, "SyncWaiterLoop.enqueue blocking due to insufficient channel capacity")
		select {
		case w.q <- b:
		case <-w.stopped:
		}
	}
}
