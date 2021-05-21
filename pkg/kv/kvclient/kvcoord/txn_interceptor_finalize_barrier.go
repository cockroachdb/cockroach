// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvcoord

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// txnFinalizeBarrier is a barrier for EndTxn(commit=false) requests, forcing
// them (and any requests that arrive while they are processed) to wait until
// any in-flight requests have completed before being continuing. While the
// TxnCoordSender assumes synchronous client operation, EndTnx(commit=false) can
// be sent asynchronously due to e.g. the txnHeartbeater or client disconnects.
//
// This is necessary because the txnPipeliner, which is further below in the
// interceptor stack, will attach the lock spans and in-flight writes to the
// EndTxn request. However, the txnPipeliner only updates this state _after_ the
// requests have completed, so if we send off EndTxn before these have returned
// then it will not contain the complete state.
//
// TODO(erikgrinaker): it doesn't seem necessary to send additional EndTxn
// requests after one succeeds, so we should possibly have a fast-path to
// abort those here.
type txnFinalizeBarrier struct {
	wrapped lockedSender

	// mu contains state protected by the TxnCoordSender's mutex.
	mu struct {
		sync.Locker

		// ifReqs counts the number of current in-flight requests.
		ifReqs uint64

		// queue contains a FIFO queue of signal channels for requests that are
		// waiting for earlier requests to complete. When nil, it indicates that
		// the barrier is lowered and requests flow freely. If non-nil, all
		// incoming requests must check for in-flight and queued requests,
		// and if any are found they must wait in the queue. This serializes all
		// requests until the barrier is lowered by setting queue=nil again.
		//
		// We don't normally check for in-flight requests because the client
		// protocol is synchronous and we want violations to return errors.
		// However, asynchronous rollbacks inject themselves into this flow, and
		// must queue for in-flight requests. Furthermore, client requests and
		// additional rollbacks must queue behind them while they are processed.
		// Otherwise, client requests may be sent concurrently with the async
		// rollbacks, which would trigger the txnLockGatekeeper concurrency
		// assertion.
		//
		// Once the queue is cleared out and there are no further in-flight
		// requests, the barrier can be safely lowered by setting the queue to
		// nil. This shouldn't make much difference though, since we don't
		// expect many requests to arrive afterwards.
		queue []chan struct{}
	}
}

// SendLocked is part of the txnInterceptor interface.
func (b *txnFinalizeBarrier) SendLocked(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	et, hasET := ba.GetArg(roachpb.EndTxn)
	isRollback := hasET && !et.(*roachpb.EndTxnRequest).Commit

	// If this is a rollback request, it may have been sent asynchronously. We
	// therefore raise the barrier by setting b.mu.queue non-nil. This instructs
	// both the rollback request *and subsequent requests* to queue if they find
	// any in-flight or queued requests.
	if isRollback && b.mu.queue == nil {
		b.mu.queue = []chan struct{}{} // raise barrier
	}

	// If the barrier is active and there are queued or in-flight requests, we
	// must queue behind them. The b.mu.queue != nil (barrier active) condition
	// is important: it is necessary to queue a non-rollback request that
	// arrives immediately after a lone rollback request is sent. Without this,
	// it would be sent concurrently with the rollback and violate the
	// txnLockGatekeeper concurrency assertion.
	if b.mu.queue != nil && (b.mu.ifReqs > 0 || len(b.mu.queue) > 0) {
		waitC := make(chan struct{})
		b.mu.queue = append(b.mu.queue, waitC)
		b.mu.Unlock()
		select {
		case <-waitC:
		case <-ctx.Done():
		}
		b.mu.Lock()
		// We must remove from the queue here, because we must guarantee that a
		// lone waiter in the queue is sent next, even if an incoming request
		// acquires b.mu.Lock() before the waiter does. The waiter therefore has
		// to remove itself from the queue only after acquiring the lock.
		//
		// If the context was cancelled, we're not necessarily first in line.
		for i, queueC := range b.mu.queue {
			if waitC == queueC {
				b.mu.queue = append(b.mu.queue[:i], b.mu.queue[i+1:]...)
				break
			}
		}
		if err := ctx.Err(); err != nil {
			if len(b.mu.queue) == 0 && b.mu.ifReqs == 0 {
				b.mu.queue = nil // lower barrier if safe
			}
			return nil, roachpb.NewError(err)
		}
	}

	b.mu.ifReqs++
	defer func() {
		b.mu.ifReqs--

		// If this was the last in-flight request, we signal the next waiter to
		// resume, if any. If no waiters nor in-flight requests are found, we
		// can safely lower the barrier by setting b.mu.queue = nil.
		if b.mu.ifReqs == 0 {
			if len(b.mu.queue) > 0 {
				close(b.mu.queue[0])
			} else {
				b.mu.queue = nil // lower barrier
			}
		}
	}()

	return b.wrapped.SendLocked(ctx, ba)
}

// setWrapped is part of the txnInterceptor interface.
func (b *txnFinalizeBarrier) setWrapped(wrapped lockedSender) { b.wrapped = wrapped }

// populateLeafInputState is part of the txnInterceptor interface.
func (b *txnFinalizeBarrier) populateLeafInputState(tis *roachpb.LeafTxnInputState) {}

// populateLeafFinalState is part of the txnInterceptor interface.
func (b *txnFinalizeBarrier) populateLeafFinalState(tfs *roachpb.LeafTxnFinalState) {}

// importLeafFinalState is part of the txnInterceptor interface.
func (b *txnFinalizeBarrier) importLeafFinalState(ctx context.Context, tfs *roachpb.LeafTxnFinalState) {
}

// epochBumpedLocked is part of the txnInterceptor interface.
func (b *txnFinalizeBarrier) epochBumpedLocked() {}

// createSavepointLocked is part of the txnInterceptor interface.
func (b *txnFinalizeBarrier) createSavepointLocked(ctx context.Context, sp *savepoint) {}

// rollbackToSavepointLocked is part of the txnInterceptor interface.
func (*txnFinalizeBarrier) rollbackToSavepointLocked(context.Context, savepoint) {}

// closeLocked is part of the txnInterceptor interface.
func (*txnFinalizeBarrier) closeLocked() {
	// We don't clear out the waiters here, since some of them may be e.g. async
	// txn cleanups that should get a chance to complete.
}
