// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package concurrency

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/intentresolver"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// Default delay before pushing in order to detect dependency cycles.
const defaultDependencyCyclePushDelay = 100 * time.Millisecond

// Silence unused warning.
var _ = defaultDependencyCyclePushDelay

// lockTableWaiterImpl is an implementation of lockTableWaiter.
type lockTableWaiterImpl struct {
	nodeID  roachpb.NodeID
	stopper *stop.Stopper

	// Used to push conflicting transactions and resolve conflicting intents.
	ir IntentResolver

	// How long to wait until pushing conflicting transactions to detect
	// dependency cycles.
	dependencyCyclePushDelay time.Duration
}

// IntentResolver is an interface used by lockTableWaiterImpl to push
// transactions and to resolve intents. It contains only the subset of the
// intentresolver.IntentResolver interface that lockTableWaiterImpl needs.
type IntentResolver interface {
	// PushTransaction pushes the provided transaction. The method will push the
	// provided pushee transaction immediately, if possible. Otherwise, it will
	// block until the pushee transaction is finalized or eventually can be
	// pushed successfully.
	PushTransaction(
		context.Context, *enginepb.TxnMeta, roachpb.Header, roachpb.PushTxnType,
	) (roachpb.Transaction, *Error)

	// ResolveIntent resolves the provided intent according to the options.
	ResolveIntent(context.Context, roachpb.LockUpdate, intentresolver.ResolveOptions) *Error
}

// WaitOn implements the lockTableWaiter interface.
func (w *lockTableWaiterImpl) WaitOn(
	ctx context.Context, req Request, guard lockTableGuard,
) *Error {
	newStateC := guard.NewStateChan()
	ctxDoneC := ctx.Done()
	shouldQuiesceC := w.stopper.ShouldQuiesce()
	var timer *timeutil.Timer
	var timerC <-chan time.Time
	var timerWaitingState waitingState
	for {
		select {
		case <-newStateC:
			timerC = nil
			state := guard.CurState()
			if !state.held {
				// If the lock is not held and instead has a reservation, we don't
				// want to push the reservation transaction. A transaction push will
				// block until the pushee transaction has either committed, aborted,
				// pushed, or rolled back savepoints, i.e., there is some state
				// change that has happened to the transaction record that unblocks
				// the pusher. It will not unblock merely because a request issued
				// by the pushee transaction has completed and released a
				// reservation. Note that:
				// - reservations are not a guarantee that the lock will be acquired.
				// - the following two reasons to push do not apply to requests
				// holding reservations:
				//  1. competing requests compete at exactly one lock table, so there
				//  is no possibility of distributed deadlock due to reservations.
				//  2. the lock table can prioritize requests based on transaction
				//  priorities.
				//
				// TODO(sbhola): remove the need for this by only notifying waiters
				// for held locks and never for reservations.
				// TODO(sbhola): now that we never push reservation holders, we
				// should stop special-casing non-transactional writes and let them
				// acquire reservations.
				switch state.stateKind {
				case waitFor, waitForDistinguished:
					continue
				case waitElsewhere:
					return nil
				}
			}

			switch state.stateKind {
			case waitFor:
				// waitFor indicates that the request is waiting on another
				// transaction. This transaction may be the lock holder of a
				// conflicting lock or the head of a lock-wait queue that the
				// request is a part of.

				// For non-transactional requests, there's no need to perform
				// deadlock detection and the other "distinguished" (see below)
				// pusher will already push to detect coordinator failures and
				// abandoned locks, so there's no need to do anything in this
				// state.
				if req.Txn == nil {
					continue
				}

				// For transactional requests, the request should push to
				// resolve this conflict and detect deadlocks, but only after
				// delay. This delay avoids unnecessary push traffic when the
				// conflicting transaction is continuing to make forward
				// progress.
				delay := w.dependencyCyclePushDelay
				if hasMinPriority(state.txn) || hasMaxPriority(&req.Txn.TxnMeta) {
					// However, if the pushee has the minimum priority or if the
					// pusher has the maximum priority, push immediately.
					delay = 0
				}
				if timer == nil {
					timer = timeutil.NewTimer()
					defer timer.Stop()
				}
				timer.Reset(delay)
				timerC = timer.C
				timerWaitingState = state

			case waitForDistinguished:
				// waitForDistinguished is like waitFor, except it instructs the
				// waiter to immediately push the conflicting transaction instead of
				// first waiting out the dependencyCyclePushDelay. The lockTable
				// guarantees that there is always at least one request in the
				// waitForDistinguished state for each lock that has any waiters.
				//
				// The purpose of the waitForDistinguished state is to avoid adding
				// a delay of dependencyCyclePushDelay to the process of recovering
				// from the failure of a transaction coordinator for *each* of that
				// transaction's previously written intents. If we had a cache of
				// aborted transaction IDs that allowed us to notice and immediately
				// resolve abandoned intents then we might be able to get rid of
				// this state.
				if err := w.pushTxn(ctx, req, state); err != nil {
					return err
				}

			case waitElsewhere:
				// The lockTable has hit a memory limit and is no longer maintaining
				// proper lock wait-queues. However, the waiting request is still
				// not safe to proceed with evaluation because there is still a
				// transaction holding the lock. It should push the transaction it
				// is blocked on immediately to wait in that transaction's
				// txnWaitQueue. Once this completes, the request should stop
				// waiting on this lockTableGuard, as it will no longer observe
				// lock-table state transitions.
				return w.pushTxn(ctx, req, state)

			case waitSelf:
				// Another request from the same transaction is the reservation
				// holder of this lock wait-queue. This can only happen when the
				// request's transaction is sending multiple requests concurrently.
				// Proceed with waiting without pushing anyone.

			case doneWaiting:
				// The request has waited for all conflicting locks to be released
				// and is at the front of any lock wait-queues. It can now stop
				// waiting, re-acquire latches, and check the lockTable again for
				// any new conflicts. If it find none, it can proceed with
				// evaluation.
				return nil

			default:
				panic("unexpected waiting state")
			}

		case <-timerC:
			// If the transactional request was in the waitFor state and did not
			// observe any update to its state for a dependencyCyclePushDelay,
			// it should push. It may be the case that the transaction is part
			// of a dependency cycle.
			timerC = nil
			timer.Read = true
			if err := w.pushTxn(ctx, req, timerWaitingState); err != nil {
				return err
			}

		case <-ctxDoneC:
			return roachpb.NewError(ctx.Err())

		case <-shouldQuiesceC:
			return roachpb.NewError(&roachpb.NodeUnavailableError{})
		}
	}
}

func (w *lockTableWaiterImpl) pushTxn(ctx context.Context, req Request, ws waitingState) *Error {
	h := roachpb.Header{
		Timestamp:    req.Timestamp,
		UserPriority: req.Priority,
	}
	if req.Txn != nil {
		// We are going to hand the header (and thus the transaction proto)
		// to the RPC framework, after which it must not be changed (since
		// that could race). Since the subsequent execution of the original
		// request might mutate the transaction, make a copy here.
		//
		// See #9130.
		h.Txn = req.Txn.Clone()

		// We must push at least to h.Timestamp, but in fact we want to
		// go all the way up to a timestamp which was taken off the HLC
		// after our operation started. This allows us to not have to
		// restart for uncertainty as we come back and read.
		obsTS, ok := h.Txn.GetObservedTimestamp(w.nodeID)
		if !ok {
			// This was set earlier, so it's completely unexpected to
			// not be found now.
			return roachpb.NewErrorf("missing observed timestamp: %+v", h.Txn)
		}
		h.Timestamp.Forward(obsTS)
	}

	var pushType roachpb.PushTxnType
	switch ws.guardAccess {
	case spanset.SpanReadOnly:
		pushType = roachpb.PUSH_TIMESTAMP
	case spanset.SpanReadWrite:
		pushType = roachpb.PUSH_ABORT
	}

	pusheeTxn, err := w.ir.PushTransaction(ctx, ws.txn, h, pushType)
	if err != nil {
		return err
	}
	if !ws.held {
		return nil
	}

	// We always poison due to limitations of the API: not poisoning equals
	// clearing the AbortSpan, and if our pushee transaction first got pushed
	// for timestamp (by us), then (by someone else) aborted and poisoned, and
	// then we run the below code, we're clearing the AbortSpan illegaly.
	// Furthermore, even if our pushType is not PUSH_ABORT, we may have ended
	// up with the responsibility to abort the intents (for example if we find
	// the transaction aborted).
	//
	// To do better here, we need per-intent information on whether we need to
	// poison.
	resolve := roachpb.MakeLockUpdateWithDur(&pusheeTxn, roachpb.Span{Key: ws.key}, ws.dur)
	opts := intentresolver.ResolveOptions{Wait: false, Poison: true}
	return w.ir.ResolveIntent(ctx, resolve, opts)
}

func hasMinPriority(txn *enginepb.TxnMeta) bool {
	return txn.Priority == enginepb.MinTxnPriority
}

func hasMaxPriority(txn *enginepb.TxnMeta) bool {
	return txn.Priority == enginepb.MaxTxnPriority
}
