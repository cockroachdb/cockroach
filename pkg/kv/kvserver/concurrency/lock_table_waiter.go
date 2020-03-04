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
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/intentresolver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// LockTableLivenessPushDelay sets the delay before pushing in order to detect
// coordinator failures of conflicting transactions.
var LockTableLivenessPushDelay = settings.RegisterDurationSetting(
	"kv.lock_table.coordinator_liveness_push_delay",
	"the delay before pushing in order to detect coordinator failures of conflicting transactions",
	// This is set to a short duration to ensure that we quickly detect failed
	// transaction coordinators that have abandoned one or many locks. We don't
	// want to wait out a long timeout on each of these locks to detect that
	// they are abandoned. However, we also don't want to push immediately in
	// cases where the lock is going to be resolved shortly.
	//
	// We could increase this default to somewhere on the order of the
	// transaction heartbeat timeout (5s) if we had a better way to avoid paying
	// the cost on each of a transaction's abandoned locks and instead only pay
	// it once per abandoned transaction per range or per node. This could come
	// in a few different forms, including:
	// - a per-wide cache of recently detected abandoned transaction IDs
	// - a per-range reverse index from transaction ID to locked keys
	//
	// TODO(nvanbenschoten): increasing this default value.
	10*time.Millisecond,
)

// LockTableDeadlockDetectionPushDelay sets the delay before pushing in order to
// detect dependency cycles between transactions.
var LockTableDeadlockDetectionPushDelay = settings.RegisterDurationSetting(
	"kv.lock_table.deadlock_detection_push_delay",
	"the delay before pushing in order to detect dependency cycles between transactions",
	// This is set to a medium duration to ensure that deadlock caused by
	// dependency cycles between transactions are eventually detected, but that
	// the deadlock detection does not impose any overhead in the vastly common
	// case where there are no dependency cycles. We optimistically assume that
	// deadlocks are not common in production applications and wait locally on
	// locks for a while before checking for a deadlock. Increasing this value
	// reduces the amount of time wasted in needless deadlock checks, but slows
	// down reporting of real deadlock scenarios.
	//
	// The value is analogous to Postgres' deadlock_timeout setting, which has a
	// default value of 1s:
	//  https://www.postgresql.org/docs/current/runtime-config-locks.html#GUC-DEADLOCK-TIMEOUT.
	//
	// We could increase this default to somewhere around 250ms - 1000ms if we
	// confirmed that we do not observe deadlocks in any of the workloads that
	// we care about. When doing so, we should be conscious that even once
	// distributed deadlock detection begins, there is some latency proportional
	// to the length of the dependency cycle before the deadlock is detected.
	//
	// TODO(nvanbenschoten): increasing this default value.
	100*time.Millisecond,
)

// lockTableWaiterImpl is an implementation of lockTableWaiter.
type lockTableWaiterImpl struct {
	nodeID  roachpb.NodeID
	st      *cluster.Settings
	stopper *stop.Stopper
	ir      IntentResolver

	// When set, WriteIntentError are propagated instead of pushing
	// conflicting transactions.
	disableTxnPushing bool
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
	// Used to delay liveness and deadlock detection pushes.
	var timer *timeutil.Timer
	var timerC <-chan time.Time
	var timerWaitingState waitingState
	for {
		select {
		case <-newStateC:
			timerC = nil
			state := guard.CurState()
			switch state.stateKind {
			case waitFor, waitForDistinguished:
				// waitFor indicates that the request is waiting on another
				// transaction. This transaction may be the lock holder of a
				// conflicting lock or the head of a lock-wait queue that the
				// request is a part of.
				//
				// waitForDistinguished is like waitFor, except it instructs the
				// waiter to quickly push the conflicting transaction after a short
				// liveness push delay instead of waiting out the full deadlock
				// detection push delay. The lockTable guarantees that there is
				// always at least one request in the waitForDistinguished state for
				// each lock that has any waiters.
				//
				// The purpose of the waitForDistinguished state is to avoid waiting
				// out the longer deadlock detection delay before recognizing and
				// recovering from the failure of a transaction coordinator for
				// *each* of that transaction's previously written intents. If we
				// had a cache of aborted transaction IDs that allowed us to notice
				// and quickly resolve abandoned intents then we might be able to
				// get rid of this state.
				livenessPush := state.stateKind == waitForDistinguished
				deadlockPush := true

				// If the conflict is a reservation holder and not a held lock then
				// there's no need to perform a liveness push - the request must be
				// alive or its context would have been canceled and it would have
				// exited its lock wait-queues.
				if !state.held {
					livenessPush = false
				}

				// For non-transactional requests, there's no need to perform
				// deadlock detection because a non-transactional request can
				// not be part of a dependency cycle. Non-transactional requests
				// cannot hold locks or reservations.
				if req.Txn == nil {
					deadlockPush = false
				}

				// If the request doesn't want to perform a push for either
				// reason, continue waiting.
				if !livenessPush && !deadlockPush {
					continue
				}

				// The request should push to detect abandoned locks due to
				// failed transaction coordinators, detect deadlocks between
				// transactions, or both, but only after delay. This delay
				// avoids unnecessary push traffic when the conflicting
				// transaction is continuing to make forward progress.
				delay := time.Duration(math.MaxInt64)
				if livenessPush {
					delay = minDuration(delay, LockTableLivenessPushDelay.Get(&w.st.SV))
				}
				if deadlockPush {
					delay = minDuration(delay, LockTableDeadlockDetectionPushDelay.Get(&w.st.SV))
				}

				// However, if the pushee has the minimum priority or if the
				// pusher has the maximum priority, push immediately.
				// TODO(nvanbenschoten): flesh these interactions out more and
				// add some testing.
				if hasMinPriority(state.txn) || hasMaxPriority(req.Txn) {
					delay = 0
				}

				if timer == nil {
					timer = timeutil.NewTimer()
					defer timer.Stop()
				}
				timer.Reset(delay)
				timerC = timer.C
				timerWaitingState = state

			case waitElsewhere:
				// The lockTable has hit a memory limit and is no longer maintaining
				// proper lock wait-queues.
				if !state.held {
					// If the lock is not held, exit immediately. Requests will
					// be ordered when acquiring latches.
					return nil
				}
				// The waiting request is still not safe to proceed with
				// evaluation because there is still a transaction holding the
				// lock. It should push the transaction it is blocked on
				// immediately to wait in that transaction's txnWaitQueue. Once
				// this completes, the request should stop waiting on this
				// lockTableGuard, as it will no longer observe lock-table state
				// transitions.
				return w.pushLockTxn(ctx, req, state)

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
			// If the request was in the waitFor or waitForDistinguished states
			// and did not observe any update to its state for the entire delay,
			// it should push. It may be the case that the transaction is part
			// of a dependency cycle or that the lock holder's coordinator node
			// has crashed.
			timerC = nil
			timer.Read = true

			// If the request is conflicting with a held lock then it pushes its
			// holder synchronously - there is no way it will be able to proceed
			// until the lock's transaction undergoes a state transition (either
			// completing or being pushed) and then updates the lock's state
			// through intent resolution. The request has a dependency on the
			// entire conflicting transaction.
			//
			// However, if the request is conflicting with another request (a
			// reservation holder) then it pushes the reservation holder
			// asynchronously while continuing to listen to state transition in
			// the lockTable. This allows the request to cancel its push if the
			// conflicting reservation exits the lock wait-queue without leaving
			// behind a lock. In this case, the request has a dependency on the
			// conflicting request but not necessarily the entire conflicting
			// transaction.
			var err *Error
			if timerWaitingState.held {
				err = w.pushLockTxn(ctx, req, timerWaitingState)
			} else {
				// It would be more natural to launch an async task for the push
				// and continue listening on this goroutine for lockTable state
				// transitions, but doing so is harder to test against. Instead,
				// we launch an async task to listen to lockTable state and
				// synchronously push. If the watcher goroutine detects a
				// lockTable change, it cancels the context on the push.
				pushCtx, pushCancel := context.WithCancel(ctx)
				go w.watchForNotifications(pushCtx, pushCancel, newStateC)
				err = w.pushRequestTxn(pushCtx, req, timerWaitingState)
				pushCancel()
				if pushCtx.Err() == context.Canceled {
					// Ignore the context canceled error. If this was for the
					// parent context then we'll notice on the next select.
					err = nil
				}
			}
			if err != nil {
				return err
			}

		case <-ctxDoneC:
			return roachpb.NewError(ctx.Err())

		case <-shouldQuiesceC:
			return roachpb.NewError(&roachpb.NodeUnavailableError{})
		}
	}
}

// pushLockTxn pushes the holder of the provided lock.
//
// The method blocks until the lock holder transaction experiences a state
// transition such that it no longer conflicts with the pusher's request. The
// method then synchronously updates the lock to trigger a state transition in
// the lockTable that will free up the request to proceed. If the method returns
// successfully then the caller can expect to have an updated waitingState.
func (w *lockTableWaiterImpl) pushLockTxn(
	ctx context.Context, req Request, ws waitingState,
) *Error {
	if w.disableTxnPushing {
		return roachpb.NewError(&roachpb.WriteIntentError{
			Intents: []roachpb.Intent{roachpb.MakeIntent(ws.txn, ws.key)},
		})
	}

	// Determine which form of push to use. For read-write conflicts, try to
	// push the lock holder's timestamp forward so the read request can read
	// under the lock. For write-write conflicts, try to abort the lock holder
	// entirely so the write request can revoke and replace the lock with its
	// own lock.
	var pushType roachpb.PushTxnType
	switch ws.guardAccess {
	case spanset.SpanReadOnly:
		pushType = roachpb.PUSH_TIMESTAMP
	case spanset.SpanReadWrite:
		pushType = roachpb.PUSH_ABORT
	}

	h := w.pushHeader(req)
	pusheeTxn, err := w.ir.PushTransaction(ctx, ws.txn, h, pushType)
	if err != nil {
		return err
	}

	// If the push succeeded then the lock holder transaction must have
	// experienced a state transition such that it no longer conflicts with
	// the pusher's request. This state transition could have been any of the
	// following, each of which would be captured in the pusheeTxn proto:
	// 1. the pushee was committed
	// 2. the pushee was aborted
	// 3. the pushee was pushed to a higher provisional commit timestamp such
	//    that once its locks are updated to reflect this, they will no longer
	//    conflict with the pusher request. This is only applicable if pushType
	//    is PUSH_TIMESTAMP.
	// 4. the pushee rolled back all sequence numbers that it held the
	//    conflicting lock at. This allows the lock to be revoked entirely.
	//    TODO(nvanbenschoten): we do not currently detect this case. Doing so
	//    would not be useful until we begin eagerly updating a transaction's
	//    record upon rollbacks to savepoints.
	//
	// Update the conflicting lock to trigger the desired state transition in
	// the lockTable itself, which will allow the request to proceed.
	//
	// We always poison due to limitations of the API: not poisoning equals
	// clearing the AbortSpan, and if our pushee transaction first got pushed
	// for timestamp (by us), then (by someone else) aborted and poisoned, and
	// then we run the below code, we're clearing the AbortSpan illegaly.
	// Furthermore, even if our pushType is not PUSH_ABORT, we may have ended up
	// with the responsibility to abort the intents (for example if we find the
	// transaction aborted). To do better here, we need per-intent information
	// on whether we need to poison.
	resolve := roachpb.MakeLockUpdateWithDur(&pusheeTxn, roachpb.Span{Key: ws.key}, ws.dur)
	opts := intentresolver.ResolveOptions{Poison: true}
	return w.ir.ResolveIntent(ctx, resolve, opts)
}

// pushRequestTxn pushes the owner of the provided request.
//
// The method blocks until either the pusher's transaction is aborted or the
// pushee's transaction is finalized (committed or aborted). If the pusher's
// transaction is aborted then the method will send an error on the channel and
// the pusher should exit its lock wait-queues. If the pushee's transaction is
// finalized then the method will send no error on the channel. The pushee is
// expected to notice that it has been aborted during its next attempt to push
// another transaction and will exit its lock wait-queues.
//
// However, the method responds to context cancelation and will terminate the
// push attempt if its context is canceled. This allows the caller to revoke a
// push if it determines that the pushee is no longer blocking the request. The
// caller is expected to terminate the push if it observes any state transitions
// in the lockTable. As such, the push is only expected to be allowed to run to
// completion in cases where requests are truly deadlocked.
func (w *lockTableWaiterImpl) pushRequestTxn(
	ctx context.Context, req Request, ws waitingState,
) *Error {
	// Regardless of whether the waiting request is reading from or writing to a
	// key, it always performs a PUSH_ABORT when pushing a conflicting request
	// because it wants to block until either a) the pushee or the pusher is
	// aborted due to a deadlock or b) the request exits the lock wait-queue and
	// the caller of this function cancels the push.
	pushType := roachpb.PUSH_ABORT

	h := w.pushHeader(req)
	_, err := w.ir.PushTransaction(ctx, ws.txn, h, pushType)
	// Even if the push succeeded and aborted the other transaction to break a
	// deadlock, there's nothing for the pusher to clean up. The conflicting
	// request will quickly exit the lock wait-queue and release its reservation
	// once it notices that it is aborted and the pusher will be free to proceed
	// because it was not waiting on any locks. If the pusher's request does end
	// up hitting a lock which the pushee fails to clean up, it will perform the
	// cleanup itself using pushLockTxn.
	return err
}

func (w *lockTableWaiterImpl) pushHeader(req Request) roachpb.Header {
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
		if ok {
			h.Timestamp.Forward(obsTS)
		}
	}
	return h
}

// watchForNotifications selects on the provided channel and watches for any
// updates. If the channel is ever notified, it calls the provided context
// cancelation function and exits.
func (w *lockTableWaiterImpl) watchForNotifications(
	ctx context.Context, cancel func(), newStateC chan struct{},
) {
	select {
	case <-newStateC:
		// Re-signal the channel.
		select {
		case newStateC <- struct{}{}:
		default:
		}
		// Cancel the context of the async task.
		cancel()
	case <-ctx.Done():
	}
}

func hasMinPriority(txn *enginepb.TxnMeta) bool {
	return txn != nil && txn.Priority == enginepb.MinTxnPriority
}

func hasMaxPriority(txn *roachpb.Transaction) bool {
	return txn != nil && txn.Priority == enginepb.MaxTxnPriority
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
