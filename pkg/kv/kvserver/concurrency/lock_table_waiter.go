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
	"bytes"
	"context"
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/intentresolver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
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
	// - a per-store cache of recently detected abandoned transaction IDs
	// - a per-range reverse index from transaction ID to locked keys
	//
	// EDIT: The finalizedTxnCache gets us part of the way here. It allows us to
	// pay the liveness push delay cost once per abandoned transaction per range
	// instead of once per each of an abandoned transaction's locks. This helped
	// us to feel comfortable increasing the default delay from the original
	// 10ms to the current 50ms. Still, to feel comfortable increasing this
	// further, we'd want to improve this cache (e.g. lifting it to the store
	// level) to reduce the cost to once per abandoned transaction per store.
	//
	// TODO(nvanbenschoten): continue increasing this default value.
	50*time.Millisecond,
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
	st      *cluster.Settings
	clock   *hlc.Clock
	stopper *stop.Stopper
	ir      IntentResolver
	lt      lockTable

	// When set, WriteIntentError are propagated instead of pushing
	// conflicting transactions.
	disableTxnPushing bool
	// When set, called just before each ContentionEvent is emitted.
	// Is allowed to mutate the event.
	onContentionEvent func(ev *roachpb.ContentionEvent)

	// Metric reporting intent resolution failures.
	conflictingIntentsResolveRejections *metric.Counter
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
	) (*roachpb.Transaction, *Error)

	// ResolveIntent synchronously resolves the provided intent.
	ResolveIntent(context.Context, roachpb.LockUpdate, intentresolver.ResolveOptions) *Error

	// ResolveIntents synchronously resolves the provided batch of intents.
	ResolveIntents(context.Context, []roachpb.LockUpdate, intentresolver.ResolveOptions) *Error
}

// WaitOn implements the lockTableWaiter interface.
func (w *lockTableWaiterImpl) WaitOn(
	ctx context.Context, req Request, guard lockTableGuard,
) (err *Error) {
	newStateC := guard.NewStateChan()
	ctxDoneC := ctx.Done()
	shouldQuiesceC := w.stopper.ShouldQuiesce()
	// Used to delay liveness and deadlock detection pushes.
	var timer *timeutil.Timer
	var timerC <-chan time.Time
	var timerWaitingState waitingState

	h := contentionEventHelper{
		sp:      tracing.SpanFromContext(ctx),
		onEvent: w.onContentionEvent,
	}
	defer h.emit()

	for {
		select {
		// newStateC will be signaled for the transaction we are currently
		// contending on. We will continue to receive updates about this
		// transaction until it no longer contends with us, at which point
		// either one of the other channels fires or we receive state
		// about another contending transaction on newStateC.
		case <-newStateC:
			timerC = nil
			state := guard.CurState()
			log.Eventf(ctx, "lock wait-queue event: %s", state)
			h.emitAndInit(state)
			switch state.kind {
			case waitFor, waitForDistinguished:
				if req.WaitPolicy == lock.WaitPolicy_Error {
					// If the waiter has an Error wait policy, resolve the conflict
					// immediately without waiting. If the conflict is a lock then
					// push the lock holder's transaction using a PUSH_TOUCH to
					// determine whether the lock is abandoned or whether its holder
					// is still active. If the conflict is a reservation holder,
					// raise an error immediately, we know the reservation holder is
					// active.
					if state.held {
						err = w.pushLockTxn(ctx, req, state)
					} else {
						err = newWriteIntentErr(state)
					}
					if err != nil {
						return err
					}
					continue
				}

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
				// *each* of that transaction's previously written intents.
				livenessPush := state.kind == waitForDistinguished
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

				if delay > 0 {
					if timer == nil {
						timer = timeutil.NewTimer()
						defer timer.Stop()
					}
					timer.Reset(delay)
					timerC = timer.C
				} else {
					// If we don't want to delay the push, don't use a real timer.
					// Doing so is both a waste of resources and, more importantly,
					// makes TestConcurrencyManagerBasic flaky because there's no
					// guarantee that the timer will fire before the goroutine enters
					// a "select" waiting state on the next iteration of this loop.
					timerC = closedTimerC
				}
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
				// Note that the lockTable "claims" the list to resolve when this
				// waiter is transitioning to doneWaiting, to increase the likelihood
				// that this waiter will indeed do the resolution. However, it is
				// possible for this transition to doneWaiting to race with
				// cancellation of the request and slip in after the cancellation and
				// before lockTable.Dequeue() is called. This will result in the locks
				// being removed from the lockTable data-structure without subsequent
				// resolution. Another requester will discover these locks during
				// evaluation and add them back to the lock table data-structure. See
				// the comment in lockTableImpl.tryActiveWait for the proper way to
				// remove this and other evaluation races.
				toResolve := guard.ResolveBeforeScanning()
				return w.ResolveDeferredIntents(ctx, toResolve)

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
			if timer != nil {
				timer.Read = true
			}

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
				if errors.Is(pushCtx.Err(), context.Canceled) {
					// Ignore the context canceled error. If this was for the
					// parent context then we'll notice on the next select.
					err = nil
				}
				pushCancel()
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

// WaitOnLock implements the lockTableWaiter interface.
func (w *lockTableWaiterImpl) WaitOnLock(
	ctx context.Context, req Request, intent *roachpb.Intent,
) *Error {
	sa, _, err := findAccessInSpans(intent.Key, req.LockSpans)
	if err != nil {
		return roachpb.NewError(err)
	}
	return w.pushLockTxn(ctx, req, waitingState{
		kind:        waitFor,
		txn:         &intent.Txn,
		key:         intent.Key,
		held:        true,
		guardAccess: sa,
	})
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
		return newWriteIntentErr(ws)
	}

	// Construct the request header and determine which form of push to use.
	h := w.pushHeader(req)
	var pushType roachpb.PushTxnType
	switch req.WaitPolicy {
	case lock.WaitPolicy_Block:
		// This wait policy signifies that the request wants to wait until the
		// conflicting lock is released. For read-write conflicts, try to push
		// the lock holder's timestamp forward so the read request can read
		// under the lock. For write-write conflicts, try to abort the lock
		// holder entirely so the write request can revoke and replace the lock
		// with its own lock.
		switch ws.guardAccess {
		case spanset.SpanReadOnly:
			pushType = roachpb.PUSH_TIMESTAMP
			log.VEventf(ctx, 2, "pushing timestamp of txn %s above %s", ws.txn.ID.Short(), h.Timestamp)

		case spanset.SpanReadWrite:
			pushType = roachpb.PUSH_ABORT
			log.VEventf(ctx, 2, "pushing txn %s to abort", ws.txn.ID.Short())
		}

	case lock.WaitPolicy_Error:
		// This wait policy signifies that the request wants to raise an error
		// upon encountering a conflicting lock. We still need to push the lock
		// holder to ensure that it is active and that this isn't an abandoned
		// lock, but we push using a PUSH_TOUCH to immediately return an error
		// if the lock hold is still active.
		pushType = roachpb.PUSH_TOUCH
		log.VEventf(ctx, 2, "pushing txn %s to check if abandoned", ws.txn.ID.Short())

	default:
		log.Fatalf(ctx, "unexpected WaitPolicy: %v", req.WaitPolicy)
	}

	pusheeTxn, err := w.ir.PushTransaction(ctx, ws.txn, h, pushType)
	if err != nil {
		// If pushing with an Error WaitPolicy and the push fails, then the lock
		// holder is still active. Transform the error into a WriteIntentError.
		if _, ok := err.GetDetail().(*roachpb.TransactionPushError); ok && req.WaitPolicy == lock.WaitPolicy_Error {
			err = newWriteIntentErr(ws)
		}
		return err
	}

	// If the transaction is finalized, add it to the finalizedTxnCache. This
	// avoids needing to push it again if we find another one of its locks and
	// allows for batching of intent resolution.
	if pusheeTxn.Status.IsFinalized() {
		w.lt.TransactionIsFinalized(pusheeTxn)
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
	// TODO(sumeer): it is possible that the lock is an unreplicated lock,
	// for which doing intent resolution is unnecessary -- we only need
	// to remove it from the lock table data-structure.
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
	resolve := roachpb.MakeLockUpdate(pusheeTxn, roachpb.Span{Key: ws.key})
	opts := intentresolver.ResolveOptions{Poison: true}
	if err := w.ir.ResolveIntent(ctx, resolve, opts); err != nil {
		// If pusheeTxn was finalized, then we record that cleanup was unsuccessful,
		// otherwise transaction should be cleaning up intents by itself.
		if pusheeTxn.Status.IsFinalized() {
			w.conflictingIntentsResolveRejections.Inc(1)
		}
		return err
	}
	return nil
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
	h := w.pushHeader(req)
	pushType := roachpb.PUSH_ABORT
	log.VEventf(ctx, 3, "pushing txn %s to detect request deadlock", ws.txn.ID.Short())

	_, err := w.ir.PushTransaction(ctx, ws.txn, h, pushType)
	if err != nil {
		return err
	}

	// Even if the push succeeded and aborted the other transaction to break a
	// deadlock, there's nothing for the pusher to clean up. The conflicting
	// request will quickly exit the lock wait-queue and release its reservation
	// once it notices that it is aborted and the pusher will be free to proceed
	// because it was not waiting on any locks. If the pusher's request does end
	// up hitting a lock which the pushee fails to clean up, it will perform the
	// cleanup itself using pushLockTxn.
	//
	// It may appear that there is a bug here in the handling of request-only
	// dependency cycles. If such a cycle was broken by simultaneously aborting
	// the transactions responsible for each of the request, there would be no
	// guarantee that an aborted pusher would notice that its own transaction
	// was aborted before it notices that its pushee's transaction was aborted.
	// For example, in the simplest case, imagine two requests deadlocked on
	// each other. If their transactions are both aborted and each push notices
	// the pushee is aborted first, they will both return here triumphantly and
	// wait for the other to exit its lock wait-queues, leading to deadlock.
	// Even if they eventually pushed each other again, there would be no
	// guarantee that the same thing wouldn't happen.
	//
	// However, such a situation is not possible in practice because such a
	// dependency cycle is never constructed by the lockTable. The lockTable
	// assigns each request a monotonically increasing sequence number upon its
	// initial entrance to the lockTable. This sequence number is used to
	// straighten out dependency chains of requests such that a request only
	// waits on conflicting requests with lower sequence numbers than its own
	// sequence number. This behavior guarantees that request-only dependency
	// cycles are never constructed by the lockTable. Put differently, all
	// dependency cycles must include at least one dependency on a lock and,
	// therefore, one call to pushLockTxn. Unlike pushRequestTxn, pushLockTxn
	// actively removes the conflicting lock and removes the dependency when it
	// determines that its pushee transaction is aborted. This means that the
	// call to pushLockTxn will continue to make forward progress in the case of
	// a simultaneous abort of all transactions behind the members of the cycle,
	// preventing such a hypothesized deadlock from ever materializing.
	//
	// Example:
	//
	//  req(1, txn1), req(1, txn2) are both waiting on a lock held by txn3, and
	//  they respectively hold a reservation on key "a" and key "b". req(2, txn2)
	//  queues up behind the reservation on key "a" and req(2, txn1) queues up
	//  behind the reservation on key "b". Now the dependency cycle between txn1
	//  and txn2 only involves requests, but some of the requests here also
	//  depend on a lock. So when both txn1, txn2 are aborted, the req(1, txn1),
	//  req(1, txn2) are guaranteed to eventually notice through self-directed
	//  QueryTxn requests and will exit the lockTable, allowing req(2, txn1) and
	//  req(2, txn2) to get the reservation and now they no longer depend on each
	//  other.
	//
	return nil
}

// pushHeader returns a BatchRequest header to be used for pushing other
// transactions on behalf of the provided request.
func (w *lockTableWaiterImpl) pushHeader(req Request) roachpb.Header {
	h := roachpb.Header{
		Timestamp:    req.Timestamp,
		UserPriority: req.Priority,
	}
	if req.Txn != nil {
		// We are going to hand the header (and thus the transaction proto) to
		// the RPC framework, after which it must not be changed (since that
		// could race). Since the subsequent execution of the original request
		// might mutate the transaction, make a copy here. See #9130.
		h.Txn = req.Txn.Clone()

		// We must push at least to req.Timestamp, but for transactional
		// requests we actually want to go all the way up to the top of the
		// transaction's uncertainty interval. This allows us to not have to
		// restart for uncertainty if the push succeeds and we come back and
		// read.
		//
		// NOTE: GlobalUncertaintyLimit is effectively synthetic because it does
		// not come from an HLC clock, but it does not currently get marked as
		// so. See the comment in roachpb.MakeTransaction. This synthetic flag
		// is then removed if we call Backward(clock.Now()) below.
		uncertaintyLimit := req.Txn.GlobalUncertaintyLimit.WithSynthetic(true)
		if !h.Timestamp.Synthetic {
			// Because we intend to read on the same node, we can limit this to a
			// clock reading from the local clock, relying on the fact that an
			// observed timestamp from this node will limit our local uncertainty
			// limit when we return to read.
			//
			// We intentionally do not use an observed timestamp directly to limit
			// the push timestamp, because observed timestamps are not applicable in
			// some cases (e.g. across lease changes). So to avoid an infinite loop
			// where we continue to push to an unusable observed timestamp and
			// continue to find the pushee in our uncertainty interval, we instead
			// use the present time to limit the push timestamp, which is less
			// optimal but is guaranteed to progress.
			//
			// There is some inherent raciness here, because the lease may move
			// between when we push and when we later read. In such cases, we may
			// need to push again, but expect to eventually succeed in reading,
			// either after lease movement subsides or after the reader's read
			// timestamp surpasses its global uncertainty limit.
			//
			// However, this argument only holds if we expect to be able to use a
			// local uncertainty limit when we return to read the pushed intent.
			// Notably, local uncertainty limits can not be used to ignore intents
			// with synthetic timestamps that would otherwise be in a reader's
			// uncertainty interval. This is because observed timestamps do not
			// apply to intents/values with synthetic timestamps. So if we know
			// that we will be pushing an intent to a synthetic timestamp, we
			// don't limit the value to a clock reading from the local clock.
			uncertaintyLimit.Backward(w.clock.Now())
		}
		h.Timestamp.Forward(uncertaintyLimit)
	}
	return h
}

// ResolveDeferredIntents implements the lockTableWaiter interface.
func (w *lockTableWaiterImpl) ResolveDeferredIntents(
	ctx context.Context, deferredResolution []roachpb.LockUpdate,
) *Error {
	if len(deferredResolution) == 0 {
		return nil
	}
	// See pushLockTxn for an explanation of these options.
	opts := intentresolver.ResolveOptions{Poison: true}
	err := w.ir.ResolveIntents(ctx, deferredResolution, opts)
	if err != nil {
		w.conflictingIntentsResolveRejections.Inc(int64(len(deferredResolution)))
	}
	return err
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

// txnCache is a small LRU cache that holds Transaction objects.
//
// The zero value of this struct is ready for use.
type txnCache struct {
	mu   syncutil.Mutex
	txns [8]*roachpb.Transaction // [MRU, ..., LRU]
}

func (c *txnCache) get(id uuid.UUID) (*roachpb.Transaction, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if idx := c.getIdxLocked(id); idx >= 0 {
		txn := c.txns[idx]
		c.moveFrontLocked(txn, idx)
		return txn, true
	}
	return nil, false
}

func (c *txnCache) add(txn *roachpb.Transaction) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if idx := c.getIdxLocked(txn.ID); idx >= 0 {
		c.moveFrontLocked(txn, idx)
	} else {
		c.insertFrontLocked(txn)
	}
}

func (c *txnCache) clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := range c.txns {
		c.txns[i] = nil
	}
}

func (c *txnCache) getIdxLocked(id uuid.UUID) int {
	for i, txn := range c.txns {
		if txn != nil && txn.ID == id {
			return i
		}
	}
	return -1
}

func (c *txnCache) moveFrontLocked(txn *roachpb.Transaction, cur int) {
	copy(c.txns[1:cur+1], c.txns[:cur])
	c.txns[0] = txn
}

func (c *txnCache) insertFrontLocked(txn *roachpb.Transaction) {
	copy(c.txns[1:], c.txns[:])
	c.txns[0] = txn
}

// contentionEventHelper tracks and emits ContentionEvents.
type contentionEventHelper struct {
	sp      *tracing.Span
	onEvent func(event *roachpb.ContentionEvent) // may be nil

	// Internal.
	ev     *roachpb.ContentionEvent
	tBegin time.Time
}

// emit emits the open contention event, if any.
func (h *contentionEventHelper) emit() {
	if h.ev == nil {
		return
	}
	h.ev.Duration = timeutil.Since(h.tBegin)
	if h.onEvent != nil {
		// NB: this is intentionally above the call to RecordStructured so that
		// this interceptor gets to mutate the event (used for test determinism).
		h.onEvent(h.ev)
	}
	h.sp.RecordStructured(h.ev)
	h.ev = nil
}

// emitAndInit compares the waitingState's active txn (if any) against the current
// ContentionEvent (if any). If the they match, we are continuing to handle the
// same event and no action is taken. If they differ, the open event (if any) is
// finalized and added to the Span, and a new event initialized from the inputs.
func (h *contentionEventHelper) emitAndInit(s waitingState) {
	if h.sp == nil {
		// No span to attach payloads to - don't do any work.
		//
		// TODO(tbg): we could special case the noop span here too, but the plan is for
		// nobody to use noop spans any more (trace.mode=background).
		return
	}

	// If true, we want to emit the current event and possibly start a new one.
	// Otherwise,
	switch s.kind {
	case waitFor, waitForDistinguished, waitSelf:
		// If we're tracking an event and see a different txn/key, the event is
		// done and we initialize the new event tracking the new txn/key.
		//
		// NB: we're guaranteed to have `s.{txn,key}` populated here.
		if h.ev != nil &&
			(!h.ev.TxnMeta.ID.Equal(s.txn.ID) || !bytes.Equal(h.ev.Key, s.key)) {
			h.emit() // h.ev is now nil
		}

		if h.ev == nil {
			h.ev = &roachpb.ContentionEvent{
				Key:     s.key,
				TxnMeta: *s.txn,
			}
			h.tBegin = timeutil.Now()
		}
	case waitElsewhere, doneWaiting:
		// If we have an event, emit it now and that's it - the case we're in
		// does not give us a new transaction/key.
		if h.ev != nil {
			h.emit()
		}
	default:
		panic("unhandled waitingState.kind")
	}
}

func newWriteIntentErr(ws waitingState) *Error {
	return roachpb.NewError(&roachpb.WriteIntentError{
		Intents: []roachpb.Intent{roachpb.MakeIntent(ws.txn, ws.key)},
	})
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

var closedTimerC chan time.Time

func init() {
	closedTimerC = make(chan time.Time)
	close(closedTimerC)
}
