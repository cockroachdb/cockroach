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
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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
	stopper *stop.Stopper
	ir      IntentResolver
	lm      LockManager

	// finalizedTxnCache is a small LRU cache that tracks transactions that
	// were pushed and found to be finalized (COMMITTED or ABORTED). It is
	// used as an optimization to avoid repeatedly pushing the transaction
	// record when cleaning up the intents of an abandoned transaction.
	//
	// NOTE: it probably makes sense to maintain a single finalizedTxnCache
	// across all Ranges on a Store instead of an individual cache per
	// Range. For now, we don't do this because we don't share any state
	// between separate concurrency.Manager instances.
	finalizedTxnCache txnCache

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
	// Used to defer the resolution of duplicate intents. Intended to allow
	// batching of intent resolution while cleaning up after abandoned txns. A
	// request may begin deferring intent resolution and then be forced to wait
	// again on other locks. This is ok, as the request that deferred intent
	// resolution will often be the new reservation holder for those intents'
	// keys. Even when this is not the case (e.g. the request is read-only so it
	// can't hold reservations), any other requests that slip ahead will simply
	// re-discover the intent(s) during evaluation and resolve them themselves.
	var deferredResolution []roachpb.LockUpdate
	defer w.resolveDeferredIntents(ctx, &err, &deferredResolution)
	for {
		select {
		case <-newStateC:
			timerC = nil
			state := guard.CurState()
			switch state.kind {
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

				// If we know that a lock holder is already finalized (COMMITTED
				// or ABORTED), there's no reason to push it again. Instead, we
				// can skip directly to intent resolution.
				//
				// As an optimization, we defer the intent resolution until the
				// we're done waiting on all conflicting locks in this function.
				// This allows us to accumulate a group of intents to resolve
				// and send them together as a batch.
				//
				// Remember that if the lock is held, there will be at least one
				// waiter with livenessPush = true (the distinguished waiter),
				// so at least one request will enter this branch and perform
				// the cleanup on behalf of all other waiters.
				if livenessPush {
					if pusheeTxn, ok := w.finalizedTxnCache.get(state.txn.ID); ok {
						resolve := roachpb.MakeLockUpdate(pusheeTxn, roachpb.Span{Key: state.key})
						deferredResolution = append(deferredResolution, resolve)

						// Inform the LockManager that the lock has been updated with a
						// finalized status so that it gets removed from the lockTable
						// and we are allowed to proceed.
						//
						// For unreplicated locks, this is all that is needed - the
						// lockTable is the source of truth so, once removed, the
						// unreplicated lock is gone. It is perfectly valid for us to
						// instruct the lock to be released because we know that the
						// lock's owner is finalized.
						//
						// For replicated locks, this is a bit of a lie. The lock hasn't
						// actually been updated yet, but we will be conducting intent
						// resolution in the future (before we observe the corresponding
						// MVCC state). This is safe because we already handle cases
						// where locks exist only in the MVCC keyspace and not in the
						// lockTable.
						//
						// In the future, we'd like to make this more explicit.
						// Specifically, we'd like to augment the lockTable with an
						// understanding of finalized but not yet resolved locks. These
						// locks will allow conflicting transactions to proceed with
						// evaluation without the need to first remove all traces of
						// them via a round of replication. This is discussed in more
						// detail in #41720. Specifically, see mention of "contention
						// footprint" and COMMITTED_BUT_NOT_REMOVABLE.
						w.lm.OnLockUpdated(ctx, &deferredResolution[len(deferredResolution)-1])
						continue
					}
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

// ClearCaches implements the lockTableWaiter interface.
func (w *lockTableWaiterImpl) ClearCaches() {
	w.finalizedTxnCache.clear()
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
	h := w.pushHeader(req)
	var pushType roachpb.PushTxnType
	switch ws.guardAccess {
	case spanset.SpanReadOnly:
		pushType = roachpb.PUSH_TIMESTAMP
		log.VEventf(ctx, 3, "pushing timestamp of txn %s above %s", ws.txn.ID.Short(), h.Timestamp)
	case spanset.SpanReadWrite:
		pushType = roachpb.PUSH_ABORT
		log.VEventf(ctx, 3, "pushing txn %s to abort", ws.txn.ID.Short())
	}

	pusheeTxn, err := w.ir.PushTransaction(ctx, ws.txn, h, pushType)
	if err != nil {
		return err
	}

	// If the transaction is finalized, add it to the finalizedTxnCache. This
	// avoids needing to push it again if we find another one of its locks and
	// allows for batching of intent resolution.
	if pusheeTxn.Status.IsFinalized() {
		w.finalizedTxnCache.add(pusheeTxn)
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
	resolve := roachpb.MakeLockUpdate(pusheeTxn, roachpb.Span{Key: ws.key})
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

func (w *lockTableWaiterImpl) pushHeader(req Request) roachpb.Header {
	h := roachpb.Header{
		Timestamp:    req.readConflictTimestamp(),
		UserPriority: req.Priority,
	}
	if req.Txn != nil {
		// We are going to hand the header (and thus the transaction proto) to
		// the RPC framework, after which it must not be changed (since that
		// could race). Since the subsequent execution of the original request
		// might mutate the transaction, make a copy here. See #9130.
		h.Txn = req.Txn.Clone()
	}
	return h
}

// resolveDeferredIntents resolves the batch of intents if the provided error is
// nil. The batch of intents may be resolved more efficiently than if they were
// resolved individually.
func (w *lockTableWaiterImpl) resolveDeferredIntents(
	ctx context.Context, err **Error, deferredResolution *[]roachpb.LockUpdate,
) {
	if (*err != nil) || (len(*deferredResolution) == 0) {
		return
	}
	// See pushLockTxn for an explanation of these options.
	opts := intentresolver.ResolveOptions{Poison: true}
	*err = w.ir.ResolveIntents(ctx, *deferredResolution, opts)
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
