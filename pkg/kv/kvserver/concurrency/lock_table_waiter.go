// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package concurrency

import (
	"context"
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/intentresolver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/txnwait"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"go.opentelemetry.io/otel/attribute"
)

// LockTableDeadlockOrLivenessDetectionPushDelay sets the delay before pushing
// in order to detect dependency cycles between transactions or coordinator
// failure of conflicting transactions.
var LockTableDeadlockOrLivenessDetectionPushDelay = settings.RegisterDurationSetting(
	settings.SystemOnly,
	"kv.lock_table.deadlock_detection_push_delay",
	"the delay before pushing in order to detect dependency cycles between transactions",
	// Transactions that come across contending locks must push the lock holder
	// to detect whether they're abandoned or are part of a dependency cycle that
	// would cause a deadlock. We optimistically assume failed transaction
	// coordinators and deadlocks are not common in production applications, and
	// wait locally on locks for a while before checking for a deadlock. This
	// reduces needless pushing in cases where there is no deadlock or coordinator
	// failure to speak of.
	//
	// Assuming we care about detecting failed coordinators more than deadlocks
	// (which are a true rarity and a case we don't want to optimize for),
	// increasing the value of this setting would come down to optimizing the cost
	// we pay per-abandoned transaction. Currently, with the txnStatusCache,
	// we pay a cost per abandoned transaction per range. The txnStatusCache is
	// also quite small. Some improvements here could be:
	// - A per-store cache instead of a per-range cache.
	// - Increasing the number of finalized transactions we keep track of, even if
	// this is per-request instead of per-range.
	//
	// The deadlock portion of this setting is analogous to Postgres'
	// deadlock_timeout setting, which has a default value of 1s:
	//  https://www.postgresql.org/docs/current/runtime-config-locks.html#GUC-DEADLOCK-TIMEOUT.
	//
	// When increasing this value, we should be conscious that even once
	// distributed deadlock detection begins, there is some latency proportional
	// to the length of the dependency cycle before the deadlock is detected.
	//
	//
	// TODO(nvanbenschoten): increasing this default value.
	100*time.Millisecond,
	settings.WithName("kv.lock_table.deadlock_detection_or_liveness_push_delay"),
	// Old setting name when liveness and deadlock push delays could be set
	// independently.
	settings.WithRetiredName("kv.lock_table.coordinator_liveness_push_delay"),
)

// lockTableWaiterImpl is an implementation of lockTableWaiter.
type lockTableWaiterImpl struct {
	nodeDesc *roachpb.NodeDescriptor
	st       *cluster.Settings
	clock    *hlc.Clock
	stopper  *stop.Stopper
	ir       IntentResolver
	lt       lockTable

	// When set, WriteIntentErrors are propagated instead of pushing
	// conflicting transactions.
	disableTxnPushing bool
	// When set, called just before each push timer event is processed.
	onPushTimer func()
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
		context.Context, *enginepb.TxnMeta, kvpb.Header, kvpb.PushTxnType,
	) (*roachpb.Transaction, bool, *Error)

	// ResolveIntent synchronously resolves the provided intent.
	ResolveIntent(context.Context, roachpb.LockUpdate, intentresolver.ResolveOptions) *Error

	// ResolveIntents synchronously resolves the provided batch of intents.
	ResolveIntents(context.Context, []roachpb.LockUpdate, intentresolver.ResolveOptions) *Error
}

// WaitOn implements the lockTableWaiter interface.
func (w *lockTableWaiterImpl) WaitOn(
	ctx context.Context, req Request, guard lockTableGuard,
) *Error {
	newStateC := guard.NewStateChan()
	ctxDoneC := ctx.Done()
	shouldQuiesceC := w.stopper.ShouldQuiesce()
	// Used to delay liveness and deadlock detection pushes.
	var timer timeutil.Timer
	defer timer.Stop()
	var timerC <-chan time.Time
	var timerWaitingState waitingState
	// Used to enforce lock timeouts.
	var lockDeadline time.Time

	tracer := newContentionEventTracer(tracing.SpanFromContext(ctx), w.clock)
	// Make sure the contention time info is finalized when exiting the function.
	defer tracer.notify(ctx, waitingState{kind: doneWaiting})

	for {
		select {
		// newStateC will be signaled for the transaction we are currently
		// contending on. We will continue to receive updates about this
		// transaction until it no longer contends with us, at which point
		// either one of the other channels fires or we receive state
		// about another contending transaction on newStateC.
		case <-newStateC:
			timerC = nil
			state, err := guard.CurState()
			if err != nil {
				return kvpb.NewError(err)
			}
			log.VEventf(ctx, 3, "lock wait-queue event: %s", state)
			tracer.notify(ctx, state)
			switch state.kind {
			case waitFor:
				// waitFor indicates that the request is waiting on another
				// transaction. This transaction may be the lock holder of a
				// conflicting lock or the head of a lock-wait queue that the
				// request is a part of.
				waitPolicyPush := req.WaitPolicy == lock.WaitPolicy_Error

				deadlockOrLivenessPush := true

				// Non-transactional requests do not need to perform a deadlock push as
				// they can't hold locks that outlive the request's duration. As such,
				// they can't be part of deadlock cycles.
				//
				// Now, if the lock isn't held, we don't need to check for liveness -
				// the request must be default alive, or its context would have been
				// cancelled, and it would have exited the lock's wait-queues.
				if !state.held && req.Txn == nil {
					deadlockOrLivenessPush = false
				}

				// For requests that have a lock timeout, push after the timeout to
				// determine whether the lock is abandoned or whether its holder is
				// still active.
				timeoutPush := req.LockTimeout != 0

				// If the pushee has the minimum priority or if the pusher has the
				// maximum priority, push immediately to proceed without queueing.
				// The push should succeed without entering the txn wait-queue.
				priorityPush := canPushWithPriority(req, state)

				// If the request doesn't want to perform a delayed push for any reason,
				// continue waiting without a timer.
				if !(deadlockOrLivenessPush || timeoutPush || priorityPush || waitPolicyPush) {
					log.VEventf(ctx, 3, "not pushing")
					continue
				}

				// Most[2] requests perform a delayed[1] push for liveness and/or
				// deadlock detection. A request's priority, wait policy, and lock
				// timeout may shorten this delay.
				//
				// [1] The request should push to detect abandoned locks due to failed
				// transaction coordinators, detect deadlocks between transactions, or
				// both, but only after delay. This delay avoids unnecessary push
				// traffic when the conflicting transaction is continuing to make
				// forward progress.
				//
				// [2] The only exception being non-transactional requests (that can't
				// be part of deadlock cycles) that are waiting on a known to be live
				// transaction (one that's acquired a claim but not the lock).
				delay := time.Duration(math.MaxInt64)
				if deadlockOrLivenessPush {
					if req.DeadlockTimeout == 0 {
						delay = LockTableDeadlockOrLivenessDetectionPushDelay.Get(&w.st.SV)
					} else {
						delay = req.DeadlockTimeout
					}
				}
				if timeoutPush {
					// Only reset the lock timeout deadline if this is the first time
					// seeing this lock. Specifically, reset the deadline if this is a
					// new key or if this is a new transaction for the same key.
					oldState := timerWaitingState // empty on first pass
					newLock := !oldState.key.Equal(state.key) || oldState.txn.ID != state.txn.ID
					if newLock {
						lockDeadline = w.clock.PhysicalTime().Add(req.LockTimeout)
					}
					delay = minDuration(delay, w.timeUntilDeadline(lockDeadline))
				}

				// If the waiter has priority or an Error wait policy, resolve the
				// conflict immediately without waiting.
				if priorityPush || waitPolicyPush {
					delay = 0
				}

				log.VEventf(ctx, 3, "pushing after %s for: "+
					"deadlock/liveness detection = %t, timeout enforcement = %t, "+
					"priority enforcement = %t, wait policy error = %t",
					delay, deadlockOrLivenessPush, timeoutPush, priorityPush, waitPolicyPush)

				if delay > 0 {
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
				if req.LockTimeout != 0 {
					return doWithTimeoutAndFallback(
						ctx, req.LockTimeout,
						func(ctx context.Context) *Error { return w.pushLockTxn(ctx, req, state) },
						func(ctx context.Context) *Error { return w.pushLockTxnAfterTimeout(ctx, req, state) },
					)
				}
				return w.pushLockTxn(ctx, req, state)

			case waitSelf:
				// Another request from the same transaction has claimed the lock (but
				// not yet acquired it). This can only happen when the request's
				// transaction is sending multiple requests concurrently. Proceed with
				// waiting without pushing anyone.

			case waitQueueMaxLengthExceeded:
				// The request attempted to wait in a lock wait-queue whose length was
				// already equal to or exceeding the request's configured maximum. As a
				// result, the request was rejected.
				return newWriteIntentErr(req, state, reasonWaitQueueMaxLengthExceeded)

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
				return w.ResolveDeferredIntents(ctx, req.AdmissionHeader, toResolve)

			default:
				panic("unexpected waiting state")
			}

		case <-timerC:
			// If the request was in the waitFor state and did not observe any update
			// to its state for the entire delay, it should push. It may be the case
			// that the transaction is part of a dependency cycle or that the lock
			// holder's coordinator node has crashed.
			timer.Read = true
			timerC = nil
			if w.onPushTimer != nil {
				w.onPushTimer()
			}

			// push with the option to wait on the conflict if active.
			pushWait := func(ctx context.Context) *Error {
				// It would be more natural to launch an async task for the push and
				// continue listening on this goroutine for lockTable state transitions,
				// but doing so is harder to test against. Instead, we launch an async
				// task to listen to lockTable state and synchronously push. If the
				// watcher goroutine detects a lockTable change, it cancels the context
				// on the push.
				pushCtx, pushCancel := context.WithCancel(ctx)
				defer pushCancel()
				go watchForNotifications(pushCtx, pushCancel, newStateC)

				var err *Error
				if timerWaitingState.held {
					// Note that even though the request has a dependency on the
					// transaction that holds the lock, this dependency can be broken
					// without the holder's transaction getting finalized[1] such that the
					// pusher can proceed before the synchronous push below returns. The
					// pusher must detect such cases (watchForNotifications) and cancel
					// its push in such cases.
					//
					// [1] This can happen for a few reasons:
					// 1. The pusher may not conflict with the lock holder itself, but one
					// of the waiting requests instead. If the waiting request drops out
					// of the lock's wait queue the pusher should be allowed to proceed.
					// Concretely, a construction like follows:
					//   - holder: shared
					//     - wait-queue: exclusive, shared
					// In this case, the waiting shared lock request will push the
					// holder[*] However, if the waiting exclusive locking request drops
					// out of the wait queue, the shared locking request no longer needs
					// to wait/push the holder.
					// 2. The lock may be rolled back because of savepoints even if the
					// transaction isn't finalized/pushed successfully.
					// 3. The lock may no longer be tracked by the lock table even though
					// the holder's transaction is still pending. This can happen if it's
					// an intent that's pushed to a higher timestamp by a different
					// request. In such cases, the lock table will simply forget the lock
					// when the intent is resolved. Note that in such cases, the pusher
					// may still conflict with the intent and rediscover it -- that's
					// okay.
					//
					// [*] The shared locking request will push the lock holder (strength
					// shared) instead of the exclusive lock requesting (the one it
					// actually conflicts with) because it transitively depends on the
					// shared locking request. In doing so, it is essentially collapsing
					// edges in the local portion of its dependency graph for deadlock
					// detection, as doing so is cheaper that finding out the same
					// information using (QueryTxnRequest) RPCs.
					err = w.pushLockTxn(pushCtx, req, timerWaitingState)
				} else {
					// The request conflicts with another request that's claimed an unheld
					// lock. The conflicting request may exit the lock table without
					// actually acquiring the lock. If that happens, we may be able to
					// proceed without needing to wait for the push to successfully
					// complete. Such cases will be detected by listening for lock state
					// transitions (watchForNotifications).
					err = w.pushRequestTxn(pushCtx, req, timerWaitingState)
				}
				// Ignore the context canceled error. If this was for the parent context
				// then we'll notice on the next select.
				//
				// NOTE: we look at pushCtx.Err() and not err to avoid the potential for
				// bugs if context cancellation is not propagated correctly on some
				// error paths.
				if errors.Is(pushCtx.Err(), context.Canceled) {
					return nil
				}
				return err
			}

			// push without the option to wait on the conflict if active.
			pushNoWait := func(ctx context.Context) *Error {
				// Resolve the conflict without waiting by pushing the lock holder's
				// transaction.
				return w.pushLockTxnAfterTimeout(ctx, req, timerWaitingState)
			}

			// We push with or without the option to wait on the conflict,
			// depending on the state of the lock timeout, if one exists,
			// and depending on the wait policy.
			var err *Error
			if req.WaitPolicy == lock.WaitPolicy_Error {
				err = w.pushLockTxn(ctx, req, timerWaitingState)
			} else if !lockDeadline.IsZero() {
				untilDeadline := w.timeUntilDeadline(lockDeadline)
				if untilDeadline == 0 {
					// Deadline already exceeded.
					err = pushNoWait(ctx)
				} else {
					// Deadline not yet exceeded.
					err = doWithTimeoutAndFallback(ctx, untilDeadline, pushWait, pushNoWait)
				}
			} else {
				// No deadline.
				err = pushWait(ctx)
			}
			if err != nil {
				return err
			}

		case <-ctxDoneC:
			return kvpb.NewError(ctx.Err())

		case <-shouldQuiesceC:
			return kvpb.NewError(&kvpb.NodeUnavailableError{})
		}
	}
}

// pushLockTxn pushes the holder of the provided lock.
//
// If a Block wait policy is set on the request, method blocks until the lock
// holder transaction experiences a state transition such that it no longer
// conflicts with the pusher's request. The method then synchronously updates
// the lock to trigger a state transition in the lockTable that will free up the
// request to proceed. If the method returns successfully then the caller can
// expect to have an updated waitingState.
//
// If an Error wait policy is set on the request, the method checks if the lock
// holder transaction is abandoned. If so, the method synchronously updates the
// lock to trigger a state transition in the lockTable that will free up the
// request to proceed. If the method returns successfully then the caller can
// expect to have an updated waitingState. Otherwise, the method returns with a
// WriteIntentError and without blocking on the lock holder transaction.
func (w *lockTableWaiterImpl) pushLockTxn(
	ctx context.Context, req Request, ws waitingState,
) *Error {
	if w.disableTxnPushing {
		return newWriteIntentErr(req, ws, reasonWaitPolicy)
	}

	// Construct the request header and determine which form of push to use.
	h := w.pushHeader(req)
	var pushType kvpb.PushTxnType
	var beforePushObs roachpb.ObservedTimestamp
	if ws.guardStrength == lock.None {
		pushType = kvpb.PUSH_TIMESTAMP
		beforePushObs = roachpb.ObservedTimestamp{
			NodeID:    w.nodeDesc.NodeID,
			Timestamp: w.clock.NowAsClockTimestamp(),
		}
		// TODO(nvanbenschoten): because information about the local_timestamp
		// leading the MVCC timestamp of an intent is lost, we also need to push
		// the intent up to the top of the transaction's local uncertainty limit
		// on this node. This logic currently lives in pushHeader, but we could
		// simplify it and move it out here.
		//
		// We could also explore adding a preserve_local_timestamp flag to
		// MVCCValue that would explicitly store the local timestamp even in
		// cases where it would normally be omitted. This could be set during
		// intent resolution when a push observation is provided. Or we could
		// not persist this, but still preserve the local timestamp when the
		// adjusting the intent, accepting that the intent would then no longer
		// round-trip and would lose the local timestamp if rewritten later.
		log.VEventf(ctx, 2, "pushing timestamp of txn %s above %s", ws.txn.Short(), h.Timestamp)
	} else {
		pushType = kvpb.PUSH_ABORT
		log.VEventf(ctx, 2, "pushing txn %s to abort", ws.txn.Short())
	}

	pusheeTxn, _, err := w.ir.PushTransaction(ctx, ws.txn, h, pushType)
	if err != nil {
		// If pushing with an Error WaitPolicy and the push fails, then the lock
		// holder is still active. Transform the error into a WriteIntentError.
		if _, ok := err.GetDetail().(*kvpb.TransactionPushError); ok && req.WaitPolicy == lock.WaitPolicy_Error {
			err = newWriteIntentErr(req, ws, reasonWaitPolicy)
		}
		return err
	}

	// If the transaction was pushed, add it to the txnStatusCache. This avoids
	// needing to push it again if we find another one of its locks and allows for
	// batching of intent resolution.
	w.lt.PushedTransactionUpdated(pusheeTxn)

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
	// then we run the below code, we're clearing the AbortSpan illegally.
	// Furthermore, even if our pushType is not PUSH_ABORT, we may have ended up
	// with the responsibility to abort the intents (for example if we find the
	// transaction aborted). To do better here, we need per-intent information
	// on whether we need to poison.
	resolve := roachpb.MakeLockUpdate(pusheeTxn, roachpb.Span{Key: ws.key})
	if pusheeTxn.Status == roachpb.PENDING {
		// The pushee was still PENDING at the time that the push observed its
		// transaction record. It is safe to use the clock observation we gathered
		// before initiating the push during intent resolution, as we know that this
		// observation must have been made before the pushee committed (implicitly
		// or explicitly) and acknowledged its client, assuming it does commit at
		// some point.
		//
		// This observation can be used to forward the local timestamp of the intent
		// when intent resolution forwards its version timestamp. This is important,
		// as it prevents the pusher, who has an even earlier observed timestamp
		// from this node, from considering this intent to be uncertain after the
		// resolution succeeds and the pusher returns to read.
		//
		// For example, consider a reader with a read timestamp of 10, a global
		// uncertainty limit of 25, and a local uncertainty limit (thanks to an
		// observed timestamp) of 15. The reader conflicts with an intent that has a
		// version timestamp and local timestamp of 8. The reader observes the local
		// clock at 16 before pushing and then succeeds in pushing the intent's
		// holder txn to timestamp 11 (read_timestamp + 1). If the reader were to
		// resolve the intent to timestamp 11 but leave its local timestamp at 8
		// then the reader would consider the value "uncertain" upon re-evaluation.
		// However, if the reader also updates the value's local timestamp to 16
		// during intent resolution then it will not consider the value to be
		// "uncertain".
		//
		// Unfortunately, this does not quite work as written, as the MVCC key
		// encoding logic normalizes (as an optimization) keys with
		//  local timestamp >= mvcc timestamp
		// to
		//  local timestamp == mvcc timestamp
		// To work around this, the pusher must also push the mvcc timestamp of the
		// intent above its own local uncertainty limit. In the example above, this
		// would mean pushing the intent's holder txn to timestamp 16 as well. The
		// logic that handles this is in pushHeader.
		//
		// Note that it would be incorrect to update the intent's local timestamp if
		// the pushee was found to be committed (implicitly or explicitly), as the
		// pushee may have already acknowledged its client by the time the clock
		// observation was taken and the value should be considered uncertain. Doing
		// so could allow the pusher to serve a stale read.
		//
		// For example, if we used the observation after the push found a committed
		// pushee, we would be susceptible to a stale read that looks like:
		// 1. txn1 writes intent on key k @ ts 10, on node N
		// 2. txn1 commits @ ts 15, acks client
		// 3. txn1's async intent resolution of key k stalls
		// 4. txn2 begins after txn1 with read timestamp @ 11
		// 5. txn2 collects observed timestamp @ 12 from node N
		// 6. txn2 encounters intent on key k, observes clock @ ts 13, pushes, finds
		//    committed record, resolves intent with observation. Committed version
		//    now has mvcc timestamp @ 15 and local timestamp @ 13
		// 7. txn2 reads @ 11 with local uncertainty limit @ 12, fails to observe
		//    key k's new version. Stale read!
		//
		// More subtly, it would also be incorrect to update the intent's local
		// timestamp using an observation captured _after_ the push completed, even
		// if it had found a PENDING record. This is because this ordering makes no
		// guarantee that the clock observation is captured before the pushee
		// commits and acknowledges its client. This could not lead to the pusher
		// serving a stale read, but it could lead to other transactions serving
		// stale reads.
		//
		// For example, if we captured the observation after the push completed, we
		// would be susceptible to a stale read that looks like:
		// 1. txn1 writes intent on key k @ ts 10, on node N
		// 2. txn2 (concurrent with txn1, so no risk of stale read itself) encounters
		//    intent on key k, pushes, finds pending record and pushes to timestamp 14
		// 3. txn1 commits @ ts 15, acks client
		// 4. txn1's async intent resolution of key k stalls
		// 5. txn3 begins after txn1 with read timestamp @ 11
		// 6. txn3 collects observed timestamp @ 12 from node N
		// 7. txn2 observes clock @ 13 _after_ push, resolves intent (still pending)
		//    with observation. Intent now has mvcc timestamp @ 14 and local
		//    timestamp @ 13
		// 8. txn3 reads @ 11 with local uncertainty limit @ 12, fails to observe
		//    key k's intent so it does not resolve it to committed. Stale read!
		//
		// There is some inherent raciness here, because the lease may move between
		// when we push and when the reader later read. In such cases, the reader's
		// local uncertainty limit may exceed the intent's local timestamp during
		// the subsequent read and it may need to push again. However, we expect to
		// eventually succeed in reading, either after lease movement subsides or
		// after the reader's read timestamp surpasses its global uncertainty limit.
		resolve.ClockWhilePending = beforePushObs
	}
	logResolveIntent(ctx, resolve)
	opts := intentresolver.ResolveOptions{Poison: true, AdmissionHeader: req.AdmissionHeader}
	return w.ir.ResolveIntent(ctx, resolve, opts)
}

// pushLockTxnAfterTimeout is like pushLockTxn, but it sets the Error wait
// policy on its request so that the request will not block on the lock holder
// if it is still active. It is meant to be used after a lock timeout has been
// elapsed, and returns a WriteIntentErrors with a LOCK_TIMEOUT reason if the
// lock holder is not abandoned.
func (w *lockTableWaiterImpl) pushLockTxnAfterTimeout(
	ctx context.Context, req Request, ws waitingState,
) *Error {
	req.WaitPolicy = lock.WaitPolicy_Error
	err := w.pushLockTxn(ctx, req, ws)
	if _, ok := err.GetDetail().(*kvpb.WriteIntentError); ok {
		err = newWriteIntentErr(req, ws, reasonLockTimeout)
	}
	return err
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
	pushType := kvpb.PUSH_ABORT
	log.VEventf(ctx, 3, "pushing txn %s to detect request deadlock", ws.txn.Short())

	_, _, err := w.ir.PushTransaction(ctx, ws.txn, h, pushType)
	if err != nil {
		return err
	}

	// Even if the push succeeded and aborted the other transaction to break a
	// deadlock, there's nothing for the pusher to clean up. The conflicting
	// request will quickly exit the lock wait-queue and release its claim
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
	//  they respectively hold a claim (but not the lock itself) on key "a" and
	//  key "b". req(2, txn2) queues up behind the claim on key "a" and req(2,
	//  txn1) queues up behind the claim on key "b". Now the dependency cycle
	//  between txn1 and txn2 only involves requests, but some of the requests
	//  here also depend on a lock. So when both txn1, txn2 are aborted, the
	//  req(1, txn1), req(1, txn2) are guaranteed to eventually notice through
	//  self-directed QueryTxn requests and will exit the lockTable, allowing
	//  req(2, txn1) and req(2, txn2) to claim the lock and now they no longer
	//  depend on each other.
	return nil
}

// pushHeader returns a BatchRequest header to be used for pushing other
// transactions on behalf of the provided request.
func (w *lockTableWaiterImpl) pushHeader(req Request) kvpb.Header {
	h := kvpb.Header{
		Timestamp:    req.Timestamp,
		UserPriority: req.NonTxnPriority,
		WaitPolicy:   req.WaitPolicy,
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
		uncertaintyLimit := req.Txn.GlobalUncertaintyLimit
		// However, because we intend to read on the same node, we can limit
		// this to a clock reading from the local clock, relying on the fact
		// that an observed timestamp from this node will limit our local
		// uncertainty limit when we return to read.
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
		uncertaintyLimit.Backward(w.clock.Now())
		h.Timestamp.Forward(uncertaintyLimit)
	}
	return h
}

// timeUntilDeadline computes the duration until the specified deadline is
// reached. If the deadline has already been reached, the method returns 0. As
// an optimization and as a convenience for tests, if the deadline is within a
// threshold such that it does not make sense to begin an expensive operation
// that is limited by the deadline, the method considers the deadline to have
// already been reached.
func (w *lockTableWaiterImpl) timeUntilDeadline(deadline time.Time) time.Duration {
	dur := deadline.Sub(w.clock.PhysicalTime())
	const soon = 250 * time.Microsecond
	if dur <= soon {
		return 0
	}
	return dur
}

// ResolveDeferredIntents implements the lockTableWaiter interface.
func (w *lockTableWaiterImpl) ResolveDeferredIntents(
	ctx context.Context,
	admissionHeader kvpb.AdmissionHeader,
	deferredResolution []roachpb.LockUpdate,
) *Error {
	if len(deferredResolution) == 0 {
		return nil
	}
	log.VEventf(ctx, 2, "resolving a batch of %d intent(s)", len(deferredResolution))
	for _, intent := range deferredResolution {
		logResolveIntent(ctx, intent)
	}
	// See pushLockTxn for an explanation of these options.
	opts := intentresolver.ResolveOptions{Poison: true, AdmissionHeader: admissionHeader}
	return w.ir.ResolveIntents(ctx, deferredResolution, opts)
}

// doWithTimeoutAndFallback runs the withTimeout function with the specified
// timeout. If the function completes before hitting the timeout, its result
// is returned. Otherwise, the afterTimeout function is run without a timeout
// and its result is returned.
//
// The function is called in a few locations to run a blocking push with a
// timeout and then to fall back to a non-blocking push if that timeout is
// reached. This pattern is used because on a timeout, we don't know whether a
// blocking PUSH_TIMESTAMP / PUSH_ABORT push actually got to the point of
// proving that the pushee was active and began waiting in its txnwait.Queue.
// The push may have timed out before this point due to a slow network, slow
// CPU, or for some other reason. But just like with WaitPolicy_Error, we don't
// want to throw a WriteIntentError on abandoned locks. So on timeout, we issue
// a new request with WaitPolicy_Error that is not subject to the lock_timeout
// to check with certainty whether the conflict is active or not, but without
// blocking if it happens to be active.
func doWithTimeoutAndFallback(
	ctx context.Context,
	timeout time.Duration,
	withTimeout, afterTimeout func(ctx context.Context) *Error,
) *Error {
	timeoutCtx, timeoutCancel := context.WithTimeout(ctx, timeout) // nolint:context
	defer timeoutCancel()
	err := withTimeout(timeoutCtx)
	if !errors.Is(timeoutCtx.Err(), context.DeadlineExceeded) {
		// If the context deadline was not exceeded, return the result.
		//
		// NOTE: we look at timeoutCtx.Err() and not err to avoid the
		// potential for bugs if context cancellation is not propagated
		// correctly on some error paths.
		return err
	}
	// Otherwise, run the fallback function without a timeout.
	return afterTimeout(ctx)
}

// watchForNotifications selects on the provided channel and watches for any
// updates. If the channel is ever notified, it calls the provided context
// cancellation function and exits.
func watchForNotifications(ctx context.Context, cancel func(), newStateC chan struct{}) {
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

// txnStatusCache is a small LRU cache that tracks the status of transactions
// have been successfully pushed. The caches are partitioned into finalized and
// pending transactions. Users are responsible for accessing the partition that
// interests them.
//
// The zero value of this struct is ready for use.
type txnStatusCache struct {
	// finalizedTxns is a small LRU cache that tracks transactions that were
	// pushed and found to be finalized (COMMITTED or ABORTED). It is used as an
	// optimization to avoid repeatedly pushing the transaction record when
	// cleaning up the intents of an abandoned transaction.
	finalizedTxns txnCache

	// pendingTxns is a small LRU cache that tracks transactions whose minimum
	// commit timestamp was pushed but whose final status is not yet known. It is
	// used an an optimization to avoid repeatedly pushing the transaction record
	// when transaction priorities allow a pusher to move many intents of a
	// lower-priority transaction.
	pendingTxns txnCache
}

func (c *txnStatusCache) add(txn *roachpb.Transaction) {
	if txn.Status.IsFinalized() {
		c.finalizedTxns.add(txn)
	} else {
		c.pendingTxns.add(txn)
	}
}

func (c *txnStatusCache) clear() {
	c.finalizedTxns.clear()
	c.pendingTxns.clear()
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
		if curTxn := c.txns[idx]; txn.WriteTimestamp.Less(curTxn.WriteTimestamp) {
			// If the new txn has a lower write timestamp than the cached txn,
			// just move the cached txn to the front of the LRU cache.
			txn = curTxn
		}
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

// tagContentionTracer is the tracing span tag that the *contentionEventTracer
// lives under.
const tagContentionTracer = "locks"

// tagWaitKey is the tracing span tag indicating the key of the lock the request
// is currently waiting on.
const tagWaitKey = "wait_key"

// tagWaitStart is the tracing span tag indicating when the request started
// waiting on the lock (a unique key,txn pair) it's currently waiting on.
const tagWaitStart = "wait_start"

// tagLockHolderTxn is the tracing span tag indicating the ID of the txn holding
// the lock (or has claimed the lock) that the request is currently waiting
// on.
const tagLockHolderTxn = "holder_txn"

// tagNumLocks is the tracing span tag indicating the number of locks that the
// request has previously waited on. If the request is currently waiting on
// a lock, that lock is included.
const tagNumLocks = "num"

// tagWaited is the tracing span tag indicating the total time that the span has
// waited on locks. If the span is currently waiting on a lock, the time it has
// already waited on that lock is included.
const tagWaited = "wait"

// contentionEventTracer adds lock contention information to the trace, in the
// form of events and tags. The contentionEventTracer is associated with a
// tracing span.
type contentionEventTracer struct {
	sp      *tracing.Span
	onEvent func(event *kvpb.ContentionEvent) // may be nil
	tag     contentionTag
}

// contentionTag represents a lazy tracing span tag containing lock contention
// information. The contentionTag is fed info from the parent
// contentionEventTracer.
type contentionTag struct {
	clock *hlc.Clock
	mu    struct {
		syncutil.Mutex

		// lockWait accumulates time waited for locks before the current waiting
		// period (the current waiting period starts at waitStart).
		lockWait time.Duration

		// waiting is set if the contentionEventTracer has been notified of a lock
		// that the underlying request is waiting on. The contentionEventTracer
		// starts with waiting=false, and transitions to waiting=true on the first
		// notify() call. It transitions back to waiting=false on terminal events,
		// and can then continue transitioning back and forth (in case the request
		// is sequenced again and encounters more locks).
		waiting bool

		// waitStart represents the timestamp when the request started waiting on
		// a lock, as defined by a unique (key,txn) pair, in the current iteration
		// of the contentionEventTracer. The wait time in previous iterations is
		// accumulated in lockWait. When not waiting anymore or when waiting on a
		// new (key,txn), timeutil.Since(waitStart) is added to lockWait.
		waitStart time.Time

		// curStateKey and curStateTxn are the current waitingState's key and txn,
		// if any. They are overwritten every time the lock table notify()s the
		// contentionEventTracer of a new state. They are not set if waiting is
		// false.
		curStateKey roachpb.Key
		curStateTxn *enginepb.TxnMeta

		// numLocks counts the number of locks this contentionEventTracer has seen so
		// far, including the one we're currently waiting on (if any).
		numLocks int
	}
}

// newContentionEventTracer creates a contentionEventTracer and associates it
// with the provided tracing span. The contentionEventTracer will emit events to
// the respective span and will also act as a lazy tag on the span.
//
// sp can be nil, in which case the tracer will not do anything.
//
// It is legal to create a tracer on a span that has previously had another
// tracer. In that case, the new tracer will absorb the counters from the
// previous one, and replace it as a span tag. However, it is illegal to create
// a contentionEventTracer on a span that already has an "active"
// contentionEventTracer; two tracers sharing a span concurrently doesn't work,
// as they'd clobber each other. The expectation is that, if this span had a
// tracer on it, that tracer should have been properly shutdown.
func newContentionEventTracer(sp *tracing.Span, clock *hlc.Clock) *contentionEventTracer {
	t := &contentionEventTracer{}
	t.tag.clock = clock

	// If the span had previously had contention info, we'll absorb the info into
	// the new tracer/tag.
	oldTag, ok := sp.GetLazyTag(tagContentionTracer)
	if ok {
		oldContentionTag := oldTag.(*contentionTag)
		oldContentionTag.mu.Lock()
		waiting := oldContentionTag.mu.waiting
		if waiting {
			oldContentionTag.mu.Unlock()
			panic("span already contains contention tag in the waiting state")
		}
		t.tag.mu.numLocks = oldContentionTag.mu.numLocks
		t.tag.mu.lockWait = oldContentionTag.mu.lockWait
		oldContentionTag.mu.Unlock()
	}

	sp.SetLazyTag(tagContentionTracer, &t.tag)
	t.sp = sp
	return t
}

// SetOnContentionEvent registers a callback to be called before each event is
// emitted. The callback may modify the event.
func (h *contentionEventTracer) SetOnContentionEvent(f func(ev *kvpb.ContentionEvent)) {
	h.onEvent = f
}

var _ tracing.LazyTag = &contentionTag{}

// notify processes an event from the lock table.
// compares the waitingState's active txn (if any) against the current
// ContentionEvent (if any). If they match, we are continuing to handle the
// same event and no action is taken. If they differ, the open event (if any) is
// finalized and added to the Span, and a new event initialized from the inputs.
func (h *contentionEventTracer) notify(ctx context.Context, s waitingState) {
	if h.sp == nil {
		// No span to manipulate - don't do any work.
		return
	}

	event := h.tag.notify(ctx, s)
	if event != nil {
		h.emit(event)
	}
}

// emit records a ContentionEvent to the tracing span corresponding to the
// current wait state (if any).
func (h *contentionEventTracer) emit(event *kvpb.ContentionEvent) {
	if event == nil {
		return
	}
	if h.onEvent != nil {
		// NB: this is intentionally above the call to RecordStructured so that
		// this interceptor gets to mutate the event (used for test determinism).
		h.onEvent(event)
	}
	h.sp.RecordStructured(event)
}

func (tag *contentionTag) generateEventLocked() *kvpb.ContentionEvent {
	if !tag.mu.waiting {
		return nil
	}

	return &kvpb.ContentionEvent{
		Key:      tag.mu.curStateKey,
		TxnMeta:  *tag.mu.curStateTxn,
		Duration: tag.clock.PhysicalTime().Sub(tag.mu.waitStart),
	}
}

// See contentionEventTracer.notify.
func (tag *contentionTag) notify(ctx context.Context, s waitingState) *kvpb.ContentionEvent {
	tag.mu.Lock()
	defer tag.mu.Unlock()

	// Depending on the kind of notification, we check whether we're now waiting
	// on a different key than we were previously. If we're now waiting on a
	// different key, we'll return an event corresponding to the previous key.
	switch s.kind {
	case waitFor, waitSelf, waitElsewhere:
		// If we're tracking an event and see a different txn/key, the event is
		// done and we initialize the new event tracking the new txn/key.
		//
		// NB: we're guaranteed to have `curState{Txn,Key}` populated here.
		if tag.mu.waiting {
			curLockTxn, curLockKey := tag.mu.curStateTxn.ID, tag.mu.curStateKey
			differentLock := !curLockTxn.Equal(s.txn.ID) || !curLockKey.Equal(s.key)
			if !differentLock {
				return nil
			}
		}
		res := tag.generateEventLocked()
		tag.mu.waiting = true
		tag.mu.curStateKey = s.key
		tag.mu.curStateTxn = s.txn
		// Accumulate the wait time.
		now := tag.clock.PhysicalTime()
		if !tag.mu.waitStart.IsZero() {
			tag.mu.lockWait += now.Sub(tag.mu.waitStart)
		}
		tag.mu.waitStart = now
		tag.mu.numLocks++
		return res
	case doneWaiting, waitQueueMaxLengthExceeded:
		// There will be no more state updates; we're done waiting.
		res := tag.generateEventLocked()
		tag.mu.waiting = false
		tag.mu.curStateKey = nil
		tag.mu.curStateTxn = nil
		// Accumulate the wait time.
		now := tag.clock.PhysicalTime()
		tag.mu.lockWait += now.Sub(tag.mu.waitStart)
		tag.mu.waitStart = time.Time{}
		return res
	default:
		kind := s.kind // escapes to the heap
		log.Fatalf(ctx, "unhandled waitingState.kind: %v", kind)
	}
	panic("unreachable")
}

// Render implements the tracing.LazyTag interface.
func (tag *contentionTag) Render() []attribute.KeyValue {
	tag.mu.Lock()
	defer tag.mu.Unlock()
	tags := make([]attribute.KeyValue, 0, 4)
	if tag.mu.numLocks > 0 {
		tags = append(tags, attribute.KeyValue{
			Key:   tagNumLocks,
			Value: attribute.IntValue(tag.mu.numLocks),
		})
	}
	// Compute how long the request has waited on locks by adding the prior wait
	// time (if any) and the current wait time (if we're currently waiting).
	lockWait := tag.mu.lockWait
	if !tag.mu.waitStart.IsZero() {
		lockWait += tag.clock.PhysicalTime().Sub(tag.mu.waitStart)
	}
	if lockWait != 0 {
		tags = append(tags, attribute.KeyValue{
			Key:   tagWaited,
			Value: attribute.StringValue(string(humanizeutil.Duration(lockWait))),
		})
	}

	if tag.mu.waiting {
		tags = append(tags, attribute.KeyValue{
			Key:   tagWaitKey,
			Value: attribute.StringValue(tag.mu.curStateKey.String()),
		})
		tags = append(tags, attribute.KeyValue{
			Key:   tagLockHolderTxn,
			Value: attribute.StringValue(tag.mu.curStateTxn.ID.String()),
		})
		tags = append(tags, attribute.KeyValue{
			Key:   tagWaitStart,
			Value: attribute.StringValue(tag.mu.waitStart.Format("15:04:05.123")),
		})
	}
	return tags
}

const (
	reasonWaitPolicy                 = kvpb.WriteIntentError_REASON_WAIT_POLICY
	reasonLockTimeout                = kvpb.WriteIntentError_REASON_LOCK_TIMEOUT
	reasonWaitQueueMaxLengthExceeded = kvpb.WriteIntentError_REASON_LOCK_WAIT_QUEUE_MAX_LENGTH_EXCEEDED
)

func newWriteIntentErr(req Request, ws waitingState, reason kvpb.WriteIntentError_Reason) *Error {
	// TODO(nvanbenschoten): we should use the correct Lock strength of the lock
	// holder, once we have it.
	err := kvpb.NewError(&kvpb.WriteIntentError{
		Locks:  []roachpb.Lock{roachpb.MakeIntent(ws.txn, ws.key).AsLock()},
		Reason: reason,
	})
	// TODO(nvanbenschoten): setting an error index can assist the KV client in
	// understanding which request hit an error. This is not necessary, but can
	// improve error handling, leading to better error messages and performance
	// optimizations in some cases. We don't have an easy way to associate a given
	// conflict with a specific request in a batch because we don't retain a
	// mapping from lock span to request. However, as a best-effort optimization,
	// we set the error index to 0 if this is the only request in the batch (that
	// landed on this range, from the client's perspective).
	if len(req.Requests) == 1 {
		err.SetErrorIndex(0)
	}
	return err
}

func canPushWithPriority(req Request, s waitingState) bool {
	if s.txn == nil {
		// Can't push a non-transactional request.
		return false
	}
	var pushType kvpb.PushTxnType
	if s.guardStrength == lock.None {
		pushType = kvpb.PUSH_TIMESTAMP
	} else {
		pushType = kvpb.PUSH_ABORT
	}
	var pusherIso, pusheeIso isolation.Level
	var pusherPri, pusheePri enginepb.TxnPriority
	if req.Txn != nil {
		pusherIso = req.Txn.IsoLevel
		pusherPri = req.Txn.Priority
	} else {
		pusherIso = isolation.Serializable
		pusherPri = roachpb.MakePriority(req.NonTxnPriority)
	}
	pusheeIso = s.txn.IsoLevel
	pusheePri = s.txn.Priority
	// We assume that the pushee is in the PENDING state when deciding whether
	// to push. A push may determine that the pushee is STAGING or has already
	// been finalized.
	pusheeStatus := roachpb.PENDING
	return txnwait.CanPushWithPriority(pushType, pusherIso, pusheeIso, pusherPri, pusheePri, pusheeStatus)
}

func logResolveIntent(ctx context.Context, intent roachpb.LockUpdate) {
	if !log.ExpensiveLogEnabled(ctx, 2) {
		return
	}
	var obsStr redact.RedactableString
	if obs := intent.ClockWhilePending; obs != (roachpb.ObservedTimestamp{}) {
		obsStr = redact.Sprintf(" and clock observation {%d %v}", obs.NodeID, obs.Timestamp)
	}
	log.VEventf(ctx, 2, "resolving intent %s for txn %s with %s status%s",
		intent.Key, intent.Txn.Short(), intent.Status, obsStr)
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
