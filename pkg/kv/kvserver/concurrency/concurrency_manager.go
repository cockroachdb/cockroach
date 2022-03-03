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
	"sync"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanlatch"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/txnwait"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// MaxLockWaitQueueLength sets the maximum length of a lock wait-queue that a
// read-write request is willing to enter and wait in. Used to provide a release
// valve and ensure some level of quality-of-service under severe per-key
// contention. If set to a non-zero value and an existing lock wait-queue is
// already equal to or exceeding this length, the request will be rejected
// eagerly instead of entering the queue and waiting.
//
// This is a fairly blunt mechanism to place an upper bound on resource
// utilization per lock wait-queue and ensure some reasonable level of
// quality-of-service for transactions that enter a lock wait-queue. More
// sophisticated queueing alternatives exist that account for queueing time and
// detect sustained queue growth before rejecting:
// - https://queue.acm.org/detail.cfm?id=2209336
// - https://queue.acm.org/detail.cfm?id=2839461
//
// We could explore these algorithms if this setting is too coarse grained and
// not serving its purpose well enough.
//
// Alternatively, we could implement the lock_timeout session variable that
// exists in Postgres (#67513) and use that to ensure quality-of-service for
// requests that wait for locks. With that configuration, this cluster setting
// would be relegated to a guardrail that protects against unbounded resource
// utilization and runaway queuing for misbehaving clients, a role it is well
// positioned to serve.
var MaxLockWaitQueueLength = settings.RegisterIntSetting(
	settings.TenantWritable,
	"kv.lock_table.maximum_lock_wait_queue_length",
	"the maximum length of a lock wait-queue that read-write requests are willing "+
		"to enter and wait in. The setting can be used to ensure some level of quality-of-service "+
		"under severe per-key contention. If set to a non-zero value and an existing lock "+
		"wait-queue is already equal to or exceeding this length, requests will be rejected "+
		"eagerly instead of entering the queue and waiting. Set to 0 to disable.",
	0,
	func(v int64) error {
		if v < 0 {
			return errors.Errorf("cannot be set to a negative value: %d", v)
		}
		if v == 0 {
			return nil // disabled
		}
		// Don't let the setting be dropped below a reasonable value that we don't
		// expect to impact internal transaction processing.
		const minSafeMaxLength = 3
		if v < minSafeMaxLength {
			return errors.Errorf("cannot be set below %d: %d", minSafeMaxLength, v)
		}
		return nil
	},
)

// DiscoveredLocksThresholdToConsultFinalizedTxnCache sets a threshold as
// mentioned in the description string. The default of 200 is somewhat
// arbitrary but should suffice for small OLTP transactions. Given the default
// 10,000 lock capacity of the lock table, 200 is small enough to not matter
// much against the capacity, which is desirable. We have seen examples with
// discoveredCount > 100,000, caused by stats collection, where we definitely
// want to avoid adding these locks to the lock table, if possible.
var DiscoveredLocksThresholdToConsultFinalizedTxnCache = settings.RegisterIntSetting(
	settings.TenantWritable,
	"kv.lock_table.discovered_locks_threshold_for_consulting_finalized_txn_cache",
	"the maximum number of discovered locks by a waiter, above which the finalized txn cache"+
		"is consulted and resolvable locks are not added to the lock table -- this should be a small"+
		"fraction of the maximum number of locks in the lock table",
	200,
	settings.NonNegativeInt,
)

// managerImpl implements the Manager interface.
type managerImpl struct {
	st *cluster.Settings
	// Synchronizes conflicting in-flight requests.
	lm latchManager
	// Synchronizes conflicting in-progress transactions.
	lt lockTable
	// Waits for locks that conflict with a request to be released.
	ltw lockTableWaiter
	// Waits for transaction completion and detects deadlocks.
	twq txnWaitQueue
}

// Config contains the dependencies to construct a Manager.
type Config struct {
	// Identification.
	NodeDesc  *roachpb.NodeDescriptor
	RangeDesc *roachpb.RangeDescriptor
	// Components.
	Settings       *cluster.Settings
	DB             *kv.DB
	Clock          *hlc.Clock
	Stopper        *stop.Stopper
	IntentResolver IntentResolver
	// Metrics.
	TxnWaitMetrics *txnwait.Metrics
	SlowLatchGauge *metric.Gauge
	// Configs + Knobs.
	MaxLockTableSize  int64
	DisableTxnPushing bool
	OnContentionEvent func(*roachpb.ContentionEvent) // may be nil; allowed to mutate the event
	TxnWaitKnobs      txnwait.TestingKnobs
}

func (c *Config) initDefaults() {
	if c.MaxLockTableSize == 0 {
		c.MaxLockTableSize = defaultLockTableSize
	}
}

// NewManager creates a new concurrency Manager structure.
func NewManager(cfg Config) Manager {
	cfg.initDefaults()
	m := new(managerImpl)
	lt := newLockTable(cfg.MaxLockTableSize, timeutil.DefaultTimeSource{})
	*m = managerImpl{
		st: cfg.Settings,
		// TODO(nvanbenschoten): move pkg/storage/spanlatch to a new
		// pkg/storage/concurrency/latch package. Make it implement the
		// latchManager interface directly, if possible.
		lm: &latchManagerImpl{
			m: spanlatch.Make(
				cfg.Stopper,
				cfg.SlowLatchGauge,
			),
		},
		lt: lt,
		ltw: &lockTableWaiterImpl{
			st:                cfg.Settings,
			clock:             cfg.Clock,
			stopper:           cfg.Stopper,
			ir:                cfg.IntentResolver,
			lt:                lt,
			disableTxnPushing: cfg.DisableTxnPushing,
			onContentionEvent: cfg.OnContentionEvent,
		},
		// TODO(nvanbenschoten): move pkg/storage/txnwait to a new
		// pkg/storage/concurrency/txnwait package.
		twq: txnwait.NewQueue(txnwait.Config{
			RangeDesc: cfg.RangeDesc,
			DB:        cfg.DB,
			Clock:     cfg.Clock,
			Stopper:   cfg.Stopper,
			Metrics:   cfg.TxnWaitMetrics,
			Knobs:     cfg.TxnWaitKnobs,
		}),
	}
	return m
}

// SequenceReq implements the RequestSequencer interface.
func (m *managerImpl) SequenceReq(
	ctx context.Context, prev *Guard, req Request, evalKind RequestEvalKind,
) (*Guard, Response, *Error) {
	var g *Guard
	if prev == nil {
		switch evalKind {
		case PessimisticEval:
			log.Event(ctx, "sequencing request")
		case OptimisticEval:
			log.Event(ctx, "optimistically sequencing request")
		case PessimisticAfterFailedOptimisticEval:
			panic("retry should have non-nil guard")
		}
		g = newGuard(req)
	} else {
		g = prev
		switch evalKind {
		case PessimisticEval:
			g.AssertNoLatches()
			log.Event(ctx, "re-sequencing request")
		case OptimisticEval:
			panic("optimistic eval cannot happen when re-sequencing")
		case PessimisticAfterFailedOptimisticEval:
			if !shouldIgnoreLatches(g.Req) {
				g.AssertLatches()
			}
			log.Event(ctx, "re-sequencing request after optimistic sequencing failed")
		}
	}
	g.EvalKind = evalKind
	resp, err := m.sequenceReqWithGuard(ctx, g)
	if resp != nil || err != nil {
		// Ensure that we release the guard if we return a response or an error.
		m.FinishReq(g)
		return nil, resp, err
	}
	return g, nil, nil
}

func (m *managerImpl) sequenceReqWithGuard(ctx context.Context, g *Guard) (Response, *Error) {
	// Some requests don't need to acquire latches at all.
	if shouldIgnoreLatches(g.Req) {
		log.Event(ctx, "not acquiring latches")
		return nil, nil
	}

	// Check if this is a request that waits on latches, but does not acquire
	// them.
	if shouldWaitOnLatchesWithoutAcquiring(g.Req) {
		log.Event(ctx, "waiting on latches without acquiring")
		return nil, m.lm.WaitFor(ctx, g.Req.LatchSpans, g.Req.PoisonPolicy)
	}

	// Provide the manager with an opportunity to intercept the request. It
	// may be able to serve the request directly, and even if not, it may be
	// able to update its internal state based on the request.
	resp, err := m.maybeInterceptReq(ctx, g.Req)
	if resp != nil || err != nil {
		return resp, err
	}

	// Only the first iteration can sometimes already be holding latches -- we
	// use this to assert below.
	firstIteration := true
	for {
		if !g.HoldingLatches() {
			if g.EvalKind == OptimisticEval {
				if !firstIteration {
					// The only way we loop more than once is when conflicting locks are
					// found -- see below where that happens and the comment there on
					// why it will never happen with OptimisticEval.
					panic("optimistic eval should not loop in sequenceReqWithGuard")
				}
				log.Event(ctx, "optimistically acquiring latches")
				g.lg = m.lm.AcquireOptimistic(g.Req)
				g.lm = m.lm
			} else {
				// Acquire latches for the request. This synchronizes the request
				// with all conflicting in-flight requests.
				log.Event(ctx, "acquiring latches")
				g.lg, err = m.lm.Acquire(ctx, g.Req)
				if err != nil {
					return nil, err
				}
				g.lm = m.lm
			}
		} else {
			if !firstIteration {
				panic(errors.AssertionFailedf("second or later iteration cannot be holding latches"))
			}
			if g.EvalKind != PessimisticAfterFailedOptimisticEval {
				panic("must not be holding latches")
			}
			log.Event(ctx, "optimistic failed, so waiting for latches")
			g.lg, err = m.lm.WaitUntilAcquired(ctx, g.lg)
			if err != nil {
				return nil, err
			}
		}
		firstIteration = false

		// Some requests don't want the wait on locks.
		if g.Req.LockSpans.Empty() {
			return nil, nil
		}

		// Set the request's MaxWaitQueueLength based on the cluster setting, if not
		// already set.
		if g.Req.MaxLockWaitQueueLength == 0 {
			g.Req.MaxLockWaitQueueLength = int(MaxLockWaitQueueLength.Get(&m.st.SV))
		}

		if g.EvalKind == OptimisticEval {
			if g.ltg != nil {
				panic("Optimistic locking should not have a non-nil lockTableGuard")
			}
			log.Event(ctx, "optimistically scanning lock table for conflicting locks")
			g.ltg = m.lt.ScanOptimistic(g.Req)
		} else {
			// Scan for conflicting locks.
			log.Event(ctx, "scanning lock table for conflicting locks")
			g.ltg = m.lt.ScanAndEnqueue(g.Req, g.ltg)
		}

		// Wait on conflicting locks, if necessary. Note that this will never be
		// true if ScanOptimistic was called above. Therefore it will also never
		// be true if latchManager.AcquireOptimistic was called.
		if g.ltg.ShouldWait() {
			m.lm.Release(g.moveLatchGuard())

			log.Event(ctx, "waiting in lock wait-queues")
			if err := m.ltw.WaitOn(ctx, g.Req, g.ltg); err != nil {
				return nil, err
			}
			continue
		}
		return nil, nil
	}
}

// maybeInterceptReq allows the concurrency manager to intercept requests before
// sequencing and evaluation so that it can immediately act on them. This allows
// the concurrency manager to route certain concurrency control-related requests
// into queues and optionally update its internal state based on the requests.
func (m *managerImpl) maybeInterceptReq(ctx context.Context, req Request) (Response, *Error) {
	switch {
	case req.isSingle(roachpb.PushTxn):
		// If necessary, wait in the txnWaitQueue for the pushee transaction to
		// expire or to move to a finalized state.
		t := req.Requests[0].GetPushTxn()
		resp, err := m.twq.MaybeWaitForPush(ctx, t)
		if err != nil {
			return nil, err
		} else if resp != nil {
			return makeSingleResponse(resp), nil
		}
	case req.isSingle(roachpb.QueryTxn):
		// If necessary, wait in the txnWaitQueue for a transaction state update
		// or for a dependent transaction to change.
		t := req.Requests[0].GetQueryTxn()
		return nil, m.twq.MaybeWaitForQuery(ctx, t)
	default:
		// TODO(nvanbenschoten): in the future, use this hook to update the lock
		// table to allow contending transactions to proceed.
		// for _, arg := range req.Requests {
		// 	switch t := arg.GetInner().(type) {
		// 	case *roachpb.ResolveIntentRequest:
		// 		_ = t
		// 	case *roachpb.ResolveIntentRangeRequest:
		// 		_ = t
		// 	}
		// }
	}
	return nil, nil
}

// shouldIgnoreLatches determines whether the request should ignore latches
// before proceeding to evaluate. Latches are used to synchronize with other
// conflicting requests, based on the Spans collected for the request. Most
// request types will want to acquire latches. Requests that return true for
// shouldWaitOnLatchesWithoutAcquiring will not completely ignore latches as
// they could wait on them, even if they don't acquire latches.
func shouldIgnoreLatches(req Request) bool {
	switch {
	case req.ReadConsistency != roachpb.CONSISTENT:
		// Only acquire latches for consistent operations.
		return true
	case req.isSingle(roachpb.RequestLease):
		// Ignore latches for lease requests. These requests are run on replicas
		// that do not hold the lease, so acquiring latches wouldn't help
		// synchronize with other requests.
		return true
	}
	return false
}

// shouldWaitOnLatchesWithoutAcquiring determines if this is a request that
// only waits on existing latches without acquiring any new ones.
func shouldWaitOnLatchesWithoutAcquiring(req Request) bool {
	return req.isSingle(roachpb.Barrier)
}

// PoisonReq implements the RequestSequencer interface.
func (m *managerImpl) PoisonReq(g *Guard) {
	// NB: g.lg == nil is the case for requests that ignore latches, see
	// shouldIgnoreLatches.
	if g.lg != nil {
		m.lm.Poison(g.lg)
	}
}

// FinishReq implements the RequestSequencer interface.
func (m *managerImpl) FinishReq(g *Guard) {
	// NOTE: we release latches _before_ exiting lock wait-queues deliberately.
	// Either order would be correct, but the order here avoids non-determinism in
	// cases where a request A holds both latches and lock wait-queue reservations
	// and has a request B waiting on its reservations. If request A released its
	// reservations before releasing its latches, it would be possible for B to
	// beat A to the latch manager and end up blocking on its latches briefly. Not
	// only is this confusing in traces, but it is slightly less efficient than if
	// request A released latches before letting anyone waiting on it in the lock
	// table proceed, ensuring that waiters do not hit its latches.
	//
	// Elsewhere, we relate the relationship of between the latch manager and the
	// lock-table to that of a mutex and condition variable pair. Following that
	// analogy, this release ordering is akin to signaling a condition variable
	// after releasing its associated mutex. Doing so ensures that whoever the
	// signaler wakes up (if anyone) will never bump into its mutex immediately
	// upon resumption.
	if lg := g.moveLatchGuard(); lg != nil {
		m.lm.Release(lg)
	}
	if ltg := g.moveLockTableGuard(); ltg != nil {
		m.lt.Dequeue(ltg)
	}
	releaseGuard(g)
}

// HandleWriterIntentError implements the ContentionHandler interface.
func (m *managerImpl) HandleWriterIntentError(
	ctx context.Context, g *Guard, seq roachpb.LeaseSequence, t *roachpb.WriteIntentError,
) (*Guard, *Error) {
	if g.ltg == nil {
		log.Fatalf(ctx, "cannot handle WriteIntentError %v for request without "+
			"lockTableGuard; were lock spans declared for this request?", t)
	}

	// Add a discovered lock to lock-table for each intent and enter each lock's
	// wait-queue.
	//
	// If the lock-table is disabled and one or more of the intents are ignored
	// then we proceed without the intent being added to the lock table. In such
	// cases, we know that this replica is no longer the leaseholder. One of two
	// things can happen next.
	// 1) if the request cannot be served on this follower replica according to
	//    the closed timestamp then it will be redirected to the leaseholder on
	//    its next evaluation attempt, where it may discover the same intent and
	//    wait in the new leaseholder's lock table.
	// 2) if the request can be served on this follower replica according to the
	//    closed timestamp then it will likely re-encounter the same intent on its
	//    next evaluation attempt. The WriteIntentError will then be mapped to an
	//    InvalidLeaseError in maybeAttachLease, which will indicate that the
	//    request cannot be served as a follower read after all and cause the
	//    request to be redirected to the leaseholder.
	//
	// Either way, there is no possibility of the request entering an infinite
	// loop without making progress.
	consultFinalizedTxnCache :=
		int64(len(t.Intents)) > DiscoveredLocksThresholdToConsultFinalizedTxnCache.Get(&m.st.SV)
	for i := range t.Intents {
		intent := &t.Intents[i]
		added, err := m.lt.AddDiscoveredLock(intent, seq, consultFinalizedTxnCache, g.ltg)
		if err != nil {
			log.Fatalf(ctx, "%v", err)
		}
		if !added {
			log.VEventf(ctx, 2,
				"intent on %s discovered but not added to disabled lock table",
				intent.Key.String())
		}
	}

	// Release the Guard's latches but continue to remain in lock wait-queues by
	// not releasing lockWaitQueueGuards. We expect the caller of this method to
	// then re-sequence the Request by calling SequenceReq with the un-latched
	// Guard. This is analogous to iterating through the loop in SequenceReq.
	m.lm.Release(g.moveLatchGuard())

	// If the discovery process collected a set of intents to resolve before the
	// next evaluation attempt, do so.
	if toResolve := g.ltg.ResolveBeforeScanning(); len(toResolve) > 0 {
		if err := m.ltw.ResolveDeferredIntents(ctx, toResolve); err != nil {
			m.FinishReq(g)
			return nil, err
		}
	}

	return g, nil
}

// HandleTransactionPushError implements the ContentionHandler interface.
func (m *managerImpl) HandleTransactionPushError(
	ctx context.Context, g *Guard, t *roachpb.TransactionPushError,
) *Guard {
	m.twq.EnqueueTxn(&t.PusheeTxn)

	// Release the Guard's latches. The PushTxn request should not be in any
	// lock wait-queues because it does not scan the lockTable. We expect the
	// caller of this method to then re-sequence the Request by calling
	// SequenceReq with the un-latched Guard. This is analogous to iterating
	// through the loop in SequenceReq.
	m.lm.Release(g.moveLatchGuard())
	return g
}

// OnLockAcquired implements the LockManager interface.
func (m *managerImpl) OnLockAcquired(ctx context.Context, acq *roachpb.LockAcquisition) {
	if err := m.lt.AcquireLock(&acq.Txn, acq.Key, lock.Exclusive, acq.Durability); err != nil {
		log.Fatalf(ctx, "%v", err)
	}
}

// OnLockUpdated implements the LockManager interface.
func (m *managerImpl) OnLockUpdated(ctx context.Context, up *roachpb.LockUpdate) {
	if err := m.lt.UpdateLocks(up); err != nil {
		log.Fatalf(ctx, "%v", err)
	}
}

// OnTransactionUpdated implements the TransactionManager interface.
func (m *managerImpl) OnTransactionUpdated(ctx context.Context, txn *roachpb.Transaction) {
	m.twq.UpdateTxn(ctx, txn)
}

// GetDependents implements the TransactionManager interface.
func (m *managerImpl) GetDependents(txnID uuid.UUID) []uuid.UUID {
	return m.twq.GetDependents(txnID)
}

// OnRangeDescUpdated implements the RangeStateListener interface.
func (m *managerImpl) OnRangeDescUpdated(desc *roachpb.RangeDescriptor) {
	m.twq.OnRangeDescUpdated(desc)
}

// OnRangeLeaseUpdated implements the RangeStateListener interface.
func (m *managerImpl) OnRangeLeaseUpdated(seq roachpb.LeaseSequence, isLeaseholder bool) {
	if isLeaseholder {
		m.lt.Enable(seq)
		m.twq.Enable(seq)
	} else {
		// Disable all queues - the concurrency manager will no longer be
		// informed about all state transitions to locks and transactions.
		const disable = true
		m.lt.Clear(disable)
		m.twq.Clear(disable)
	}
}

// OnRangeSplit implements the RangeStateListener interface.
func (m *managerImpl) OnRangeSplit() {
	// TODO(nvanbenschoten): it only essential that we clear the half of the
	// lockTable which contains locks in the key range that is being split off
	// from the current range. For now though, we clear it all.
	const disable = false
	m.lt.Clear(disable)
	m.twq.Clear(disable)
}

// OnRangeMerge implements the RangeStateListener interface.
func (m *managerImpl) OnRangeMerge() {
	// Disable all queues - the range is being merged into its LHS neighbor.
	// It will no longer be informed about all state transitions to locks and
	// transactions.
	const disable = true
	m.lt.Clear(disable)
	m.twq.Clear(disable)
}

// OnReplicaSnapshotApplied implements the RangeStateListener interface.
func (m *managerImpl) OnReplicaSnapshotApplied() {
	// A snapshot can cause discontinuities in raft entry application. The
	// lockTable expects to observe all lock state transitions on the range
	// through LockManager listener methods. If there's a chance it missed a
	// state transition, it is safer to simply clear the lockTable and rebuild
	// it from persistent intent state by allowing requests to discover locks
	// and inform the manager through calls to HandleWriterIntentError.
	//
	// A range only maintains locks in the lockTable of its leaseholder replica
	// even thought it runs a concurrency manager on all replicas. Because of
	// this, we expect it to be very rare that this actually clears any locks.
	// Still, it is possible for the leaseholder replica to receive a snapshot
	// when it is not also the raft leader.
	const disable = false
	m.lt.Clear(disable)
}

// LatchMetrics implements the MetricExporter interface.
func (m *managerImpl) LatchMetrics() LatchMetrics {
	return m.lm.Metrics()
}

// LockTableMetrics implements the MetricExporter interface.
func (m *managerImpl) LockTableMetrics() LockTableMetrics {
	return m.lt.Metrics()
}

// TestingLockTableString implements the MetricExporter interface.
func (m *managerImpl) TestingLockTableString() string {
	return m.lt.String()
}

// TestingTxnWaitQueue implements the MetricExporter interface.
func (m *managerImpl) TestingTxnWaitQueue() *txnwait.Queue {
	return m.twq.(*txnwait.Queue)
}

func (r *Request) txnMeta() *enginepb.TxnMeta {
	if r.Txn == nil {
		return nil
	}
	return &r.Txn.TxnMeta
}

func (r *Request) isSingle(m roachpb.Method) bool {
	if len(r.Requests) != 1 {
		return false
	}
	return r.Requests[0].GetInner().Method() == m
}

// Used to avoid allocations.
var guardPool = sync.Pool{
	New: func() interface{} { return new(Guard) },
}

func newGuard(req Request) *Guard {
	g := guardPool.Get().(*Guard)
	g.Req = req
	return g
}

func releaseGuard(g *Guard) {
	if g.Req.LatchSpans != nil {
		g.Req.LatchSpans.Release()
	}
	if g.Req.LockSpans != nil {
		g.Req.LockSpans.Release()
	}
	*g = Guard{}
	guardPool.Put(g)
}

// LatchSpans returns the maximal set of spans that the request will access.
// The returned spanset is not safe for use after the guard has been finished.
func (g *Guard) LatchSpans() *spanset.SpanSet {
	return g.Req.LatchSpans
}

// TakeSpanSets transfers ownership of the Guard's LatchSpans and LockSpans
// SpanSets to the caller, ensuring that the SpanSets are not destroyed with the
// Guard. The method is only safe if called immediately before passing the Guard
// to FinishReq.
func (g *Guard) TakeSpanSets() (*spanset.SpanSet, *spanset.SpanSet) {
	la, lo := g.Req.LatchSpans, g.Req.LockSpans
	g.Req.LatchSpans, g.Req.LockSpans = nil, nil
	return la, lo
}

// HoldingLatches returned whether the guard is holding latches or not.
func (g *Guard) HoldingLatches() bool {
	return g != nil && g.lg != nil
}

// AssertLatches asserts that the guard is non-nil and holding latches, if the
// request is supposed to hold latches while evaluating in the first place.
func (g *Guard) AssertLatches() {
	if !shouldIgnoreLatches(g.Req) && !shouldWaitOnLatchesWithoutAcquiring(g.Req) && !g.HoldingLatches() {
		panic("expected latches held, found none")
	}
}

// AssertNoLatches asserts that the guard is non-nil and not holding latches.
func (g *Guard) AssertNoLatches() {
	if g.HoldingLatches() {
		panic("unexpected latches held")
	}
}

// IsolatedAtLaterTimestamps returns whether the request holding the guard would
// continue to be isolated from other requests / transactions even if it were to
// increase its request timestamp while evaluating. If the method returns false,
// the concurrency guard must be dropped and re-acquired with the new timestamp
// before the request can evaluate at that later timestamp.
func (g *Guard) IsolatedAtLaterTimestamps() bool {
	// If the request acquired any read latches with bounded (MVCC) timestamps
	// then it can not trivially bump its timestamp without dropping and
	// re-acquiring those latches. Doing so could allow the request to read at an
	// unprotected timestamp. We only look at global latch spans because local
	// latch spans always use unbounded (NonMVCC) timestamps.
	return len(g.Req.LatchSpans.GetSpans(spanset.SpanReadOnly, spanset.SpanGlobal)) == 0 &&
		// Similarly, if the request declared any global or local read lock spans
		// then it can not trivially bump its timestamp without dropping its
		// lockTableGuard and re-scanning the lockTable. Doing so could allow the
		// request to conflict with locks that it previously did not conflict with.
		len(g.Req.LockSpans.GetSpans(spanset.SpanReadOnly, spanset.SpanGlobal)) == 0 &&
		len(g.Req.LockSpans.GetSpans(spanset.SpanReadOnly, spanset.SpanLocal)) == 0
}

// CheckOptimisticNoConflicts checks that the {latch,lock}SpansRead do not
// have a conflicting latch, lock.
func (g *Guard) CheckOptimisticNoConflicts(
	latchSpansRead *spanset.SpanSet, lockSpansRead *spanset.SpanSet,
) (ok bool) {
	if g.EvalKind != OptimisticEval {
		panic(errors.AssertionFailedf("unexpected EvalKind: %d", g.EvalKind))
	}
	if g.lg == nil && g.ltg == nil {
		return true
	}
	if g.lg == nil {
		panic("expected non-nil latchGuard")
	}
	// First check the latches, since a conflict there could mean that racing
	// requests in the lock table caused a conflicting lock to not be noticed.
	if g.lm.CheckOptimisticNoConflicts(g.lg, latchSpansRead) {
		return g.ltg.CheckOptimisticNoConflicts(lockSpansRead)
	}
	return false
}

// CheckOptimisticNoLatchConflicts checks that the declared latch spans for
// the request do not have a conflicting latch.
func (g *Guard) CheckOptimisticNoLatchConflicts() (ok bool) {
	if g.EvalKind != OptimisticEval {
		panic(errors.AssertionFailedf("unexpected EvalKind: %d", g.EvalKind))
	}
	if g.lg == nil {
		return true
	}
	return g.lm.CheckOptimisticNoConflicts(g.lg, g.Req.LatchSpans)
}

func (g *Guard) moveLatchGuard() latchGuard {
	lg := g.lg
	g.lg = nil
	g.lm = nil
	return lg
}

func (g *Guard) moveLockTableGuard() lockTableGuard {
	ltg := g.ltg
	g.ltg = nil
	return ltg
}

func makeSingleResponse(r roachpb.Response) Response {
	ru := make(Response, 1)
	ru[0].MustSetInner(r)
	return ru
}
