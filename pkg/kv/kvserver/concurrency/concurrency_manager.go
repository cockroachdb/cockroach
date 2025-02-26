// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package concurrency

import (
	"context"
	"fmt"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/lockspanset"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanlatch"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/txnwait"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/debugutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
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
	settings.SystemOnly,
	"kv.lock_table.maximum_lock_wait_queue_length",
	"the maximum length of a lock wait-queue that read-write requests are willing "+
		"to enter and wait in. The setting can be used to ensure some level of quality-of-service "+
		"under severe per-key contention. If set to a non-zero value and an existing lock "+
		"wait-queue is already equal to or exceeding this length, requests will be rejected "+
		"eagerly instead of entering the queue and waiting. Set to 0 to disable.",
	0,
	settings.WithValidateInt(func(v int64) error {
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
	}),
)

// DiscoveredLocksThresholdToConsultTxnStatusCache sets a threshold as mentioned
// in the description string. The default of 200 is somewhat arbitrary but
// should suffice for small OLTP transactions. Given the default
// 10,000 lock capacity of the lock table, 200 is small enough to not matter
// much against the capacity, which is desirable. We have seen examples with
// discoveredCount > 100,000, caused by stats collection, where we definitely
// want to avoid adding these locks to the lock table, if possible.
var DiscoveredLocksThresholdToConsultTxnStatusCache = settings.RegisterIntSetting(
	settings.SystemOnly,
	// NOTE: the name of this setting mentions "finalized" for historical reasons.
	"kv.lock_table.discovered_locks_threshold_for_consulting_finalized_txn_cache",
	"the maximum number of discovered locks by a waiter, above which the txn status cache"+
		"is consulted and resolvable locks are not added to the lock table -- this should be a small"+
		"fraction of the maximum number of locks in the lock table",
	200,
	settings.NonNegativeInt,
)

// BatchPushedLockResolution controls whether the lock table should allow
// non-locking readers to defer and batch the resolution of conflicting locks
// whose holder is known to be pending and have been pushed above the reader's
// timestamp.
var BatchPushedLockResolution = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"kv.lock_table.batch_pushed_lock_resolution.enabled",
	"whether the lock table should allow non-locking readers to defer and batch the resolution of "+
		"conflicting locks whose holder is known to be pending and have been pushed above the reader's "+
		"timestamp",
	true,
)

// UnreplicatedLockReliability controls whether the replica will attempt
// to keep unreplicated locks during node operations such as split.
var UnreplicatedLockReliability = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"kv.lock_table.unreplicated_lock_reliability.enabled",
	"whether the replica should attempt to keep unreplicated locks during various node operations",
	metamorphic.ConstantWithTestBool("kv.lock_table.unreplicated_lock_reliability_upgrade.enabled", true),
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
	TxnWaitMetrics     *txnwait.Metrics
	SlowLatchGauge     *metric.Gauge
	LatchWaitDurations metric.IHistogram
	// Configs + Knobs.
	MaxLockTableSize  int64
	DisableTxnPushing bool
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
	lt := maybeWrapInVerifyingLockTable(
		newLockTable(cfg.MaxLockTableSize, cfg.RangeDesc.RangeID, cfg.Clock, cfg.Settings),
	)
	*m = managerImpl{
		st: cfg.Settings,
		// TODO(nvanbenschoten): move pkg/storage/spanlatch to a new
		// pkg/storage/concurrency/latch package. Make it implement the
		// latchManager interface directly, if possible.
		lm: &latchManagerImpl{
			m: spanlatch.Make(
				cfg.Stopper,
				cfg.SlowLatchGauge,
				cfg.Settings,
				cfg.LatchWaitDurations,
			),
		},
		lt: lt,
		ltw: &lockTableWaiterImpl{
			nodeDesc:          cfg.NodeDesc,
			st:                cfg.Settings,
			clock:             cfg.Clock,
			stopper:           cfg.Stopper,
			ir:                cfg.IntentResolver,
			lt:                lt,
			disableTxnPushing: cfg.DisableTxnPushing,
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
	var branch int
	if prev == nil {
		switch evalKind {
		case PessimisticEval:
			branch = 1
			log.Event(ctx, "sequencing request")
		case OptimisticEval:
			branch = 2
			log.Event(ctx, "optimistically sequencing request")
		case PessimisticAfterFailedOptimisticEval:
			panic("retry should have non-nil guard")
		default:
			panic("unexpected evalKind")
		}
		g = newGuard(req)
	} else {
		g = prev
		switch evalKind {
		case PessimisticEval:
			branch = 3
			g.AssertNoLatches()
			log.Event(ctx, "re-sequencing request")
		case OptimisticEval:
			panic("optimistic eval cannot happen when re-sequencing")
		case PessimisticAfterFailedOptimisticEval:
			branch = 4
			if !shouldIgnoreLatches(g.Req) {
				g.AssertLatches()
			}
			log.Event(ctx, "re-sequencing request after optimistic sequencing failed")
		default:
			panic("unexpected evalKind")
		}
	}
	g.EvalKind = evalKind
	resp, err := m.sequenceReqWithGuard(ctx, g, branch)
	if resp != nil || err != nil {
		// Ensure that we release the guard if we return a response or an error.
		m.FinishReq(ctx, g)
		return nil, resp, err
	}
	return g, nil, nil
}

func (m *managerImpl) sequenceReqWithGuard(
	ctx context.Context, g *Guard, branch int,
) (Response, *Error) {
	// Some requests don't need to acquire latches at all.
	if shouldIgnoreLatches(g.Req) {
		log.Event(ctx, "not acquiring latches")
		return nil, nil
	}

	// Check if this is a request that waits on latches, but does not acquire
	// them.
	if shouldWaitOnLatchesWithoutAcquiring(g.Req) {
		log.Event(ctx, "waiting on latches without acquiring")
		return nil, m.lm.WaitFor(ctx, g.Req.LatchSpans, g.Req.PoisonPolicy, g.Req.BaFmt)
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
				panic(redact.Safe(fmt.Sprintf("must not be holding latches\n"+
					"this is tracked in github.com/cockroachdb/cockroach/issues/77663; please comment if seen\n"+
					"eval_kind=%d, holding_latches=%t, branch=%d, first_iteration=%t, stack=\n%s",
					g.EvalKind, g.HoldingLatches(), branch, firstIteration, debugutil.Stack())))
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
			g.ltg, err = m.lt.ScanAndEnqueue(g.Req, g.ltg)
			if err != nil {
				return nil, err
			}
		}

		// Wait on conflicting locks, if necessary. Note that this will never be
		// true if ScanOptimistic was called above. Therefore it will also never
		// be true if latchManager.AcquireOptimistic was called.
		if g.ltg.ShouldWait() {
			m.lm.Release(ctx, g.moveLatchGuard())

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
	case req.isSingle(kvpb.PushTxn):
		// If necessary, wait in the txnWaitQueue for the pushee transaction to
		// expire or to move to a finalized state.
		t := req.Requests[0].GetPushTxn()
		resp, err := m.twq.MaybeWaitForPush(ctx, t, req.WaitPolicy)
		if err != nil {
			return nil, err
		} else if resp != nil {
			return makeSingleResponse(resp), nil
		}
	case req.isSingle(kvpb.QueryTxn):
		// If necessary, wait in the txnWaitQueue for a transaction state update
		// or for a dependent transaction to change.
		t := req.Requests[0].GetQueryTxn()
		return nil, m.twq.MaybeWaitForQuery(ctx, t)
	default:
		// TODO(nvanbenschoten): in the future, use this hook to update the lock
		// table to allow contending transactions to proceed.
		// for _, arg := range req.Requests {
		// 	switch t := arg.GetInner().(type) {
		// 	case *kvpb.ResolveIntentRequest:
		// 		_ = t
		// 	case *kvpb.ResolveIntentRangeRequest:
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
	case req.ReadConsistency != kvpb.CONSISTENT:
		// Only acquire latches for consistent operations.
		return true
	case req.isSingle(kvpb.RequestLease):
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
	return req.isSingle(kvpb.Barrier)
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
func (m *managerImpl) FinishReq(ctx context.Context, g *Guard) {
	// NOTE: we release latches _before_ exiting lock wait-queues deliberately.
	// Either order would be correct, but the order here avoids non-determinism in
	// cases where a request A holds both latches and has claimed some keys by
	// virtue of being the first request in a lock wait-queue and has a request B
	// waiting on its claim. If request A released its claim (by exiting the lock
	// wait-queue) before releasing its latches, it would be possible for B to
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
		m.lm.Release(ctx, lg)
	}
	if ltg := g.moveLockTableGuard(); ltg != nil {
		m.lt.Dequeue(ltg)
	}
	releaseGuard(g)
}

// HandleLockConflictError implements the ContentionHandler interface.
func (m *managerImpl) HandleLockConflictError(
	ctx context.Context, g *Guard, seq roachpb.LeaseSequence, t *kvpb.LockConflictError,
) (*Guard, *Error) {
	if g.ltg == nil {
		log.Fatalf(ctx, "cannot handle LockConflictError %v for request without "+
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
	//    next evaluation attempt. The LockConflictError will then be mapped to an
	//    InvalidLeaseError in maybeAttachLease, which will indicate that the
	//    request cannot be served as a follower read after all and cause the
	//    request to be redirected to the leaseholder.
	//
	// Either way, there is no possibility of the request entering an infinite
	// loop without making progress.
	consultTxnStatusCache :=
		int64(len(t.Locks)) > DiscoveredLocksThresholdToConsultTxnStatusCache.Get(&m.st.SV)
	for i := range t.Locks {
		foundLock := &t.Locks[i]
		added, err := m.lt.AddDiscoveredLock(foundLock, seq, consultTxnStatusCache, g.ltg)
		if err != nil {
			log.Fatalf(ctx, "%v", err)
		}
		if !added {
			log.VEventf(ctx, 2,
				"intent on %s discovered but not added to disabled lock table",
				foundLock.Key.String())
		}
	}

	// Release the Guard's latches but continue to remain in lock wait-queues by
	// not releasing lockWaitQueueGuards. We expect the caller of this method to
	// then re-sequence the Request by calling SequenceReq with the un-latched
	// Guard. This is analogous to iterating through the loop in SequenceReq.
	m.lm.Release(ctx, g.moveLatchGuard())

	// If the discovery process collected a set of intents to resolve before the
	// next evaluation attempt, do so.
	if toResolve := g.ltg.ResolveBeforeScanning(); len(toResolve) > 0 {
		if err := m.ltw.ResolveDeferredIntents(ctx, g.Req.AdmissionHeader, toResolve); err != nil {
			m.FinishReq(ctx, g)
			return nil, err
		}
	}

	return g, nil
}

// HandleTransactionPushError implements the ContentionHandler interface.
func (m *managerImpl) HandleTransactionPushError(
	ctx context.Context, g *Guard, t *kvpb.TransactionPushError,
) *Guard {
	m.twq.EnqueueTxn(&t.PusheeTxn)

	// Release the Guard's latches. The PushTxn request should not be in any
	// lock wait-queues because it does not scan the lockTable. We expect the
	// caller of this method to then re-sequence the Request by calling
	// SequenceReq with the un-latched Guard. This is analogous to iterating
	// through the loop in SequenceReq.
	m.lm.Release(ctx, g.moveLatchGuard())
	return g
}

// OnLockAcquired implements the LockManager interface.
func (m *managerImpl) OnLockAcquired(ctx context.Context, acq *roachpb.LockAcquisition) {
	if err := m.lt.AcquireLock(acq); err != nil {
		if errors.IsAssertionFailure(err) {
			log.Fatalf(ctx, "%v", err)
		}
		// It's reasonable to expect benign errors here that the layer above
		// (command evaluation) isn't equipped to deal with. As long as we're not
		// violating any assertions, we simply log and move on. One benign case is
		// when an unreplicated lock is being acquired by a transaction at an older
		// epoch.
		log.Errorf(ctx, "%v", err)
	}
}

// OnLockUpdated implements the LockManager interface.
func (m *managerImpl) OnLockUpdated(ctx context.Context, up *roachpb.LockUpdate) {
	if err := m.lt.UpdateLocks(up); err != nil {
		log.Fatalf(ctx, "%v", err)
	}
}

// QueryLockTableState implements the LockManager interface.
func (m *managerImpl) QueryLockTableState(
	ctx context.Context, span roachpb.Span, opts QueryLockTableOptions,
) ([]roachpb.LockStateInfo, QueryLockTableResumeState) {
	return m.lt.QueryLockTableState(span, opts)
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

var allKeysSpan = roachpb.Span{Key: keys.MinKey, EndKey: keys.MaxKey}

// OnRangeLeaseTransferEval implements the RangeStateListener interface.
func (m *managerImpl) OnRangeLeaseTransferEval() []*roachpb.LockAcquisition {
	if !UnreplicatedLockReliability.Get(&m.st.SV) {
		return nil
	}

	// TODO(ssd): Expose a function that allows us to pre-allocate this a bit better.
	acquistions := make([]*roachpb.LockAcquisition, 0)
	m.lt.ExportUnreplicatedLocks(allKeysSpan, func(acq *roachpb.LockAcquisition) {
		acquistions = append(acquistions, acq)
	})
	return acquistions
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

// OnRangeSplit implements the RangeStateListener interface. It is called on the
// LHS replica of a split and should be passed the new RHS start key (LHS
// EndKey).
func (m *managerImpl) OnRangeSplit(rhsStartKey roachpb.Key) []roachpb.LockAcquisition {
	if UnreplicatedLockReliability.Get(&m.st.SV) {
		lockToMove := m.lt.ClearGE(rhsStartKey)
		m.twq.ClearGE(rhsStartKey)
		return lockToMove
	} else {
		// TODO(ssd): We could call ClearGE here but ignore the
		// response. But for now we leave the old behaviour unchanged.
		const disable = false
		m.lt.Clear(disable)
		m.twq.Clear(disable)
		return nil
	}
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
	// and inform the manager through calls to HandleLockConflictError.
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

// TestingSetMaxLocks implements the TestingAccessor interface.
func (m *managerImpl) TestingSetMaxLocks(maxLocks int64) {
	m.lt.TestingSetMaxLocks(maxLocks)
}

func (r *Request) isSingle(m kvpb.Method) bool {
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
func (g *Guard) TakeSpanSets() (*spanset.SpanSet, *lockspanset.LockSpanSet) {
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
	// then it cannot trivially bump its timestamp without dropping and
	// re-acquiring those latches. Doing so could allow the request to read at
	// an unprotected timestamp. We only look at global latch spans because local
	// latch spans always use unbounded (NonMVCC) timestamps.
	//
	// Even still, the existence of read only global latch spans is not enough for
	// us to determine that the request is not isolated at higher timestamps -- we
	// must check the timestamps at which the latches are declared as well. That's
	// because if a read latch is declared at hlc.MaxTimestamp, it is isolated at
	// higher timestamps; shared locking requests do exactly this.
	readLatchesIsolatedAtHigherTimestamp := true
	for _, l := range g.Req.LatchSpans.GetSpans(spanset.SpanReadOnly, spanset.SpanGlobal) {
		if !l.Timestamp.Equal(hlc.MaxTimestamp) {
			readLatchesIsolatedAtHigherTimestamp = false
			break
		}
	}
	// If read latches are isolated at higher timestamps then the request can
	// trivially bump its timestamp without dropping and re-acquiring those
	// latches. There's no need to check write latches, as they're always isolated
	// at higher timestamps.
	return readLatchesIsolatedAtHigherTimestamp &&
		// If the request intends to perform any non-locking reads, it cannot
		// trivially bump its timestamp and expect to be isolated at the higher
		// timestamp. Bumping its timestamp could cause the request to conflict with
		// locks that it previously did not conflict with. It must drop its
		// lockTableGuard and re-scan the lockTable.
		len(g.Req.LockSpans.GetSpans(lock.None)) == 0
}

// CheckOptimisticNoConflicts checks that the {latch,lock}SpansRead do not
// have a conflicting latch, lock.
func (g *Guard) CheckOptimisticNoConflicts(
	latchSpansRead *spanset.SpanSet, lockSpansRead *lockspanset.LockSpanSet,
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

// IsKeyLockedByConflictingTxn returns whether the specified key is locked by
// a conflicting transaction in the lockTableGuard's snapshot of the lock
// table, given the caller's own desired locking strength. If so, true is
// returned and so is the lock holder. If the lock is held by the transaction
// itself, there's no conflict to speak of, so false is returned.
//
// This method is used by requests in conjunction with the SkipLocked wait
// policy to determine which keys they should skip over during evaluation.
//
// If the supplied lock strength is locking (!= lock.None), then any queued
// locking requests that came before the lockTableGuard will also be checked
// for conflicts. This helps prevent a stream of locking SKIP LOCKED requests
// from starving out regular locking requests. In such cases, true is
// returned, but so is nil.
func (g *Guard) IsKeyLockedByConflictingTxn(
	ctx context.Context, key roachpb.Key, strength lock.Strength,
) (bool, *enginepb.TxnMeta, error) {
	return g.ltg.IsKeyLockedByConflictingTxn(ctx, key, strength)
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

func makeSingleResponse(r kvpb.Response) Response {
	ru := make(Response, 1)
	ru[0].MustSetInner(r)
	return ru
}
