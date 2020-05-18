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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanlatch"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/txnwait"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// managerImpl implements the Manager interface.
type managerImpl struct {
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
	*m = managerImpl{
		// TODO(nvanbenschoten): move pkg/storage/spanlatch to a new
		// pkg/storage/concurrency/latch package. Make it implement the
		// latchManager interface directly, if possible.
		lm: &latchManagerImpl{
			m: spanlatch.Make(
				cfg.Stopper,
				cfg.SlowLatchGauge,
			),
		},
		lt: &lockTableImpl{
			maxLocks: cfg.MaxLockTableSize,
		},
		ltw: &lockTableWaiterImpl{
			st:                cfg.Settings,
			stopper:           cfg.Stopper,
			ir:                cfg.IntentResolver,
			lm:                m,
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
	ctx context.Context, prev *Guard, req Request,
) (*Guard, Response, *Error) {
	var g *Guard
	if prev == nil {
		g = newGuard(req)
		log.Event(ctx, "sequencing request")
	} else {
		g = prev
		g.AssertNoLatches()
		log.Event(ctx, "re-sequencing request")
	}

	resp, err := m.sequenceReqWithGuard(ctx, g, req)
	if resp != nil || err != nil {
		// Ensure that we release the guard if we return a response or an error.
		m.FinishReq(g)
		return nil, resp, err
	}
	return g, nil, nil
}

func (m *managerImpl) sequenceReqWithGuard(
	ctx context.Context, g *Guard, req Request,
) (Response, *Error) {
	// Some requests don't need to acquire latches at all.
	if !shouldAcquireLatches(req) {
		log.Event(ctx, "not acquiring latches")
		return nil, nil
	}

	// Provide the manager with an opportunity to intercept the request. It
	// may be able to serve the request directly, and even if not, it may be
	// able to update its internal state based on the request.
	resp, err := m.maybeInterceptReq(ctx, req)
	if resp != nil || err != nil {
		return resp, err
	}

	for {
		// Acquire latches for the request. This synchronizes the request
		// with all conflicting in-flight requests.
		log.Event(ctx, "acquiring latches")
		g.lg, err = m.lm.Acquire(ctx, req)
		if err != nil {
			return nil, err
		}

		// Some requests don't want the wait on locks.
		if req.LockSpans.Empty() {
			return nil, nil
		}

		// Scan for conflicting locks.
		log.Event(ctx, "scanning lock table for conflicting locks")
		g.ltg = m.lt.ScanAndEnqueue(g.Req, g.ltg)

		// Wait on conflicting locks, if necessary.
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

// shouldAcquireLatches determines whether the request should acquire latches
// before proceeding to evaluate. Latches are used to synchronize with other
// conflicting requests, based on the Spans collected for the request. Most
// request types will want to acquire latches.
func shouldAcquireLatches(req Request) bool {
	switch {
	case req.ReadConsistency != roachpb.CONSISTENT:
		// Only acquire latches for consistent operations.
		return false
	case req.isSingle(roachpb.RequestLease):
		// Do not acquire latches for lease requests. These requests are run on
		// replicas that do not hold the lease, so acquiring latches wouldn't
		// help synchronize with other requests.
		return false
	}
	return true
}

// FinishReq implements the RequestSequencer interface.
func (m *managerImpl) FinishReq(g *Guard) {
	if ltg := g.moveLockTableGuard(); ltg != nil {
		m.lt.Dequeue(ltg)
	}
	if lg := g.moveLatchGuard(); lg != nil {
		m.lm.Release(lg)
	}
	releaseGuard(g)
}

// HandleWriterIntentError implements the ContentionHandler interface.
func (m *managerImpl) HandleWriterIntentError(
	ctx context.Context, g *Guard, t *roachpb.WriteIntentError,
) (*Guard, *Error) {
	if g.ltg == nil {
		log.Fatalf(ctx, "cannot handle WriteIntentError %v for request without "+
			"lockTableGuard; were lock spans declared for this request?", t)
	}

	// Add a discovered lock to lock-table for each intent and enter each lock's
	// wait-queue. If the lock-table is disabled and one or more of the intents
	// are ignored then we immediately wait on all intents.
	wait := false
	for i := range t.Intents {
		intent := &t.Intents[i]
		added, err := m.lt.AddDiscoveredLock(intent, g.ltg)
		if err != nil {
			log.Fatalf(ctx, "%v", errors.HandleAsAssertionFailure(err))
		}
		if !added {
			wait = true
		}
	}

	// Release the Guard's latches but continue to remain in lock wait-queues by
	// not releasing lockWaitQueueGuards. We expect the caller of this method to
	// then re-sequence the Request by calling SequenceReq with the un-latched
	// Guard. This is analogous to iterating through the loop in SequenceReq.
	m.lm.Release(g.moveLatchGuard())

	// If the lockTable was disabled then we need to immediately wait on the
	// intents to ensure that they are resolved and moved out of the request's
	// way.
	if wait {
		for i := range t.Intents {
			intent := &t.Intents[i]
			if err := m.ltw.WaitOnLock(ctx, g.Req, intent); err != nil {
				m.FinishReq(g)
				return nil, err
			}
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
		log.Fatalf(ctx, "%v", errors.HandleAsAssertionFailure(err))
	}
}

// OnLockUpdated implements the LockManager interface.
func (m *managerImpl) OnLockUpdated(ctx context.Context, up *roachpb.LockUpdate) {
	if err := m.lt.UpdateLocks(up); err != nil {
		log.Fatalf(ctx, "%v", errors.HandleAsAssertionFailure(err))
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
func (m *managerImpl) OnRangeLeaseUpdated(isLeaseholder bool) {
	if isLeaseholder {
		m.lt.Enable()
		m.twq.Enable()
	} else {
		// Disable all queues - the concurrency manager will no longer be
		// informed about all state transitions to locks and transactions.
		const disable = true
		m.lt.Clear(disable)
		m.twq.Clear(disable)
		// Also clear caches, since they won't be needed any time soon and
		// consume memory.
		m.ltw.ClearCaches()
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
func (m *managerImpl) LatchMetrics() (global, local kvserverpb.LatchManagerInfo) {
	return m.lm.Info()
}

// LockTableDebug implements the MetricExporter interface.
func (m *managerImpl) LockTableDebug() string {
	return m.lt.String()
}

// TxnWaitQueue implements the MetricExporter interface.
func (m *managerImpl) TxnWaitQueue() *txnwait.Queue {
	return m.twq.(*txnwait.Queue)
}

func (r *Request) txnMeta() *enginepb.TxnMeta {
	if r.Txn == nil {
		return nil
	}
	return &r.Txn.TxnMeta
}

// readConflictTimestamp returns the maximum timestamp at which the request
// conflicts with locks acquired by other transaction. The request must wait
// for all locks acquired by other transactions at or below this timestamp
// to be released. All locks acquired by other transactions above this
// timestamp are ignored.
func (r *Request) readConflictTimestamp() hlc.Timestamp {
	ts := r.Timestamp
	if r.Txn != nil {
		ts = r.Txn.ReadTimestamp
		ts.Forward(r.Txn.MaxTimestamp)
	}
	return ts
}

// writeConflictTimestamp returns the minimum timestamp at which the request
// acquires locks when performing mutations. All writes performed by the
// requests must take place at or above this timestamp.
func (r *Request) writeConflictTimestamp() hlc.Timestamp {
	ts := r.Timestamp
	if r.Txn != nil {
		ts = r.Txn.WriteTimestamp
	}
	return ts
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
	*g = Guard{}
	guardPool.Put(g)
}

// LatchSpans returns the maximal set of spans that the request will access.
func (g *Guard) LatchSpans() *spanset.SpanSet {
	return g.Req.LatchSpans
}

// HoldingLatches returned whether the guard is holding latches or not.
func (g *Guard) HoldingLatches() bool {
	return g != nil && g.lg != nil
}

// AssertLatches asserts that the guard is non-nil and holding latches.
func (g *Guard) AssertLatches() {
	if !g.HoldingLatches() {
		panic("expected latches held, found none")
	}
}

// AssertNoLatches asserts that the guard is non-nil and not holding latches.
func (g *Guard) AssertNoLatches() {
	if g.HoldingLatches() {
		panic("unexpected latches held")
	}
}

func (g *Guard) moveLatchGuard() latchGuard {
	lg := g.lg
	g.lg = nil
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
