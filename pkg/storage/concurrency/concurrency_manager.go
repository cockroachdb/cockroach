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

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/storage/spanlatch"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/cockroachdb/cockroach/pkg/storage/txnwait"
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
	// The Store and Range that the manager is responsible for.
	str Store
	rng *roachpb.RangeDescriptor
}

// Store provides some parts of a Store without incurring a dependency. It is a
// superset of txnwait.StoreInterface.
type Store interface {
	// Identification.
	NodeDescriptor() *roachpb.NodeDescriptor
	// Components.
	DB() *client.DB
	Clock() *hlc.Clock
	Stopper() *stop.Stopper
	IntentResolver() IntentResolver
	// Knobs.
	GetTxnWaitKnobs() txnwait.TestingKnobs
	// Metrics.
	GetTxnWaitMetrics() *txnwait.Metrics
	GetSlowLatchGauge() *metric.Gauge
}

// NewManager creates a new concurrency Manager structure.
func NewManager(store Store, rng *roachpb.RangeDescriptor) Manager {
	// TODO(nvanbenschoten): make the lockTable size and lockTableWaiter
	// dependencyCyclePushDelay configurable.
	m := new(managerImpl)
	*m = managerImpl{
		// TODO(nvanbenschoten): move pkg/storage/spanlatch to a new
		// pkg/storage/concurrency/latch package. Make it implement the
		// latchManager interface directly, if possible.
		lm: &latchManagerImpl{
			m: spanlatch.Make(store.Stopper(), store.GetSlowLatchGauge()),
		},
		lt: newLockTable(10000 /* arbitrary */),
		ltw: &lockTableWaiterImpl{
			nodeID:                   store.NodeDescriptor().NodeID,
			stopper:                  store.Stopper(),
			ir:                       store.IntentResolver(),
			dependencyCyclePushDelay: defaultDependencyCyclePushDelay,
		},
		// TODO(nvanbenschoten): move pkg/storage/txnwait to a new
		// pkg/storage/concurrency/txnwait package.
		twq: txnwait.NewQueue(store, m),
		str: store,
		rng: rng,
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
		g.assertNoLatches()
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
		if !shouldWaitOnConflicts(req) {
			return nil, nil
		}

		// Scan for conflicting locks.
		log.Event(ctx, "scanning lock table for conflicting locks")
		g.ltg = m.lt.ScanAndEnqueue(g.req, g.ltg)

		// Wait on conflicting locks, if necessary.
		if g.ltg.ShouldWait() {
			m.lm.Release(g.moveLatchGuard())

			log.Event(ctx, "waiting in lock wait-queues")
			if err := m.ltw.WaitOn(ctx, g.req, g.ltg); err != nil {
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

// shouldWaitOnConflicts determines whether the request should wait on locks and
// wait-queues owned by other transactions before proceeding to evaluate. Most
// requests will want to wait on conflicting transactions to ensure that they
// are sufficiently isolated during their evaluation, but some "isolation aware"
// requests want to proceed to evaluation even in the presence of conflicts
// because they know how to handle them.
func shouldWaitOnConflicts(req Request) bool {
	for _, ru := range req.Requests {
		arg := ru.GetInner()
		if roachpb.IsTransactional(arg) {
			switch arg.Method() {
			case roachpb.Refresh, roachpb.RefreshRange:
				// Refresh and RefreshRange requests scan inconsistently so that
				// they can collect all intents in their key span. If they run
				// into intents written by other transactions, they simply fail
				// the refresh without trying to push the intents and blocking.
			default:
				return true
			}
		}
	}
	return false
}

// FinishReq implements the RequestSequencer interface.
func (m *managerImpl) FinishReq(g *Guard) {
	if ltg := g.moveLockTableGuard(); ltg != nil {
		m.lt.Dequeue(ltg)
	}
	if lg := g.moveLatchGuard(); lg != nil {
		m.lm.Release(lg)
	}
}

// HandleWriterIntentError implements the ContentionHandler interface.
func (m *managerImpl) HandleWriterIntentError(
	ctx context.Context, g *Guard, t *roachpb.WriteIntentError,
) *Guard {
	// Add a discovered lock to lock-table for each intent and enter each lock's
	// wait-queue.
	for i := range t.Intents {
		intent := &t.Intents[i]
		if err := m.lt.AddDiscoveredLock(intent, g.ltg); err != nil {
			log.Fatal(ctx, errors.HandleAsAssertionFailure(err))
		}
	}

	// Release the Guard's latches but continue to remain in lock wait-queues by
	// not releasing lockWaitQueueGuards. We expect the caller of this method to
	// then re-sequence the Request by calling SequenceReq with the un-latched
	// Guard. This is analogous to iterating through the loop in SequenceReq.
	m.lm.Release(g.moveLatchGuard())
	return g
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
func (m *managerImpl) OnLockAcquired(ctx context.Context, up *roachpb.LockUpdate) {
	if err := m.lt.AcquireLock(&up.Txn, up.Key, lock.Exclusive, up.Durability); err != nil {
		log.Fatal(ctx, errors.HandleAsAssertionFailure(err))
	}
}

// OnLockUpdated implements the LockManager interface.
func (m *managerImpl) OnLockUpdated(ctx context.Context, up *roachpb.LockUpdate) {
	if err := m.lt.UpdateLocks(up); err != nil {
		log.Fatal(ctx, errors.HandleAsAssertionFailure(err))
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

// OnDescriptorUpdated implements the RangeStateListener interface.
func (m *managerImpl) OnDescriptorUpdated(desc *roachpb.RangeDescriptor) {
	m.rng = desc
}

// OnLeaseUpdated implements the RangeStateListener interface.
func (m *managerImpl) OnLeaseUpdated(iAmTheLeaseHolder bool) {
	if iAmTheLeaseHolder {
		m.twq.Enable()
	} else {
		m.lt.Clear()
		m.twq.Clear(true /* disable */)
	}
}

// OnSplit implements the RangeStateListener interface.
func (m *managerImpl) OnSplit() {
	// TODO(nvanbenschoten): it only essential that we clear the half of the
	// lockTable which contains locks in the key range that is being split off
	// from the current range. For now though, we clear it all.
	m.lt.Clear()
	m.twq.Clear(false /* disable */)
}

// OnMerge implements the RangeStateListener interface.
func (m *managerImpl) OnMerge() {
	m.lt.Clear()
	m.twq.Clear(true /* disable */)
}

// LatchMetrics implements the MetricExporter interface.
func (m *managerImpl) LatchMetrics() (global, local storagepb.LatchManagerInfo) {
	return m.lm.Info()
}

// LockTableDebug implements the MetricExporter interface.
func (m *managerImpl) LockTableDebug() string {
	return m.lt.String()
}

// ContainsKey implements the txnwait.ReplicaInterface interface.
func (m *managerImpl) ContainsKey(key roachpb.Key) bool {
	return storagebase.ContainsKey(m.rng, key)
}

func (r *Request) isSingle(m roachpb.Method) bool {
	if len(r.Requests) != 1 {
		return false
	}
	return r.Requests[0].GetInner().Method() == m
}

func newGuard(req Request) *Guard {
	// TODO(nvanbenschoten): Pool these guard objects.
	return &Guard{req: req}
}

func (g *Guard) assertNoLatches() {
	if g.lg != nil {
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
