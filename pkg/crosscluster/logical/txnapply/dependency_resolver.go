// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnapply

import (
	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/ldrdecoder"
	"github.com/cockroachdb/cockroach/pkg/util/container/heap"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// DependencyUpdate is sent to an applier when a remote dependency is resolved.
type DependencyUpdate struct {
	TxnID        ldrdecoder.TxnID
	ResolvedTime hlc.Timestamp
}

// DependencyResolver coordinates cross-applier transaction dependencies.
type DependencyResolver interface {
	// Wait registers that the calling applier is waiting on the given txns
	// (owned by other appliers) to complete.
	Wait(waitingID ldrdecoder.ApplierID, txns []ldrdecoder.TxnID)

	// WaitHorizon registers that the calling applier (waitingID) is blocked
	// on the given applier's (dependID) resolvedTime advancing past the given
	// txnHorizon. If the dependency's resolvedTime already exceeds txnHorizon,
	// the waiter is not registered and an update is sent eagerly.
	WaitHorizon(waitingID, dependID ldrdecoder.ApplierID, txnHorizon hlc.Timestamp)

	// Ready signals that the calling applier has completed the given txn
	// and provides the applier's latest resolvedTime timestamp.
	Ready(txn ldrdecoder.TxnID, resolvedTime hlc.Timestamp)

	// Receive returns a channel that delivers resolved dependency
	// notifications and resolvedTime updates for the given applier.
	Receive(applier ldrdecoder.ApplierID) <-chan DependencyUpdate
}

// trackerServer holds the dependency tracking state for a single applier. It
// stores the txns that this applier owns (and that other appliers may be
// waiting on), along with the set of waiters to notify when txns complete.
type trackerServer struct {
	mu struct {
		syncutil.Mutex

		// waiters maps a txn (owned by this applier) to the set of applier
		// IDs waiting for that txn to resolve.
		waiters map[ldrdecoder.TxnID][]ldrdecoder.ApplierID

		// horizonWaiters is a min-heap of horizon timestamps that
		// remote appliers are waiting on. Each entry's txnID.ApplierID
		// identifies the waiting applier. Entries are popped when the
		// resolvedTime advances past their horizon.
		horizonWaiters horizonHeap

		// committed tracks which transactions have been committed.
		committed committedSet
	}
}

// maybeAddWaiter registers a waiter for the given txn unless the txn is
// already resolved (at or below the resolvedTime, or in completedTxns). Returns
// whether the txn is already resolved and the current resolvedTime.
func (inst *trackerServer) maybeAddWaiter(
	txn ldrdecoder.TxnID, waiter ldrdecoder.ApplierID,
) (bool, hlc.Timestamp) {
	inst.mu.Lock()
	defer inst.mu.Unlock()

	if inst.mu.committed.IsResolved(txn) {
		return true, inst.mu.committed.ResolvedTime()
	}
	inst.mu.waiters[txn] = append(inst.mu.waiters[txn], waiter)
	return false, hlc.Timestamp{}
}

func (inst *trackerServer) waitHorizon(
	waitingID ldrdecoder.ApplierID, txnHorizon hlc.Timestamp,
) (alreadyResolved bool, resolvedTime hlc.Timestamp) {
	inst.mu.Lock()
	defer inst.mu.Unlock()

	if inst.mu.committed.IsResolvedAt(txnHorizon) {
		return true, inst.mu.committed.ResolvedTime()
	}
	heap.Push(&inst.mu.horizonWaiters, horizonWaiter{
		txnID:   ldrdecoder.TxnID{ApplierID: waitingID},
		horizon: txnHorizon,
	})
	return false, hlc.Timestamp{}
}

// ready records the completion of a txn and returns a map of updates to send
// to waiting appliers. The caller is responsible for delivering the updates
// over the appropriate channels.
func (inst *trackerServer) ready(
	txn ldrdecoder.TxnID, resolvedTime hlc.Timestamp,
) map[ldrdecoder.ApplierID]DependencyUpdate {
	inst.mu.Lock()
	defer inst.mu.Unlock()

	inst.mu.committed.Resolve(txn)
	inst.mu.committed.UpdateResolvedTime(resolvedTime)

	waitingAppliers := inst.mu.waiters[txn]
	delete(inst.mu.waiters, txn)

	update := DependencyUpdate{TxnID: txn, ResolvedTime: resolvedTime}

	updates := make(map[ldrdecoder.ApplierID]DependencyUpdate, len(waitingAppliers))

	for _, applierID := range waitingAppliers {
		updates[applierID] = update
	}

	// Notify horizon waiters whose required horizon is satisfied by the
	// new resolvedTime.
	for inst.mu.horizonWaiters.Len() > 0 {
		top := inst.mu.horizonWaiters.peek()
		if !top.horizon.LessEq(resolvedTime) {
			break
		}
		heap.Pop(&inst.mu.horizonWaiters)
		waiterID := top.txnID.ApplierID
		if _, ok := updates[waiterID]; !ok {
			updates[waiterID] = update
		}
	}

	return updates
}

// trackerClient is a DependencyResolver that tracks cross-applier dependencies
// and notifies waiting appliers via buffered channels when dependencies are
// resolved.
//
// This prototype models the future distql flow we'll use for the distributed
// applier: each applier processor will be able to communicate with each
// applier's associated dependency tracker processor. In this prototype, the
// trackerClient simply queries each tracker server via a map, and the tracker
// client directly sends updates to the applier via the inbox channels. In the
// distsql flow, the trackerClient will send Ready RPCs to an appliers
// dependency tracker processor which will forward updates to the applier
// processor.
type trackerClient struct {
	servers map[ldrdecoder.ApplierID]*trackerServer

	// inboxes holds one buffered channel per applier for receiving
	// DependencyUpdates. Populated at construction and never modified.
	//
	// TODO(msbutler): if these channels fill up, it could block Ready() leading
	// to deadlock. In the productionized version, Ready will never be blocking,
	// so we'll probably store some ring buffer of ready transactions on the
	// tracker processor. Then, each processor will have some Run method that will
	// watch the buffer state and send data to applier channels.
	inboxes map[ldrdecoder.ApplierID]chan DependencyUpdate
}

// NewDependencyTracker creates a DependencyResolver for the given set of
// applier IDs.
func NewDependencyTracker(appliers []ldrdecoder.ApplierID) DependencyResolver {
	inboxes := make(map[ldrdecoder.ApplierID]chan DependencyUpdate, len(appliers))
	for _, id := range appliers {
		// TODO(msbutler): the only reason this buffer has 1000 slots is to ensure
		// the random dag test doesn't deadlock. This will be replaced by a ring
		// buffer in a future PR.
		inboxes[id] = make(chan DependencyUpdate, 1000)
	}

	instances := make(map[ldrdecoder.ApplierID]*trackerServer, len(appliers))
	for _, id := range appliers {
		inst := &trackerServer{}
		inst.mu.waiters = make(map[ldrdecoder.TxnID][]ldrdecoder.ApplierID)
		inst.mu.committed = makeCommittedSet()
		instances[id] = inst
	}

	return &trackerClient{servers: instances, inboxes: inboxes}
}

// Wait implements DependencyResolver.
//
// TODO(msbutler): consider passing the waiting applier A's resolvedTime here, which
// would allow the depending applier B to cache the resolvedTime timestamp,
// potentially avoiding a WaitHorizon from B to A.
func (d *trackerClient) Wait(waiter ldrdecoder.ApplierID, txns []ldrdecoder.TxnID) {
	for _, txn := range txns {
		resolved, resolvedTime := d.servers[txn.ApplierID].maybeAddWaiter(txn, waiter)
		if resolved {
			d.inboxes[waiter] <- DependencyUpdate{TxnID: txn, ResolvedTime: resolvedTime}
		}
	}
}

// WaitHorizon implements DependencyResolver.
func (d *trackerClient) WaitHorizon(
	applierID, dependID ldrdecoder.ApplierID, txnHorizon hlc.Timestamp,
) {
	resolved, resolvedTime := d.servers[dependID].waitHorizon(applierID, txnHorizon)
	if resolved {
		d.inboxes[applierID] <- DependencyUpdate{
			TxnID:        ldrdecoder.TxnID{ApplierID: dependID},
			ResolvedTime: resolvedTime,
		}
	}
}

// Ready implements DependencyResolver.
func (d *trackerClient) Ready(txn ldrdecoder.TxnID, resolvedTime hlc.Timestamp) {
	updates := d.servers[txn.ApplierID].ready(txn, resolvedTime)
	for applierID, update := range updates {
		d.inboxes[applierID] <- update
	}
}

// Receive implements DependencyResolver.
func (d *trackerClient) Receive(applier ldrdecoder.ApplierID) <-chan DependencyUpdate {
	return d.inboxes[applier]
}
