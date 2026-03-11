// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnapply

import (
	"slices"

	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/ldrdecoder"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// DependencyUpdate is sent to an applier when a remote dependency is resolved.
type DependencyUpdate struct {
	TxnID    ldrdecoder.TxnID
	Frontier hlc.Timestamp
}

// DependencyResolver coordinates cross-applier transaction dependencies.
type DependencyResolver interface {
	// Wait registers that the calling applier is waiting on the given txns
	// (owned by other appliers) to complete.
	Wait(waiter ldrdecoder.ApplierID, txns []ldrdecoder.TxnID)

	// WaitHorizon registers that the calling applier (applierID) is blocked
	// on the given applier's (dependID) frontier advancing past the given
	// txnHorizon. If the dependency's frontier already exceeds txnHorizon,
	// the waiter is not registered and an update is sent eagerly.
	WaitHorizon(applierID, dependID ldrdecoder.ApplierID, txnHorizon hlc.Timestamp)

	// Ready signals that the calling applier has completed the given txn
	// and provides the applier's latest frontier timestamp.
	Ready(txn ldrdecoder.TxnID, frontier hlc.Timestamp)

	// Receive returns a channel that delivers resolved dependency
	// notifications and frontier updates for the given applier.
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

		// horizonWaiters maps waiting applier IDs to the set of event
		// horizon timestamps they are waiting on. Individual timestamps
		// are cleared when the frontier advances past them; a
		// notification is sent whenever at least one is cleared.
		horizonWaiters map[ldrdecoder.ApplierID][]hlc.Timestamp

		// frontier is the applier's latest frontier, updated on every
		// ready() call.
		frontier hlc.Timestamp

		// completedTxns tracks txns completed with timestamps above the
		// current frontier. Pruned when the frontier advances. This
		// prevents lost notifications when Ready() fires before Wait().
		completedTxns map[ldrdecoder.TxnID]struct{}
	}
}

// maybeAddWaiter registers a waiter for the given txn unless the txn is
// already resolved (at or below the frontier, or in completedTxns). Returns
// whether the txn is already resolved and the current frontier.
func (inst *trackerServer) maybeAddWaiter(
	txn ldrdecoder.TxnID, waiter ldrdecoder.ApplierID,
) (bool, hlc.Timestamp) {
	inst.mu.Lock()
	defer inst.mu.Unlock()

	if txn.Timestamp.LessEq(inst.mu.frontier) {
		return true, inst.mu.frontier
	}
	if _, ok := inst.mu.completedTxns[txn]; ok {
		return true, inst.mu.frontier
	}
	inst.mu.waiters[txn] = append(inst.mu.waiters[txn], waiter)
	return false, hlc.Timestamp{}
}

func (inst *trackerServer) waitHorizon(
	applierID ldrdecoder.ApplierID, txnHorizon hlc.Timestamp,
) (alreadyResolved bool, frontier hlc.Timestamp) {
	inst.mu.Lock()
	defer inst.mu.Unlock()

	if txnHorizon.LessEq(inst.mu.frontier) {
		return true, inst.mu.frontier
	}
	inst.mu.horizonWaiters[applierID] = append(
		inst.mu.horizonWaiters[applierID], txnHorizon)
	return false, hlc.Timestamp{}
}

// ready records the completion of a txn and returns a map of updates to send
// to waiting appliers. The caller is responsible for delivering the updates
// over the appropriate channels.
func (inst *trackerServer) ready(
	txn ldrdecoder.TxnID, frontier hlc.Timestamp,
) map[ldrdecoder.ApplierID]DependencyUpdate {
	inst.mu.Lock()
	defer inst.mu.Unlock()

	// Update frontier and track completed txn.
	inst.mu.frontier = frontier
	if txn.Timestamp.Less(frontier) {
		// Txn timestamp is below frontier; no need to track it since
		// maybeAddWaiter will see it's at or below the frontier.
	} else {
		inst.mu.completedTxns[txn] = struct{}{}
	}

	// Prune completedTxns entries at or below the new frontier.
	for completed := range inst.mu.completedTxns {
		if completed.Timestamp.LessEq(frontier) {
			delete(inst.mu.completedTxns, completed)
		}
	}

	waitingAppliers := inst.mu.waiters[txn]
	delete(inst.mu.waiters, txn)

	update := DependencyUpdate{TxnID: txn, Frontier: frontier}

	updates := make(map[ldrdecoder.ApplierID]DependencyUpdate, len(waitingAppliers))

	for _, applierID := range waitingAppliers {
		updates[applierID] = update
	}

	// Notify horizon waiters whose required horizon is satisfied by the
	// new frontier. Clear individual timestamps that are at or below the
	// frontier, and notify if at least one was cleared.
	for waiterID, horizons := range inst.mu.horizonWaiters {
		remaining := slices.DeleteFunc(horizons, func(h hlc.Timestamp) bool {
			return h.LessEq(frontier)
		})
		if len(remaining) < len(horizons) {
			if _, ok := updates[waiterID]; !ok {
				updates[waiterID] = update
			}
		}
		inst.mu.horizonWaiters[waiterID] = remaining
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
	// TODO(msbutler): if these channels fill up, it could block Ready(). In
	// the productionized version, Ready will never be blocking, so we'll
	// probably store some ring buffer of ready transactions on the tracker
	// processor. Then, each processor will have some Run method that will
	// watch the buffer state and send data to applier channels.
	inboxes map[ldrdecoder.ApplierID]chan DependencyUpdate
}

// NewDependencyTracker creates a DependencyResolver for the given set of
// applier IDs.
func NewDependencyTracker(appliers []ldrdecoder.ApplierID) DependencyResolver {
	inboxes := make(map[ldrdecoder.ApplierID]chan DependencyUpdate, len(appliers))
	for _, id := range appliers {
		// EXP
		inboxes[id] = make(chan DependencyUpdate, 1000)
	}

	instances := make(map[ldrdecoder.ApplierID]*trackerServer, len(appliers))
	for _, id := range appliers {
		inst := &trackerServer{}
		inst.mu.waiters = make(map[ldrdecoder.TxnID][]ldrdecoder.ApplierID)
		inst.mu.completedTxns = make(map[ldrdecoder.TxnID]struct{})
		inst.mu.horizonWaiters = make(map[ldrdecoder.ApplierID][]hlc.Timestamp)
		instances[id] = inst
	}

	return &trackerClient{servers: instances, inboxes: inboxes}
}

// Wait implements DependencyResolver.
//
// TODO(msbutler): consider passing the waiting applier A's frontier here, which
// would allow the depending applier B to cache the frontier timestamp,
// potentially avoiding a WaitHorizon from B to A.
func (d *trackerClient) Wait(waiter ldrdecoder.ApplierID, txns []ldrdecoder.TxnID) {
	for _, txn := range txns {
		resolved, frontier := d.servers[txn.ApplierID].maybeAddWaiter(txn, waiter)
		if resolved {
			d.inboxes[waiter] <- DependencyUpdate{TxnID: txn, Frontier: frontier}
		}
	}
}

// WaitHorizon implements DependencyResolver.
func (d *trackerClient) WaitHorizon(
	applierID, dependID ldrdecoder.ApplierID, txnHorizon hlc.Timestamp,
) {
	resolved, frontier := d.servers[dependID].waitHorizon(applierID, txnHorizon)
	if resolved {
		d.inboxes[applierID] <- DependencyUpdate{
			TxnID:    ldrdecoder.TxnID{ApplierID: dependID},
			Frontier: frontier,
		}
	}
}

// Ready implements DependencyResolver.
func (d *trackerClient) Ready(txn ldrdecoder.TxnID, frontier hlc.Timestamp) {
	updates := d.servers[txn.ApplierID].ready(txn, frontier)
	for applierID, update := range updates {
		d.inboxes[applierID] <- update
	}
}

// Receive implements DependencyResolver.
func (d *trackerClient) Receive(applier ldrdecoder.ApplierID) <-chan DependencyUpdate {
	return d.inboxes[applier]
}
