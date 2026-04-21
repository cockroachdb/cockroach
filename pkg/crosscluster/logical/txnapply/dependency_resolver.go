// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnapply

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/ldrdecoder"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/txnpb"
	"github.com/cockroachdb/cockroach/pkg/util/container/heap"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// DependencyUpdate is sent to an applier when a remote dependency is resolved.
type DependencyUpdate struct {
	TxnID        ldrdecoder.TxnID
	ResolvedTime hlc.Timestamp
	// TargetApplierID identifies the applier that should receive this update,
	// distinct from TxnID.ApplierID which identifies the txn owner.
	TargetApplierID ldrdecoder.ApplierID
}

// DependencyResolverClient coordinates cross-applier transaction dependencies.
type DependencyResolverClient interface {
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

// TrackerServer holds the dependency tracking state for a single applier. It
// stores the txns that this applier owns (and that other appliers may be
// waiting on), along with the set of waiters to notify when txns complete.
type TrackerServer struct {
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

// NewTrackerServer creates a TrackerServer for use in a dependency resolver
// processor.
func NewTrackerServer() *TrackerServer {
	s := &TrackerServer{}
	s.mu.waiters = make(map[ldrdecoder.TxnID][]ldrdecoder.ApplierID)
	s.mu.committed = makeCommittedSet()
	return s
}

// MaybeAddWaiter registers a waiter for the given txn unless the txn is
// already resolved (at or below the resolvedTime, or in completedTxns). Returns
// whether the txn is already resolved and the current resolvedTime.
func (inst *TrackerServer) MaybeAddWaiter(
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

// WaitHorizon registers a horizon wait for the given applier. Returns whether
// the horizon is already satisfied and the current resolvedTime.
func (inst *TrackerServer) WaitHorizon(
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

// Ready records the completion of a txn and returns a map of updates to send
// to waiting appliers. The caller is responsible for delivering the updates
// over the appropriate channels.
func (inst *TrackerServer) Ready(
	txn ldrdecoder.TxnID, resolvedTime hlc.Timestamp,
) map[ldrdecoder.ApplierID]DependencyUpdate {
	inst.mu.Lock()
	defer inst.mu.Unlock()

	inst.mu.committed.Resolve(txn)
	inst.mu.committed.UpdateResolvedTime(resolvedTime)

	waitingAppliers := inst.mu.waiters[txn]
	delete(inst.mu.waiters, txn)

	updates := make(map[ldrdecoder.ApplierID]DependencyUpdate, len(waitingAppliers))

	for _, applierID := range waitingAppliers {
		updates[applierID] = DependencyUpdate{
			TxnID:           txn,
			ResolvedTime:    resolvedTime,
			TargetApplierID: applierID,
		}
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
			updates[waiterID] = DependencyUpdate{
				TxnID:           txn,
				ResolvedTime:    resolvedTime,
				TargetApplierID: waiterID,
			}
		}
	}

	return updates
}

// DepResolverEvent is a message sent from a DistDepResolverClient (running in
// the applier processor) to the dep resolver processor. The applier processor
// serializes these as DistSQL rows routed by the target applier ID. The Event
// field carries one of the four dep resolver event types defined in
// txnpb.LDRDepResolverEvent.
type DepResolverEvent struct {
	// TargetApplierID is the applier whose dep resolver should process this
	// event. Used as the routing key for BY_RANGE output.
	TargetApplierID ldrdecoder.ApplierID
	Event           txnpb.LDRDepResolverEvent
}

// DistDepResolverClient implements DependencyResolverClient for use in the
// DistSQL applier processor. It routes Wait/WaitHorizon/Ready requests through
// a channel that the applier processor's Next() drains as DistSQL rows, and
// receives DependencyUpdates via a loopback backchannel from the co-located
// dep resolver processor.
//
// When the backchannel delivers an update for a remote applier, the client
// re-sends it through the output channel so it reaches the correct dep
// resolver, which then backchannels it to the correct applier.
type DistDepResolverClient struct {
	localApplierID ldrdecoder.ApplierID

	// outCh sends events to the applier processor's Next(), which
	// serializes and routes them to the correct dep resolver processor.
	outCh chan DepResolverEvent

	// receiveCh delivers DependencyUpdates for the local applier. This is
	// the channel returned by Receive().
	receiveCh chan DependencyUpdate
}

var _ DependencyResolverClient = (*DistDepResolverClient)(nil)

// NewDistDepResolverClient creates a DistDepResolverClient for the given
// applier.
func NewDistDepResolverClient(localApplierID ldrdecoder.ApplierID) *DistDepResolverClient {
	return &DistDepResolverClient{
		localApplierID: localApplierID,

		// TODO(msbutler): we should not be using buffered channels here. Probably
		// should use channels with length 1 and a ring buffer.
		outCh:     make(chan DepResolverEvent, 1000),
		receiveCh: make(chan DependencyUpdate, 1000),
	}
}

// OutCh returns the channel from which the router (DistSQL Next() or test
// router) reads dep resolver events produced by this client.
func (c *DistDepResolverClient) OutCh() <-chan DepResolverEvent { return c.outCh }

// Wait implements DependencyResolverClient.
func (c *DistDepResolverClient) Wait(waitingID ldrdecoder.ApplierID, txns []ldrdecoder.TxnID) {
	grouped := make(map[ldrdecoder.ApplierID][]ldrdecoder.TxnID)
	for _, txn := range txns {
		grouped[txn.ApplierID] = append(grouped[txn.ApplierID], txn)
	}
	for targetID, txnGroup := range grouped {
		c.outCh <- DepResolverEvent{
			TargetApplierID: targetID,
			Event: txnpb.LDRDepResolverEvent{
				Type: txnpb.DEP_RESOLVER_EVENT_WAIT,
				Wait: txnpb.DepResolverWait{
					WaitingID: waitingID,
					WaitTxns:  txnGroup,
				},
			},
		}
	}
}

// WaitHorizon implements DependencyResolverClient.
func (c *DistDepResolverClient) WaitHorizon(
	waitingID, dependID ldrdecoder.ApplierID, txnHorizon hlc.Timestamp,
) {
	c.outCh <- DepResolverEvent{
		TargetApplierID: dependID,
		Event: txnpb.LDRDepResolverEvent{
			Type: txnpb.DEP_RESOLVER_EVENT_WAIT_HORIZON,
			WaitHorizon: txnpb.DepResolverWaitHorizon{
				WaitingID:  waitingID,
				DependID:   dependID,
				TxnHorizon: txnHorizon,
			},
		},
	}
}

// Ready implements DependencyResolverClient.
func (c *DistDepResolverClient) Ready(txn ldrdecoder.TxnID, resolvedTime hlc.Timestamp) {
	c.outCh <- DepResolverEvent{
		TargetApplierID: txn.ApplierID,
		Event: txnpb.LDRDepResolverEvent{
			Type: txnpb.DEP_RESOLVER_EVENT_READY,
			Ready: txnpb.DepResolverReady{
				ReadyTxn:     txn,
				ResolvedTime: resolvedTime,
			},
		},
	}
}

// Receive implements DependencyResolverClient.
func (c *DistDepResolverClient) Receive(_ ldrdecoder.ApplierID) <-chan DependencyUpdate {
	return c.receiveCh
}

// RunBackchannelForwarder reads DependencyUpdates from the loopback
// backchannel. Updates for the local applier are pushed to receiveCh; updates
// for remote appliers are forwarded through outCh so they reach the correct
// dep resolver processor via DistSQL routing.
func (c *DistDepResolverClient) RunBackchannelForwarder(
	ctx context.Context, loopbackUpdateCh <-chan DependencyUpdate,
) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case update, ok := <-loopbackUpdateCh:
			if !ok {
				return nil
			}
			if update.TargetApplierID == c.localApplierID {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case c.receiveCh <- update:
				}
			} else {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case c.outCh <- DepResolverEvent{
					TargetApplierID: update.TargetApplierID,
					Event: txnpb.LDRDepResolverEvent{
						Type: txnpb.DEP_RESOLVER_EVENT_FORWARD_UPDATE,
						ForwardUpdate: txnpb.DepResolverForwardUpdate{
							TxnID:        update.TxnID,
							ResolvedTime: update.ResolvedTime,
						},
					},
				}:
				}
			}
		}
	}
}
