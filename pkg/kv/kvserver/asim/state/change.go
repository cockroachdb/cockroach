// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package state

import (
	"fmt"
	"time"

	"github.com/google/btree"
)

// Change is a state change for a range, to a target store that has some delay.
type Change interface {
	// Apply applies a change to the state.
	Apply(s State)
	// Target returns the recipient store of the change.
	Target() StoreID
	// Range returns the range id the change is for.
	Range() RangeID
	// Delay returns the duration taken to complete this state change.
	Delay() time.Duration
	// Blocking indicates whether the change should wait for other changes on
	// the same target to complete, or if other changes should be blocking on
	// it.
	Blocking() bool
}

// Changer takes state changes and applies them with delay.
type Changer interface {
	// Push appends a state change to occur. There must not be more than one
	// state change per Range() at any time. Push returns the time the state
	// change will apply, and true if this is satisfied; else it will return
	// false.
	Push(tick time.Time, sc Change) (time.Time, bool)
	// Tick updates state changer to apply any changes that have occurred
	// between the last tick and this one.
	Tick(tick time.Time, state State)
}

// ReplicaChange contains information necessary to add, remove or move (both) a
// replica for a range.
type ReplicaChange struct {
	RangeID             RangeID
	Add, Remove, Author StoreID
	Wait                time.Duration
}

// RangeSplitChange contains information necessary to split a range at a given
// key. It implements the change interface.
type RangeSplitChange struct {
	RangeID             RangeID
	Leaseholder, Author StoreID
	SplitKey            Key
	Wait                time.Duration
}

// LeaseTransferChange contains information necessary to transfer the lease for a
// range to a an existing replica, on the target store.
type LeaseTransferChange struct {
	RangeID                RangeID
	TransferTarget, Author StoreID
	Wait                   time.Duration
}

// Apply applies a change to the state.
func (lt *LeaseTransferChange) Apply(s State) {
	if s.TransferLease(lt.RangeID, lt.TransferTarget) {
		s.ClusterUsageInfo().storeRef(lt.Author).LeaseTransfers++
	}
}

// Target returns the recipient store of the change.
func (lt *LeaseTransferChange) Target() StoreID {
	return lt.TransferTarget
}

// Range returns the range id the change is for.
func (lt *LeaseTransferChange) Range() RangeID {
	return lt.RangeID
}

// Delay returns the duration taken to complete this state change.
func (lt *LeaseTransferChange) Delay() time.Duration {
	return lt.Wait
}

// Blocking indicates whether the change should wait for other changes on
// the same target to complete, or if other changes should be blocking on
// it. Lease transfers do not block.
func (lt *LeaseTransferChange) Blocking() bool {
	return false
}

// Apply applies a change to the state.
func (rsc *RangeSplitChange) Apply(s State) {
	if _, _, ok := s.SplitRange(rsc.SplitKey); ok {
		s.ClusterUsageInfo().storeRef(rsc.Author).RangeSplits++
	}
}

// Target returns the recipient store of the change.
func (rsc *RangeSplitChange) Target() StoreID {
	return rsc.Leaseholder
}

// Range returns the range id the change is for.
func (rsc *RangeSplitChange) Range() RangeID {
	return rsc.RangeID
}

// Delay returns the duration taken to complete this state change.
func (rsc *RangeSplitChange) Delay() time.Duration {
	return rsc.Wait
}

// Blocking indicates whether the change should wait for other changes on
// the same target to complete, or if other changes should be blocking on
// it. Range splits do not block.
func (rsc *RangeSplitChange) Blocking() bool {
	return false
}

// Apply applies a replica change for a range. This is an implementation of the
// Change interface. It requires that a replica being removed, must not hold
// the lease unless a replica is also being added in the same change.
func (rc *ReplicaChange) Apply(s State) {
	switch {
	case rc.Add == 0 && rc.Remove == 0:
		// Nothing to do.
	case rc.Add > 0 && rc.Remove == 0:
		if s.CanAddReplica(rc.RangeID, rc.Add) {
			s.AddReplica(rc.RangeID, rc.Add)
		}
	case rc.Add == 0 && rc.Remove > 0:
		if s.CanRemoveReplica(rc.RangeID, rc.Remove) {
			s.RemoveReplica(rc.RangeID, rc.Remove)
		}
	case rc.Add > 0 && rc.Remove > 0:
		if !s.CanAddReplica(rc.RangeID, rc.Add) {
			return
		}

		s.AddReplica(rc.RangeID, rc.Add)
		if !s.CanRemoveReplica(rc.RangeID, rc.Remove) {
			// We want to remove a replica, however we cannot currently. This can only
			// be due to the requested remove store holding a lease. Check if it's
			// possible to transfer to the incoming replica if we added one, if not
			// fail. This follows joint configuration lh removal in the real code.
			if !s.ValidTransfer(rc.RangeID, rc.Add) {
				// Cannot transfer lease, bail out and revert the added replica.
				s.RemoveReplica(rc.RangeID, rc.Add)
			}
			s.TransferLease(rc.RangeID, rc.Add)
			// NB: We don't update the usage info for lease transfer here
			// despite the transfer occurring. In the real cluster, lease
			// transfers due to joint config removing the current leaseholder
			// do not bump the lease transfer metric.
		}

		// A rebalance is allowed.
		s.RemoveReplica(rc.RangeID, rc.Remove)

		r, _ := s.Range(rc.RangeID)
		// Update the rebalancing usage info for the author store.
		if rc.Author == 0 {
			panic("no author set on replica change")
		}

		authorUsageInfo := s.ClusterUsageInfo().storeRef(rc.Author)
		authorUsageInfo.RebalanceSentBytes += r.Size()
		authorUsageInfo.Rebalances++

		// Update the rebalancing recieved bytes for the receiving store.
		s.ClusterUsageInfo().storeRef(rc.Add).RebalanceRcvdBytes += r.Size()
	default:
		panic("unknown change")
	}
}

// Target returns the recipient of any added data for a change.
func (rc *ReplicaChange) Target() StoreID {
	return rc.Add
}

// Range returns the ID the change is for.
func (rc *ReplicaChange) Range() RangeID {
	return rc.RangeID
}

// Delay returns the duration taken to complete this state change.
func (rc *ReplicaChange) Delay() time.Duration {
	return rc.Wait
}

// Blocking indicates whether the change should wait for other changes on
// the same target to complete, or if other changes should be blocking on
// it. Replica changes block.
func (rc *ReplicaChange) Blocking() bool {
	return true
}

// replicaChanger is an implementation of the changer interface, for replica
// changes. It maintains a pending list of changes for ranges, applying changes
// to state given the delay and other pending changes for the same receiver,
// pushed before a change.
type replicaChanger struct {
	lastTicket     int
	completeAt     *btree.BTree
	pendingTickets map[int]Change
	pendingTarget  map[StoreID]time.Time
	pendingRange   map[RangeID]int
	lastTick       time.Time
}

// NewReplicaChanger returns an implementation of the changer interface for
// replica changes.
func NewReplicaChanger() Changer {
	return &replicaChanger{
		completeAt:     btree.New(8),
		pendingTickets: make(map[int]Change),
		pendingTarget:  make(map[StoreID]time.Time),
		pendingRange:   make(map[RangeID]int),
		lastTick:       time.Time{},
	}
}

type pendingChange struct {
	ticket     int
	completeAt time.Time
}

// Less is part of the btree.Item interface.
func (pc *pendingChange) Less(than btree.Item) bool {
	// Targettal order on (completeAt, ticket)
	return pc.completeAt.Before(than.(*pendingChange).completeAt) ||
		(pc.completeAt.Equal(than.(*pendingChange).completeAt) && pc.ticket < than.(*pendingChange).ticket)
}

// Push appends a state change to occur. There must not be more than one
// state change per Range() at any time. Push returns the time the state
// change will apply, and true if this is satisfied; else it will return
// false.
func (rc *replicaChanger) Push(tick time.Time, change Change) (time.Time, bool) {
	if tick.Before(rc.lastTick) {
		panic(fmt.Sprintf(
			"Only monotonic calls to changer are allowed for pushes. "+
				"Lowest acceptable tick is greater than given (%d > %d)",
			rc.lastTick.UTC().Second(), tick.UTC().Second(),
		))
	}

	// Allow at most one pending action per range at any point in time.
	if _, ok := rc.pendingRange[change.Range()]; ok {
		return tick, false
	}

	// Collect a ticket and update the pending state for this change.
	rc.lastTicket++
	ticket := rc.lastTicket
	rc.pendingTickets[ticket] = change
	rc.pendingRange[change.Range()] = ticket

	completeAt := tick
	if change.Blocking() {
		// If there are pending changes for the target, we queue them and return
		// the last completion timestamp + delay. Otherwise, there is no queuing
		// and the change applies at tick + delay.
		if lastAppliedAt, ok := rc.pendingTarget[change.Target()]; !ok || !lastAppliedAt.After(tick) {
			rc.pendingTarget[change.Target()] = tick
		}
		completeAt = rc.pendingTarget[change.Target()]
		rc.pendingTarget[change.Target()].Add(change.Delay())
	}
	completeAt = completeAt.Add(change.Delay())

	// Create a unique entry (completionTime, ticket) and append it to the
	// completion queue.
	pc := &pendingChange{ticket: ticket, completeAt: completeAt}
	rc.completeAt.ReplaceOrInsert(pc)

	return completeAt, true
}

// Tick updates state changer to apply any changes that have occurred
// between the last tick and this one.
func (rc *replicaChanger) Tick(tick time.Time, state State) {
	changeList := []*pendingChange{}

	// NB: Add the smallest unit of time, in order to find all items in
	// [smallest, tick].
	pivot := &pendingChange{completeAt: tick.Add(time.Nanosecond)}
	rc.completeAt.AscendLessThan(pivot, func(i btree.Item) bool {
		nextChange, _ := i.(*pendingChange)
		changeList = append(changeList, nextChange)
		return true
	})

	for _, nextChange := range changeList {
		change := rc.pendingTickets[nextChange.ticket]
		change.Apply(state)

		// Cleanup the pending trackers for this ticket. This allows another
		// change to be pushed for Range().
		rc.completeAt.Delete(nextChange)
		delete(rc.pendingTickets, nextChange.ticket)
		delete(rc.pendingRange, change.Range())
	}
}
