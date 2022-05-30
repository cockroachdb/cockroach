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
	RangeID     RangeID
	Add, Remove StoreID
	Wait        time.Duration
}

// Apply applies a replica change for a range. This is an implementation of the
// Change interface. It requires that a replica being removed, must not hold
// the lease unless a replica is also being added in the same change.
func (rc *ReplicaChange) Apply(s State) {
	if s.CanAddReplica(rc.RangeID, rc.Add) {
		s.AddReplica(rc.RangeID, rc.Add)
	}

	if s.CanRemoveReplica(rc.RangeID, rc.Remove) {
		s.RemoveReplica(rc.RangeID, rc.Remove)
		return
	}

	if rc.Remove == 0 {
		return
	}

	// We want to remove a replica, however we cannot currently. This can only
	// be due to the requested remove store holding a lease. Check if it's
	// possible to transfer to the incoming replica if we added one, if not
	// fail.
	// TODO(kvoli): Lease transfers should be a separate state change
	// operation, when they are supported in simulating rebalancing.
	if !s.ValidTransfer(rc.RangeID, rc.Add) && rc.Remove > 0 {
		// Cannot transfer lease, bail out and revert if we performed an
		// add.
		s.RemoveReplica(rc.RangeID, rc.Add)
		return
	}

	s.TransferLease(rc.RangeID, rc.Add)
	s.RemoveReplica(rc.RangeID, rc.Remove)
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
}

// NewReplicaChanger returns an implementation of the changer interface for
// replica changes.
func NewReplicaChanger() Changer {
	return &replicaChanger{
		completeAt:     btree.New(8),
		pendingTickets: make(map[int]Change),
		pendingTarget:  make(map[StoreID]time.Time),
		pendingRange:   make(map[RangeID]int),
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
	// Allow at most one pending action per range at any point in time.
	if _, ok := rc.pendingRange[change.Range()]; ok {
		return tick, false
	}

	// Collect a ticket and update the pending state for this change.
	rc.lastTicket++
	ticket := rc.lastTicket
	rc.pendingTickets[ticket] = change
	rc.pendingRange[change.Range()] = ticket

	// If there are pending changes for the target, we queue them and return
	// the last completion timestamp + delay. Otherwise, there is no queuing
	// and the change applies at tick + delay.
	if lastAppliedAt, ok := rc.pendingTarget[change.Target()]; !ok || !lastAppliedAt.After(tick) {
		rc.pendingTarget[change.Target()] = tick
	}
	completeAt := rc.pendingTarget[change.Target()].Add(change.Delay())

	// Create a unique entry (completionTime, ticket) and append it to the
	// completion queue.
	pc := &pendingChange{ticket: ticket, completeAt: completeAt}
	rc.completeAt.ReplaceOrInsert(pc)

	return completeAt, true
}

// Tick updates state changer to apply any changes that have occurred
// between the last tick and this one.
func (rc *replicaChanger) Tick(tick time.Time, state State) {
	changeList := make(map[int]*pendingChange)

	// NB: Add the smallest unit of time, in order to find all items in
	// [smallest, tick].
	pivot := &pendingChange{completeAt: tick.Add(time.Nanosecond)}
	rc.completeAt.AscendLessThan(pivot, func(i btree.Item) bool {
		nextChange, _ := i.(*pendingChange)
		changeList[nextChange.ticket] = nextChange
		return true
	})

	for ticket, nextChange := range changeList {
		change := rc.pendingTickets[nextChange.ticket]
		change.Apply(state)

		// Cleanup the pending trackers for this ticket. This allows another
		// change to be pushed for Range().
		rc.completeAt.Delete(nextChange)
		delete(rc.pendingTickets, ticket)
		delete(rc.pendingRange, change.Range())
	}
}
