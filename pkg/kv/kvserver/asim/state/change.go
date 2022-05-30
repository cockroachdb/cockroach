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
	// Apply applies a change to given state.
	Apply(s State)
	// To returns the recipient of any added data for a change.
	To() StoreID
	// For returns the ID the change is for.
	For() RangeID
	// Delay returns the duration taken to complete this state change.
	Delay() time.Duration
}

// Changer takes state changes and applies them with delay.
type Changer interface {
	// Push appends a state change to occur. There must not be more than one
	// state change per For() at any time. Push returns the time the state
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

// Apply applies a replica change for a range.
func (rc *ReplicaChange) Apply(s State) {
	if !s.CanAddReplica(rc.RangeID, rc.Add) {
		return
	}
	s.AddReplica(rc.RangeID, rc.Add)
	if !s.CanRemoveReplica(rc.RangeID, rc.Remove) {
		// If the replica change involves removing the leaseholder replica,
		// first transfer the lease to the newly added replica.
		// TODO(kvoli): Lease transfers should be a separate state change
		// operation, when they are supported in simulating rebalancing.
		if !s.ValidTransfer(rc.RangeID, rc.Add) {
			// Cannot transfer lease, bail out and revert.
			s.RemoveReplica(rc.RangeID, rc.Add)
		}
		s.TransferLease(rc.RangeID, rc.Add)
	}
	s.RemoveReplica(rc.RangeID, rc.Remove)
}

// To returns the recipient of any added data for a change.
func (rc *ReplicaChange) To() StoreID {
	return rc.Add
}

// For returns the ID the change is for.
func (rc *ReplicaChange) For() RangeID {
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
	pendingTo      map[StoreID]time.Time
	pendingFor     map[RangeID]int
}

// NewReplicaChanger returns an implementation of the changer interface for
// replica changes.
func NewReplicaChanger() Changer {
	return &replicaChanger{
		completeAt:     btree.New(8),
		pendingTickets: make(map[int]Change),
		pendingTo:      make(map[StoreID]time.Time),
		pendingFor:     make(map[RangeID]int),
	}
}

type pendingChange struct {
	ticket     int
	completeAt time.Time
}

// Less is part of the btree.Item interface.
func (pc *pendingChange) Less(than btree.Item) bool {
	// Total order on (completeAt, ticket)
	return pc.completeAt.Before(than.(*pendingChange).completeAt) ||
		(pc.completeAt.Equal(than.(*pendingChange).completeAt) && pc.ticket < than.(*pendingChange).ticket)
}

// Push appends a state change to occur. There must not be more than one
// state change per For() at any time. Push returns the time the state
// change will apply, and true if this is satisfied; else it will return
// false.
func (sc *replicaChanger) Push(tick time.Time, change Change) (time.Time, bool) {
	// Allow at most one pending action per range at any point in time.
	if _, ok := sc.pendingFor[change.For()]; ok {
		return tick, false
	}

	// Collect a ticket and update the pending state for this change.
	sc.lastTicket++
	ticket := sc.lastTicket
	sc.pendingTickets[ticket] = change
	sc.pendingFor[change.For()] = ticket

	// If there are pending changes for the target, we queue them and return
	// the last completion timestamp + delay. Otherwise, there is no queuing
	// and the change applies at tick + delay.
	if lastAppliedAt, ok := sc.pendingTo[change.To()]; !ok || !lastAppliedAt.After(tick) {
		sc.pendingTo[change.To()] = tick
	}
	completeAt := sc.pendingTo[change.To()].Add(change.Delay())

	// Create a unique entry (completionTime, ticket) and append it to the
	// completion queue.
	pc := &pendingChange{ticket: ticket, completeAt: completeAt}
	sc.completeAt.ReplaceOrInsert(pc)

	return completeAt, true
}

// Tick updates state changer to apply any changes that have occurred
// between the last tick and this one.
func (sc *replicaChanger) Tick(tick time.Time, state State) {
	changeList := make(map[int]*pendingChange)

	// NB: Add the smallest unit of time, in order to find all items in
	// [smallest, tick].
	pivot := &pendingChange{completeAt: tick.Add(time.Nanosecond)}
	sc.completeAt.AscendLessThan(pivot, func(i btree.Item) bool {
		nextChange, _ := i.(*pendingChange)
		changeList[nextChange.ticket] = nextChange
		return true
	})

	for ticket, nextChange := range changeList {
		change := sc.pendingTickets[nextChange.ticket]
		change.Apply(state)

		// Cleanup the pending trackers for this ticket. This allows another
		// change to be pushed for For().
		sc.completeAt.Delete(nextChange)
		delete(sc.pendingTickets, ticket)
		delete(sc.pendingFor, change.For())
	}
}
