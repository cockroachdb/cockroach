// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package state

import (
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
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
	RangeID RangeID
	Author  StoreID
	Changes kvpb.ReplicationChanges
	Wait    time.Duration
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

func removeReplica(
	s State, rangeID RangeID, storeID StoreID, current roachpb.ReplicaType,
) (ok bool, rollback func()) {
	if s.CanRemoveReplica(rangeID, storeID) {
		s.RemoveReplica(rangeID, storeID)
		rollback = func() {
			s.AddReplica(rangeID, storeID, current)
		}
		ok = true
	}
	return
}

func addReplica(
	s State, rangeID RangeID, storeID StoreID, next roachpb.ReplicaType,
) (ok bool, rollback func()) {
	if s.CanAddReplica(rangeID, storeID) {
		s.AddReplica(rangeID, storeID, next)
		rollback = func() {
			s.RemoveReplica(rangeID, storeID)
		}
		ok = true
	}
	return
}

func promoDemo(
	s State, rangeID RangeID, storeID StoreID, current, next roachpb.ReplicaType,
) (ok bool, rollback []func()) {
	removeOk, revertRemove := removeReplica(s, rangeID, storeID, current)
	rollback = append(rollback, revertRemove)
	if !removeOk {
		return
	}
	addOk, revertAdd := addReplica(s, rangeID, storeID, next)
	rollback = append(rollback, revertAdd)
	if !addOk {
		return
	}

	ok = true
	return
}

func replChangeHasStoreID(storeID StoreID, changes []roachpb.ReplicationTarget) bool {
	for _, change := range changes {
		if change.StoreID == roachpb.StoreID(storeID) {
			return true
		}
	}
	return false
}

// Apply applies a replica change for a range. This is an implementation of the
// Change interface. It requires that a replica being removed, must not hold
// the lease unless a replica is also being added in the same change.
func (rc *ReplicaChange) Apply(s State) {
	if len(rc.Changes) == 0 {
		// Nothing to do.
		return
	}

	// We track every change that is applied and apply the reverse in a list of
	// changes which will rollback the intermediate state if any step fails. The
	// rollback list is applied in reverse.
	rollback := []func(){}
	targets := kvserver.SynthesizeTargetsByChangeType(rc.Changes)
	rangeID := rc.RangeID

	defer func() {
		n := len(rollback)
		for i := n - 1; i > -1; i-- {
			if rollback[i] != nil {
				rollback[i]()
			}
		}
	}()

	// If the leaseholder is being removed, we additionally have to transfer the
	// lease away during one of the steps. We find this out here and then apply
	// the logic below.
	lhStore, ok := s.LeaseholderStore(rc.RangeID)
	nextLH := StoreID(-1)
	if !ok {
		panic(fmt.Sprintf("programming error: no leaseholder found for range %d",
			rc.RangeID))
	}

	lhBeingRemoved := false
	if replChangeHasStoreID(lhStore.StoreID(), targets.VoterDemotions) {
		lhBeingRemoved = true
	}
	if replChangeHasStoreID(lhStore.StoreID(), targets.VoterRemovals) {
		lhBeingRemoved = true
	}

	if lhBeingRemoved {
		if len(targets.NonVoterPromotions) > 0 {
			nextLH = StoreID(targets.NonVoterPromotions[0].StoreID)
		}
		if len(targets.VoterAdditions) > 0 {
			nextLH = StoreID(targets.VoterAdditions[0].StoreID)
		}
		// Couldn't find a leaseholder target, abort the change.
		if nextLH == -1 {
			return
		}
	} else {
		nextLH = lhStore.StoreID()
	}

	// Figure out which store is receiving a snapshot, in order to bump tracking
	// metrics for snapshot bytes.
	requiresUpReplication := len(targets.NonVoterAdditions) > 0 || len(targets.VoterAdditions) > 0
	var storeNeedingSnapshot StoreID
	// NB: We have the invariant that there is at most 1 total addition for
	// change replicas, we only check the first addition if it exists. This
	// applies for both voter and non-voter additions together.
	if len(targets.NonVoterAdditions) > 0 {
		storeNeedingSnapshot = StoreID(targets.NonVoterAdditions[0].StoreID)
	}
	if len(targets.VoterAdditions) > 0 {
		storeNeedingSnapshot = StoreID(targets.VoterAdditions[0].StoreID)
	}

	adding := len(rc.Changes.VoterAdditions()) > 0 || len(rc.Changes.NonVoterAdditions()) > 0
	removing := len(rc.Changes.VoterRemovals()) > 0 || len(rc.Changes.NonVoterRemovals()) > 0
	rebalancingReplicas := adding && removing

	// Start applying the changes. We deviate slightly from the order in
	// kvserver.changeReplicasImpl:
	// +-------+--------------------------------+---------------------+
	// | Order | Actual                         | Simulator           |
	// +-------+--------------------------------+---------------------+
	// | 1     | Promotions / demotions / swaps | Promotions          |
	// | 2     | Voter additions                | Voter additions     |
	// | 3     | Voter removals                 | Voter demotions     |
	// | 4     | Non-voter additions            | Voter removals      |
	// | 5     | Non-voter removals             | Non-voter additions |
	// | 6     | -                              | Non-voter removals  |
	// +-------+--------------------------------+---------------------+
	// The ordering is changed slightly, so that all the final voters are added
	// or promoted first before removing any previous voters. This is done so
	// that a lease transfer can occur, if needed, whilst the previous
	// leaseholder is still a voter.
	for _, nonVoterPromotion := range targets.NonVoterPromotions {
		ok, revert := promoDemo(
			s, rangeID, StoreID(nonVoterPromotion.StoreID), roachpb.NON_VOTER, roachpb.VOTER_FULL)
		rollback = append(rollback, revert...)
		if !ok {
			return
		}
	}
	for _, voterAddition := range targets.VoterAdditions {
		ok, revert := addReplica(
			s, rangeID, StoreID(voterAddition.StoreID), roachpb.VOTER_FULL)
		rollback = append(rollback, revert)
		if !ok {
			return
		}
	}

	// Before doing any removals, transfer the lease if necessary. After
	// promotions and additions of voters, the replicas who could take a lease
	// should all be finalized as voters.
	if lhBeingRemoved && nextLH != lhStore.StoreID() {
		if !s.TransferLease(rangeID, nextLH) {
			return
		}
		rollback = append(rollback, func() {
			if !s.TransferLease(rangeID, lhStore.StoreID()) {
				panic("unable to rollback lease transfer")
			}
		})
	}

	for _, voterDemotion := range targets.VoterDemotions {
		ok, revert := promoDemo(
			s, rangeID, StoreID(voterDemotion.StoreID), roachpb.VOTER_FULL, roachpb.NON_VOTER)
		rollback = append(rollback, revert...)
		if !ok {
			return
		}
	}
	for _, voterRemoval := range targets.VoterRemovals {
		ok, revert := removeReplica(
			s, rangeID, StoreID(voterRemoval.StoreID), roachpb.VOTER_FULL)
		rollback = append(rollback, revert)
		if !ok {
			return
		}
	}
	for _, nonVoterAddition := range targets.NonVoterAdditions {
		ok, revert := addReplica(
			s, rangeID, StoreID(nonVoterAddition.StoreID), roachpb.NON_VOTER)
		rollback = append(rollback, revert)
		if !ok {
			return
		}
	}
	for _, nonVoterRemoval := range targets.NonVoterRemovals {
		ok, revert := removeReplica(
			s, rangeID, StoreID(nonVoterRemoval.StoreID), roachpb.NON_VOTER)
		rollback = append(rollback, revert)
		if !ok {
			return
		}
	}

	if rebalancingReplicas {
		r, _ := s.Range(rc.RangeID)
		// Update the rebalancing usage info for the author store.
		if rc.Author == 0 {
			panic("no author set on replica change")
		}

		authorUsageInfo := s.ClusterUsageInfo().storeRef(rc.Author)
		authorUsageInfo.Rebalances++
		if requiresUpReplication {
			authorUsageInfo.RebalanceSentBytes += r.Size()
			s.ClusterUsageInfo().storeRef(storeNeedingSnapshot).RebalanceRcvdBytes += r.Size()
		}
	}

	// We successfully made it through applying all changes. Clear the list of
	// rollback functions.
	rollback = nil
}

// Target returns the recipient of any added data for a change.
func (rc *ReplicaChange) Target() StoreID {
	targets := kvserver.SynthesizeTargetsByChangeType(rc.Changes)
	voterAdditions := targets.VoterAdditions
	nonVoterAdditions := targets.NonVoterAdditions
	if len(voterAdditions)+len(nonVoterAdditions) >= 2 {
		panic(fmt.Sprintf(
			"programming error: unexpected multiple snapshot targets for replica "+
				"change (+voter=%v +non-voter=%v) changes=%v",
			voterAdditions,
			nonVoterAdditions,
			rc.Changes,
		))
	}

	if len(voterAdditions) == 1 {
		return StoreID(voterAdditions[0].StoreID)
	}
	if len(nonVoterAdditions) == 1 {
		return StoreID(nonVoterAdditions[0].StoreID)
	}

	return 0
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

	completeAt := tick
	if change.Blocking() {
		// If there are pending changes for the target, we queue them and return
		// the last completion timestamp + delay. Otherwise, there is no queuing
		// and the change applies at tick + delay.
		if lastAppliedAt, ok := rc.pendingTarget[change.Target()]; !ok || !lastAppliedAt.After(tick) {
			rc.pendingTarget[change.Target()] = tick
		}
		completeAt = rc.pendingTarget[change.Target()]
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
	var changeList []*pendingChange

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
