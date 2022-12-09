// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package roachpb

import (
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"go.etcd.io/raft/v3/raftpb"
)

// ReplicaSet is a set of replicas, usually the nodes/stores on which
// replicas of a range are stored.
type ReplicaSet struct {
	wrapped []ReplicaDescriptor
}

// TODO(aayush): Add a `Size` or `NumReplicas` method to ReplicaSet and amend
// usages that call `len(replicaSet.Descriptors())`

// MakeReplicaSet creates a ReplicaSet wrapper from a raw slice of individual
// descriptors.
func MakeReplicaSet(replicas []ReplicaDescriptor) ReplicaSet {
	return ReplicaSet{wrapped: replicas}
}

// SafeFormat implements redact.SafeFormatter.
func (d ReplicaSet) SafeFormat(w redact.SafePrinter, _ rune) {
	for i, desc := range d.wrapped {
		if i > 0 {
			w.SafeRune(',')
		}
		w.Print(desc)
	}
}

func (d ReplicaSet) String() string {
	return redact.StringWithoutMarkers(d)
}

// Descriptors returns every replica descriptor in the set, including both voter
// replicas and learner replicas. Voter replicas are ordered first in the
// returned slice.
func (d ReplicaSet) Descriptors() []ReplicaDescriptor {
	return d.wrapped
}

func predVoterFull(rDesc ReplicaDescriptor) bool {
	switch rDesc.Type {
	case VOTER_FULL:
		return true
	default:
	}
	return false
}

func predVoterFullOrIncoming(rDesc ReplicaDescriptor) bool {
	switch rDesc.Type {
	case VOTER_FULL, VOTER_INCOMING:
		return true
	default:
	}
	return false
}

func predVoterIncoming(rDesc ReplicaDescriptor) bool {
	switch rDesc.Type {
	case VOTER_INCOMING:
		return true
	default:
	}
	return false
}

func predLearner(rDesc ReplicaDescriptor) bool {
	return rDesc.Type == LEARNER
}

func predNonVoter(rDesc ReplicaDescriptor) bool {
	return rDesc.Type == NON_VOTER
}

func predVoterOrNonVoter(rDesc ReplicaDescriptor) bool {
	return predVoterFullOrIncoming(rDesc) || predNonVoter(rDesc)
}

func predVoterFullOrNonVoter(rDesc ReplicaDescriptor) bool {
	return predVoterFull(rDesc) || predNonVoter(rDesc)
}

// Voters returns a ReplicaSet of current and future voter replicas in `d`. This
// means that during an atomic replication change, only the replicas that will
// be voters once the change completes will be returned; "outgoing" voters will
// not be returned even though they do in the current state retain their voting
// rights.
//
// This may allocate, but it also may return the underlying slice as a
// performance optimization, so it's not safe to modify the returned value.
//
// TODO(tbg): go through the callers and figure out the few which want a
// different subset of voters. Consider renaming this method so that it's
// more descriptive.
func (d ReplicaSet) Voters() ReplicaSet {
	return d.Filter(predVoterFullOrIncoming)
}

// VoterDescriptors returns the descriptors of current and future voter replicas
// in the set.
func (d ReplicaSet) VoterDescriptors() []ReplicaDescriptor {
	return d.FilterToDescriptors(predVoterFullOrIncoming)
}

func (d ReplicaSet) containsVoterIncoming() bool {
	return len(d.FilterToDescriptors(predVoterIncoming)) > 0
}

// LearnerDescriptors returns a slice of ReplicaDescriptors corresponding to
// learner replicas in `d`. This may allocate, but it also may return the
// underlying slice as a performance optimization, so it's not safe to modify
// the returned value.
//
// A learner is a participant in a raft group that accepts messages but doesn't
// vote. This means it doesn't affect raft quorum and thus doesn't affect the
// fragility of the range, even if it's very far behind or many learners are
// down.
//
// At the time of writing, learners are used in CockroachDB as an interim state
// while adding a replica. A learner replica is added to the range via raft
// ConfChange, a raft snapshot (of type INITIAL) is sent to catch it up, and
// then a second ConfChange promotes it to a full replica.
//
// This means that learners are currently always expected to have a short
// lifetime, approximately the time it takes to send a snapshot.
//
// For simplicity, CockroachDB treats learner replicas the same as voter
// replicas as much as possible, but there are a few exceptions:
//
//   - Learner replicas are not considered when calculating quorum size, and thus
//     do not affect the computation of which ranges are under-replicated for
//     upreplication/alerting/debug/etc purposes. Ditto for over-replicated.
//   - Learner replicas cannot become raft leaders, so we also don't allow them to
//     become leaseholders. As a result, DistSender and the various oracles don't
//     try to send them traffic.
//   - The raft snapshot queue tries to avoid sending snapshots to ephemeral
//     learners (but not to non-voting replicas, which are also etcd learners) for
//     reasons described below.
//   - Merges won't run while a learner replica is present.
//
// Replicas are now added in two ConfChange transactions. The first creates the
// learner and the second promotes it to a voter. If the node that is
// coordinating this dies in the middle, we're left with an orphaned learner.
// For this reason, the replicate queue always first removes any learners it
// sees before doing anything else. We could instead try to finish off the
// learner snapshot and promotion, but this is more complicated and it's not yet
// clear the efficiency win is worth it.
//
// This introduces some rare races between the replicate queue and
// AdminChangeReplicas or if a range's lease is moved to a new owner while the
// old leaseholder is still processing it in the replicate queue. These races
// are handled by retrying if a learner disappears during the
// snapshot/promotion.
//
// If the coordinator otherwise encounters an error while sending the learner
// snapshot or promoting it (which can happen for a number of reasons, including
// the node getting the learner going away), it tries to clean up after itself
// by rolling back the addition of the learner.
//
// [*] There is another race between the learner snapshot being sent and the
// raft snapshot queue happening to check the replica at the same time, also
// sending it a snapshot. This is safe but wasteful, so the raft snapshot queue
// won't try to send snapshots to learners if there is already a snapshot to
// that range in flight.
//
// *However*, raft is currently pickier than the needs to be about the snapshots
// it requests and it can get stuck in StateSnapshot if it doesn't receive
// exactly the index it wants. As a result, for now, the raft snapshot queue
// will send one if it's still needed after the learner snapshot finishes (or
// times out). To make this work in a timely manner (i.e. without relying on the
// replica scanner) but without blocking the raft snapshot queue, when a
// snapshot is skipped, this is reported to raft as an error sending the
// snapshot. This causes raft to eventually re-enqueue it in the raft snapshot
// queue. All of this is quite hard to reason about, so it'd be nice to make
// this go away at some point.
//
// Merges are blocked if either side has a learner (to avoid working out the
// edge cases) but it's historically turned out to be a bad idea to get in the
// way of splits, so we allow them even when some of the replicas are learners.
// This orphans a learner on each side of the split (the original coordinator
// will not be able to finish either of them), but the replication queue will
// eventually clean them up.
//
// Learner replicas don't affect quorum but they do affect the system in other
// ways. The most obvious way is that the leader sends them the raft traffic it
// would send to any follower, consuming resources. More surprising is that once
// the learner has received a snapshot, it's considered by the quota pool that
// prevents the raft leader from getting too far ahead of the followers.
// However, it means a slow learner can slow down regular traffic.
//
// For some related mega-comments, see Replica.sendSnapshot.
func (d ReplicaSet) LearnerDescriptors() []ReplicaDescriptor {
	return d.FilterToDescriptors(predLearner)
}

// NonVoters returns a ReplicaSet containing only the non-voters in `d`.
// Non-voting replicas are treated differently from learner replicas. Learners
// are a temporary internal state used to make atomic replication changes less
// disruptive to the system. Even though learners and non-voting replicas are
// both etcd/raft LearnerNodes under the hood, non-voting replicas are meant to
// be a user-visible state and are explicitly chosen to be placed inside certain
// localities via zone configs.
func (d ReplicaSet) NonVoters() ReplicaSet {
	return d.Filter(predNonVoter)
}

// NonVoterDescriptors returns the non-voting replica descriptors in the set.
func (d ReplicaSet) NonVoterDescriptors() []ReplicaDescriptor {
	return d.FilterToDescriptors(predNonVoter)
}

// VoterFullAndNonVoterDescriptors returns the descriptors of
// VOTER_FULL/NON_VOTER replicas in the set. This set will not contain learners
// or, during an atomic replication change, incoming or outgoing voters.
// Notably, this set must encapsulate all replicas of a range for a range merge
// to proceed.
func (d ReplicaSet) VoterFullAndNonVoterDescriptors() []ReplicaDescriptor {
	return d.FilterToDescriptors(predVoterFullOrNonVoter)
}

// VoterAndNonVoterDescriptors returns the descriptors of VOTER_FULL,
// VOTER_INCOMING and NON_VOTER replicas in the set. Notably, this is the set of
// replicas the DistSender will consider routing follower read requests to.
func (d ReplicaSet) VoterAndNonVoterDescriptors() []ReplicaDescriptor {
	return d.FilterToDescriptors(predVoterOrNonVoter)
}

// Filter returns a ReplicaSet corresponding to the replicas for which the
// supplied predicate returns true.
func (d ReplicaSet) Filter(pred func(rDesc ReplicaDescriptor) bool) ReplicaSet {
	return MakeReplicaSet(d.FilterToDescriptors(pred))
}

// FilterToDescriptors returns only the replica descriptors for which the
// supplied method returns true. The memory returned may be shared with the
// receiver.
func (d ReplicaSet) FilterToDescriptors(
	pred func(rDesc ReplicaDescriptor) bool,
) []ReplicaDescriptor {
	// Fast path when all or none match to avoid allocations.
	fastpath := true
	out := d.wrapped
	for i := range d.wrapped {
		if pred(d.wrapped[i]) {
			if !fastpath {
				out = append(out, d.wrapped[i])
			}
		} else {
			if fastpath {
				out = nil
				out = append(out, d.wrapped[:i]...)
				fastpath = false
			}
		}
	}
	return out
}

// AsProto returns the protobuf representation of these replicas, suitable for
// setting the InternalReplicas field of a RangeDescriptor. When possible the
// SetReplicas method of RangeDescriptor should be used instead, this is only
// here for the convenience of tests.
func (d ReplicaSet) AsProto() []ReplicaDescriptor {
	return d.wrapped
}

// DeepCopy returns a copy of this set of replicas. Modifications to the
// returned set will not affect this one and vice-versa.
func (d ReplicaSet) DeepCopy() ReplicaSet {
	return ReplicaSet{
		wrapped: append([]ReplicaDescriptor(nil), d.wrapped...),
	}
}

// AddReplica adds the given replica to this set.
func (d *ReplicaSet) AddReplica(r ReplicaDescriptor) {
	d.wrapped = append(d.wrapped, r)
}

// RemoveReplica removes the matching replica from this set. If it wasn't found
// to remove, false is returned.
func (d *ReplicaSet) RemoveReplica(nodeID NodeID, storeID StoreID) (ReplicaDescriptor, bool) {
	idx := -1
	for i := range d.wrapped {
		if d.wrapped[i].NodeID == nodeID && d.wrapped[i].StoreID == storeID {
			idx = i
			break
		}
	}
	if idx == -1 {
		return ReplicaDescriptor{}, false
	}
	// Swap with the last element so we can simply truncate the slice.
	d.wrapped[idx], d.wrapped[len(d.wrapped)-1] = d.wrapped[len(d.wrapped)-1], d.wrapped[idx]
	removed := d.wrapped[len(d.wrapped)-1]
	d.wrapped = d.wrapped[:len(d.wrapped)-1]
	return removed, true
}

// InAtomicReplicationChange returns true if the descriptor is in the middle of
// an atomic replication change.
func (d ReplicaSet) InAtomicReplicationChange() bool {
	for _, rDesc := range d.wrapped {
		switch rDesc.Type {
		case VOTER_INCOMING, VOTER_OUTGOING, VOTER_DEMOTING_LEARNER,
			VOTER_DEMOTING_NON_VOTER:
			return true
		case VOTER_FULL, LEARNER, NON_VOTER:
		default:
			panic(fmt.Sprintf("unknown replica type %d", rDesc.Type))
		}
	}
	return false
}

// ConfState returns the Raft configuration described by the set of replicas.
func (d ReplicaSet) ConfState() raftpb.ConfState {
	var cs raftpb.ConfState
	joint := d.InAtomicReplicationChange()
	// The incoming config is taken verbatim from the full voters when the
	// config is not joint. If it is joint, slot the voters into the right
	// category.
	for _, rep := range d.wrapped {
		id := uint64(rep.ReplicaID)
		switch rep.Type {
		case VOTER_FULL:
			cs.Voters = append(cs.Voters, id)
			if joint {
				cs.VotersOutgoing = append(cs.VotersOutgoing, id)
			}
		case VOTER_INCOMING:
			cs.Voters = append(cs.Voters, id)
		case VOTER_OUTGOING:
			cs.VotersOutgoing = append(cs.VotersOutgoing, id)
		case VOTER_DEMOTING_LEARNER, VOTER_DEMOTING_NON_VOTER:
			cs.VotersOutgoing = append(cs.VotersOutgoing, id)
			cs.LearnersNext = append(cs.LearnersNext, id)
		case LEARNER:
			cs.Learners = append(cs.Learners, id)
		case NON_VOTER:
			cs.Learners = append(cs.Learners, id)
		default:
			panic(fmt.Sprintf("unknown ReplicaType %d", rep.Type))
		}
	}
	return cs
}

// HasReplicaOnNode returns true iff the given nodeID is present in the
// ReplicaSet.
func (d ReplicaSet) HasReplicaOnNode(nodeID NodeID) bool {
	for _, rep := range d.wrapped {
		if rep.NodeID == nodeID {
			return true
		}
	}
	return false
}

// CanMakeProgress reports whether the given descriptors can make progress at
// the replication layer. This is more complicated than just counting the number
// of replicas due to the existence of joint quorums.
func (d ReplicaSet) CanMakeProgress(liveFunc func(descriptor ReplicaDescriptor) bool) bool {
	return d.ReplicationStatus(liveFunc, 0 /* neededVoters */, -1 /* neededNonVoters*/).Available
}

// RangeStatusReport contains info about a range's replication status. Returned
// by ReplicaSet.ReplicationStatus.
type RangeStatusReport struct {
	// Available is set if the range can make progress, based on replica liveness
	// info passed to ReplicationStatus().
	Available bool
	// UnderReplicated is set if the range is considered under-replicated
	// according to the desired replication factor and the replica liveness info
	// passed to ReplicationStatus. Only voting replicas are counted here. Dead
	// replicas are considered to be missing.
	UnderReplicated bool
	// OverReplicated is set if the range is considered over-replicated
	// according to the desired replication factor passed to ReplicationStatus.
	// Only voting replicas are counted here. Replica liveness is not
	// considered.
	//
	// Note that a range can be under-replicated and over-replicated at the same
	// time if it has many replicas, but sufficiently many of them are on dead
	// nodes.
	OverReplicated bool
	// {Under,Over}ReplicatedNonVoters are like their {Under,Over}Replicated
	// counterparts but applying only to non-voters.
	UnderReplicatedNonVoters, OverReplicatedNonVoters bool
}

// ReplicationStatus returns availability and over/under-replication
// determinations for the range.
//
// neededVoters is the replica's desired replication for purposes of determining
// over/under-replication of voters. If the caller is only interested in
// availability of voting replicas, 0 can be passed in. neededNonVoters is the
// counterpart for non-voting replicas but with -1 as the sentinel value (unlike
// voters, it's possible to expect 0 non-voters).
func (d ReplicaSet) ReplicationStatus(
	liveFunc func(descriptor ReplicaDescriptor) bool, neededVoters int, neededNonVoters int,
) RangeStatusReport {
	var res RangeStatusReport
	// isBoth takes two replica predicates and returns their conjunction.
	isBoth := func(
		pred1 func(rDesc ReplicaDescriptor) bool,
		pred2 func(rDesc ReplicaDescriptor) bool) func(ReplicaDescriptor) bool {
		return func(rDesc ReplicaDescriptor) bool {
			return pred1(rDesc) && pred2(rDesc)
		}
	}

	// This functions handles regular, or joint-consensus replica groups. In the
	// joint-consensus case, we'll independently consider the health of the
	// outgoing group ("old") and the incoming group ("new"). In the regular case,
	// the two groups will be identical.

	votersOldGroup := d.FilterToDescriptors(ReplicaDescriptor.IsVoterOldConfig)
	liveVotersOldGroup := d.FilterToDescriptors(isBoth(ReplicaDescriptor.IsVoterOldConfig, liveFunc))

	n := len(votersOldGroup)
	// Empty groups succeed by default, to match the Raft implementation.
	availableOutgoingGroup := (n == 0) || (len(liveVotersOldGroup) >= n/2+1)

	votersNewGroup := d.FilterToDescriptors(ReplicaDescriptor.IsVoterNewConfig)
	liveVotersNewGroup := d.FilterToDescriptors(isBoth(ReplicaDescriptor.IsVoterNewConfig, liveFunc))

	n = len(votersNewGroup)
	availableIncomingGroup := len(liveVotersNewGroup) >= n/2+1

	res.Available = availableIncomingGroup && availableOutgoingGroup

	// Determine over/under-replication of voting replicas. Note that learners
	// don't matter.
	underReplicatedOldGroup := len(liveVotersOldGroup) < neededVoters
	underReplicatedNewGroup := len(liveVotersNewGroup) < neededVoters
	overReplicatedOldGroup := len(votersOldGroup) > neededVoters
	overReplicatedNewGroup := len(votersNewGroup) > neededVoters
	res.UnderReplicated = underReplicatedOldGroup || underReplicatedNewGroup
	res.OverReplicated = overReplicatedOldGroup || overReplicatedNewGroup
	if neededNonVoters == -1 {
		return res
	}

	nonVoters := d.FilterToDescriptors(ReplicaDescriptor.IsNonVoter)
	liveNonVoters := d.FilterToDescriptors(isBoth(ReplicaDescriptor.IsNonVoter, liveFunc))
	res.UnderReplicatedNonVoters = len(liveNonVoters) < neededNonVoters
	res.OverReplicatedNonVoters = len(nonVoters) > neededNonVoters
	return res
}

// Empty returns true if `target` is an empty replication target.
func Empty(target ReplicationTarget) bool {
	return target == ReplicationTarget{}
}

// ReplicationTargets returns a slice of ReplicationTargets corresponding to
// each of the replicas in the set.
func (d ReplicaSet) ReplicationTargets() (out []ReplicationTarget) {
	descs := d.Descriptors()
	out = make([]ReplicationTarget, len(descs))
	for i := range descs {
		repl := &descs[i]
		out[i].NodeID, out[i].StoreID = repl.NodeID, repl.StoreID
	}
	return out
}

// IsAddition returns true if `c` refers to a replica addition operation.
func (c ReplicaChangeType) IsAddition() bool {
	switch c {
	case ADD_NON_VOTER, ADD_VOTER:
		return true
	case REMOVE_NON_VOTER, REMOVE_VOTER:
		return false
	default:
		panic(fmt.Sprintf("unexpected ReplicaChangeType %s", c))
	}
}

// IsRemoval returns true if `c` refers a replica removal operation.
func (c ReplicaChangeType) IsRemoval() bool {
	switch c {
	case ADD_NON_VOTER, ADD_VOTER:
		return false
	case REMOVE_NON_VOTER, REMOVE_VOTER:
		return true
	default:
		panic(fmt.Sprintf("unexpected ReplicaChangeType %s", c))
	}
}

var errReplicaNotFound = errors.Errorf(`replica not found in RangeDescriptor`)

// ErrReplicaCannotHoldLease can be returned from CheckCanReceiveLease.
//
// See: https://github.com/cockroachdb/cockroach/issues/93163
var ErrReplicaCannotHoldLease = errors.Errorf("replica cannot hold lease")

// CheckCanReceiveLease checks whether `wouldbeLeaseholder` can receive a lease.
// Returns an error if the respective replica is not eligible.
//
// Previously, we were not allowed to enter a joint config where the
// leaseholder is being removed (i.e., not a full voter). In the new version
// we're allowed to enter such a joint config (if it has a VOTER_INCOMING),
// but not to exit it in this state, i.e., the leaseholder must be some
// kind of voter in the next new config (potentially VOTER_DEMOTING).
//
// It is possible (and sometimes needed) that while in the joint configuration,
// the replica being removed will receive lease. This is allowed only if
// a) there is a VOTER_INCOMING replica to which the lease will be trasferred
// when transitioning out of the joint config, and b) the replica being removed
// was the last leaseholder (as indictated by wasLastLeaseholder). The
// information we use for (b) is potentially stale, but if it incorrect
// the removed node either does not need to get the lease or will not be able
// to get it. In particular, when we think we are the last leaseholder but we
// aren't, the CAS call for extending the lease will fail (see
// wasLastLeaseholder := isExtension in cmd_lease_request.go).
//
// An error is also returned is the replica is not part of `replDescs`.
// NB: This logic should be in sync with constraint_stats_report as report
// will check voter constraint violations. When changing this method, you need
// to update replica filter in report to keep it correct.
func CheckCanReceiveLease(
	wouldbeLeaseholder ReplicaDescriptor, replDescs ReplicaSet, wasLastLeaseholder bool,
) error {
	repDesc, ok := replDescs.GetReplicaDescriptorByID(wouldbeLeaseholder.ReplicaID)
	if !ok {
		return errReplicaNotFound
	}
	if !(repDesc.IsVoterNewConfig() ||
		(repDesc.IsVoterOldConfig() && replDescs.containsVoterIncoming() && wasLastLeaseholder)) {
		// We allow a demoting / incoming voter to receive the lease if there's an incoming voter.
		// In this case, when exiting the joint config, we will transfer the lease to the incoming
		// voter.
		return ErrReplicaCannotHoldLease
	}
	return nil
}
