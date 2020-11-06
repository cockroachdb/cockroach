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

	"github.com/cockroachdb/redact"
	"go.etcd.io/etcd/raft/raftpb"
)

// ReplicaTypeVoterFull returns a VOTER_FULL pointer suitable for use in a
// nullable proto field.
func ReplicaTypeVoterFull() *ReplicaType {
	t := VOTER_FULL
	return &t
}

// ReplicaTypeVoterIncoming returns a VOTER_INCOMING pointer suitable
// for use in a nullable proto field.
func ReplicaTypeVoterIncoming() *ReplicaType {
	t := VOTER_INCOMING
	return &t
}

// ReplicaTypeVoterOutgoing returns a VOTER_OUTGOING pointer suitable
// for use in a nullable proto field.
func ReplicaTypeVoterOutgoing() *ReplicaType {
	t := VOTER_OUTGOING
	return &t
}

// ReplicaTypeVoterDemoting returns a VOTER_DEMOTING pointer suitable
// for use in a nullable proto field.
func ReplicaTypeVoterDemoting() *ReplicaType {
	t := VOTER_DEMOTING
	return &t
}

// ReplicaTypeLearner returns a LEARNER pointer suitable for use in
// a nullable proto field.
func ReplicaTypeLearner() *ReplicaType {
	t := LEARNER
	return &t
}

// ReplicaDescriptors is a set of replicas, usually the nodes/stores on which
// replicas of a range are stored.
type ReplicaDescriptors struct {
	wrapped []ReplicaDescriptor
}

// MakeReplicaDescriptors creates a ReplicaDescriptors wrapper from a raw slice
// of individual descriptors.
//
// All construction of ReplicaDescriptors is required to go through this method
// so we can guarantee sortedness, which is used to speed up accessor
// operations.
//
// The function accepts a pointer to a slice instead of a slice directly to
// avoid an allocation when boxing the argument as a sort.Interface. This may
// cause the argument to escape to the heap for some callers, at which point
// we're trading one allocation for another. However, if the caller already has
// the slice header on the heap (which is the common case for *RangeDescriptors)
// then this is a net win.
func MakeReplicaDescriptors(replicas []ReplicaDescriptor) ReplicaDescriptors {
	return ReplicaDescriptors{wrapped: replicas}
}

// SafeFormat implements redact.SafeFormatter.
func (d ReplicaDescriptors) SafeFormat(w redact.SafePrinter, _ rune) {
	for i, desc := range d.wrapped {
		if i > 0 {
			w.SafeRune(',')
		}
		w.Print(desc)
	}
}

func (d ReplicaDescriptors) String() string {
	return redact.StringWithoutMarkers(d)
}

// All returns every replica in the set, including both voter replicas and
// learner replicas. Voter replicas are ordered first in the returned slice.
func (d ReplicaDescriptors) All() []ReplicaDescriptor {
	return d.wrapped
}

func predVoterFullOrIncoming(rDesc ReplicaDescriptor) bool {
	switch rDesc.GetType() {
	case VOTER_FULL, VOTER_INCOMING:
		return true
	default:
	}
	return false
}

func predLearner(rDesc ReplicaDescriptor) bool {
	return rDesc.GetType() == LEARNER
}

func predNonVoter(rDesc ReplicaDescriptor) bool {
	return rDesc.GetType() == NON_VOTER
}

// Voters returns the current and future voter replicas in the set. This means
// that during an atomic replication change, only the replicas that will be
// voters once the change completes will be returned; "outgoing" voters will not
// be returned even though they do in the current state retain their voting
// rights. When no atomic membership change is ongoing, this is simply the set
// of all non-learners.
//
// This may allocate, but it also may return the underlying slice as a
// performance optimization, so it's not safe to modify the returned value.
//
// TODO(tbg): go through the callers and figure out the few which want a
// different subset of voters. Consider renaming this method so that it's
// more descriptive.
func (d ReplicaDescriptors) Voters() []ReplicaDescriptor {
	return d.Filter(predVoterFullOrIncoming)
}

// Learners returns the learner replicas in the set. This may allocate, but it
// also may return the underlying slice as a performance optimization, so it's
// not safe to modify the returned value.
//
// A learner is a participant in a raft group that accepts messages but doesn't
// vote. This means it doesn't affect raft quorum and thus doesn't affect the
// fragility of the range, even if it's very far behind or many learners are
// down.
//
// At the time of writing, learners are used in CockroachDB as an interim state
// while adding a replica. A learner replica is added to the range via raft
// ConfChange, a raft snapshot (of type LEARNER_INITIAL) is sent to catch it up, and
// then a second ConfChange promotes it to a full replica.
//
// This means that learners are currently always expected to have a short
// lifetime, approximately the time it takes to send a snapshot.
//
// For simplicity, CockroachDB treats learner replicas the same as voter
// replicas as much as possible, but there are a few exceptions:
//
// - Learner replicas are not considered when calculating quorum size, and thus
//   do not affect the computation of which ranges are under-replicated for
//   upreplication/alerting/debug/etc purposes. Ditto for over-replicated.
// - Learner replicas cannot become raft leaders, so we also don't allow them to
//   become leaseholders. As a result, DistSender and the various oracles don't
//   try to send them traffic.
// - The raft snapshot queue tries to avoid sending snapshots to ephemeral
//   learners (but not to non-voting replicas, which are also etcd learners) for
//   reasons described below.
// - Merges won't run while a learner replica is present.
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
func (d ReplicaDescriptors) Learners() []ReplicaDescriptor {
	return d.Filter(predLearner)
}

// NonVoters returns the non-voting replicas in the set. Non-voting replicas are
// treated differently from learner replicas. Learners are a temporary internal
// state used to make atomic replication changes less disruptive to the system.
// Even though learners and non-voting replicas are both etcd/raft LearnerNodes
// under the hood, non-voting replicas are meant to be a user-visible state and
// are explicitly chosen to be placed inside certain localities via zone
// configs.
//
// Key differences between how we treat (ephemeral) learners and (persistent)
// non-voting replicas: - Non-voting replicas rely on the raft snapshot queue in
// order to upreplicate. This is different from the way learner replicas
// upreplicate (see comment above) because of the various (necessary)
// complexities / race-conditions we've discovered between the raft snapshot
// queue and the separately-issued initial LEARNER_INITIAL snapshot (see the two
// paragraphs above [*]). This complexity was necessary in case of learner
// replicas because we _need to know_ when they finish upreplication so that we
// can initiate their promotion into full voters. We don't have a similar
// requirement for non-voting replicas and we're choosing to avoid all the
// complexity.
//
// TODO(aayush): Expand this documentation once `AdminRelocateRange` knows how
// to deal with such replicas & range merges no longer block due to the presence
// of non-voting replicas.
func (d ReplicaDescriptors) NonVoters() []ReplicaDescriptor {
	return d.Filter(predNonVoter)
}

// Filter returns only the replica descriptors for which the supplied method
// returns true. The memory returned may be shared with the receiver.
func (d ReplicaDescriptors) Filter(pred func(rDesc ReplicaDescriptor) bool) []ReplicaDescriptor {
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
func (d ReplicaDescriptors) AsProto() []ReplicaDescriptor {
	return d.wrapped
}

// DeepCopy returns a copy of this set of replicas. Modifications to the
// returned set will not affect this one and vice-versa.
func (d ReplicaDescriptors) DeepCopy() ReplicaDescriptors {
	return ReplicaDescriptors{
		wrapped: append([]ReplicaDescriptor(nil), d.wrapped...),
	}
}

// AddReplica adds the given replica to this set.
func (d *ReplicaDescriptors) AddReplica(r ReplicaDescriptor) {
	d.wrapped = append(d.wrapped, r)
}

// RemoveReplica removes the matching replica from this set. If it wasn't found
// to remove, false is returned.
func (d *ReplicaDescriptors) RemoveReplica(
	nodeID NodeID, storeID StoreID,
) (ReplicaDescriptor, bool) {
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
func (d ReplicaDescriptors) InAtomicReplicationChange() bool {
	for _, rDesc := range d.wrapped {
		switch rDesc.GetType() {
		case VOTER_INCOMING, VOTER_OUTGOING, VOTER_DEMOTING:
			return true
		case VOTER_FULL, LEARNER, NON_VOTER:
		default:
			panic(fmt.Sprintf("unknown replica type %d", rDesc.GetType()))
		}
	}
	return false
}

// ConfState returns the Raft configuration described by the set of replicas.
func (d ReplicaDescriptors) ConfState() raftpb.ConfState {
	var cs raftpb.ConfState
	joint := d.InAtomicReplicationChange()
	// The incoming config is taken verbatim from the full voters when the
	// config is not joint. If it is joint, slot the voters into the right
	// category.
	for _, rep := range d.wrapped {
		id := uint64(rep.ReplicaID)
		typ := rep.GetType()
		switch typ {
		case VOTER_FULL:
			cs.Voters = append(cs.Voters, id)
			if joint {
				cs.VotersOutgoing = append(cs.VotersOutgoing, id)
			}
		case VOTER_INCOMING:
			cs.Voters = append(cs.Voters, id)
		case VOTER_OUTGOING:
			cs.VotersOutgoing = append(cs.VotersOutgoing, id)
		case VOTER_DEMOTING:
			cs.VotersOutgoing = append(cs.VotersOutgoing, id)
			cs.LearnersNext = append(cs.LearnersNext, id)
		case LEARNER:
			cs.Learners = append(cs.Learners, id)
		case NON_VOTER:
			cs.Learners = append(cs.Learners, id)
		default:
			panic(fmt.Sprintf("unknown ReplicaType %d", typ))
		}
	}
	return cs
}

// CanMakeProgress reports whether the given descriptors can make progress at the
// replication layer. This is more complicated than just counting the number
// of replicas due to the existence of joint quorums.
func (d ReplicaDescriptors) CanMakeProgress(liveFunc func(descriptor ReplicaDescriptor) bool) bool {
	isVoterOldConfig := func(rDesc ReplicaDescriptor) bool {
		switch rDesc.GetType() {
		case VOTER_FULL, VOTER_OUTGOING, VOTER_DEMOTING:
			return true
		default:
			return false
		}
	}
	isVoterNewConfig := func(rDesc ReplicaDescriptor) bool {
		switch rDesc.GetType() {
		case VOTER_FULL, VOTER_INCOMING:
			return true
		default:
			return false
		}
	}
	// isBoth takes two replica predicates and returns their conjunction.
	isBoth := func(
		pred1 func(rDesc ReplicaDescriptor) bool,
		pred2 func(rDesc ReplicaDescriptor) bool) func(ReplicaDescriptor) bool {
		return func(rDesc ReplicaDescriptor) bool {
			return pred1(rDesc) && pred2(rDesc)
		}
	}

	votersOldGroup := d.Filter(isVoterOldConfig)
	liveVotersOldGroup := d.Filter(isBoth(isVoterOldConfig, liveFunc))

	n := len(votersOldGroup)
	// Empty groups succeed by default, to match the Raft implementation.
	if n > 0 && len(liveVotersOldGroup) < n/2+1 {
		return false
	}

	votersNewGroup := d.Filter(isVoterNewConfig)
	liveVotersNewGroup := d.Filter(isBoth(isVoterNewConfig, liveFunc))

	n = len(votersNewGroup)
	return len(liveVotersNewGroup) >= n/2+1
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
