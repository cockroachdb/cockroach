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
	"sort"
	"strings"
)

// ReplicaTypeVoter returns a ReplicaType_VOTER pointer suitable for use in a
// nullable proto field.
func ReplicaTypeVoter() *ReplicaType {
	t := ReplicaType_VOTER
	return &t
}

// ReplicaTypeLearner returns a ReplicaType_LEARNER pointer suitable for use in
// a nullable proto field.
func ReplicaTypeLearner() *ReplicaType {
	t := ReplicaType_LEARNER
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
func MakeReplicaDescriptors(replicas *[]ReplicaDescriptor) ReplicaDescriptors {
	d := ReplicaDescriptors{wrapped: *replicas}
	d.sort()
	return d
}

func (d ReplicaDescriptors) String() string {
	var buf strings.Builder
	for i, desc := range d.wrapped {
		if i > 0 {
			buf.WriteByte(',')
		}
		fmt.Fprint(&buf, desc)
	}
	return buf.String()
}

// All returns every replica in the set, including both voter replicas and
// learner replicas. Voter replicas are ordered first in the returned slice.
func (d ReplicaDescriptors) All() []ReplicaDescriptor {
	return d.wrapped
}

// Voters returns the voter replicas in the set.
func (d ReplicaDescriptors) Voters() []ReplicaDescriptor {
	// Note that the wrapped replicas are sorted first by type.
	for i := range d.wrapped {
		if d.wrapped[i].GetType() == ReplicaType_LEARNER {
			return d.wrapped[:i]
		}
	}
	return d.wrapped
}

// Learners returns the learner replicas in the set.
//
// A learner is a participant in a raft group that accepts messages but doesn't
// vote. This means it doesn't affect raft quorum and thus doesn't affect the
// fragility of the range, even if it's very far behind or many learners are
// down.
//
// At the time of writing, learners are used in CockroachDB as an interim state
// while adding a replica. A learner replica is added to the range via raft
// ConfChange, a raft snapshot (of type LEARNER) is sent to catch it up, and
// then a second ConfChange promotes it to a full replica.
//
// This means that learners are currently always expected to have a short
// lifetime, approximately the time it takes to send a snapshot. Ideas have been
// kicked around to use learners with follower reads, which could be a cheap way
// to allow many geographies to have local reads without affecting write
// latencies. If implemented, these learners would have long lifetimes.
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
// - The raft snapshot queue does not send snapshots to learners for reasons
//   described below.
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
// There is another race between the learner snapshot being sent and the raft
// snapshot queue happening to check the replica at the same time, also sending
// it a snapshot. This is safe but wasteful, so the raft snapshot queue won't
// try to send snapshots to learners.
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
// prevents the raft leader from getting too far ahead of the followers. This is
// because a learner (especially one that already has a snapshot) is expected to
// very soon be a voter, so we treat it like one. However, it means a slow
// learner can slow down regular traffic, which is possibly counterintuitive.
//
// For some related mega-comments, see Replica.sendSnapshot.
func (d ReplicaDescriptors) Learners() []ReplicaDescriptor {
	// Note that the wrapped replicas are sorted first by type.
	for i := range d.wrapped {
		if d.wrapped[i].GetType() == ReplicaType_LEARNER {
			return d.wrapped[i:]
		}
	}
	return nil
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

func (d *ReplicaDescriptors) sort() {
	sort.Sort((*byTypeThenReplicaID)(&d.wrapped))
}

// AddReplica adds the given replica to this set.
func (d *ReplicaDescriptors) AddReplica(r ReplicaDescriptor) {
	d.wrapped = append(d.wrapped, r)
	d.sort()
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
	// The swap may have broken our sortedness invariant, so re-sort.
	d.sort()
	return removed, true
}

// QuorumSize returns the number of voter replicas required for quorum in a raft
// group consisting of this set of replicas.
func (d ReplicaDescriptors) QuorumSize() int {
	return (len(d.Voters()) / 2) + 1
}

type byTypeThenReplicaID []ReplicaDescriptor

func (x *byTypeThenReplicaID) Len() int      { return len(*x) }
func (x *byTypeThenReplicaID) Swap(i, j int) { (*x)[i], (*x)[j] = (*x)[j], (*x)[i] }
func (x *byTypeThenReplicaID) Less(i, j int) bool {
	if (*x)[i].GetType() == (*x)[j].GetType() {
		return (*x)[i].ReplicaID < (*x)[j].ReplicaID
	}
	return (*x)[i].GetType() < (*x)[j].GetType()
}
