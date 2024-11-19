// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package raftutil

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/raft/tracker"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// ReplicaIsBehind returns whether the given peer replica is considered behind
// according to the raft log. If this function is called with a raft.Status that
// indicates that our local replica is not the raft leader, we pessimistically
// assume that replicaID is behind on its log.
func ReplicaIsBehind(st *raft.Status, replicaID roachpb.ReplicaID) bool {
	if st == nil {
		// Testing only.
		return true
	}
	if st.RaftState != raftpb.StateLeader {
		// If we aren't the Raft leader, we aren't tracking the replica's progress,
		// so we can't be sure it's not behind.
		return true
	}
	progress, ok := st.Progress[raftpb.PeerID(replicaID)]
	if !ok {
		return true
	}
	if raftpb.PeerID(replicaID) == st.Lead {
		// If the replica is the leader, it cannot be behind on the log.
		return false
	}
	if progress.State == tracker.StateReplicate && progress.Match >= st.Commit {
		// If the replica has a matching log entry at or above the current commit
		// index, it is caught up on its log.
		return false
	}
	return true
}

// ReplicaNeedsSnapshotStatus enumerates the possible states that a peer replica
// may be in when the local replica makes a determination of whether the peer
// may be in need of a Raft snapshot. NoSnapshotNeeded indicates that a Raft
// snapshot is definitely not needed. All other values indicate that for one
// reason or another the local replica cannot say with certainty that the peer
// does not need a snapshot, either because it knows that the peer does need a
// snapshot or because it does not know.
type ReplicaNeedsSnapshotStatus int

const (
	// NoSnapshotNeeded means that the local replica knows for sure that the peer
	// replica does not need a Raft snapshot. This status is only possible if the
	// local replica is the Raft leader, because only the Raft leader tracks the
	// progress of other replicas.
	//
	// There are two ways that a peer replica that is at some point seen to be in
	// this state can later end up in need of a Raft snapshot:
	// 1. a log truncation cuts the peer replica off from the log, preventing it
	//    from catching up using log entries. The Raft log queue attempts to avoid
	//    cutting any follower off from connectivity with the leader's log, but
	//    there are cases where it does so.
	// 2. raft leadership is transferred to a replica whose log does not extend
	//    back as far as the current raft leader's log. This is possible because
	//    different replicas can have Raft logs with different starting points
	//    ("first indexes"). See discussion about #35701 in #81561.
	NoSnapshotNeeded ReplicaNeedsSnapshotStatus = iota

	// LocalReplicaNotLeader means that the local replica is not the Raft leader,
	// so it does not keep track of enough progress information about peers to
	// determine whether they are in need of a Raft snapshot or not.
	LocalReplicaNotLeader

	// ReplicaUnknown means that the peer replica is not known by the Raft leader
	// and is not part of the Raft group.
	ReplicaUnknown

	// ReplicaStateProbe means that the local Raft leader is still probing the
	// peer replica to determine the index of matching tail of its log.
	ReplicaStateProbe

	// ReplicaStateSnapshot means that the local Raft leader has determined that
	// the peer replica needs a Raft snapshot.
	ReplicaStateSnapshot

	// ReplicaMatchBelowLeadersFirstIndex means that the local Raft leader has
	// determined that the peer replica's latest matching log index is below the
	// leader's log's current first index. This can happen if a peer replica is
	// initially connected to the Raft leader's log but gets disconnected due to a
	// log truncation. etcd/raft will notice this state after sending the next
	// MsgApp and move the peer to StateSnapshot.
	ReplicaMatchBelowLeadersFirstIndex

	// NoRaftStatusAvailable is only possible in tests.
	NoRaftStatusAvailable
)

func (s ReplicaNeedsSnapshotStatus) String() string {
	switch s {
	case NoSnapshotNeeded:
		return "no snapshot needed"
	case LocalReplicaNotLeader:
		return "local replica not raft leader"
	case ReplicaUnknown:
		return "replica unknown"
	case ReplicaStateProbe:
		return "replica in StateProbe"
	case ReplicaStateSnapshot:
		return "replica in StateSnapshot"
	case ReplicaMatchBelowLeadersFirstIndex:
		return "replica's match index below leader's first index"
	case NoRaftStatusAvailable:
		return "no raft status available"
	default:
		return "unknown ReplicaNeedsSnapshotStatus"
	}
}

// ReplicaMayNeedSnapshot determines whether the given peer replica may be in
// need of a raft snapshot. If this function is called with a raft.Status that
// indicates that our local replica is not the raft leader, we pessimistically
// assume that replicaID may need a snapshot.
func ReplicaMayNeedSnapshot(
	st *raft.Status, firstIndex kvpb.RaftIndex, replicaID roachpb.ReplicaID,
) ReplicaNeedsSnapshotStatus {
	if st == nil {
		// Testing only.
		return NoRaftStatusAvailable
	}
	if st.RaftState != raftpb.StateLeader {
		// If we aren't the Raft leader, we aren't tracking the replica's progress,
		// so we can't be sure it does not need a snapshot.
		return LocalReplicaNotLeader
	}
	progress, ok := st.Progress[raftpb.PeerID(replicaID)]
	if !ok {
		// We don't know about the specified replica.
		return ReplicaUnknown
	}
	switch progress.State {
	case tracker.StateReplicate:
		// We can only reasonably assume that the follower replica is not in need of
		// a snapshot if it is in StateReplicate.
	case tracker.StateProbe:
		// If the follower is in StateProbe then we are still in the process of
		// determining where our logs match.
		return ReplicaStateProbe
	case tracker.StateSnapshot:
		// If the follower is in StateSnapshot then it needs a snapshot.
		return ReplicaStateSnapshot
	default:
		panic("unknown tracker.StateType")
	}
	if kvpb.RaftIndex(progress.Match+1) < firstIndex {
		// Even if the follower is in StateReplicate, it could have been cut off
		// from the log by a recent log truncation that hasn't been recognized yet
		// by raft. Confirm that this is not the case.
		return ReplicaMatchBelowLeadersFirstIndex
	}
	// Even if we get here, this can still be racy because:
	// 1. we may think we are the Raft leader but may have been or will be voted
	//    out without realizing. If another peer takes over as the Raft leader, it
	//    may commit additional log entries and then cut the peer off from the log.
	// 2. we can still have an ill-timed log truncation between when we make this
	//    determination and when we act on it.
	//
	// In order to eliminate the potential for a race when acting on this
	// information, we must ensure:
	// 1. that any action we take is conditional on still being the Raft leader.
	//    In practice, this means that we should check this condition immediately
	//    before proposing a Raft command, so we can be sure that the command is
	//    not redirected through another Raft leader. That way, if we were
	//    replaced as Raft leader, the proposal will fail.
	// 2. that we do not perform a log truncation between now and when our action
	//    goes into effect. In practice, this means serializing with Raft log
	//    truncation operations using latching.
	return NoSnapshotNeeded
}
