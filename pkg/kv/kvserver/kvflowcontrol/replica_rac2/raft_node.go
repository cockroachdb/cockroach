// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package replica_rac2

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/rac2"
	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/raft/tracker"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// MakeRaftNodeBasicStateLocked makes a RaftNodeBasicState given a RawNode and
// current known leaseholder. Replica.mu must be held, at least for read.
func MakeRaftNodeBasicStateLocked(
	rn *raft.RawNode, leaseholderID roachpb.ReplicaID,
) RaftNodeBasicState {
	return RaftNodeBasicState{
		Term:              rn.Term(),
		IsLeader:          rn.State() == raftpb.StateLeader,
		Leader:            roachpb.ReplicaID(rn.Lead()),
		NextUnstableIndex: rn.NextUnstableIndex(),
		Leaseholder:       leaseholderID,
	}
}

// MakeReplicaStateInfos populates infoMap using a RawNode. Replica.mu must be
// held, at least for read.
func MakeReplicaStateInfos(rn *raft.RawNode, infoMap map[roachpb.ReplicaID]rac2.ReplicaStateInfo) {
	clear(infoMap)
	rn.WithBasicProgress(func(peerID raftpb.PeerID, progress tracker.BasicProgress) {
		infoMap[roachpb.ReplicaID(peerID)] = rac2.ReplicaStateInfo{
			State:         progress.State,
			Match:         progress.Match,
			Next:          progress.Next,
			InflightBytes: progress.InflightBytes,
		}
	})
}

type raftNodeForRACv2 struct {
	*raft.RawNode
	r ReplicaForRaftNode
}

type ReplicaForRaftNode interface {
	// MuLock acquires Replica.mu.
	MuLock()
	// MuUnlock releases Replica.mu.
	MuUnlock()
}

// NewRaftNode creates a rac2.RaftInterface implementation from the given
// RawNode.
func NewRaftNode(rn *raft.RawNode, r ReplicaForRaftNode) rac2.RaftInterface {
	return raftNodeForRACv2{RawNode: rn, r: r}
}

// SendPingRaftMuLocked implements rac2.RaftInterface.
func (rn raftNodeForRACv2) SendPingRaftMuLocked(to roachpb.ReplicaID) bool {
	rn.r.MuLock()
	defer rn.r.MuUnlock()
	return rn.RawNode.SendPing(raftpb.PeerID(to))
}

// SendMsgAppRaftMuLocked implements rac2.RaftInterface.
func (rn raftNodeForRACv2) SendMsgAppRaftMuLocked(
	replicaID roachpb.ReplicaID, ls raft.LogSlice,
) (raftpb.Message, bool) {
	rn.r.MuLock()
	defer rn.r.MuUnlock()
	return rn.RawNode.SendMsgApp(raftpb.PeerID(replicaID), ls)
}
