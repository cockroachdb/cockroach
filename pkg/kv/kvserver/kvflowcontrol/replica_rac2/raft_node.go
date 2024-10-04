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

// NewRaftNode creates a RaftNode implementation from the given RawNode.
func NewRaftNode(rn *raft.RawNode, r ReplicaForRaftNode) RaftNode {
	return raftNodeForRACv2{RawNode: rn, r: r}
}

func (rn raftNodeForRACv2) TermLocked() uint64 {
	return rn.Term()
}

func (rn raftNodeForRACv2) LeaderLocked() roachpb.ReplicaID {
	return roachpb.ReplicaID(rn.Lead())
}

func (rn raftNodeForRACv2) LogMarkLocked() rac2.LogMark {
	return rn.LogMark()
}

func (rn raftNodeForRACv2) NextUnstableIndexLocked() uint64 {
	return rn.NextUnstableIndex()
}

func (rn raftNodeForRACv2) ReplicasStateLocked(
	infoMap map[roachpb.ReplicaID]rac2.ReplicaStateInfo,
) {
	rn.WithProgress(func(peerID raftpb.PeerID, _ raft.ProgressType, progress tracker.Progress) {
		infoMap[roachpb.ReplicaID(peerID)] = rac2.ReplicaStateInfo{
			Match: progress.Match,
			Next:  progress.Next,
			State: progress.State,
		}
	})
}

// SendPingRaftMuLocked implements rac2.RaftInterface.
func (rn raftNodeForRACv2) SendPingRaftMuLocked(to roachpb.ReplicaID) bool {
	rn.r.MuLock()
	defer rn.r.MuUnlock()
	return rn.RawNode.SendPing(raftpb.PeerID(to))
}

// SendMsgAppRaftMuLocked implements rac2.RaftInterface.
func (rn raftNodeForRACv2) SendMsgAppRaftMuLocked(
	replicaID roachpb.ReplicaID, slice rac2.RaftLogSlice,
) (raftpb.Message, bool) {
	ls := slice.(raft.LogSlice)
	rn.r.MuLock()
	defer rn.r.MuUnlock()
	return rn.RawNode.SendMsgApp(raftpb.PeerID(replicaID), ls)
}

type RaftLogSnapshot raft.LogSnapshot

var _ rac2.RaftLogSnapshot = RaftLogSnapshot{}

// LogSlice implements rac2.RaftLogSnapshot.
func (l RaftLogSnapshot) LogSlice(start, end uint64, maxSize uint64) (rac2.RaftLogSlice, error) {
	return (raft.LogSnapshot(l)).LogSlice(start-1, end-1, maxSize)
}
