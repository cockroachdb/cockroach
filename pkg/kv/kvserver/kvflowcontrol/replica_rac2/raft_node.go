// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package replica_rac2

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/rac2"
	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

type raftNodeForRACv2 struct {
	*raft.RawNode
}

// NewRaftNode creates a RaftNode implementation from the given RawNode.
func NewRaftNode(rn *raft.RawNode) RaftNode {
	return raftNodeForRACv2{RawNode: rn}
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
	for peerID, progress := range rn.Status().Progress {
		infoMap[roachpb.ReplicaID(peerID)] = rac2.ReplicaStateInfo{
			Match: progress.Match,
			Next:  progress.Next,
			State: progress.State,
		}
	}
}
