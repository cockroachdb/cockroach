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
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/raft/tracker"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

type raftNodeForRACv2 struct {
	*raft.RawNode
}

// NewRaftNode creates a RaftNode implementation from the given RawNode.
func NewRaftNode(rn *raft.RawNode) RaftNode {
	return raftNodeForRACv2{RawNode: rn}
}

func (rn raftNodeForRACv2) EnablePingForAdmittedLaggingLocked() {
	panic("TODO(pav-kv): implement")
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

func (rn raftNodeForRACv2) StepMsgAppRespForAdmittedLocked(m raftpb.Message) error {
	return rn.RawNode.Step(m)
}

func (rn raftNodeForRACv2) FollowerStateRaftMuLocked(
	replicaID roachpb.ReplicaID,
) rac2.FollowerStateInfo {
	// TODO(pav-kv): this is a temporary implementation.
	status := rn.Status()
	if progress, ok := status.Progress[raftpb.PeerID(replicaID)]; ok {
		return rac2.FollowerStateInfo{
			State: progress.State,
			Match: progress.Match,
			Next:  progress.Next,
		}
	}

	return rac2.FollowerStateInfo{State: tracker.StateProbe}
}
