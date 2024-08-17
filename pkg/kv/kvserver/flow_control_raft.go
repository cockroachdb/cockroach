// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/replica_rac2"
	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

type raftNodeForRACv2 struct {
	*raft.RawNode
}

var _ replica_rac2.RaftNode = raftNodeForRACv2{}

func (rn raftNodeForRACv2) EnablePingForAdmittedLaggingLocked() {
	panic("TODO(pav-kv): implement")
}

func (rn raftNodeForRACv2) LeaderLocked() roachpb.ReplicaID {
	// This needs to be the latest leader (highest term) that this replica has
	// heard from, and not the leader recorded in HardState.
	//
	// TODO(pav-kv): implement.
	return 0
}

func (rn raftNodeForRACv2) StableIndexLocked() uint64 {
	// TODO(pav-kv): implement.
	return 0
}

func (rn raftNodeForRACv2) NextUnstableIndexLocked() uint64 {
	// TODO(pav-kv): implement.
	return 0
}

func (rn raftNodeForRACv2) GetAdmittedLocked() [raftpb.NumPriorities]uint64 {
	// TODO(pav-kv): implement.
	return [raftpb.NumPriorities]uint64{}
}

func (rn raftNodeForRACv2) MyLeaderTermLocked() uint64 {
	// TODO(pav-kv): implement.
	return 0
}

func (rn raftNodeForRACv2) SetAdmittedLocked([raftpb.NumPriorities]uint64) raftpb.Message {
	panic("TODO(pav-kv): implement")
}

func (rn raftNodeForRACv2) StepMsgAppRespForAdmittedLocked(m raftpb.Message) error {
	return rn.RawNode.Step(m)
}
