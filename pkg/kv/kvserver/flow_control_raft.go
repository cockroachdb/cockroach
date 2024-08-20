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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

type raftNodeForRACv2 struct {
	*raft.RawNode
}

var _ replica_rac2.RaftNode = raftNodeForRACv2{}

func (rn raftNodeForRACv2) LeaderLocked() roachpb.ReplicaID {
	// This needs to be the latest leader (highest term) that this replica has
	// heard from, and not the leader recorded in HardState.
	//
	// TODO(pav-kv): implement.
	return 0
}

func (rn raftNodeForRACv2) StableIndexLocked() replica_rac2.StableIndexState {
	// TODO(pav-kv): implement.
	return replica_rac2.StableIndexState{}
}

func (rn raftNodeForRACv2) NextUnstableIndexLocked() uint64 {
	// TODO(pav-kv): implement.
	return 0
}

func (rn raftNodeForRACv2) MyLeaderTermLocked() uint64 {
	// TODO(pav-kv): implement.
	return 0
}

func (rn raftNodeForRACv2) FollowerState(r roachpb.ReplicaID) replica_rac2.FollowerState {
	// TODO(pav-kv): implement.
	return replica_rac2.FollowerState{}
}
