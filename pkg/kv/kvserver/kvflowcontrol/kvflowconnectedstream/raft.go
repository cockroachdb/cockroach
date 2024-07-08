// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvflowconnectedstream

import (
	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/raft/tracker"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
)

type RaftNode interface {
	RaftInterface
	RaftAdmittedInterface
}

var _ RaftNode = raftInterfaceImpl{}

// raftInterfaceImpl implements RaftInterface.
type raftInterfaceImpl struct {
	*raft.RawNode
}

// NewRaftNode return the implementation of RaftNode wrapped around the given
// raft.RawNode. The node must have Config.EnableLazyAppends == true.
func NewRaftNode(node *raft.RawNode) RaftNode {
	return raftInterfaceImpl{RawNode: node}
}

func (r raftInterfaceImpl) FollowerState(replicaID roachpb.ReplicaID) FollowerStateInfo {
	// TODO: FollowerState is being called when the follower tracking may be
	// uninitialized for the follower replicaID on the leader. This is a bug.
	// Check whether the follower exists in the progress map, if not return
	// probe.
	status := r.RawNode.Status()
	if _, ok := status.Progress[raftpb.PeerID(replicaID)]; !ok {
		return FollowerStateInfo{
			State: tracker.StateProbe,
		}
	}

	pr := r.GetProgress(raftpb.PeerID(replicaID))
	return FollowerStateInfo{
		State:    pr.State,
		Match:    pr.Match,
		Next:     pr.Next,
		Admitted: pr.Admitted,
	}
}

func (r raftInterfaceImpl) LastEntryIndex() uint64 {
	return r.LastIndex()
}

func (r raftInterfaceImpl) MakeMsgApp(
	replicaID roachpb.ReplicaID, start, end uint64, maxSize int64,
) (raftpb.Message, error) {
	if maxSize <= 0 {
		return raftpb.Message{}, errors.New("maxSize <= 0")
	}
	return r.NextMsgApp(raftpb.PeerID(replicaID), start, end, uint64(maxSize))
}

type raftEventImpl raft.Ready

func MakeRaftEvent(ready *raft.Ready) RaftEvent {
	return (*raftEventImpl)(ready)
}

func (e *raftEventImpl) Ready() Ready {
	if e != nil {
		return e
	}
	return nil
}

func (e *raftEventImpl) GetEntries() []raftpb.Entry {
	return (*raft.Ready)(e).Entries
}
