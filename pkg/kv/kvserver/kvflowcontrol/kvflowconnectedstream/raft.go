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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
)

// raftInterfaceImpl implements RaftInterface.
type raftInterfaceImpl struct {
	n *raft.RawNode
}

var _ RaftInterface = raftInterfaceImpl{}

// NewRaftInterface return the implementation of RaftInterface wrapped around
// the given raft.RawNode. The node must have Config.EnableLazyAppends == true.
func NewRaftInterface(node *raft.RawNode) RaftInterface {
	return raftInterfaceImpl{n: node}
}

func (r raftInterfaceImpl) FollowerState(replicaID roachpb.ReplicaID) FollowerStateInfo {
	pr := r.n.GetProgress(raftpb.PeerID(replicaID))
	return FollowerStateInfo{
		State: pr.State,
		Match: pr.Match,
		Next:  pr.Next,
		// TODO: populate Admitted
	}
}

func (r raftInterfaceImpl) LastEntryIndex() uint64 {
	return r.n.LastIndex()
}

func (r raftInterfaceImpl) MakeMsgApp(
	replicaID roachpb.ReplicaID, start, end uint64, maxSize int64,
) (raftpb.Message, error) {
	if maxSize <= 0 {
		return raftpb.Message{}, errors.New("maxSize <= 0")
	}
	return r.n.NextMsgApp(raftpb.PeerID(replicaID), start, end, uint64(maxSize))
}

func NewRaftAdmittedInterface(node *raft.RawNode) RaftAdmittedInterface {
	// TODO:
	return nil
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
