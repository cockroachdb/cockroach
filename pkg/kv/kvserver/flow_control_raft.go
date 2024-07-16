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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowconnectedstream"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontrolpb"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/raft/tracker"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
)

var _ kvflowconnectedstream.RaftNode = raftInterfaceImpl{}

// raftInterfaceImpl implements RaftInterface.
type raftInterfaceImpl struct {
	*Replica
}

// NewRaftNode return the implementation of RaftNode wrapped around the given
// raft.RawNode. The node must have Config.EnableLazyAppends == true.
func NewRaftNode(replica *Replica) kvflowconnectedstream.RaftNode {
	return raftInterfaceImpl{Replica: replica}
}

// FollowerState returns the current state of a follower.
//
// Requires Replica.raftMu to be held, Replica.mu is not held.
func (r raftInterfaceImpl) FollowerState(
	replicaID roachpb.ReplicaID,
) kvflowconnectedstream.FollowerStateInfo {
	r.raftMu.AssertHeld()
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.FollowerStateRLocked(replicaID)
}

// FollowerState returns the current state of a follower.
//
// Requires Replica.raftMu and Replica.mu.RLock to be held.
func (r raftInterfaceImpl) FollowerStateRLocked(
	replicaID roachpb.ReplicaID,
) kvflowconnectedstream.FollowerStateInfo {
	r.raftMu.AssertHeld()
	r.mu.AssertRHeld()

	// TODO: FollowerState is being called when the follower tracking may be
	// uninitialized for the follower replicaID on the leader. This is a bug.
	// Check whether the follower exists in the progress map, if not return
	// probe.
	status := r.mu.internalRaftGroup.Status()
	if _, ok := status.Progress[raftpb.PeerID(replicaID)]; !ok {
		return kvflowconnectedstream.FollowerStateInfo{
			State: tracker.StateProbe,
		}
	}

	pr := r.mu.internalRaftGroup.GetProgress(raftpb.PeerID(replicaID))
	return kvflowconnectedstream.FollowerStateInfo{
		State:    pr.State,
		Match:    pr.Match,
		Next:     pr.Next,
		Admitted: pr.Admitted,
	}
}

// LastEntryIndex is the highest index assigned in the log. Only for
// debugging.
//
// Requires Replica.raftMu to be held, Replica.mu is not held.
func (r raftInterfaceImpl) LastEntryIndex() uint64 {
	r.raftMu.AssertHeld()
	r.mu.RLock()

	return r.LastEntryIndexRLocked()
}

// Requires Replica.raftMu and Replica.mu.RLock to be held.
func (r raftInterfaceImpl) LastEntryIndexRLocked() uint64 {
	r.raftMu.AssertHeld()
	r.mu.AssertRHeld()

	return r.mu.internalRaftGroup.LastIndex()
}

// MakeMsgApp is used to construct a MsgApp for entries in [start, end).
//
// Requires Replica.raftMu to be held, Replica.mu is not held.
func (r raftInterfaceImpl) MakeMsgApp(
	replicaID roachpb.ReplicaID, start, end uint64, maxSize int64,
) (raftpb.Message, error) {
	r.raftMu.AssertHeld()
	r.mu.Lock()
	defer r.mu.Unlock()

	if maxSize <= 0 {
		return raftpb.Message{}, errors.New("maxSize <= 0")
	}
	return r.mu.internalRaftGroup.NextMsgApp(raftpb.PeerID(replicaID), start, end, uint64(maxSize))
}

// StableIndex is the index up to which the raft log is stable. The
// Admitted values must be <= this index. It advances when Raft sees
// MsgStorageAppendResp.
//
// Requires Replica.mu.RLock and Replica.raftMu to be held.
func (r raftInterfaceImpl) StableIndexRLocked() uint64 {
	r.raftMu.AssertHeld()
	r.mu.AssertRHeld()

	return r.mu.internalRaftGroup.StableIndex()
}

// NextUnstableIndexRaftMuLocked returns the next unstable index.
//
// Requires Replica.raftMu to be held, Replica.mu is not held.
func (r raftInterfaceImpl) NextUnstableIndex() uint64 {
	r.raftMu.AssertHeld()
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.NextUnstableIndexRLocked()
}

// NextUnstableIndexRLocked returns the next unstable index.
//
// Requires Replica.raftMu and Replica.mu.RLock to be held.
func (r raftInterfaceImpl) NextUnstableIndexRLocked() uint64 {
	r.raftMu.AssertHeld()
	r.mu.AssertRHeld()

	return r.mu.internalRaftGroup.NextUnstableIndex()
}

// GetAdmittedRLocked returns the admitted values known to Raft. Except for
// snapshot application, this value will only advance via the caller calling
// SetAdmitted. When a snapshot is applied, the snapshot index becomes the
// value of admitted for all priorities.
//
// Requires Replica.raftMu and Replica.mu.Rlock to be held.
func (r raftInterfaceImpl) GetAdmittedRLocked() [kvflowcontrolpb.NumRaftPriorities]uint64 {
	r.raftMu.AssertHeld()
	r.mu.AssertRHeld()

	return r.mu.internalRaftGroup.GetAdmitted()
}

// SetAdmittedLocked sets the new value of admitted. Returns a MsgAppResp that
// contains these latest admitted values.
//
// Requires Replica.raftMu and Replica.mu to be held.
func (r raftInterfaceImpl) SetAdmittedLocked(
	admitted [kvflowcontrolpb.NumRaftPriorities]uint64,
) raftpb.Message {
	r.raftMu.AssertHeld()
	r.mu.AssertHeld()

	return r.mu.internalRaftGroup.SetAdmitted(admitted)
}
