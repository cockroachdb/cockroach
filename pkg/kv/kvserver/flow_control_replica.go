// Copyright 2023 The Cockroach Authors.
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
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontrolpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"go.etcd.io/raft/v3"
	rafttracker "go.etcd.io/raft/v3/tracker"
)

// replicaFlowControl is a concrete implementation of the replicaForFlowControl
// interface.
type replicaFlowControl Replica

var _ replicaForFlowControl = &replicaFlowControl{}

func (rf *replicaFlowControl) assertLocked() {
	rf.mu.AssertHeld()
}

func (rf *replicaFlowControl) annotateCtx(ctx context.Context) context.Context {
	return rf.AnnotateCtx(ctx)
}

func (rf *replicaFlowControl) getTenantID() roachpb.TenantID {
	rf.assertLocked()
	return rf.mu.tenantID
}

func (rf *replicaFlowControl) getReplicaID() roachpb.ReplicaID {
	return rf.replicaID
}

func (rf *replicaFlowControl) getRangeID() roachpb.RangeID {
	return rf.RangeID
}

func (rf *replicaFlowControl) getDescriptor() *roachpb.RangeDescriptor {
	rf.assertLocked()
	r := (*Replica)(rf)
	return r.descRLocked()
}

func (rf *replicaFlowControl) getPausedFollowers() map[roachpb.ReplicaID]struct{} {
	rf.assertLocked()
	return rf.mu.pausedFollowers
}

func (rf *replicaFlowControl) getBehindFollowers() map[roachpb.ReplicaID]struct{} {
	rf.assertLocked()
	behindFollowers := make(map[roachpb.ReplicaID]struct{})
	rf.mu.internalRaftGroup.WithProgress(func(id uint64, _ raft.ProgressType, progress rafttracker.Progress) {
		if progress.State == rafttracker.StateReplicate {
			return
		}

		replID := roachpb.ReplicaID(id)
		behindFollowers[replID] = struct{}{}

		// TODO(irfansharif): Integrating with these other progress fields
		// from raft. For replicas exiting rafttracker.StateProbe, perhaps
		// compare progress.Match against status.Commit to make sure it's
		// sufficiently caught up with respect to its raft log before we
		// start deducting tokens for it (lest we run into I3a from
		// kvflowcontrol/doc.go). To play well with the replica-level
		// proposal quota pool, maybe we also factor its base index?
		// Replicas that crashed and came back could come back in
		// StateReplicate but be behind on their logs. If we're deducting
		// tokens right away for subsequent proposals, it would take some
		// time for it to catch up and then later return those tokens to us.
		// This is I3a again; do it as part of #95563.
		_ = progress.RecentActive
		_ = progress.MsgAppFlowPaused
		_ = progress.Match
	})
	return behindFollowers
}

func (rf *replicaFlowControl) getInactiveFollowers() map[roachpb.ReplicaID]struct{} {
	rf.assertLocked()
	inactiveFollowers := make(map[roachpb.ReplicaID]struct{})
	for _, desc := range rf.getDescriptor().Replicas().Descriptors() {
		if desc.ReplicaID == rf.getReplicaID() {
			continue
		}
		if !rf.mu.lastUpdateTimes.isFollowerActiveSince(desc.ReplicaID, timeutil.Now(), rf.store.cfg.RangeLeaseDuration) {
			inactiveFollowers[desc.ReplicaID] = struct{}{}
		}
	}
	return inactiveFollowers
}

func (rf *replicaFlowControl) getDisconnectedFollowers() map[roachpb.ReplicaID]struct{} {
	rf.assertLocked()
	disconnectedFollowers := make(map[roachpb.ReplicaID]struct{})
	for _, desc := range rf.getDescriptor().Replicas().Descriptors() {
		if desc.ReplicaID == rf.getReplicaID() {
			continue
		}
		if !rf.store.raftTransportForFlowControl.isConnectedTo(desc.StoreID) {
			disconnectedFollowers[desc.ReplicaID] = struct{}{}
		}
	}
	return disconnectedFollowers
}

func (rf *replicaFlowControl) getAppliedLogPosition() kvflowcontrolpb.RaftLogPosition {
	rf.assertLocked()
	status := rf.mu.internalRaftGroup.BasicStatus()
	return kvflowcontrolpb.RaftLogPosition{
		Term:  status.Term,
		Index: status.Applied,
	}
}

func (rf *replicaFlowControl) isScratchRange() bool {
	rf.assertLocked()
	r := (*Replica)(rf)
	return r.isScratchRangeRLocked()
}
