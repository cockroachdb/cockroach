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

// replicaForFlowControl abstracts the interface of an individual Replica, as
// needed by replicaFlowControlIntegration.
type replicaForFlowControl interface {
	assertLocked()
	annotateCtx(context.Context) context.Context
	getTenantID() roachpb.TenantID
	getReplicaID() roachpb.ReplicaID
	getRangeID() roachpb.RangeID
	getDescriptor() *roachpb.RangeDescriptor
	pausedFollowers() map[roachpb.ReplicaID]struct{}
	isFollowerActive(context.Context, roachpb.ReplicaID) bool
	appliedLogPosition() kvflowcontrolpb.RaftLogPosition
	withReplicaProgress(f func(roachpb.ReplicaID, rafttracker.Progress))
}

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

func (rf *replicaFlowControl) pausedFollowers() map[roachpb.ReplicaID]struct{} {
	rf.assertLocked()
	return rf.mu.pausedFollowers
}

func (rf *replicaFlowControl) isFollowerActive(ctx context.Context, replID roachpb.ReplicaID) bool {
	rf.mu.AssertHeld()
	return rf.mu.lastUpdateTimes.isFollowerActiveSince(
		ctx,
		replID,
		timeutil.Now(),
		rf.store.cfg.RangeLeaseDuration,
	)
}

func (rf *replicaFlowControl) appliedLogPosition() kvflowcontrolpb.RaftLogPosition {
	rf.mu.AssertHeld()
	status := rf.mu.internalRaftGroup.BasicStatus()
	return kvflowcontrolpb.RaftLogPosition{
		Term:  status.Term,
		Index: status.Applied,
	}
}

func (rf *replicaFlowControl) withReplicaProgress(f func(roachpb.ReplicaID, rafttracker.Progress)) {
	rf.mu.AssertHeld()
	rf.mu.internalRaftGroup.WithProgress(func(id uint64, _ raft.ProgressType, progress rafttracker.Progress) {
		f(roachpb.ReplicaID(id), progress)
	})
}
