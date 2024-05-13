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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/storeliveness"
	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// replicaRLockedStoreLiveness implements the raft.StoreLiveness interface.
//
// The interface methods (SupportFor and SupportFrom) assume that Replica.mu is
// held in read mode by their callers.
type replicaRLockedStoreLiveness Replica

func (r *replicaRLockedStoreLiveness) fabric() storeliveness.Fabric {
	return r.store.cfg.StoreLiveness
}

func (r *replicaRLockedStoreLiveness) getStoreID(replicaID uint64) (roachpb.StoreID, bool) {
	r.mu.AssertRHeld()
	desc, ok := r.mu.state.Desc.GetReplicaDescriptorByID(roachpb.ReplicaID(replicaID))
	if ok {
		return 0, false
	}
	return desc.StoreID, true
}

// Enabled implements the raft.StoreLiveness interface.
func (r *replicaRLockedStoreLiveness) Enabled() bool {
	// TODO(nvanbenschoten): we can hook this up to a version check and cluster
	// setting.
	return true
}

// SupportFor implements the raft.StoreLiveness interface.
func (r *replicaRLockedStoreLiveness) SupportFor(replicaID uint64) (raft.StoreLivenessEpoch, bool) {
	storeID, ok := r.getStoreID(replicaID)
	if !ok {
		return 0, false
	}
	epoch, ok := r.fabric().SupportFor(storeID)
	if !ok {
		return 0, false
	}
	return raft.StoreLivenessEpoch(epoch), true
}

// SupportFrom implements the raft.StoreLiveness interface.
func (r *replicaRLockedStoreLiveness) SupportFrom(
	replicaID uint64,
) (raft.StoreLivenessEpoch, raft.StoreLivenessExpiration, bool) {
	storeID, ok := r.getStoreID(replicaID)
	if !ok {
		return 0, raft.StoreLivenessExpiration{}, false
	}
	epoch, exp, ok := r.fabric().SupportFrom(storeID)
	if !ok {
		return 0, raft.StoreLivenessExpiration{}, false
	}
	return raft.StoreLivenessEpoch(epoch), raft.StoreLivenessExpiration(exp), true
}
