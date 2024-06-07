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
	slpb "github.com/cockroachdb/cockroach/pkg/kv/kvserver/storeliveness/storelivenesspb"
	"github.com/cockroachdb/cockroach/pkg/raft/raftstoreliveness"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// replicaRLockedStoreLiveness implements the raft.StoreLiveness interface.
//
// The interface methods (SupportFor and SupportFrom) assume that Replica.mu is
// held in read mode by their callers.
type replicaRLockedStoreLiveness Replica

func (r *replicaRLockedStoreLiveness) getStoreIdentifier(replicaID uint64) (slpb.StoreIdent, bool) {
	r.mu.AssertRHeld()
	desc, ok := r.mu.state.Desc.GetReplicaDescriptorByID(roachpb.ReplicaID(replicaID))
	if !ok {
		return slpb.StoreIdent{}, false
	}
	return slpb.StoreIdent{NodeID: desc.NodeID, StoreID: desc.StoreID}, true
}

// Enabled implements the raft.StoreLiveness interface.
func (r *replicaRLockedStoreLiveness) Enabled() bool {
	// TODO(nvanbenschoten): we can hook this up to a version check and cluster
	// setting.
	return storeliveness.StoreLivenessEnabled.Get(&r.store.cfg.Settings.SV)
}

// SupportFor implements the raftstoreliveness.StoreLiveness interface.
func (r *replicaRLockedStoreLiveness) SupportFor(
	replicaID uint64,
) (raftstoreliveness.StoreLivenessEpoch, bool) {
	storeID, ok := r.getStoreIdentifier(replicaID)
	if !ok {
		return 0, false
	}
	epoch, ok := r.store.storeLiveness.SupportFor(storeID)
	if !ok {
		return 0, false
	}
	return raftstoreliveness.StoreLivenessEpoch(epoch), true
}

// SupportFrom implements the raftstoreliveness.StoreLiveness interface.
func (r *replicaRLockedStoreLiveness) SupportFrom(
	replicaID uint64,
) (raftstoreliveness.StoreLivenessEpoch, raftstoreliveness.StoreLivenessExpiration, bool) {
	storeID, ok := r.getStoreIdentifier(replicaID)
	if !ok {
		return 0, raftstoreliveness.StoreLivenessExpiration{}, false
	}
	epoch, exp, ok := r.store.storeLiveness.SupportFrom(storeID)
	if !ok {
		return 0, raftstoreliveness.StoreLivenessExpiration{}, false
	}
	return raftstoreliveness.StoreLivenessEpoch(epoch), raftstoreliveness.StoreLivenessExpiration(exp), true
}

// InPast implements the raftstoreliveness.StoreLiveness interface.
func (r *replicaRLockedStoreLiveness) InPast(exp raftstoreliveness.StoreLivenessExpiration) bool {
	return hlc.Timestamp(exp).Less(r.store.Clock().Now())
}
