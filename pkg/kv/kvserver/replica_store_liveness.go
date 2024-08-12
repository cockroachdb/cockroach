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
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	slpb "github.com/cockroachdb/cockroach/pkg/kv/kvserver/storeliveness/storelivenesspb"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/raft/raftstoreliveness"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// replicaRLockedStoreLiveness implements the raftstoreliveness.StoreLiveness
// interface. The interface methods assume that Replica.mu is held in read mode
// by their callers.
type replicaRLockedStoreLiveness Replica

var _ raftstoreliveness.StoreLiveness = (*replicaRLockedStoreLiveness)(nil)

func (r *replicaRLockedStoreLiveness) getStoreIdent(
	replicaID raftpb.PeerID,
) (slpb.StoreIdent, bool) {
	r.mu.AssertRHeld()
	desc, ok := r.mu.state.Desc.GetReplicaDescriptorByID(roachpb.ReplicaID(replicaID))
	if !ok {
		return slpb.StoreIdent{}, false
	}
	return slpb.StoreIdent{NodeID: desc.NodeID, StoreID: desc.StoreID}, true
}

// SupportFor implements the raftstoreliveness.StoreLiveness interface.
func (r *replicaRLockedStoreLiveness) SupportFor(replicaID raftpb.PeerID) (raftpb.Epoch, bool) {
	storeID, ok := r.getStoreIdent(replicaID)
	if !ok {
		return 0, false
	}
	if r.store.storeLiveness == nil {
		return 0, false
	}
	epoch, ok := r.store.storeLiveness.SupportFor(storeID)
	if !ok {
		return 0, false
	}
	return raftpb.Epoch(epoch), true
}

// SupportFrom implements the raftstoreliveness.StoreLiveness interface.
func (r *replicaRLockedStoreLiveness) SupportFrom(
	replicaID raftpb.PeerID,
) (raftpb.Epoch, hlc.Timestamp, bool) {
	storeID, ok := r.getStoreIdent(replicaID)
	if !ok {
		return 0, hlc.Timestamp{}, false
	}
	epoch, exp, ok := r.store.storeLiveness.SupportFrom(storeID)
	if !ok {
		return 0, hlc.Timestamp{}, false
	}
	return raftpb.Epoch(epoch), exp, true
}

// SupportFromEnabled implements the raftstoreliveness.StoreLiveness interface.
func (r *replicaRLockedStoreLiveness) SupportFromEnabled() bool {
	// TODO(mira): this version check is incorrect. For one, it doesn't belong
	// here. Instead, the version should be checked when deciding to enable
	// StoreLiveness or not. Then, the check here should only check whether store
	// liveness is enabled. The other incorrect bit about this version check is
	// that it re-uses clusterversion.V24_2_LeaseMinTimestamp; we should instead
	// introduce a new version (V24_3_StoreLivenessEnabled); this can only happen
	// once https://github.com/cockroachdb/cockroach/pull/128616 lands.
	//
	// TODO(nvanbenschoten): hook this up to a cluster setting to gradually roll
	// out raft fortification.
	return r.store.ClusterSettings().Version.IsActive(context.TODO(), clusterversion.V24_2_LeaseMinTimestamp)
}

// SupportExpired implements the raftstoreliveness.StoreLiveness interface.
func (r *replicaRLockedStoreLiveness) SupportExpired(ts hlc.Timestamp) bool {
	return ts.Less(r.store.Clock().Now())
}
