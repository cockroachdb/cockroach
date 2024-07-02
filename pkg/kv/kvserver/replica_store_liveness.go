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
	slpb "github.com/cockroachdb/cockroach/pkg/kv/kvserver/storeliveness/storelivenesspb"
	"github.com/cockroachdb/cockroach/pkg/raft/raftstoreliveness"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// replicaRLockedStoreLiveness implements the raftstoreliveness.StoreLiveness
// interface. The interface methods assume that Replica.mu is held in read mode
// by their callers.
type replicaRLockedStoreLiveness Replica

var _ raftstoreliveness.StoreLiveness = (*replicaRLockedStoreLiveness)(nil)

func (r *replicaRLockedStoreLiveness) getStoreIdent(replicaID uint64) (slpb.StoreIdent, bool) {
	r.mu.AssertRHeld()
	desc, ok := r.mu.state.Desc.GetReplicaDescriptorByID(roachpb.ReplicaID(replicaID))
	if !ok {
		return slpb.StoreIdent{}, false
	}
	return slpb.StoreIdent{NodeID: desc.NodeID, StoreID: desc.StoreID}, true
}

// SupportFor implements the raftstoreliveness.StoreLiveness interface.
func (r *replicaRLockedStoreLiveness) SupportFor(
	replicaID uint64,
) (raftstoreliveness.StoreLivenessEpoch, bool) {
	storeID, ok := r.getStoreIdent(replicaID)
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
) (raftstoreliveness.StoreLivenessEpoch, hlc.Timestamp, bool) {
	storeID, ok := r.getStoreIdent(replicaID)
	if !ok {
		return 0, hlc.Timestamp{}, false
	}
	epoch, exp, ok := r.store.storeLiveness.SupportFrom(storeID)
	if !ok {
		return 0, hlc.Timestamp{}, false
	}
	return raftstoreliveness.StoreLivenessEpoch(epoch), exp, true
}

// SupportFromEnabled implements the raftstoreliveness.StoreLiveness interface.
func (r *replicaRLockedStoreLiveness) SupportFromEnabled() bool {
	// TODO(nvanbenschoten): hook this up to a version check and cluster setting.
	return false
}

// SupportInPast implements the raftstoreliveness.StoreLiveness interface.
func (r *replicaRLockedStoreLiveness) SupportInPast(ts hlc.Timestamp) bool {
	return ts.Less(r.store.Clock().Now())
}
