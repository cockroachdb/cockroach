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
	"encoding/binary"
	"fmt"
	"hash/fnv"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	slpb "github.com/cockroachdb/cockroach/pkg/kv/kvserver/storeliveness/storelivenesspb"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/raft/raftstoreliveness"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

var raftLeaderFortificationFractionEnabled = settings.RegisterFloatSetting(
	settings.SystemOnly,
	"kv.raft.leader_fortification.fraction_enabled",
	"controls the fraction of ranges for which the raft leader fortification "+
		"protocol is enabled. Leader fortification is needed for a range to use a "+
		"Leader lease. Set to 0.0 to disable leader fortification and, by extension, "+
		"Leader leases. Set to 1.0 to enable leader fortification for all ranges and, "+
		"by extension, use Leader leases for all ranges which do not require "+
		"expiration-based leases. Set to a value between 0.0 and 1.0 to gradually "+
		"roll out Leader leases across the ranges in a cluster.",
	envutil.EnvOrDefaultFloat64("COCKROACH_LEADER_FORTIFICATION_FRACTION_ENABLED", 0.0),
	settings.FloatInRange(0.0, 1.0),
	settings.WithPublic,
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
	// TODO(arul): we can remove this once we start to assign storeLiveness in the
	// Store constructor.
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
	// liveness is enabled.
	storeLivenessEnabled := r.store.ClusterSettings().Version.IsActive(context.TODO(), clusterversion.V24_3_StoreLivenessEnabled)
	if !storeLivenessEnabled {
		return false
	}
	fracEnabled := raftLeaderFortificationFractionEnabled.Get(&r.store.ClusterSettings().SV)
	fortifyEnabled := raftFortificationEnabledForRangeID(fracEnabled, r.RangeID)
	return fortifyEnabled
}

func raftFortificationEnabledForRangeID(fracEnabled float64, rangeID roachpb.RangeID) bool {
	if fracEnabled < 0 || fracEnabled > 1 {
		panic(fmt.Sprintf("unexpected fraction enabled value: %f", fracEnabled))
	}
	const percPrecision = 10_000                      // 0.01% precision
	percEnabled := int64(percPrecision * fracEnabled) // [0, percPrecision]

	// Compute a random, but stable hash of the range ID to determine whether this
	// range should be fortifying its leader lease or not.
	// NOTE: this looks expensive, but it compiles to zero allocations and takes
	// about 4ns.
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], uint64(rangeID))
	h := fnv.New64()
	_, _ = h.Write(b[:])
	hash := h.Sum64()
	perc := int64(hash % percPrecision) // [0, percPrecision)

	return perc < percEnabled
}

// SupportExpired implements the raftstoreliveness.StoreLiveness interface.
func (r *replicaRLockedStoreLiveness) SupportExpired(ts hlc.Timestamp) bool {
	return ts.Less(r.store.Clock().Now())
}
