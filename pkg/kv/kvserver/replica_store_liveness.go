// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/fnv"

	slpb "github.com/cockroachdb/cockroach/pkg/kv/kvserver/storeliveness/storelivenesspb"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/raft/raftstoreliveness"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
)

// RaftLeaderFortificationFractionEnabled controls the fraction of ranges for
// which the raft leader fortification protocol is enabled.
var RaftLeaderFortificationFractionEnabled = settings.RegisterFloatSetting(
	settings.SystemOnly,
	"kv.raft.leader_fortification.fraction_enabled",
	"controls the fraction of ranges for which the raft leader fortification "+
		"protocol is enabled. Leader fortification is needed for a range to use a "+
		"Leader lease. Set to 0.0 to disable leader fortification and, by extension, "+
		"Leader leases. Set to 1.0 to enable leader fortification for all ranges and, "+
		"by extension, use Leader leases for all ranges which do not require "+
		"expiration-based leases. Set to a value between 0.0 and 1.0 to gradually "+
		"roll out Leader leases across the ranges in a cluster.",
	metamorphic.ConstantWithTestChoice("kv.raft.leader_fortification.fraction_enabled",
		0.0, /* defaultValue */
		1.0 /* otherValues */),
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
	desc, ok := r.shMu.state.Desc.GetReplicaDescriptorByID(roachpb.ReplicaID(replicaID))
	if !ok {
		return slpb.StoreIdent{}, false
	}
	return slpb.StoreIdent{NodeID: desc.NodeID, StoreID: desc.StoreID}, true
}

// SupportFor implements the raftstoreliveness.StoreLiveness interface.
func (r *replicaRLockedStoreLiveness) SupportFor(replicaID raftpb.PeerID) (raftpb.Epoch, bool) {
	storeID, ok := r.getStoreIdent(replicaID)
	if !ok {
		ctx := r.AnnotateCtx(context.TODO())
		log.Warningf(ctx, "store not found for replica %d in SupportFor", replicaID)
		return 0, false
	}
	epoch, ok := r.store.storeLiveness.SupportFor(storeID)
	return raftpb.Epoch(epoch), ok
}

// SupportFrom implements the raftstoreliveness.StoreLiveness interface.
func (r *replicaRLockedStoreLiveness) SupportFrom(
	replicaID raftpb.PeerID,
) (raftpb.Epoch, hlc.Timestamp) {
	storeID, ok := r.getStoreIdent(replicaID)
	if !ok {
		ctx := r.AnnotateCtx(context.TODO())
		log.Warningf(ctx, "store not found for replica %d in SupportFrom", replicaID)
		return 0, hlc.Timestamp{}
	}
	epoch, exp := r.store.storeLiveness.SupportFrom(storeID)
	return raftpb.Epoch(epoch), exp
}

// SupportFromEnabled implements the raftstoreliveness.StoreLiveness interface.
func (r *replicaRLockedStoreLiveness) SupportFromEnabled() bool {
	if !r.store.storeLiveness.SupportFromEnabled(context.TODO()) {
		return false
	}
	if (*Replica)(r).shouldUseExpirationLeaseRLocked() {
		// If this range wants to use an expiration based lease, either because it's
		// one of the system ranges (NodeLiveness, Meta) or because the cluster
		// setting to always use expiration based leases is turned on, then do not
		// fortify the leader. There's no benefit to doing so because we aren't
		// going to acquire a leader lease on top of it. On the other hand, by not
		// fortifying, we ensure there's no StoreLiveness dependency for these
		// ranges.
		return false
	}
	fracEnabled := RaftLeaderFortificationFractionEnabled.Get(&r.store.ClusterSettings().SV)
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
	// A support expiration timestamp equal to the current time is considered
	// expired, to be consistent with support withdrawal in Store Liveness.
	return ts.LessEq(r.mu.lastTickTimestamp.ToTimestamp())
}
