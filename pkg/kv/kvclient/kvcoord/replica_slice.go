// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvcoord

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/shuffle"
)

// ReplicaInfo extends the Replica structure with the associated node
// descriptor.
type ReplicaInfo struct {
	roachpb.ReplicaDescriptor
	NodeDesc *roachpb.NodeDescriptor
}

func (i ReplicaInfo) locality() []roachpb.Tier {
	return i.NodeDesc.Locality.Tiers
}

func (i ReplicaInfo) addr() string {
	return i.NodeDesc.Address.String()
}

// A ReplicaSlice is a slice of ReplicaInfo.
type ReplicaSlice []ReplicaInfo

// ReplicaSliceFilter controls which kinds of replicas are to be included in
// the slice for routing BatchRequests to.
type ReplicaSliceFilter int

const (
	// OnlyPotentialLeaseholders prescribes that the ReplicaSlice should include
	// only replicas that are allowed to be leaseholders (i.e. replicas of type
	// VOTER_FULL).
	OnlyPotentialLeaseholders ReplicaSliceFilter = iota
	// AllExtantReplicas prescribes that the ReplicaSlice should include all
	// replicas that are not LEARNERs, VOTER_OUTGOING, or
	// VOTER_DEMOTING_{LEARNER/NON_VOTER}.
	AllExtantReplicas
)

// NewReplicaSlice creates a ReplicaSlice from the replicas listed in the range
// descriptor and using gossip to lookup node descriptors. Replicas on nodes
// that are not gossiped are omitted from the result.
//
// Generally, learners are not returned. However, if a non-nil leaseholder is
// passed in, it will be included in the result even if the descriptor has it as
// a learner (we assert that the leaseholder is part of the descriptor). The
// idea is that the descriptor might be stale and list the leaseholder as a
// learner erroneously, and lease info is a strong signal in that direction.
// Note that the returned ReplicaSlice might still not include the leaseholder
// if info for the respective node is missing from the NodeDescStore.
//
// If there's no info in gossip for any of the nodes in the descriptor, a
// sendError is returned.
func NewReplicaSlice(
	ctx context.Context,
	nodeDescs NodeDescStore,
	desc *roachpb.RangeDescriptor,
	leaseholder *roachpb.ReplicaDescriptor,
	filter ReplicaSliceFilter,
) (ReplicaSlice, error) {
	if leaseholder != nil {
		if _, ok := desc.GetReplicaDescriptorByID(leaseholder.ReplicaID); !ok {
			log.Fatalf(ctx, "leaseholder not in descriptor; leaseholder: %s, desc: %s", leaseholder, desc)
		}
	}
	canReceiveLease := func(rDesc roachpb.ReplicaDescriptor) bool {
		if err := roachpb.CheckCanReceiveLease(rDesc, desc); err != nil {
			return false
		}
		return true
	}

	// Learner replicas won't serve reads/writes, so we'll send only to the voters
	// and non-voting replicas. This is just an optimization to save a network
	// hop, everything would still work if we had `All` here.
	var replicas []roachpb.ReplicaDescriptor
	switch filter {
	case OnlyPotentialLeaseholders:
		replicas = desc.Replicas().Filter(canReceiveLease).Descriptors()
	case AllExtantReplicas:
		replicas = desc.Replicas().VoterAndNonVoterDescriptors()
	default:
		log.Fatalf(ctx, "unknown ReplicaSliceFilter %v", filter)
	}
	// If we know a leaseholder, though, let's make sure we include it.
	if leaseholder != nil && len(replicas) < len(desc.Replicas().Descriptors()) {
		found := false
		for _, v := range replicas {
			if v == *leaseholder {
				found = true
				break
			}
		}
		if !found {
			log.Eventf(ctx, "the descriptor has the leaseholder as a learner; including it anyway")
			replicas = append(replicas, *leaseholder)
		}
	}
	rs := make(ReplicaSlice, 0, len(replicas))
	for _, r := range replicas {
		nd, err := nodeDescs.GetNodeDescriptor(r.NodeID)
		if err != nil {
			if log.V(1) {
				log.Infof(ctx, "node %d is not gossiped: %v", r.NodeID, err)
			}
			continue
		}
		rs = append(rs, ReplicaInfo{
			ReplicaDescriptor: r,
			NodeDesc:          nd,
		})
	}
	if len(rs) == 0 {
		return nil, newSendError(
			fmt.Sprintf("no replica node addresses available via gossip for r%d", desc.RangeID))
	}
	return rs, nil
}

// ReplicaSlice implements shuffle.Interface.
var _ shuffle.Interface = ReplicaSlice{}

// Len returns the total number of replicas in the slice.
func (rs ReplicaSlice) Len() int { return len(rs) }

// Swap swaps the replicas with indexes i and j.
func (rs ReplicaSlice) Swap(i, j int) { rs[i], rs[j] = rs[j], rs[i] }

// Find returns the index of the specified ReplicaID, or -1 if missing.
func (rs ReplicaSlice) Find(id roachpb.ReplicaID) int {
	for i := range rs {
		if rs[i].ReplicaID == id {
			return i
		}
	}
	return -1
}

// MoveToFront moves the replica at the given index to the front
// of the slice, keeping the order of the remaining elements stable.
// The function will panic when invoked with an invalid index.
func (rs ReplicaSlice) MoveToFront(i int) {
	if i >= len(rs) {
		panic("out of bound index")
	}
	front := rs[i]
	// Move the first i elements one index to the right
	copy(rs[1:], rs[:i])
	rs[0] = front
}

// localityMatch returns the number of consecutive locality tiers
// which match between a and b.
func localityMatch(a, b []roachpb.Tier) int {
	if len(a) == 0 {
		return 0
	}
	for i := range a {
		if i >= len(b) || a[i] != b[i] {
			return i
		}
	}
	return len(a)
}

// A LatencyFunc returns the latency from this node to a remote
// address and a bool indicating whether the latency is valid.
type LatencyFunc func(string) (time.Duration, bool)

// OptimizeReplicaOrder sorts the replicas in the order in which
// they're to be used for sending RPCs (meaning in the order in which
// they'll be probed for the lease). Lower latency and "closer"
// (matching in more attributes) replicas are ordered first. If the
// current node is a replica, then it'll be the first one.
//
// nodeDesc is the descriptor of the current node. It can be nil, in
// which case information about the current descriptor is not used in
// optimizing the order.
//
// Note that this method is not concerned with any information the
// node might have about who the lease holder might be. If the
// leaseholder is known by the caller, the caller will move it to the
// front if appropriate.
func (rs ReplicaSlice) OptimizeReplicaOrder(
	nodeDesc *roachpb.NodeDescriptor, latencyFn LatencyFunc,
) {
	// If we don't know which node we're on, send the RPCs randomly.
	if nodeDesc == nil {
		shuffle.Shuffle(rs)
		return
	}

	// Sort replicas by latency and then attribute affinity.
	sort.Slice(rs, func(i, j int) bool {
		// Replicas on the same node have the same latency.
		if rs[i].NodeID == rs[j].NodeID {
			return false // i == j
		}
		// Replicas on the local node sort first.
		if rs[i].NodeID == nodeDesc.NodeID {
			return true // i < j
		}
		if rs[j].NodeID == nodeDesc.NodeID {
			return false // j < i
		}

		if latencyFn != nil {
			latencyI, okI := latencyFn(rs[i].addr())
			latencyJ, okJ := latencyFn(rs[j].addr())
			if okI && okJ {
				return latencyI < latencyJ
			}
		}
		attrMatchI := localityMatch(nodeDesc.Locality.Tiers, rs[i].locality())
		attrMatchJ := localityMatch(nodeDesc.Locality.Tiers, rs[j].locality())
		// Longer locality matches sort first (the assumption is that
		// they'll have better latencies).
		return attrMatchI > attrMatchJ
	})
}

// Descriptors returns the ReplicaDescriptors inside the ReplicaSlice.
func (rs ReplicaSlice) Descriptors() []roachpb.ReplicaDescriptor {
	reps := make([]roachpb.ReplicaDescriptor, len(rs))
	for i := range rs {
		reps[i] = rs[i].ReplicaDescriptor
	}
	return reps
}
