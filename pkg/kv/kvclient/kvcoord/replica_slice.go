// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvcoord

import (
	"context"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/shuffle"
	"github.com/cockroachdb/errors"
)

// ReplicaInfo extends the Replica structure with the associated node
// Locality information.
type ReplicaInfo struct {
	roachpb.ReplicaDescriptor
	Tiers []roachpb.Tier
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
	// AllReplicas prescribes that the ReplicaSlice should include all replicas.
	AllReplicas
)

// NewReplicaSlice creates a ReplicaSlice from the replicas listed in the range
// descriptor and using gossip to lookup node descriptors. Replicas on nodes
// that are not gossiped are omitted from the result.
//
// Generally, learners are not returned, unless AllReplicas was passed in as a
// filter, which in that case, everything will be returned. However, if a
// non-nil leaseholder is passed in, it will be included in the result even if
// the descriptor has it as a learner (we assert that the leaseholder is part
// of the descriptor). The idea is that the descriptor might be stale and list
// the leaseholder as a learner erroneously, and lease info is a strong signal
// in that direction. Note that the returned ReplicaSlice might still not
// include the leaseholder if info for the respective node is missing from the
// NodeDescStore.
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
		// NOTE: This logic is client-side and it’s trying to determine the set of
		// all replicas that could potentially be leaseholders. We pass
		// wasLastLeaseholder = true because we don't know who the
		// leaseholder is, so it's possible that a VOTER_DEMOTING still holds on to
		// the lease.
		if err := roachpb.CheckCanReceiveLease(
			rDesc, desc.Replicas(), true, /* wasLastLeaseholder */
		); err != nil {
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
	case AllReplicas:
		replicas = desc.Replicas().Descriptors()
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
			Tiers:             nd.Locality.Tiers,
		})
	}
	if len(rs) == 0 {
		return nil, newSendError(
			errors.Errorf("no replica node information available via gossip for r%d", desc.RangeID))
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
// node and a bool indicating whether the latency is valid.
type LatencyFunc func(roachpb.NodeID) (time.Duration, bool)

// HealthFunc returns true if the node should be considered alive. Unhealthy
// nodes are sorted behind healthy nodes.
type HealthFunc func(roachpb.NodeID) bool

// OptimizeReplicaOrder sorts the replicas in the order in which
// they're to be used for sending RPCs (meaning in the order in which
// they'll be probed for the lease). Lower latency and "closer"
// (matching in more attributes) replicas are ordered first. If the
// current node has a replica (and the current node's ID is supplied)
// then it'll be the first one.
//
// nodeID is the ID of the current node the current node. It can be 0, in which
// case information about the current node is not used in optimizing the order.
// Similarly, latencyFn can be nil, in which case it will not be used.
//
// Note that this method is not concerned with any information the
// node might have about who the lease holder might be. If the
// leaseholder is known by the caller, the caller will move it to the
// front if appropriate.
func (rs ReplicaSlice) OptimizeReplicaOrder(
	st *cluster.Settings,
	nodeID roachpb.NodeID,
	healthFn HealthFunc,
	latencyFn LatencyFunc,
	locality roachpb.Locality,
) {
	// If we don't know which node we're on or its locality, and we don't have
	// latency information to other nodes, send the RPCs randomly.
	if nodeID == 0 && latencyFn == nil && len(locality.Tiers) == 0 {
		shuffle.Shuffle(rs)
		return
	}

	// Sort replicas by latency and then attribute affinity.
	sort.Slice(rs, func(i, j int) bool {
		// Replicas on the same node have the same score.
		if rs[i].NodeID == rs[j].NodeID {
			return false // i == j
		}

		if !FollowerReadsUnhealthy.Get(&st.SV) {
			// Sort healthy nodes before unhealthy nodes.
			// NB: This is checked before checking if we are on the local node because
			// if we are unhealthy, then we prefer to choose a different follower.
			healthI := healthFn(rs[i].NodeID)
			healthJ := healthFn(rs[j].NodeID)
			if healthI != healthJ {
				return healthI
			}
		}

		// Replicas on the local node sort first.
		if rs[i].NodeID == nodeID {
			return true // i < j
		}
		if rs[j].NodeID == nodeID {
			return false // j < i
		}

		if latencyFn != nil {
			latencyI, okI := latencyFn(rs[i].NodeID)
			latencyJ, okJ := latencyFn(rs[j].NodeID)
			if okI && okJ {
				return latencyI < latencyJ
			}
		}
		attrMatchI := localityMatch(locality.Tiers, rs[i].Tiers)
		attrMatchJ := localityMatch(locality.Tiers, rs[j].Tiers)
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

// LocalityValue returns the value of the locality tier associated with the
// given key.
func (ri *ReplicaInfo) LocalityValue(key string) string {
	for _, tier := range ri.Tiers {
		if tier.Key == key {
			return tier.Value
		}
	}
	return ""
}
