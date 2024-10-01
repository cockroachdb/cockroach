// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvcoord

import (
	"cmp"
	"context"
	"slices"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvclient"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/shuffle"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// ReplicaInfo extends the Replica structure with the associated node
// Locality information.
// NB: tierMatchLength, latency and healthy are only computed and used within
// OptimizeReplicaOrder. They measure these properties as the distance from the
// current node.
// TODO(baptist): Convert ReplicaInfo and ReplicaSlice package scope.
type ReplicaInfo struct {
	roachpb.ReplicaDescriptor
	Locality        roachpb.Locality
	tierMatchLength int
	latency         time.Duration
	healthy         bool
}

// A ReplicaSlice is a slice of ReplicaInfo.
type ReplicaSlice []ReplicaInfo

func (rs ReplicaSlice) String() string {
	return redact.StringWithoutMarkers(rs)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (rs ReplicaSlice) SafeFormat(w redact.SafePrinter, _ rune) {
	var buf redact.StringBuilder
	buf.Print("[")
	for i, r := range rs {
		if i > 0 {
			buf.Print(",")
		}
		buf.Printf("%v(health=%v match=%d latency=%v)",
			r, r.healthy, r.tierMatchLength, humanizeutil.Duration(r.latency))
	}
	buf.Print("]")
	w.Print(buf)
}

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
	nodeDescs kvclient.NodeDescStore,
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
			Locality:          nd.Locality,
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
	ctx context.Context,
	st *cluster.Settings,
	nodeID roachpb.NodeID,
	healthFn HealthFunc,
	latencyFn LatencyFunc,
	locality roachpb.Locality,
) {
	// If we don't know which node we're on or its locality, and we don't have
	// latency information to other nodes, send the RPCs randomly.
	if nodeID == 0 && latencyFn == nil && len(locality.Tiers) == 0 {
		log.VEvent(ctx, 2, "randomly shuffling replicas to route to")
		shuffle.Shuffle(rs)
		return
	}
	followerReadsUnhealthy := FollowerReadsUnhealthy.Get(&st.SV)
	sortByLocalityFirst := sortByLocalityFirst.Get(&st.SV)
	// Populate the health, tier match length and locality before the sort loop.
	for i := range rs {
		rs[i].tierMatchLength = locality.SharedPrefix(rs[i].Locality)

		// Latency to the local node is always the "best" use the special -1
		// value to sort before any node other than itself.
		// NB: -1 => Local node, 0 => unknown, >0 => remote node.
		if rs[i].NodeID == nodeID {
			rs[i].latency = -1
		} else if latencyFn != nil {
			if l, ok := latencyFn(rs[i].NodeID); ok {
				rs[i].latency = l
			}
		}

		if !followerReadsUnhealthy {
			rs[i].healthy = healthFn(rs[i].NodeID)
		}
	}

	// Sort replicas by latency and then attribute affinity.
	slices.SortFunc(rs, func(a, b ReplicaInfo) int {
		// Always sort healthy nodes before unhealthy nodes.
		if a.healthy != b.healthy {
			if a.healthy {
				return -1
			}
			return +1
		}

		// If the region is different choose the closer one.
		// If the setting is true(default) consider locality before latency.
		if sortByLocalityFirst {
			// If the region is different choose the closer one.
			if a.tierMatchLength != b.tierMatchLength {
				return -cmp.Compare(a.tierMatchLength, b.tierMatchLength)
			}
		}

		// Use latency if they are different. The local node has a latency of -1
		// so will sort before any other node.
		if a.latency != b.latency {
			return cmp.Compare(a.latency, b.latency)
		}

		// If the setting is false, sort locality after latency.
		if !sortByLocalityFirst {
			// If the region is different choose the closer one.
			if a.tierMatchLength != b.tierMatchLength {
				return -cmp.Compare(a.tierMatchLength, b.tierMatchLength)
			}
		}

		// If everything else is equal sort by node id.
		return cmp.Compare(a.NodeID, b.NodeID)
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
