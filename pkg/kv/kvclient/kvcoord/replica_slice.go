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
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/gossip"
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

// NewReplicaSlice creates a ReplicaSlice from the replicas listed in the range
// descriptor and using gossip to lookup node descriptors. Replicas on nodes
// that are not gossiped are omitted from the result.
func NewReplicaSlice(gossip *gossip.Gossip, replicas []roachpb.ReplicaDescriptor) ReplicaSlice {
	if gossip == nil {
		return nil
	}
	rs := make(ReplicaSlice, 0, len(replicas))
	for _, r := range replicas {
		nd, err := gossip.GetNodeDescriptor(r.NodeID)
		if err != nil {
			if log.V(1) {
				log.Infof(context.TODO(), "node %d is not gossiped: %v", r.NodeID, err)
			}
			continue
		}
		rs = append(rs, ReplicaInfo{
			ReplicaDescriptor: r,
			NodeDesc:          nd,
		})
	}
	return rs
}

// ReplicaSlice implements shuffle.Interface.
var _ shuffle.Interface = ReplicaSlice{}

// Len returns the total number of replicas in the slice.
func (rs ReplicaSlice) Len() int { return len(rs) }

// Swap swaps the replicas with indexes i and j.
func (rs ReplicaSlice) Swap(i, j int) { rs[i], rs[j] = rs[j], rs[i] }

// FindReplica returns the index of the replica which matches the specified store
// ID. If no replica matches, -1 is returned.
func (rs ReplicaSlice) FindReplica(storeID roachpb.StoreID) int {
	for i := range rs {
		if rs[i].StoreID == storeID {
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
		// If there is a replica in local node, it sorts first.
		if rs[i].NodeID == nodeDesc.NodeID {
			return true
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
