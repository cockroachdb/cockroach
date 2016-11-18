// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)
// Author: Peter Mattis (peter@cockroachlabs.com)

package kv

import (
	"golang.org/x/net/context"

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

func (i ReplicaInfo) attrs() []string {
	return i.NodeDesc.Attrs.Attrs
}

// A ReplicaSlice is a slice of ReplicaInfo.
type ReplicaSlice []ReplicaInfo

// NewReplicaSlice creates a ReplicaSlice from the replicas listed in the range
// descriptor and using gossip to lookup node descriptors. Replicas on nodes
// that are not gossiped are omitted from the result.
func NewReplicaSlice(gossip *gossip.Gossip, desc *roachpb.RangeDescriptor) ReplicaSlice {
	if gossip == nil {
		return nil
	}
	replicas := make(ReplicaSlice, 0, len(desc.Replicas))
	for _, r := range desc.Replicas {
		nd, err := gossip.GetNodeDescriptor(r.NodeID)
		if err != nil {
			if log.V(1) {
				log.Infof(context.TODO(), "node %d is not gossiped: %v", r.NodeID, err)
			}
			continue
		}
		replicas = append(replicas, ReplicaInfo{
			ReplicaDescriptor: r,
			NodeDesc:          nd,
		})
	}
	return replicas
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

// FindReplicaByNodeID returns the index of the replica which matches the specified node
// ID. If no replica matches, -1 is returned.
func (rs ReplicaSlice) FindReplicaByNodeID(nodeID roachpb.NodeID) int {
	for i := range rs {
		if rs[i].NodeID == nodeID {
			return i
		}
	}
	return -1
}

// SortByCommonAttributePrefix rearranges the ReplicaSlice by comparing the
// attributes to the given reference attributes. The basis for the comparison
// is that of the common prefix of replica attributes (i.e. the number of equal
// attributes, starting at the first), with a longer prefix sorting first. The
// number of attributes successfully matched to at least one replica is
// returned (hence, if the return value equals the length of the ReplicaSlice,
// at least one replica matched all attributes).
func (rs ReplicaSlice) SortByCommonAttributePrefix(attrs []string) int {
	if len(rs) < 2 {
		return 0
	}
	topIndex := len(rs) - 1
	for bucket := 0; bucket < len(attrs); bucket++ {
		firstNotOrdered := 0
		for i := 0; i <= topIndex; i++ {
			if bucket < len(rs[i].attrs()) && rs[i].attrs()[bucket] == attrs[bucket] {
				// Move replica which matches this attribute to an earlier
				// place in the array, just behind the last matching replica.
				// This packs all matching replicas together.
				rs.Swap(firstNotOrdered, i)
				firstNotOrdered++
			}
		}
		if topIndex < len(rs)-1 {
			shuffle.Shuffle(rs[firstNotOrdered : topIndex+1])
		}
		if firstNotOrdered == 0 {
			return bucket
		}
		topIndex = firstNotOrdered - 1
	}
	return len(attrs)
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

// OptimizeReplicaOrder sorts the replicas in the order in which they're to be
// used for sending RPCs (meaning in the order in which they'll be probed for
// the lease).  "Closer" (matching in more attributes) replicas are ordered
// first. If the current node is a replica, then it'll be the first one.
//
// nodeDesc is the descriptor of the current node. It can be nil, in which case
// information about the current descriptor is not used in optimizing the order.
//
// Note that this method is not concerned with any information the node might
// have about who the lease holder might be. If there is such info (e.g. in a
// LeaseHolderCache), the caller will probably want to further tweak the head of
// the ReplicaSlice.
func (rs ReplicaSlice) OptimizeReplicaOrder(nodeDesc *roachpb.NodeDescriptor) {
	// If we don't know which node we're on, send the RPCs randomly.
	if nodeDesc == nil {
		shuffle.Shuffle(rs)
		return
	}
	// Sort replicas by attribute affinity, which we treat as a stand-in for
	// proximity (for now).
	rs.SortByCommonAttributePrefix(nodeDesc.Attrs.Attrs)

	// If there is a replica in local node, move it to the front.
	if i := rs.FindReplicaByNodeID(nodeDesc.NodeID); i > 0 {
		rs.MoveToFront(i)
	}
}
