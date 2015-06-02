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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)
// Author: Peter Mattis (peter@cockroachlabs.com)

package kv

import (
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util/log"
)

// replicaInfo extends the Replica structure with the associated node
// descriptor.
type replicaInfo struct {
	proto.Replica
	NodeDesc proto.NodeDescriptor
}

func (i *replicaInfo) attrs() []string {
	return i.NodeDesc.GetAttrs().Attrs
}

// A replicaSlice is a slice of replicaInfo.
type replicaSlice []replicaInfo

// newReplicaSlice creates a replicaSlice from the replicas listed in the range
// descriptor and using gossip to lookup node descriptors. Replicas on nodes
// that are not gossipped are omitted from the result.
func newReplicaSlice(gossip *gossip.Gossip, desc *proto.RangeDescriptor) replicaSlice {
	replicas := make(replicaSlice, 0, len(desc.Replicas))
	for _, r := range desc.Replicas {
		nd, err := gossip.GetNodeDescriptor(r.NodeID)
		if err != nil {
			if log.V(1) {
				log.Infof("node %d is not gossiped: %v", r.NodeID, err)
			}
			continue
		}
		replicas = append(replicas, replicaInfo{
			Replica:  r,
			NodeDesc: nd,
		})
	}
	return replicas
}

// Swap interchanges the replicas stored at the given indices.
func (rs replicaSlice) Swap(i, j int) {
	rs[i], rs[j] = rs[j], rs[i]
}

// FindReplica returns the index of the replica which matches the specified store
// ID. If no replica matches, -1 is returned.
func (rs replicaSlice) FindReplica(storeID proto.StoreID) int {
	for i := range rs {
		if rs[i].StoreID == storeID {
			return i
		}
	}
	return -1
}

// SortByCommonAttributePrefix rearranges the replicaSlice by comparing the
// attributes to the given reference attributes. The basis for the comparison
// is that of the common prefix of replica attributes (i.e. the number of equal
// attributes, starting at the first), with a longer prefix sorting first. The
// number of attributes successfully matched to at least one replica is
// returned (hence, if the return value equals the length of the replicaSlice,
// at least one replica matched all attributes).
//
// TODO(peter): need to randomize the replica order within each bucket.
func (rs replicaSlice) SortByCommonAttributePrefix(attrs []string) int {
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
func (rs replicaSlice) MoveToFront(i int) {
	l := len(rs) - 1
	if i > l {
		panic("out of bound index")
	}
	front := rs[i]
	// Move the first i-1 elements to the right
	copy(rs[1:i+1], rs[0:i])
	rs[0] = front
}
