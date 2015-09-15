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
// Author: Bram Gruneir (bram+code@cockroachlabs.com)

package main

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/cockroachdb/cockroach/proto"
)

const (
	bytesPerRange    = 64 * (int64(1) << 20) // 64 MiB
	capacityPerStore = int64(1) << 40        // 1 TiB - 32768 ranges per store
)

// store is a very basic struct that hold very little about the store. Mostly
// for holding the store descriptor. To access the replicas in a store, use the
// ranges instead.
type store struct {
	sync.RWMutex
	desc proto.StoreDescriptor
}

// newStore returns a new store with using the passed in ID and node
// descriptor.
func newStore(storeID proto.StoreID, nodeDesc proto.NodeDescriptor) *store {
	return &store{
		desc: proto.StoreDescriptor{
			StoreID: storeID,
			Node:    nodeDesc,
		},
	}
}

// getIDs returns the store's ID and its node's IDs.
func (s *store) getIDs() (proto.StoreID, proto.NodeID) {
	s.RLock()
	defer s.RUnlock()
	return s.desc.StoreID, s.desc.Node.NodeID
}

// getDesc returns the store descriptor. The rangeCount is required to
// determine the current capacity.
func (s *store) getDesc(rangeCount int) proto.StoreDescriptor {
	s.RLock()
	defer s.RUnlock()
	desc := s.desc
	desc.Capacity = s.getCapacity(rangeCount)
	return desc
}

// getCapacity returns the store capacity based on the numbers of ranges in
// located in the store.
func (s *store) getCapacity(rangeCount int) proto.StoreCapacity {
	s.RLock()
	defer s.RUnlock()
	return proto.StoreCapacity{
		Capacity:   capacityPerStore,
		Available:  capacityPerStore - int64(rangeCount)*bytesPerRange,
		RangeCount: int32(rangeCount),
	}
}

// String returns the current status of the store in human readable format.
// Like the getDesc and getCapacity, it requires the number of ranges currently
// housed in the store.
func (s *store) String(rangeCount int) string {
	s.RLock()
	defer s.RUnlock()
	var buffer bytes.Buffer
	desc := s.getDesc(rangeCount)
	buffer.WriteString(fmt.Sprintf("Store %d - Node:%d, Replicas:%d, AvailableReplicas:%d, Capacity:%d, Available:%d",
		desc.StoreID, desc.Node.NodeID, desc.Capacity.RangeCount, desc.Capacity.Available/bytesPerRange,
		desc.Capacity.Capacity, desc.Capacity.Available))
	return buffer.String()
}
