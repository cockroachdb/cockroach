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
	"sort"
	"sync"

	"github.com/cockroachdb/cockroach/proto"
)

const defaultReplicationFactor = 3

// rng contains all details about a single range. It also contains a map with
// the stores that contain replicas from this range.
type rng struct {
	sync.RWMutex
	factor int // replication factor
	desc   proto.RangeDescriptor
	stores map[proto.StoreID]*store
}

// newRange returns a new range with the given rangeID.
func newRange(rangeID proto.RangeID) *rng {
	return &rng{
		desc: proto.RangeDescriptor{
			RangeID: rangeID,
		},
		factor: defaultReplicationFactor,
		stores: make(map[proto.StoreID]*store),
	}
}

// getID returns the range's ID.
func (r *rng) getID() proto.RangeID {
	return r.getDesc().RangeID
}

// getDesc returns the range's descriptor.
func (r *rng) getDesc() proto.RangeDescriptor {
	r.RLock()
	defer r.RUnlock()
	return r.desc
}

// getFactor returns the range's replication factor.
func (r *rng) getFactor() int {
	r.RLock()
	defer r.RUnlock()
	return r.factor
}

// setFactor sets the range's replication factor.
func (r *rng) setFactor(factor int) {
	r.Lock()
	defer r.Unlock()
	r.factor = factor
}

// attachRangeToStore adds a new replica on the passed in store. It adds it to
// both the range descriptor and the store map.
func (r *rng) attachRangeToStore(s *store) {
	r.Lock()
	defer r.Unlock()
	storeID, nodeID := s.getIDs()
	r.desc.Replicas = append(r.desc.Replicas, proto.Replica{
		NodeID:  nodeID,
		StoreID: storeID,
	})
	r.stores[storeID] = s
}

// getStoreIDs returns the list of all stores where this range has replicas.
func (r *rng) getStoreIDs() []proto.StoreID {
	r.RLock()
	defer r.RUnlock()
	var storeIDs []proto.StoreID
	for storeID := range r.stores {
		storeIDs = append(storeIDs, storeID)
	}
	return storeIDs
}

// String returns a human readable string with details about the range.
func (r *rng) String() string {
	r.RLock()
	defer r.RUnlock()

	var storeIDs []int
	for storeID := range r.stores {
		storeIDs = append(storeIDs, int(storeID))
	}
	sort.Ints(storeIDs)

	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("Range:%d, Factor:%d, Stores:[", r.desc.RangeID, r.factor))

	first := true
	for _, storeID := range storeIDs {
		if first {
			first = false
		} else {
			buffer.WriteString(",")
		}
		buffer.WriteString(fmt.Sprintf("%d", storeID))
	}
	buffer.WriteString("]")
	return buffer.String()
}
