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

// Range is a simulated cockroach range.
type Range struct {
	sync.RWMutex
	factor int // replication factor
	desc   proto.RangeDescriptor
	stores map[proto.StoreID]*Store
}

// newRange returns a new range with the given rangeID.
func newRange(rangeID proto.RangeID) *Range {
	return &Range{
		desc: proto.RangeDescriptor{
			RangeID: rangeID,
		},
		factor: defaultReplicationFactor,
		stores: make(map[proto.StoreID]*Store),
	}
}

// getID returns the range's ID.
func (r *Range) getID() proto.RangeID {
	return r.getDesc().RangeID
}

// getDesc returns the range's descriptor.
func (r *Range) getDesc() proto.RangeDescriptor {
	r.RLock()
	defer r.RUnlock()
	return r.desc
}

// getFactor returns the range's replication factor.
func (r *Range) getFactor() int {
	r.RLock()
	defer r.RUnlock()
	return r.factor
}

// setFactor sets the range's replication factor.
func (r *Range) setFactor(factor int) {
	r.Lock()
	defer r.Unlock()
	r.factor = factor
}

// attachRangeToStore adds a new replica on the passed in store. It adds it to
// both the range descriptor and the store map.
func (r *Range) attachRangeToStore(s *Store) {
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
func (r *Range) getStoreIDs() []proto.StoreID {
	r.RLock()
	defer r.RUnlock()
	var storeIDs []proto.StoreID
	for storeID := range r.stores {
		storeIDs = append(storeIDs, storeID)
	}
	return storeIDs
}

// getStores returns a shallow copy of the internal stores map.
func (r *Range) getStores() map[proto.StoreID]*Store {
	r.RLock()
	defer r.RUnlock()
	stores := make(map[proto.StoreID]*Store)
	for storeID, store := range r.stores {
		stores[storeID] = store
	}
	return stores
}

// split range adds a replica to all the stores from the passed in range. This
// function should only be called on new ranges as it will overwrite all of the
// replicas in the range.
func (r *Range) splitRange(originalRange *Range) {
	desc := originalRange.getDesc()
	stores := originalRange.getStores()
	r.Lock()
	defer r.Unlock()
	r.desc.Replicas = desc.Replicas
	r.stores = stores
}

// String returns a human readable string with details about the range.
func (r *Range) String() string {
	r.RLock()
	defer r.RUnlock()

	var storeIDs []int
	for storeID := range r.stores {
		storeIDs = append(storeIDs, int(storeID))
	}
	sort.Ints(storeIDs)

	var buf bytes.Buffer
	fmt.Fprintf(&buf, "Range:%d, Factor:%d, Stores:[", r.desc.RangeID, r.factor)

	first := true
	for _, storeID := range storeIDs {
		if first {
			first = false
		} else {
			buf.WriteString(",")
		}
		fmt.Fprintf(&buf, "%d", storeID)
	}
	buf.WriteString("]")
	return buf.String()
}
