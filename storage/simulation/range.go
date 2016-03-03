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
// Author: Bram Gruneir (bram+code@cockroachlabs.com)

package main

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage"
)

// replica holds the results from calling the allocator as to what the range
// should do if it was part of the replicate queue and the store to which the
// replica is attached.
type replica struct {
	store     *Store
	action    storage.AllocatorAction
	priority  float64
	rebalance bool
}

// Range is a simulated cockroach range.
type Range struct {
	zone      config.ZoneConfig
	desc      roachpb.RangeDescriptor
	replicas  map[roachpb.StoreID]replica
	allocator storage.Allocator
}

// newRange returns a new range with the given rangeID.
func newRange(rangeID roachpb.RangeID, allocator storage.Allocator) *Range {
	return &Range{
		desc: roachpb.RangeDescriptor{
			RangeID: rangeID,
		},
		zone:      config.DefaultZoneConfig(),
		replicas:  make(map[roachpb.StoreID]replica),
		allocator: allocator,
	}
}

// addReplica adds a new replica on the passed in store. It adds it to
// both the range descriptor and the store map.
func (r *Range) addReplica(s *Store) {
	r.desc.Replicas = append(r.desc.Replicas, roachpb.ReplicaDescriptor{
		NodeID:  s.desc.Node.NodeID,
		StoreID: s.desc.StoreID,
	})
	r.replicas[s.desc.StoreID] = replica{
		store: s,
	}
}

// removeReplica removes an existing replica from the passed in store. It
// removes it from both the range descriptor and the store map.
func (r *Range) removeReplica(s *Store) {
	for i, replica := range r.desc.Replicas {
		if replica.StoreID == s.desc.StoreID {
			r.desc.Replicas = append(r.desc.Replicas[:i], r.desc.Replicas[i+1:]...)
			break
		}
	}
	delete(r.replicas, s.desc.StoreID)
}

// getStoreIDs returns the list of all stores where this range has replicas.
func (r *Range) getStoreIDs() []roachpb.StoreID {
	var storeIDs []roachpb.StoreID
	for storeID := range r.replicas {
		storeIDs = append(storeIDs, storeID)
	}
	return storeIDs
}

// getStores returns a shallow copy of the internal stores map.
func (r *Range) getStores() map[roachpb.StoreID]*Store {
	stores := make(map[roachpb.StoreID]*Store)
	for storeID, replica := range r.replicas {
		stores[storeID] = replica.store
	}
	return stores
}

// split range adds a replica to all the stores from the passed in range. This
// function should only be called on new ranges as it will overwrite all of the
// replicas in the range.
func (r *Range) splitRange(originalRange *Range) {
	stores := originalRange.getStores()
	r.desc.Replicas = append([]roachpb.ReplicaDescriptor(nil), originalRange.desc.Replicas...)
	for storeID, store := range stores {
		r.replicas[storeID] = replica{
			store: store,
		}
	}
}

// getAllocateTarget queries the allocator for the store that would be the best
// candidate to take on a new replica.
func (r *Range) getAllocateTarget() (roachpb.StoreID, error) {
	newStore, err := r.allocator.AllocateTarget(r.zone.ReplicaAttrs[0], r.desc.Replicas, true, nil)
	if err != nil {
		return 0, err
	}
	return newStore.StoreID, nil
}

// getRemoveTarget queries the allocator for the store that contains a replica
// that can be removed.
func (r *Range) getRemoveTarget() (roachpb.StoreID, error) {
	removeStore, err := r.allocator.RemoveTarget(r.desc.Replicas)
	if err != nil {
		return 0, err
	}
	return removeStore.StoreID, nil
}

// getRebalanceTarget queries the allocator for the store that would be the best
// candidate to add a replica for rebalancing. Returns true only if a target is
// found.
func (r *Range) getRebalanceTarget(storeID roachpb.StoreID) (roachpb.StoreID, bool) {
	rebalanceTarget := r.allocator.RebalanceTarget(storeID, r.zone.ReplicaAttrs[0], r.desc.Replicas)
	if rebalanceTarget == nil {
		return 0, false
	}
	return rebalanceTarget.StoreID, true
}

// String returns a human readable string with details about the range.
func (r *Range) String() string {
	var storeIDs roachpb.StoreIDSlice
	for storeID := range r.replicas {
		storeIDs = append(storeIDs, storeID)
	}
	sort.Sort(storeIDs)

	var buf bytes.Buffer
	fmt.Fprintf(&buf, "Range:%d, Factor:%d, Stores:[", r.desc.RangeID, len(r.zone.ReplicaAttrs))

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
