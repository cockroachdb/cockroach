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

	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/proto"
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
	sync.RWMutex
	zone      config.ZoneConfig
	desc      proto.RangeDescriptor
	replicas  map[proto.StoreID]replica
	allocator storage.Allocator
}

// newRange returns a new range with the given rangeID.
func newRange(rangeID proto.RangeID, allocator storage.Allocator) *Range {
	return &Range{
		desc: proto.RangeDescriptor{
			RangeID: rangeID,
		},
		zone:      *config.DefaultZoneConfig,
		replicas:  make(map[proto.StoreID]replica),
		allocator: allocator,
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

// getFactor returns the range's zone config.
func (r *Range) getZoneConfig() config.ZoneConfig {
	r.RLock()
	defer r.RUnlock()
	return r.zone
}

// setFactor sets the range's replication factor.
func (r *Range) setZoneConfig(zone config.ZoneConfig) {
	r.Lock()
	defer r.Unlock()
	r.zone = zone
}

// addReplica adds a new replica on the passed in store. It adds it to
// both the range descriptor and the store map.
func (r *Range) addReplica(s *Store) {
	r.Lock()
	defer r.Unlock()
	storeID, nodeID := s.getIDs()
	r.desc.Replicas = append(r.desc.Replicas, proto.Replica{
		NodeID:  nodeID,
		StoreID: storeID,
	})
	r.replicas[storeID] = replica{
		store: s,
	}
}

// getStoreIDs returns the list of all stores where this range has replicas.
func (r *Range) getStoreIDs() []proto.StoreID {
	r.RLock()
	defer r.RUnlock()
	var storeIDs []proto.StoreID
	for storeID := range r.replicas {
		storeIDs = append(storeIDs, storeID)
	}
	return storeIDs
}

// getStores returns a shallow copy of the internal stores map.
func (r *Range) getStores() map[proto.StoreID]*Store {
	r.RLock()
	defer r.RUnlock()
	stores := make(map[proto.StoreID]*Store)
	for storeID, replica := range r.replicas {
		stores[storeID] = replica.store
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
	r.desc.Replicas = append([]proto.Replica(nil), desc.Replicas...)
	for storeID, store := range stores {
		r.replicas[storeID] = replica{
			store: store,
		}
	}
}

// getNextAction returns the action and rebalance from the replica with the
// highest action priority.
func (r *Range) getNextAction() (storage.AllocatorAction, bool) {
	r.RLock()
	defer r.RUnlock()
	var topReplica replica
	if len(r.replicas) == 0 {
		return storage.AllocatorNoop, false
	}
	// TODO(bram): This is random. Might want to make it deterministic for
	// repeatability.
	for _, replica := range r.replicas {
		if replica.priority > topReplica.priority {
			topReplica = replica
		}
	}
	return topReplica.action, topReplica.rebalance
}

// getAllocateTarget calls allocateTarget for the range and returns the top
// target store.
func (r *Range) getAllocateTarget() (proto.StoreID, error) {
	r.RLock()
	defer r.RUnlock()
	newStore, err := r.allocator.AllocateTarget(r.zone.ReplicaAttrs[0], r.desc.Replicas, true, nil)
	if err != nil {
		return 0, err
	}
	return newStore.StoreID, nil
}

// String returns a human readable string with details about the range.
func (r *Range) String() string {
	r.RLock()
	defer r.RUnlock()

	var storeIDs proto.StoreIDSlice
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
