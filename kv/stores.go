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
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package kv

import (
	"fmt"
	"sync"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

// Stores provides methods to access a collection of local stores.
type Stores struct {
	mu       sync.RWMutex                       // Protects storeMap and addrs
	storeMap map[roachpb.StoreID]*storage.Store // Map from StoreID to Store
}

// NewStores creates a new set of Stores.
func NewStores() *Stores {
	return &Stores{
		storeMap: map[roachpb.StoreID]*storage.Store{},
	}
}

// GetStoreCount returns the number of stores this node is exporting.
func (s *Stores) GetStoreCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.storeMap)
}

// HasStore returns true if the specified store is present.
func (s *Stores) HasStore(storeID roachpb.StoreID) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.storeMap[storeID]
	return ok
}

// GetStore looks up the store by store ID. Returns an error
// if not found.
func (s *Stores) GetStore(storeID roachpb.StoreID) (*storage.Store, error) {
	s.mu.RLock()
	store, ok := s.storeMap[storeID]
	s.mu.RUnlock()
	if !ok {
		return nil, util.Errorf("store %d not found", storeID)
	}
	return store, nil
}

// AddStore adds the specified store to the store map.
func (s *Stores) AddStore(store *storage.Store) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.storeMap[store.Ident.StoreID]; ok {
		panic(fmt.Sprintf("cannot add store twice to local db: %+v", store.Ident))
	}
	s.storeMap[store.Ident.StoreID] = store
}

// RemoveStore removes the specified store from the store map.
func (s *Stores) RemoveStore(store *storage.Store) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.storeMap, store.Ident.StoreID)
}

// VisitStores implements a visitor pattern over stores in the storeMap.
// The specified function is invoked with each store in turn. Stores are
// visited in a random order.
func (s *Stores) VisitStores(visitor func(s *storage.Store) error) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, s := range s.storeMap {
		if err := visitor(s); err != nil {
			return err
		}
	}
	return nil
}

// lookupReplica looks up replica by key [range]. Lookups are done
// by consulting each store in turn via Store.LookupRange(key).
// Returns RangeID and replica on success; RangeKeyMismatch error
// if not found.
// This is only for testing usage; performance doesn't matter.
func (s *Stores) lookupReplica(start, end roachpb.RKey) (rangeID roachpb.RangeID, replica *roachpb.ReplicaDescriptor, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var rng *storage.Replica
	for _, store := range s.storeMap {
		rng = store.LookupReplica(start, end)
		if rng == nil {
			if tmpRng := store.LookupReplica(start, nil); tmpRng != nil {
				log.Warningf(fmt.Sprintf("range not contained in one range: [%s,%s), but have [%s,%s)", start, end, tmpRng.Desc().StartKey, tmpRng.Desc().EndKey))
			}
			continue
		}
		if replica == nil {
			rangeID = rng.Desc().RangeID
			replica = rng.GetReplica()
			continue
		}
		// Should never happen outside of tests.
		return 0, nil, util.Errorf(
			"range %+v exists on additional store: %+v", rng, store)
	}
	if replica == nil {
		err = roachpb.NewRangeKeyMismatchError(start.AsRawKey(), end.AsRawKey(), nil)
	}
	return rangeID, replica, err
}
