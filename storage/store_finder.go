// Copyright 2014 The Cockroach Authors.
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
// Author: Kathy Spradlin (kathyspradlin@gmail.com)

package storage

import (
	"fmt"
	"sync"

	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/proto"
)

// StoreFinder finds the disks in a datacenter that have the requested
// attributes.
type StoreFinder func(proto.Attributes) ([]*StoreDescriptor, error)

type stringSet map[string]struct{}

type storeFinderContext struct {
	sync.Mutex
	capacityKeys stringSet
}

// capacityGossipUpdate is a gossip callback triggered whenever capacity
// information is gossiped. It just tracks keys used for capacity gossip.
func (s *Store) capacityGossipUpdate(key string, contentsChanged bool) {
	s.finderContext.Lock()
	defer s.finderContext.Unlock()

	if s.finderContext.capacityKeys == nil {
		s.finderContext.capacityKeys = stringSet{}
	}
	s.finderContext.capacityKeys[key] = struct{}{}
}

// findStores is the Store's implementation of a StoreFinder. It returns a list
// of stores with attributes that are a superset of the required attributes. It
// never returns an error.
//
// If it cannot retrieve a StoreDescriptor from the Store's gossip, it garbage
// collects the failed key.
//
// TODO(embark, spencer): consider using a reverse index map from Attr->stores,
// for efficiency.  Ensure that entries in this map still have an opportunity
// to be garbage collected.
func (s *Store) findStores(required proto.Attributes) ([]*StoreDescriptor, error) {
	s.finderContext.Lock()
	defer s.finderContext.Unlock()
	var stores []*StoreDescriptor
	for key := range s.finderContext.capacityKeys {
		storeDesc, err := storeDescFromGossip(key, s.gossip)
		if err != nil {
			// We can no longer retrieve this key from the gossip store,
			// perhaps it expired.
			delete(s.finderContext.capacityKeys, key)
		} else if required.IsSubset(storeDesc.Attrs) {
			stores = append(stores, storeDesc)
		}
	}
	return stores, nil
}

// storeDescFromGossip retrieves a StoreDescriptor from the specified capacity
// gossip key. Returns an error if the gossip doesn't exist or is not
// a StoreDescriptor.
func storeDescFromGossip(key string, g *gossip.Gossip) (*StoreDescriptor, error) {
	info, err := g.GetInfo(key)

	if err != nil {
		return nil, err
	}
	storeDesc, ok := info.(StoreDescriptor)
	if !ok {
		return nil, fmt.Errorf("gossiped info is not a StoreDescriptor: %+v", info)
	}
	return &storeDesc, nil
}
