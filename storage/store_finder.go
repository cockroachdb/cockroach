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

// FindStoreFunc finds the disks in a datacenter that have the requested
// attributes.
type FindStoreFunc func(proto.Attributes) ([]*proto.StoreDescriptor, error)

type stringSet map[string]struct{}

// StoreFinder provides the data necessary to find stores with particular
// attributes.
type StoreFinder struct {
	finderMu     sync.Mutex
	cond         *sync.Cond
	capacityKeys stringSet // Tracks gosisp keys used for capacity
	gossip       *gossip.Gossip
}

// newStoreFinder creates a StoreFinder.
func newStoreFinder(g *gossip.Gossip) *StoreFinder {
	sf := &StoreFinder{gossip: g}
	sf.cond = sync.NewCond(&sf.finderMu)
	return sf
}

// capacityGossipUpdate is a gossip callback triggered whenever capacity
// information is gossiped. It just tracks keys used for capacity gossip.
func (sf *StoreFinder) capacityGossipUpdate(key string, contentsChanged bool) {
	sf.finderMu.Lock()
	defer sf.finderMu.Unlock()

	if sf.capacityKeys == nil {
		sf.capacityKeys = stringSet{}
	}
	sf.capacityKeys[key] = struct{}{}
	sf.cond.Broadcast()
}

// WaitForNodes blocks until at least the given number of nodes are present in the
// capacity map. Used for tests.
func (sf *StoreFinder) WaitForNodes(n int) {
	sf.finderMu.Lock()
	defer sf.finderMu.Unlock()

	for len(sf.capacityKeys) < n {
		sf.cond.Wait()
	}
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
func (sf *StoreFinder) findStores(required proto.Attributes) ([]*proto.StoreDescriptor, error) {
	sf.finderMu.Lock()
	defer sf.finderMu.Unlock()
	var stores []*proto.StoreDescriptor
	for key := range sf.capacityKeys {
		storeDesc, err := storeDescFromGossip(key, sf.gossip)
		if err != nil {
			// We can no longer retrieve this key from the gossip store,
			// perhaps it expired.
			delete(sf.capacityKeys, key)
		} else if required.IsSubset(storeDesc.Attrs) {
			stores = append(stores, storeDesc)
		}
	}
	return stores, nil
}

// storeDescFromGossip retrieves a StoreDescriptor from the specified capacity
// gossip key. Returns an error if the gossip doesn't exist or is not
// a StoreDescriptor.
func storeDescFromGossip(key string, g *gossip.Gossip) (*proto.StoreDescriptor, error) {
	info, err := g.GetInfo(key)

	if err != nil {
		return nil, err
	}
	storeDesc, ok := info.(proto.StoreDescriptor)
	if !ok {
		return nil, fmt.Errorf("gossiped info is not a StoreDescriptor: %+v", info)
	}
	return &storeDesc, nil
}
