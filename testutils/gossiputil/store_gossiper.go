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
// Author: Matt Tracy (matt@cockroachlabs.com)
// Author: Bram Gruneir (bram+code@cockroachlabs.com)

package gossiputil

import (
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/proto"
)

// StoreGossiper allows tests to push storeDescriptors into gossip and
// synchronize on their callbacks. There can only be one storeGossiper used per
// gossip instance.
type StoreGossiper struct {
	g           *gossip.Gossip
	wg          sync.WaitGroup
	mu          sync.Mutex
	storeKeyMap map[string]struct{}
}

// NewStoreGossiper creates a store gossiper for use by tests. It adds the
// callback to gossip.
func NewStoreGossiper(g *gossip.Gossip) *StoreGossiper {
	sg := &StoreGossiper{
		g:           g,
		storeKeyMap: make(map[string]struct{}),
	}
	g.RegisterCallback(gossip.MakePrefixPattern(gossip.KeyStorePrefix), func(key string, _ []byte) {
		sg.mu.Lock()
		defer sg.mu.Unlock()
		if _, ok := sg.storeKeyMap[key]; ok {
			sg.wg.Done()
		}
	})
	return sg
}

// GossipStores queues up a list of stores to gossip and blocks until each one
// is gossiped before returning.
func (sg *StoreGossiper) GossipStores(stores []*proto.StoreDescriptor, t *testing.T) {
	sg.mu.Lock()
	sg.storeKeyMap = make(map[string]struct{})
	sg.wg.Add(len(stores))
	for _, s := range stores {
		storeKey := gossip.MakeStoreKey(s.StoreID)
		sg.storeKeyMap[storeKey] = struct{}{}
		// Gossip store descriptor.
		err := sg.g.AddInfoProto(storeKey, s, 0)
		if err != nil {
			t.Fatal(err)
		}
	}
	sg.mu.Unlock()

	// Wait for all gossip callbacks to be invoked.
	sg.wg.Wait()
}

// GossipWithFunction is similar to GossipStores but instead of gossiping the
// store descriptors directly, call the passed in function to do so.
func (sg *StoreGossiper) GossipWithFunction(stores []proto.StoreID, gossiper func()) {
	sg.mu.Lock()
	sg.storeKeyMap = make(map[string]struct{})
	sg.wg.Add(len(stores))
	for _, s := range stores {
		storeKey := gossip.MakeStoreKey(s)
		sg.storeKeyMap[storeKey] = struct{}{}
	}

	// Gossip the stores via the passed in function.
	gossiper()

	sg.mu.Unlock()

	// Wait for all gossip callbacks to be invoked.
	sg.wg.Wait()
}
