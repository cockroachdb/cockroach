// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package gossiputil

import (
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// StoreGossiper allows tests to push storeDescriptors into gossip and
// synchronize on their callbacks. There can only be one storeGossiper used per
// gossip instance.
type StoreGossiper struct {
	g           *gossip.Gossip
	mu          syncutil.Mutex
	cond        *sync.Cond
	storeKeyMap map[string]struct{}
}

// NewStoreGossiper creates a store gossiper for use by tests. It adds the
// callback to gossip.
func NewStoreGossiper(g *gossip.Gossip) *StoreGossiper {
	sg := &StoreGossiper{
		g:           g,
		storeKeyMap: make(map[string]struct{}),
	}
	sg.cond = sync.NewCond(&sg.mu)
	// Redundant callbacks are required by StoreGossiper. See GossipWithFunction
	// which waits for all of the callbacks to be invoked.
	g.RegisterCallback(gossip.MakePrefixPattern(gossip.KeyStorePrefix), func(key string, _ roachpb.Value) {
		sg.mu.Lock()
		defer sg.mu.Unlock()
		delete(sg.storeKeyMap, key)
		sg.cond.Broadcast()
	}, gossip.Redundant)
	return sg
}

// GossipStores queues up a list of stores to gossip and blocks until each one
// is gossiped before returning.
func (sg *StoreGossiper) GossipStores(storeDescs []*roachpb.StoreDescriptor, t *testing.T) {
	storeIDs := make([]roachpb.StoreID, len(storeDescs))
	for i, store := range storeDescs {
		storeIDs[i] = store.StoreID
	}
	sg.GossipWithFunction(storeIDs, func() {
		for i, storeDesc := range storeDescs {
			if err := sg.g.AddInfoProto(gossip.MakeStoreKey(storeIDs[i]), storeDesc, 0); err != nil {
				t.Fatal(err)
			}
		}
	})
}

// GossipWithFunction calls gossipFn and blocks until gossip callbacks have
// fired on each of the stores specified by storeIDs.
func (sg *StoreGossiper) GossipWithFunction(storeIDs []roachpb.StoreID, gossipFn func()) {
	sg.mu.Lock()
	defer sg.mu.Unlock()
	sg.storeKeyMap = make(map[string]struct{})
	for _, storeID := range storeIDs {
		storeKey := gossip.MakeStoreKey(storeID)
		sg.storeKeyMap[storeKey] = struct{}{}
	}

	gossipFn()

	// Wait for gossip callbacks to be invoked on all the stores.
	for len(sg.storeKeyMap) > 0 {
		sg.cond.Wait()
	}
}
