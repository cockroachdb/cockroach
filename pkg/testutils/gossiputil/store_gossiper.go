// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gossiputil

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// StoreGossiper allows tests to push storeDescriptors into gossip and
// synchronize on their callbacks. There can only be one storeGossiper used per
// gossip instance.
type StoreGossiper struct {
	g *gossip.Gossip
}

// NewStoreGossiper creates a store gossiper for use by tests. It adds the
// callback to gossip.
func NewStoreGossiper(g *gossip.Gossip) *StoreGossiper {
	sg := &StoreGossiper{
		g: g,
	}
	return sg
}

// GossipStores queues up a list of stores to gossip and blocks until each one
// is gossiped before returning.
func (sg *StoreGossiper) GossipStores(storeDescs []*roachpb.StoreDescriptor, t *testing.T) {
	storeIDs := make([]roachpb.StoreID, len(storeDescs))
	for i, store := range storeDescs {
		storeIDs[i] = store.StoreID
	}

	for i, storeDesc := range storeDescs {
		if err := sg.g.TestingAddInfoProtoAndWaitForAllCallbacks(gossip.MakeStoreDescKey(storeIDs[i]),
			storeDesc, 0); err != nil {
			t.Fatal(err)
		}
	}
}
