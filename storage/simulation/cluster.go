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
	"sort"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/testutils/gossiputil"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/stop"
)

// cluster maintains a list of all nodes, stores and ranges as well as any
// shared resources.
type cluster struct {
	stopper       *stop.Stopper
	clock         *hlc.Clock
	rpc           *rpc.Context
	gossip        *gossip.Gossip
	storePool     *storage.StorePool
	allocator     storage.Allocator
	storeGossiper *gossiputil.StoreGossiper
	nodes         map[proto.NodeID]*node
	stores        map[proto.StoreID]*store
	ranges        map[proto.RangeID]*rng
}

// createCluster generates a new cluster using the provided stopper and the
// number of nodes supplied. Each node will have one store to start.
func createCluster(stopper *stop.Stopper, nodeCount int) *cluster {
	clock := hlc.NewClock(hlc.UnixNano)
	rpcContext := rpc.NewContext(&base.Context{}, clock, stopper)
	g := gossip.New(rpcContext, gossip.TestInterval, gossip.TestBootstrap)
	storePool := storage.NewStorePool(g, storage.TestTimeUntilStoreDeadOff, stopper)
	c := &cluster{
		stopper:       stopper,
		clock:         clock,
		rpc:           rpcContext,
		gossip:        g,
		storePool:     storePool,
		allocator:     storage.MakeAllocator(storePool),
		storeGossiper: gossiputil.NewStoreGossiper(g),
		nodes:         make(map[proto.NodeID]*node),
		stores:        make(map[proto.StoreID]*store),
		ranges:        make(map[proto.RangeID]*rng),
	}

	// Add the nodes.
	for i := 0; i < nodeCount; i++ {
		c.addNewNodeWithStore()
	}

	// Add a single range and add to this first node's first store.
	firstRange := c.addRange()
	firstRange.attachRangeToStore(c.stores[proto.StoreID(0)])
	return c
}

// addNewNodeWithStore adds new node with a single store.
func (c *cluster) addNewNodeWithStore() {
	nodeID := proto.NodeID(len(c.nodes))
	c.nodes[nodeID] = newNode(nodeID)
	c.addStore(nodeID)
}

// addStore adds a new store to the node with the provided nodeID.
func (c *cluster) addStore(nodeID proto.NodeID) *store {
	n := c.nodes[nodeID]
	s := n.addNewStore()
	storeID, _ := s.getIDs()
	c.stores[storeID] = s
	return s
}

// addRange adds a new range to the cluster but does not attach it to any
// store.
func (c *cluster) addRange() *rng {
	rangeID := proto.RangeID(len(c.ranges))
	newRng := newRange(rangeID)
	c.ranges[rangeID] = newRng
	return newRng
}

// String prints out the current status of the cluster.
func (c *cluster) String() string {
	storesRangeCounts := make(map[proto.StoreID]int)
	for _, r := range c.ranges {
		for _, storeID := range r.getStoreIDs() {
			storesRangeCounts[storeID]++
		}
	}

	var nodeIDs []int
	for nodeID := range c.nodes {
		nodeIDs = append(nodeIDs, int(nodeID))
	}
	sort.Ints(nodeIDs)

	var buffer bytes.Buffer
	buffer.WriteString("Node Info:\n")
	for _, nodeID := range nodeIDs {
		n := c.nodes[proto.NodeID(nodeID)]
		buffer.WriteString(n.String())
		buffer.WriteString("\n")
	}

	var storeIDs []int
	for storeID := range c.stores {
		storeIDs = append(storeIDs, int(storeID))
	}
	sort.Ints(storeIDs)

	buffer.WriteString("Store Info:\n")
	for _, storeID := range storeIDs {
		s := c.stores[proto.StoreID(storeID)]
		buffer.WriteString(s.String(storesRangeCounts[proto.StoreID(storeID)]))
		buffer.WriteString("\n")
	}

	var rangeIDs []int
	for rangeID := range c.ranges {
		rangeIDs = append(rangeIDs, int(rangeID))
	}
	sort.Ints(rangeIDs)

	buffer.WriteString("Range Info:\n")
	for _, rangeID := range rangeIDs {
		r := c.ranges[proto.RangeID(rangeID)]
		buffer.WriteString(r.String())
		buffer.WriteString("\n")
	}

	return buffer.String()
}
