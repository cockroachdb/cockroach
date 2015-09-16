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
	"math/rand"
	"sort"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/testutils/gossiputil"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/randutil"
	"github.com/cockroachdb/cockroach/util/stop"
)

// Cluster maintains a list of all nodes, stores and ranges as well as any
// shared resources.
type Cluster struct {
	stopper       *stop.Stopper
	clock         *hlc.Clock
	rpc           *rpc.Context
	gossip        *gossip.Gossip
	storePool     *storage.StorePool
	allocator     storage.Allocator
	storeGossiper *gossiputil.StoreGossiper
	nodes         map[proto.NodeID]*Node
	stores        map[proto.StoreID]*Store
	ranges        map[proto.RangeID]*Range
	rand          *rand.Rand
	seed          int64
}

// createCluster generates a new cluster using the provided stopper and the
// number of nodes supplied. Each node will have one store to start.
func createCluster(stopper *stop.Stopper, nodeCount int) *Cluster {
	rand, seed := randutil.NewPseudoRand()
	clock := hlc.NewClock(hlc.UnixNano)
	rpcContext := rpc.NewContext(&base.Context{}, clock, stopper)
	g := gossip.New(rpcContext, gossip.TestInterval, gossip.TestBootstrap)
	storePool := storage.NewStorePool(g, storage.TestTimeUntilStoreDeadOff, stopper)
	c := &Cluster{
		stopper:       stopper,
		clock:         clock,
		rpc:           rpcContext,
		gossip:        g,
		storePool:     storePool,
		allocator:     storage.MakeAllocator(storePool),
		storeGossiper: gossiputil.NewStoreGossiper(g),
		nodes:         make(map[proto.NodeID]*Node),
		stores:        make(map[proto.StoreID]*Store),
		ranges:        make(map[proto.RangeID]*Range),
		rand:          rand,
		seed:          seed,
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
func (c *Cluster) addNewNodeWithStore() {
	nodeID := proto.NodeID(len(c.nodes))
	c.nodes[nodeID] = newNode(nodeID)
	c.addStore(nodeID)
}

// addStore adds a new store to the node with the provided nodeID.
func (c *Cluster) addStore(nodeID proto.NodeID) *Store {
	n := c.nodes[nodeID]
	s := n.addNewStore()
	storeID, _ := s.getIDs()
	c.stores[storeID] = s
	return s
}

// addRange adds a new range to the cluster but does not attach it to any
// store.
func (c *Cluster) addRange() *Range {
	rangeID := proto.RangeID(len(c.ranges))
	newRng := newRange(rangeID)
	c.ranges[rangeID] = newRng
	return newRng
}

func (c *Cluster) splitRangeRandom() {
	rangeID := proto.RangeID(c.rand.Int63n(int64(len(c.ranges))))
	c.splitRange(rangeID)
}

func (c *Cluster) splitRangeLast() {
	rangeID := proto.RangeID(len(c.ranges) - 1)
	c.splitRange(rangeID)
}

func (c *Cluster) splitRange(rangeID proto.RangeID) {
	newRange := c.addRange()
	originalRange := c.ranges[rangeID]
	newRange.splitRange(originalRange)
}

// String prints out the current status of the cluster.
func (c *Cluster) String() string {
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

	var buf bytes.Buffer
	buf.WriteString("Node Info:\n")
	for _, nodeID := range nodeIDs {
		n := c.nodes[proto.NodeID(nodeID)]
		buf.WriteString(n.String())
		buf.WriteString("\n")
	}

	var storeIDs []int
	for storeID := range c.stores {
		storeIDs = append(storeIDs, int(storeID))
	}
	sort.Ints(storeIDs)

	buf.WriteString("Store Info:\n")
	for _, storeID := range storeIDs {
		s := c.stores[proto.StoreID(storeID)]
		buf.WriteString(s.String(storesRangeCounts[proto.StoreID(storeID)]))
		buf.WriteString("\n")
	}

	var rangeIDs []int
	for rangeID := range c.ranges {
		rangeIDs = append(rangeIDs, int(rangeID))
	}
	sort.Ints(rangeIDs)

	buf.WriteString("Range Info:\n")
	for _, rangeID := range rangeIDs {
		r := c.ranges[proto.RangeID(rangeID)]
		buf.WriteString(r.String())
		buf.WriteString("\n")
	}

	return buf.String()
}
