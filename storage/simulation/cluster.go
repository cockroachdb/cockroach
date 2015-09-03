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
	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/testutils/gossiputil"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/stop"
)

type cluster struct {
	stopper       *stop.Stopper
	clock         *hlc.Clock
	rpc           *rpc.Context
	gossip        *gossip.Gossip
	storePool     *storage.StorePool
	allocator     storage.Allocator
	storeGossiper *gossiputil.StoreGossiper
	nodes         []*node
	ranges        []*rng
}

func createCluster(nodeCount int) *cluster {
	stopper := stop.NewStopper()
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
	}

	for i := 0; i < nodeCount; i++ {
		c.addNode()
	}

	return c
}

func (c *cluster) addNode() {
	nodeID := proto.NodeID(len(c.nodes))
	c.nodes = append(c.nodes, newNode(nodeID))
}

func (c *cluster) addRange() *rng {
	rangeID := proto.RangeID(len(c.ranges))
	newRng := newRange(rangeID)
	c.ranges = append(c.ranges, newRng)
	return newRng
}
