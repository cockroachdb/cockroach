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
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package simulation

import (
	"time"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/gossip/resolver"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
)

// Node represents a node used in a Network. It includes information
// about the node's gossip instance, network address, and underlying
// server.
type Node struct {
	Gossip *gossip.Gossip
	Server *rpc.Server
}

// Network provides access to a test gossip network of nodes.
type Network struct {
	Nodes          []*Node
	NetworkType    string        // "tcp" or "unix"
	GossipInterval time.Duration // The length of a round of gossip
	Stopper        *util.Stopper
}

// NewNetwork creates nodeCount gossip nodes. The networkType should
// be set to either "tcp" or "unix". The gossipInterval should be set
// to a compressed simulation timescale, though large enough to give
// the concurrent goroutines enough time to pass data back and forth
// in order to yield accurate estimates of how old data actually ends
// up being at the various nodes (e.g. DefaultTestGossipInterval).
func NewNetwork(nodeCount int, networkType string,
	gossipInterval time.Duration) *Network {
	clock := hlc.NewClock(hlc.UnixNano)
	rpcContext := rpc.NewContext(&base.Context{Insecure: true}, clock, nil)

	log.Infof("simulating gossip network with %d nodes", nodeCount)

	stopper := util.NewStopper()

	nodes := make([]*Node, nodeCount)
	for i := range nodes {
		server := rpc.NewServer(util.CreateTestAddr(networkType), rpcContext)
		if err := server.Start(); err != nil {
			log.Fatal(err)
		}
		nodes[i] = &Node{Server: server}
	}

	var numResolvers int
	if len(nodes) > 3 {
		numResolvers = 3
	} else {
		numResolvers = len(nodes)
	}

	for i, leftNode := range nodes {
		// Build new resolvers for each instance or we'll get data races.
		var resolvers []resolver.Resolver
		for _, rightNode := range nodes[:numResolvers] {
			resolvers = append(resolvers, resolver.NewResolverFromAddress(rightNode.Server.Addr()))
		}

		gossipNode := gossip.New(rpcContext, gossipInterval, resolvers)
		addr := leftNode.Server.Addr()
		if err := gossipNode.SetNodeDescriptor(&proto.NodeDescriptor{
			NodeID: proto.NodeID(i + 1),
			Address: proto.Addr{
				Network: addr.Network(),
				Address: addr.String(),
			},
		}); err != nil {
			log.Fatal(err)
		}

		gossipNode.Start(leftNode.Server, stopper)
		stopper.AddCloser(leftNode.Server)

		// Node 0 gossips gossip count.
		if i == 0 {
			if err := gossipNode.AddInfo(gossip.KeyNodeCount, int64(nodeCount), time.Hour); err != nil {
				log.Fatal(err)
			}
		}
		leftNode.Gossip = gossipNode
	}

	return &Network{
		Nodes:          nodes,
		NetworkType:    networkType,
		GossipInterval: gossipInterval,
		Stopper:        stopper,
	}
}

// GetNodeFromAddr returns the simulation node associated with
// provided network address, or nil if there is no such node.
func (n *Network) GetNodeFromAddr(addr string) (*Node, bool) {
	for _, node := range n.Nodes {
		if node.Server.Addr().String() == addr {
			return node, true
		}
	}
	return nil, false
}

// GetNodeFromID returns the simulation node associated with
// provided node ID, or nil if there is no such node.
func (n *Network) GetNodeFromID(nodeID proto.NodeID) (*Node, bool) {
	for _, node := range n.Nodes {
		if node.Gossip.GetNodeID() == nodeID {
			return node, true
		}
	}
	return nil, false
}

// SimulateNetwork runs a number of gossipInterval periods within the
// given Network. After each gossipInterval period, simCallback is
// invoked.  When it returns false, the simulation ends. If it returns
// true, the simulation continues another cycle.
//
// Node0 gossips the node count as well as the gossip sentinel. The
// gossip bootstrap hosts are set to the first three nodes (or fewer
// if less than three are available).
//
// At each cycle of the simulation, node 0 gossips the sentinel. If
// the simulation requires other nodes to gossip, this should be done
// via simCallback.
//
// The simulation callback receives a map of nodes, keyed by node address.
func (n *Network) SimulateNetwork(
	simCallback func(cycle int, network *Network) bool) {
	gossipTimeout := time.Tick(n.GossipInterval)
	nodes := n.Nodes
	var complete bool
	for cycle := 0; !complete; cycle++ {
		select {
		case <-gossipTimeout:
			// Node 0 gossips sentinel every cycle.
			if err := nodes[0].Gossip.AddInfo(gossip.KeySentinel, int64(cycle), time.Hour); err != nil {
				log.Fatal(err)
			}
			if !simCallback(cycle, n) {
				complete = true
			}
		}
	}
}

// Stop all servers and gossip nodes.
func (n *Network) Stop() {
	n.Stopper.Stop()
}

// RunUntilFullyConnected blocks until the gossip network has received
// gossip from every other node in the network. It returns the gossip
// cycle at which the network became fully connected.
func (n *Network) RunUntilFullyConnected() int {
	var connectedAtCycle int
	n.SimulateNetwork(func(cycle int, network *Network) bool {
		// Every node should gossip.
		for _, node := range network.Nodes {
			if err := node.Gossip.AddInfo(node.Server.Addr().String(), int64(cycle), time.Hour); err != nil {
				log.Fatal(err)
			}
		}
		if network.isNetworkConnected() {
			connectedAtCycle = cycle
			return false
		}
		return true
	})
	return connectedAtCycle
}

// isNetworkConnected returns true if the network is fully connected
// with no partitions (i.e. every node knows every other node's
// network address).
func (n *Network) isNetworkConnected() bool {
	for _, leftNode := range n.Nodes {
		for _, rightNode := range n.Nodes {
			if _, err := leftNode.Gossip.GetInfo(rightNode.Server.Addr().String()); err != nil {
				log.Info(err)
				return false
			}
		}
	}
	return true
}
