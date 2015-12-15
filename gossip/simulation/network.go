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
	"net"
	"time"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/gossip/resolver"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/stop"
)

// Node represents a node used in a Network. It includes information
// about the node's gossip instance, network address, and underlying
// server.
type Node struct {
	Gossip *gossip.Gossip
	Server *rpc.Server
	Addr   net.Addr
}

// Network provides access to a test gossip network of nodes.
type Network struct {
	Nodes       []*Node
	NetworkType string // "tcp" or "unix"
	Stopper     *stop.Stopper
}

// NewNetwork creates nodeCount gossip nodes. The networkType should
// be set to either "tcp" or "unix".
func NewNetwork(nodeCount int, networkType string) *Network {
	clock := hlc.NewClock(hlc.UnixNano)

	log.Infof("simulating gossip network with %d nodes", nodeCount)

	stopper := stop.NewStopper()

	rpcContext := rpc.NewContext(&base.Context{Insecure: true}, clock, stopper)
	tlsConfig, err := rpcContext.GetServerTLSConfig()
	if err != nil {
		log.Fatal(err)
	}

	nodes := make([]*Node, nodeCount)
	for i := range nodes {
		server := rpc.NewServer(rpcContext)

		testAddr := util.CreateTestAddr(networkType)
		ln, err := util.ListenAndServe(stopper, server, testAddr, tlsConfig)
		if err != nil {
			log.Fatal(err)
		}

		nodes[i] = &Node{Server: server, Addr: ln.Addr()}
	}

	for i, leftNode := range nodes {
		// Build new resolvers for each instance or we'll get data races.
		resolvers := []resolver.Resolver{resolver.NewResolverFromAddress(nodes[0].Addr)}

		gossipNode := gossip.New(rpcContext, resolvers)
		addr := leftNode.Addr
		gossipNode.SetNodeID(roachpb.NodeID(i + 1))
		if err := gossipNode.SetNodeDescriptor(&roachpb.NodeDescriptor{
			NodeID:  roachpb.NodeID(i + 1),
			Address: util.MakeUnresolvedAddr(addr.Network(), addr.String()),
		}); err != nil {
			log.Fatal(err)
		}
		if err := gossipNode.AddInfo(addr.String(), encoding.EncodeUint64(nil, 0), time.Hour); err != nil {
			log.Fatal(err)
		}
		gossipNode.Start(leftNode.Server, addr, stopper)
		gossipNode.EnableSimulationCycler(true)

		leftNode.Gossip = gossipNode
	}

	return &Network{
		Nodes:       nodes,
		NetworkType: networkType,
		Stopper:     stopper,
	}
}

// GetNodeFromAddr returns the simulation node associated with
// provided network address, or nil if there is no such node.
func (n *Network) GetNodeFromAddr(addr string) (*Node, bool) {
	for _, node := range n.Nodes {
		if node.Addr.String() == addr {
			return node, true
		}
	}
	return nil, false
}

// GetNodeFromID returns the simulation node associated with
// provided node ID, or nil if there is no such node.
func (n *Network) GetNodeFromID(nodeID roachpb.NodeID) (*Node, bool) {
	for _, node := range n.Nodes {
		if node.Gossip.GetNodeID() == nodeID {
			return node, true
		}
	}
	return nil, false
}

// SimulateNetwork runs until the simCallback returns false.
//
// At each cycle, every node gossips a key equal to its address (unique)
// with the cycle as the value. The received cycle value can be used
// to determine the aging of information between any two nodes in the
// network.
//
// At each cycle of the simulation, node 0 gossips the sentinel.
//
// The simulation callback receives the cycle and the network as arguments.
func (n *Network) SimulateNetwork(simCallback func(cycle int, network *Network) bool) {
	nodes := n.Nodes
	for cycle := 1; simCallback(cycle, n); cycle++ {
		// Node 0 gossips sentinel every cycle.
		if err := nodes[0].Gossip.AddInfo(gossip.KeySentinel, encoding.EncodeUint64(nil, uint64(cycle)), time.Hour); err != nil {
			log.Fatal(err)
		}
		// Every node gossips cycle.
		for _, node := range nodes {
			if err := node.Gossip.AddInfo(node.Addr.String(), encoding.EncodeUint64(nil, uint64(cycle)), time.Hour); err != nil {
				log.Fatal(err)
			}
			node.Gossip.SimulationCycle()
		}
		time.Sleep(5 * time.Millisecond)
	}
	log.Infof("gossip network simulation: total infos sent=%d, received=%d", n.InfosSent(), n.InfosReceived())
}

// Stop all servers and gossip nodes.
func (n *Network) Stop() {
	for _, node := range n.Nodes {
		node.Gossip.EnableSimulationCycler(false)
	}
	n.Stopper.Stop()
}

// RunUntilFullyConnected blocks until the gossip network has received
// gossip from every other node in the network. It returns the gossip
// cycle at which the network became fully connected.
func (n *Network) RunUntilFullyConnected() int {
	var connectedAtCycle int
	n.SimulateNetwork(func(cycle int, network *Network) bool {
		if network.IsNetworkConnected() {
			connectedAtCycle = cycle
			return false
		}
		return true
	})
	return connectedAtCycle
}

// IsNetworkConnected returns true if the network is fully connected
// with no partitions (i.e. every node knows every other node's
// network address).
func (n *Network) IsNetworkConnected() bool {
	for _, leftNode := range n.Nodes {
		for _, rightNode := range n.Nodes {
			if _, err := leftNode.Gossip.GetInfo(rightNode.Addr.String()); err != nil {
				return false
			}
		}
	}
	return true
}

// InfosSent returns the total count of infos sent from all nodes in
// the network.
func (n *Network) InfosSent() int {
	var count int
	for _, node := range n.Nodes {
		count += node.Gossip.InfosSent()
	}
	return count
}

// InfosReceived returns the total count of infos received from all
// nodes in the network.
func (n *Network) InfosReceived() int {
	var count int
	for _, node := range n.Nodes {
		count += node.Gossip.InfosReceived()
	}
	return count
}
