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

package gossip

import (
	"fmt"
	"net"
	"time"

	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

// SimulationNode represents a node used in a SimulationNetwork.
// It includes information about the node's gossip instance,
// network address, and underlying server.
type SimulationNode struct {
	Gossip *Gossip
	Addr   net.Addr
	Server *rpc.Server
}

// SimulationNetwork provides access to a test gossip network of nodes.
type SimulationNetwork struct {
	Nodes          []*SimulationNode
	Addrs          []net.Addr
	NetworkType    string        // "tcp" or "unix"
	GossipInterval time.Duration // The length of a round of gossip
}

// DefaultTestGossipInterval is one possible compressed simulation
// on time scale for testing gossip networks.
const DefaultTestGossipInterval = 10 * time.Millisecond

// NewSimulationNetwork creates nodeCount gossip nodes. The networkType should
// be set to either "tcp" or "unix". The gossipInterval should be set
// to a compressed simulation timescale, though large enough to give
// the concurrent goroutines enough time to pass data back and forth
// in order to yield accurate estimates of how old data actually ends
// up being at the various nodes (e.g. DefaultTestGossipInterval).
func NewSimulationNetwork(nodeCount int, networkType string,
	gossipInterval time.Duration) *SimulationNetwork {

	tlsConfig := rpc.LoadInsecureTLSConfig()

	log.Infof("simulating gossip network with %d nodes", nodeCount)
	servers := make([]*rpc.Server, nodeCount)
	addrs := make([]net.Addr, nodeCount)
	for i := 0; i < nodeCount; i++ {
		addr := util.CreateTestAddr(networkType)
		servers[i] = rpc.NewServer(addr, tlsConfig)
		if err := servers[i].Start(); err != nil {
			log.Fatal(err)
		}
		addrs[i] = servers[i].Addr()
	}
	var bootstrap []net.Addr
	if nodeCount < 3 {
		bootstrap = addrs
	} else {
		bootstrap = addrs[:3]
	}

	nodes := make([]*SimulationNode, nodeCount)
	for i := 0; i < nodeCount; i++ {
		node := New(tlsConfig)
		node.Name = fmt.Sprintf("Node%d", i)
		node.SetBootstrap(bootstrap)
		node.SetInterval(gossipInterval)
		node.Start(servers[i])
		// Node 0 gossips node count.
		if i == 0 {
			node.AddInfo(KeyNodeCount, int64(nodeCount), time.Hour)
		}
		nodes[i] = &SimulationNode{Gossip: node, Addr: addrs[i], Server: servers[i]}
	}

	return &SimulationNetwork{
		Nodes:          nodes,
		Addrs:          addrs,
		NetworkType:    networkType,
		GossipInterval: gossipInterval}
}

// GetNodeFromAddr returns the simulation node associated
// with provided network address, or nil if there is no such node.
func (n *SimulationNetwork) GetNodeFromAddr(addr string) (*SimulationNode, bool) {
	for i := 0; i < len(n.Nodes); i++ {
		if n.Nodes[i].Addr.String() == addr {
			return n.Nodes[i], true
		}
	}
	return nil, false
}

// SimulateNetwork runs a number of gossipInterval periods within the given
// SimulationNetwork. After each gossipInterval period, simCallback is invoked.
// When it returns false, the simulation ends. If it returns true, the
// simulation continues another cycle.
//
// Node0 gossips the node count as well as the gossip sentinel. The gossip
// bootstrap hosts are set to the first three nodes (or fewer if less than
// three are available).
//
// At each cycle of the simulation, node 0 gossips the sentinel. If the
// simulation requires other nodes to gossip, this should be done via
// simCallback.
//
// The simulation callback receives a map of nodes, keyed by node address.
func (n *SimulationNetwork) SimulateNetwork(
	simCallback func(cycle int, network *SimulationNetwork) bool) {
	gossipTimeout := time.Tick(n.GossipInterval)
	nodes := n.Nodes
	var complete bool
	for cycle := 0; !complete; cycle++ {
		select {
		case <-gossipTimeout:
			// Node 0 gossips sentinel every cycle.
			nodes[0].Gossip.AddInfo(KeySentinel, int64(cycle), time.Hour)
			if !simCallback(cycle, n) {
				complete = true
			}
		}
	}
}

// Stop all servers and gossip nodes.
func (n *SimulationNetwork) Stop() {
	for i := 0; i < len(n.Nodes); i++ {
		n.Nodes[i].Server.Close()
		n.Nodes[i].Gossip.Stop()
	}
}

// RunUntilFullyConnected blocks until the gossip network has received gossip
// from every other node in the network. It returns the gossip cycle at which
// the network became fully connected.
func (n *SimulationNetwork) RunUntilFullyConnected() int {
	var connectedAtCycle int
	n.SimulateNetwork(func(cycle int, network *SimulationNetwork) bool {
		nodes := network.Nodes
		// Every node should gossip.
		for i := 0; i < len(nodes); i++ {
			nodes[i].Gossip.AddInfo(nodes[i].Addr.String(), int64(cycle), time.Hour)
		}
		if network.isNetworkConnected() {
			connectedAtCycle = cycle
			return false
		}
		return true
	})
	return connectedAtCycle
}

// isNetworkConnected returns true if the network is fully connected with
// no partitions (i.e. every node knows every other node's network address).
func (n *SimulationNetwork) isNetworkConnected() bool {
	for i := 0; i < len(n.Nodes); i++ {
		for keyIdx := 0; keyIdx < len(n.Addrs); keyIdx++ {
			_, err := n.Nodes[i].Gossip.GetInfo(n.Addrs[keyIdx].String())
			if err != nil {
				log.Infof("error: %v", err)
				return false
			}
		}
	}
	return true
}
