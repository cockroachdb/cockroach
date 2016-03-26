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
// permissions and limitations under the License.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package simulation

import (
	"crypto/tls"
	"net"
	"time"

	"google.golang.org/grpc"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/gossip/resolver"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/stop"
)

// Node represents a node used in a Network. It includes information
// about the node's gossip instance, network address, and underlying
// server.
type Node struct {
	Gossip *gossip.Gossip
	Server *grpc.Server
	Addr   net.Addr
}

// Network provides access to a test gossip network of nodes.
type Network struct {
	Nodes           []*Node
	Stopper         *stop.Stopper
	nodeIDAllocator roachpb.NodeID // provides unique node IDs
	rpcContext      *rpc.Context
	tlsConfig       *tls.Config
}

// NewNetwork creates nodeCount gossip nodes.
func NewNetwork(nodeCount int) *Network {
	log.Infof("simulating gossip network with %d nodes", nodeCount)

	n := &Network{
		Nodes:   []*Node{},
		Stopper: stop.NewStopper(),
	}
	n.rpcContext = rpc.NewContext(&base.Context{Insecure: true}, nil, n.Stopper)
	var err error
	n.tlsConfig, err = n.rpcContext.GetServerTLSConfig()
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < nodeCount; i++ {
		node, err := n.CreateNode()
		if err != nil {
			log.Fatal(err)
		}
		// Build a resolver for each instance or we'll get data races.
		r, err := resolver.NewResolverFromAddress(n.Nodes[0].Addr)
		if err != nil {
			log.Fatalf("bad gossip address %s: %s", n.Nodes[0].Addr, err)
		}
		node.Gossip.SetResolvers([]resolver.Resolver{r})
		if err := n.StartNode(node); err != nil {
			log.Fatal(err)
		}
	}
	return n
}

// CreateNode creates a simulation node and starts an RPC server for it.
func (n *Network) CreateNode() (*Node, error) {
	server := rpc.NewServer(n.rpcContext)
	ln, err := util.ListenAndServeGRPC(n.Stopper, server, util.TestAddr)
	if err != nil {
		return nil, err
	}
	node := &Node{Server: server, Addr: ln.Addr()}
	node.Gossip = gossip.New(n.rpcContext, nil, n.Stopper)
	n.Nodes = append(n.Nodes, node)
	return node, nil
}

// StartNode initializes a gossip instance for the simulation node and
// starts it.
func (n *Network) StartNode(node *Node) error {
	node.Gossip.Start(node.Server, node.Addr)
	node.Gossip.EnableSimulationCycler(true)
	n.nodeIDAllocator++
	node.Gossip.SetNodeID(n.nodeIDAllocator)
	if err := node.Gossip.SetNodeDescriptor(&roachpb.NodeDescriptor{
		NodeID:  node.Gossip.GetNodeID(),
		Address: util.MakeUnresolvedAddr(node.Addr.Network(), node.Addr.String()),
	}); err != nil {
		return err
	}
	if err := node.Gossip.AddInfo(node.Addr.String(),
		encoding.EncodeUint64Ascending(nil, 0), time.Hour); err != nil {
		return err
	}
	return nil
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
		// Node 0 gossips sentinel & cluster ID every cycle.
		if err := nodes[0].Gossip.AddInfo(
			gossip.KeySentinel,
			encoding.EncodeUint64Ascending(nil, uint64(cycle)),
			time.Hour); err != nil {
			log.Fatal(err)
		}
		if err := nodes[0].Gossip.AddInfo(gossip.KeyClusterID,
			encoding.EncodeUint64Ascending(nil, uint64(cycle)), 0*time.Second); err != nil {
			log.Fatal(err)
		}
		// Every node gossips cycle.
		for _, node := range nodes {
			if err := node.Gossip.AddInfo(
				node.Addr.String(),
				encoding.EncodeUint64Ascending(nil, uint64(cycle)),
				time.Hour); err != nil {
				log.Fatal(err)
			}
			node.Gossip.SimulationCycle()
		}
		time.Sleep(5 * time.Millisecond)
	}
	log.Infof("gossip network simulation: total infos sent=%d, received=%d", n.infosSent(), n.infosReceived())
}

// Stop stops all servers and gossip nodes.
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
			if _, err := leftNode.Gossip.GetInfo(rightNode.Addr.String()); err != nil {
				return false
			}
		}
	}
	return true
}

// infosSent returns the total count of infos sent from all nodes in
// the network.
func (n *Network) infosSent() int {
	var count int
	for _, node := range n.Nodes {
		count += node.Gossip.InfosSent()
	}
	return count
}

// infosReceived returns the total count of infos received from all
// nodes in the network.
func (n *Network) infosReceived() int {
	var count int
	for _, node := range n.Nodes {
		count += node.Gossip.InfosReceived()
	}
	return count
}
