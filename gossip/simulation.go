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
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package gossip

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"os"
	"time"
)

const (
	// minLocalhostPort is the starting port in a range of ports to use
	// for simulating a gossip network.
	minLocalhostPort = 9000
)

var (
	port = minLocalhostPort
)

// init seeds the random number generator for non-determinism across
// multiple runs.
func init() {
	rand.Seed(time.Now().UTC().UnixNano())
}

// tempUnixFile creates a temporary file for use with a unix domain socket.
func tempUnixFile() string {
	f, err := ioutil.TempFile("", "unix-socket")
	if err != nil {
		log.Fatalf("unable to create temp file: %s", err)
	}
	f.Close()
	os.Remove(f.Name())
	return f.Name()
}

// tempLocalhostAddr creates an address to localhost using a monotonically
// increasing port number in the range [minLocalhostPort, ...].
func tempLocalhostAddr() string {
	str := fmt.Sprintf("127.0.0.1:%d", port)
	port++
	return str
}

// createSimAddr creates an unused address for simulation gossip nodes.
// The "network" parameter should be one of "tcp" or "unix".
func createSimAddr(network string) (net.Addr, error) {
	switch network {
	case "tcp":
		return net.ResolveTCPAddr("tcp", tempLocalhostAddr())
	case "unix":
		return net.ResolveUnixAddr("unix", tempUnixFile())
	}
	return nil, fmt.Errorf("unknown network type: %s", network)
}

// SimulateNetwork creates nodeCount gossip nodes. The network should
// be set to either "tcp" or "unix". The gossipInterval should be set
// to a compressed simulation timescale, though large enough to give
// the concurrent goroutines enough time to pass data back and forth
// in order to yield accurate estimates of how old data actually ends
// up being at the various nodes. After each gossipInterval period,
// simCallback is invoked; when it returns false, the simulation
// ends. If it returns true, the simulation continues another cycle.
//
// Node0 gossips the node count as well as the gossip sentinel. The
// gossip bootstrap hosts are set to the first three nodes (or fewer if
// less than three are available).
//
// At each cycle of the simulation, node 0 gossips the sentinel. If
// the simulation requires other nodes to gossip, this should be done
// via simCallback.
//
// The simulation callback receives a map of nodes, keyed by node address.
func SimulateNetwork(nodeCount int, network string, gossipInterval time.Duration,
	simCallback func(cycle int, nodes map[string]*Gossip) bool) {

	log.Printf("simulating network with %d nodes", nodeCount)
	addrs := make([]net.Addr, nodeCount)
	for i := 0; i < nodeCount; i++ {
		addr, err := createSimAddr(network)
		if err != nil {
			log.Fatalf("failed to create address: %s", err)
		}
		addrs[i] = addr
	}
	var bootstrap []net.Addr
	if nodeCount < 3 {
		bootstrap = addrs
	} else {
		bootstrap = addrs[:3]
	}

	nodes := make(map[string]*Gossip, nodeCount)
	for i := 0; i < nodeCount; i++ {
		node := New(addrs[i])
		node.Name = fmt.Sprintf("Node%d", i)
		node.SetBootstrap(bootstrap)
		node.SetInterval(gossipInterval)
		// Node 0 gossips node count.
		if i == 0 {
			node.AddInfo(NodeCountGossip, Int64Value(nodeCount), time.Hour)
		}
		node.Start()
		nodes[addrs[i].String()] = node
	}

	gossipTimeout := time.Tick(gossipInterval)
	var complete bool
	for cycle := 0; !complete; cycle++ {
		select {
		case <-gossipTimeout:
			// Node 0 gossips sentinel every cycle.
			nodes[addrs[0].String()].AddInfo(SentinelGossip, Int64Value(cycle), time.Hour)
			if !simCallback(cycle, nodes) {
				complete = true
			}
		}
	}

	// Close all nodes.
	for _, node := range nodes {
		node.Close()
	}
}
