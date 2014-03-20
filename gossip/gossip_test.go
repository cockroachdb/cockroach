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
	"testing"
	"time"
)

const (
	// Compressed simulation time scale for testing.
	testGossipInterval = time.Millisecond * 10
)

// isNetworkConnected returns true if the network is fully connected with
// no partitions.
func isNetworkConnected(nodes map[string]*Gossip) bool {
	for _, node := range nodes {
		for infoKey := range nodes {
			if node.GetInfo(infoKey) == nil {
				return false
			}
		}
	}
	return true
}

// verifyConvergence verifies that info from each node is visible from
// every node in the network within numCycles cycles of the gossip protocol.
func verifyConvergence(numNodes, maxCycles int, t *testing.T) {
	var connectedAtCycle int
	SimulateNetwork(numNodes, "unix", testGossipInterval, func(cycle int, nodes map[string]*Gossip) bool {
		// Every node should gossip.
		for addr, node := range nodes {
			node.AddInfo(addr, Int64Value(cycle), time.Hour)
		}
		if isNetworkConnected(nodes) {
			connectedAtCycle = cycle
			return false
		}
		return true
	})

	if connectedAtCycle > maxCycles {
		t.Errorf("expected a fully-connected network within 5 cycles; took %d", connectedAtCycle)
	}
}

// TestConvergence verifies a 10 node gossip network
// converges within 5 cycles.
func TestConvergence(t *testing.T) {
	verifyConvergence(10, 5, t)
}
