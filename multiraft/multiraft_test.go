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
// Author: Ben Darnell

package multiraft

import (
	"testing"
	"time"
)

type testCluster struct {
	nodes  []*state
	clocks []*manualClock
}

func newTestCluster(size int, t *testing.T) *testCluster {
	transport := NewLocalRPCTransport()
	cluster := &testCluster{make([]*state, 0), make([]*manualClock, 0)}
	for i := 0; i < size; i++ {
		clock := newManualClock()
		storage := NewMemoryStorage()
		config := &Config{
			Transport:          transport,
			Storage:            storage,
			Clock:              clock,
			ElectionTimeoutMin: 10 * time.Millisecond,
			ElectionTimeoutMax: 20 * time.Millisecond,
			Strict:             true,
		}
		mr, err := NewMultiRaft(NodeID(i+1), config)
		if err != nil {
			t.Fatal(err)
		}
		state := newState(mr)
		cluster.nodes = append(cluster.nodes, state)
		cluster.clocks = append(cluster.clocks, clock)
	}
	// Let all the states listen before starting any.
	for _, node := range cluster.nodes {
		go node.start()
	}
	return cluster
}

func (c *testCluster) stop() {
	for _, node := range c.nodes {
		node.Stop()
	}
}

func TestInitialLeaderElection(t *testing.T) {
	// Run the test three times, each time triggering a different node's election clock.
	// The node that requests an election first should win.
	for leaderIndex := 0; leaderIndex < 3; leaderIndex++ {
		cluster := newTestCluster(3, t)
		for _, node := range cluster.nodes {
			err := node.CreateGroup(1,
				[]NodeID{cluster.nodes[0].nodeID, cluster.nodes[1].nodeID, cluster.nodes[2].nodeID})
			if err != nil {
				t.Fatal(err)
			}
		}

		cluster.clocks[leaderIndex].triggerElection()

		// Temporary hack: just wait for some instance to declare itself the winner of an
		// election.
		winner := <-hackyTestChannel
		if winner != cluster.nodes[leaderIndex].nodeID {
			t.Errorf("expected %v to win election, but was %v", cluster.nodes[leaderIndex].nodeID, winner)
		}
		cluster.stop()
	}
}
