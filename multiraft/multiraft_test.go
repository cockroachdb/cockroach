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

	"github.com/golang/glog"
)

type testCluster struct {
	t      *testing.T
	nodes  []*state
	clocks []*manualClock
	events []*eventDemux
}

func newTestCluster(size int, t *testing.T) *testCluster {
	transport := NewLocalRPCTransport()
	cluster := &testCluster{t: t}
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
		demux := newEventDemux(state.Events)
		demux.start()
		cluster.nodes = append(cluster.nodes, state)
		cluster.clocks = append(cluster.clocks, clock)
		cluster.events = append(cluster.events, demux)
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
	for _, demux := range c.events {
		demux.stop()
	}
}

// createGroup replicates a group among the first numReplicas nodes in the cluster
func (c *testCluster) createGroup(groupID GroupID, numReplicas int) {
	var replicaIDs []NodeID
	var replicaNodes []*state
	for i := 0; i < numReplicas; i++ {
		replicaNodes = append(replicaNodes, c.nodes[i])
		replicaIDs = append(replicaIDs, c.nodes[i].nodeID)
	}
	for _, node := range replicaNodes {
		err := node.CreateGroup(groupID, replicaIDs)
		if err != nil {
			c.t.Fatal(err)
		}
	}
}

func TestInitialLeaderElection(t *testing.T) {
	// Run the test three times, each time triggering a different node's election clock.
	// The node that requests an election first should win.
	for leaderIndex := 0; leaderIndex < 3; leaderIndex++ {
		cluster := newTestCluster(3, t)
		groupID := GroupID(1)
		cluster.createGroup(groupID, 3)

		cluster.clocks[leaderIndex].triggerElection()

		event := <-cluster.events[leaderIndex].LeaderElection
		if event.GroupID != groupID {
			t.Fatalf("election event had incorrect group id %v", event.GroupID)
		}
		if event.NodeID != cluster.nodes[leaderIndex].nodeID {
			t.Fatalf("expected %v to win election, but was %v", cluster.nodes[leaderIndex].nodeID,
				event.NodeID)
		}
		cluster.stop()
	}
}

func TestCommand(t *testing.T) {
	cluster := newTestCluster(3, t)
	groupID := GroupID(1)
	cluster.createGroup(groupID, 3)
	// TODO(bdarnell): once followers can forward to leaders, don't wait for the election here.
	cluster.clocks[0].triggerElection()
	<-cluster.events[0].LeaderElection

	// Submit a command to the leader
	cluster.nodes[0].SubmitCommand(groupID, []byte("command"))

	// The command will be committed on each node.
	for i, events := range cluster.events {
		glog.Infof("waiting for event to be commited on node %v", i)
		commit := <-events.CommandCommitted
		if string(commit.Command) != "command" {
			t.Errorf("unexpected value in committed command: %v", commit.Command)
		}
	}
}
