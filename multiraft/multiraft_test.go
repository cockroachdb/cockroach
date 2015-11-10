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
// Author: Ben Darnell

package multiraft

import (
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/randutil"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"golang.org/x/net/context"
)

var testRand, _ = randutil.NewPseudoRand()

func makeCommandID() string {
	return string(randutil.RandBytes(testRand, CommandIDLen))
}

type testCluster struct {
	t         *testing.T
	nodes     []*state
	tickers   []*manualTicker
	events    []*eventDemux
	storages  []*BlockableStorage
	transport Transport
	// groups maps group IDs to a list of members; members are
	// specified in terms of node index, not node ID.
	groups map[roachpb.RangeID][]int
}

func newTestCluster(transport Transport, size int, stopper *stop.Stopper, t *testing.T) *testCluster {
	if transport == nil {
		transport = NewLocalRPCTransport(stopper)
	}
	stopper.AddCloser(transport)
	cluster := &testCluster{
		t:         t,
		transport: transport,
		groups:    map[roachpb.RangeID][]int{},
	}

	for i := 0; i < size; i++ {
		ticker := newManualTicker()
		storage := &BlockableStorage{storage: NewMemoryStorage()}
		config := &Config{
			Transport:              transport,
			Storage:                storage,
			Ticker:                 ticker,
			ElectionTimeoutTicks:   2,
			HeartbeatIntervalTicks: 1,
			TickInterval:           time.Hour, // not in use
		}
		mr, err := NewMultiRaft(roachpb.NodeID(i+1), roachpb.StoreID(i+1), config, stopper)
		if err != nil {
			t.Fatal(err)
		}
		state := newState(mr)
		demux := newEventDemux(state.Events)
		demux.start(stopper)
		cluster.nodes = append(cluster.nodes, state)
		cluster.tickers = append(cluster.tickers, ticker)
		cluster.events = append(cluster.events, demux)
		cluster.storages = append(cluster.storages, storage)
	}
	cluster.start()
	return cluster
}

func (c *testCluster) start() {
	// Let all the states listen before starting any.
	for _, node := range c.nodes {
		node.start()
	}
}

// createGroup replicates a group consisting of numReplicas members,
// the first being the node at index firstNode.
func (c *testCluster) createGroup(groupID roachpb.RangeID, firstNode, numReplicas int) {
	var replicaIDs []uint64
	for i := 0; i < numReplicas; i++ {
		nodeIndex := firstNode + i
		replicaIDs = append(replicaIDs, uint64(c.nodes[nodeIndex].nodeID))
		c.groups[groupID] = append(c.groups[groupID], nodeIndex)
	}
	for i := 0; i < numReplicas; i++ {
		gs, err := c.storages[firstNode+i].GroupStorage(groupID, 0)
		if err != nil {
			c.t.Fatal(err)
		}
		memStorage := gs.(*blockableGroupStorage).s.(*raft.MemoryStorage)
		if err := memStorage.SetHardState(raftpb.HardState{
			Commit: 10,
			Term:   5,
		}); err != nil {
			c.t.Fatal(err)
		}
		if err := memStorage.ApplySnapshot(raftpb.Snapshot{
			Metadata: raftpb.SnapshotMetadata{
				ConfState: raftpb.ConfState{
					Nodes: replicaIDs,
				},
				Index: 10,
				Term:  5,
			},
		}); err != nil {
			c.t.Fatal(err)
		}

		node := c.nodes[firstNode+i]
		if err := node.CreateGroup(groupID); err != nil {
			c.t.Fatal(err)
		}
	}
}

// triggerElection starts an election in the specified group. In most situations
// the given node will win the election. Unlike elect(), triggerElection() does not
// wait for the election to resolve.
func (c *testCluster) triggerElection(nodeIndex int, groupID roachpb.RangeID) {
	if err := c.nodes[nodeIndex].multiNode.Campaign(context.Background(), uint64(groupID)); err != nil {
		c.t.Fatal(err)
	}
}

// waitForElection waits for the given node to see that an election has been completed.
// Returns the election event, which can be used to see which node was elected.
func (c *testCluster) waitForElection(i int) *EventLeaderElection {
	for {
		e := <-c.events[i].LeaderElection
		if e == nil {
			panic("got nil LeaderElection event, channel likely closed")
		}
		// Ignore events with NodeID 0; these mark elections that are in progress.
		if e.ReplicaID != 0 {
			return e
		}
	}
}

// elect is a simplified wrapper around triggerElection and waitForElection which
// waits for the election to complete on all members of a group.
// TODO(bdarnell): make this work when membership has been changed after creation.
func (c *testCluster) elect(leaderIndex int, groupID roachpb.RangeID) {
	c.triggerElection(leaderIndex, groupID)
	for _, i := range c.groups[groupID] {
		el := c.waitForElection(i)
		// With the in-memory storage used in these tests, replica and node IDs are interchangeable.
		if el.ReplicaID != roachpb.ReplicaID(c.nodes[leaderIndex].nodeID) {
			c.t.Fatalf("wrong leader elected; wanted node %d but got event %v", leaderIndex, el)
		}
		if el.GroupID != groupID {
			c.t.Fatalf("expected election event for group %d but got %d", groupID, el.GroupID)
		}
	}
}

func TestInitialLeaderElection(t *testing.T) {
	defer leaktest.AfterTest(t)
	// Run the test three times, each time triggering a different node's election clock.
	// The node that requests an election first should win.
	for leaderIndex := 0; leaderIndex < 3; leaderIndex++ {
		log.Infof("testing leader election for node %v", leaderIndex)
		stopper := stop.NewStopper()
		cluster := newTestCluster(nil, 3, stopper, t)
		groupID := roachpb.RangeID(1)
		cluster.createGroup(groupID, 0, 3)

		cluster.elect(leaderIndex, groupID)
		stopper.Stop()
	}
}

// TestProposeBadGroup ensures that unknown group IDs are an error, not a panic.
func TestProposeBadGroup(t *testing.T) {
	defer leaktest.AfterTest(t)
	stopper := stop.NewStopper()
	cluster := newTestCluster(nil, 3, stopper, t)
	defer stopper.Stop()
	err := <-cluster.nodes[1].SubmitCommand(7, "asdf", []byte{})
	if err == nil {
		t.Fatal("did not get expected error")
	}
}

func TestLeaderElectionEvent(t *testing.T) {
	defer leaktest.AfterTest(t)
	// Leader election events are fired when the leader commits an entry, not when it
	// issues a call for votes.
	stopper := stop.NewStopper()
	cluster := newTestCluster(nil, 3, stopper, t)
	defer stopper.Stop()
	groupID := roachpb.RangeID(1)
	cluster.createGroup(groupID, 0, 3)

	// Process a Ready with a new leader but no new commits.
	// This happens while an election is in progress.
	// This may be dirty, but it seems this is the only way to make testrace pass.
	cluster.nodes[1].callbackChan <- func() {
		cluster.nodes[1].maybeSendLeaderEvent(groupID, cluster.nodes[1].groups[groupID],
			&raft.Ready{
				SoftState: &raft.SoftState{
					Lead: 3,
				},
			})
	}
	// Trigger multiraft another round select
	cluster.tickers[1].Tick()
	// No events are sent.
	select {
	case e := <-cluster.events[1].LeaderElection:
		t.Fatalf("got unexpected event %v", e)
	case <-time.After(200 * time.Millisecond):
	}

	// Now there are new committed entries. A new leader always commits an entry
	// to conclude the election.
	entry := raftpb.Entry{
		Index: 42,
		Term:  42,
	}
	// This may be dirty, but it seems this is the only way to make testrace pass.
	cluster.nodes[1].callbackChan <- func() {
		cluster.nodes[1].maybeSendLeaderEvent(groupID, cluster.nodes[1].groups[groupID],
			&raft.Ready{
				Entries:          []raftpb.Entry{entry},
				CommittedEntries: []raftpb.Entry{entry},
			})
	}
	cluster.tickers[1].Tick()

	// Now we get an event.
	select {
	case e := <-cluster.events[1].LeaderElection:
		if !reflect.DeepEqual(e, &EventLeaderElection{
			GroupID:   groupID,
			ReplicaID: 3,
			Term:      42,
		}) {
			t.Errorf("election event did not match expectations: %+v", e)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("didn't get expected event")
	}
}

func TestCommand(t *testing.T) {
	defer leaktest.AfterTest(t)
	stopper := stop.NewStopper()
	cluster := newTestCluster(nil, 3, stopper, t)
	defer stopper.Stop()
	groupID := roachpb.RangeID(1)
	cluster.createGroup(groupID, 0, 3)
	cluster.triggerElection(0, groupID)

	// Submit a command to the leader
	cluster.nodes[0].SubmitCommand(groupID, makeCommandID(), []byte("command"))

	// The command will be committed on each node.
	for i, events := range cluster.events {
		log.Infof("waiting for event to be committed on node %v", i)
		commit := <-events.CommandCommitted
		if string(commit.Command) != "command" {
			t.Errorf("unexpected value in committed command: %v", commit.Command)
		}
	}
}

func TestSlowStorage(t *testing.T) {
	defer leaktest.AfterTest(t)
	stopper := stop.NewStopper()
	cluster := newTestCluster(nil, 3, stopper, t)
	defer stopper.Stop()
	groupID := roachpb.RangeID(1)
	cluster.createGroup(groupID, 0, 3)
	cluster.triggerElection(0, groupID)

	// Block the storage on the last node.
	cluster.storages[2].Block()

	// Submit a command to the leader
	cluster.nodes[0].SubmitCommand(groupID, makeCommandID(), []byte("command"))

	// Even with the third node blocked, the other nodes can make progress.
	for i := 0; i < 2; i++ {
		events := cluster.events[i]
		log.Infof("waiting for event to be commited on node %v", i)
		commit := <-events.CommandCommitted
		if string(commit.Command) != "command" {
			t.Errorf("unexpected value in committed command: %v", commit.Command)
		}
	}

	// Ensure that node 2 is in fact blocked.
	time.Sleep(time.Millisecond)
	select {
	case commit := <-cluster.events[2].CommandCommitted:
		t.Errorf("didn't expect commits on node 2 but got %v", commit)
	default:
	}

	// After unblocking the third node, it will catch up.
	cluster.storages[2].Unblock()
	log.Infof("waiting for event to be commited on node 2")
	// When we unblock, the backlog is not guaranteed to be processed in order,
	// and in some cases the leader may need to retransmit some messages.
	for i := 0; i < 3; i++ {
		select {
		case commit := <-cluster.events[2].CommandCommitted:
			if string(commit.Command) != "command" {
				t.Errorf("unexpected value in committed command: %v", commit.Command)
			}
			return

		case <-time.After(5 * time.Millisecond):
			// Tick both node's clocks. The ticks on the follower node don't
			// really do anything, but they do ensure that that goroutine is
			// getting scheduled (and the real-time delay allows rpc responses
			// to pass between the nodes)
			cluster.tickers[0].Tick()
			cluster.tickers[2].Tick()
		}
	}
}

func TestMembershipChange(t *testing.T) {
	defer leaktest.AfterTest(t)
	stopper := stop.NewStopper()
	cluster := newTestCluster(nil, 4, stopper, t)
	defer stopper.Stop()

	// Create a group with a single member, cluster.nodes[0].
	groupID := roachpb.RangeID(1)
	cluster.createGroup(groupID, 0, 1)
	// An automatic election is triggered since this is a single-node Raft group,
	// so we don't need to call triggerElection.

	// Consume and apply the membership change events.
	for i := 0; i < 4; i++ {
		go func(i int) {
			for {
				e, ok := <-cluster.events[i].MembershipChangeCommitted
				if !ok {
					return
				}
				e.Callback(nil)
			}
		}(i)
	}

	// Add each of the other three nodes to the cluster.
	for i := 1; i < 4; i++ {
		ch := cluster.nodes[0].ChangeGroupMembership(groupID, makeCommandID(),
			raftpb.ConfChangeAddNode,
			roachpb.ReplicaDescriptor{
				NodeID:    cluster.nodes[i].nodeID,
				StoreID:   roachpb.StoreID(cluster.nodes[i].nodeID),
				ReplicaID: roachpb.ReplicaID(cluster.nodes[i].nodeID),
			}, nil)
		<-ch
	}

	// TODO(bdarnell): verify that the channel events are sent out correctly.
	/*
		for i := 0; i < 10; i++ {
			log.Infof("tick %d", i)
			cluster.tickers[0].Tick()
			time.Sleep(5 * time.Millisecond)
		}

		// Each node is notified of each other node's joining.
		for i := 0; i < 4; i++ {
			for j := 1; j < 4; j++ {
				select {
				case e := <-cluster.events[i].MembershipChangeCommitted:
					if e.NodeID != cluster.nodes[j].nodeID {
						t.Errorf("node %d expected event for %d, got %d", i, j, e.NodeID)
					}
				default:
					t.Errorf("node %d did not get expected event for %d", i, j)
				}
			}
		}*/
}

// TestRemoveLeader ensures that a group will recover if a node is
// removed from the group while it is leader. Since visibility into
// the raft state is limited, we create a three-node group in a
// six-node cluster. This group is migrated one node at a time from
// the first three nodes to the last three. In the process the initial
// leader must have removed itself.
func TestRemoveLeader(t *testing.T) {
	defer leaktest.AfterTest(t)
	stopper := stop.NewStopper()
	const clusterSize = 6
	const groupSize = 3
	cluster := newTestCluster(nil, clusterSize, stopper, t)
	defer stopper.Stop()

	// Consume and apply the membership change events.
	for i := 0; i < clusterSize; i++ {
		go func(i int) {
			for {
				if e, ok := <-cluster.events[i].MembershipChangeCommitted; ok {
					e.Callback(nil)
				} else {
					return
				}
			}
		}(i)
	}

	// Tick all the clocks in the background to ensure that all the
	// necessary elections are triggered.
	// TODO(bdarnell): newTestCluster should have an option to use a
	// real clock instead of a manual one.
	stopper.RunWorker(func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopper.ShouldStop():
				return
			case <-ticker.C:
				for _, t := range cluster.tickers {
					t.NonBlockingTick()
				}
			}
		}
	})

	// Create a group with three members.
	groupID := roachpb.RangeID(1)
	cluster.createGroup(groupID, 0, groupSize)

	// Move the group one node at a time from the first three nodes to
	// the last three. In the process, we necessarily remove the leader
	// and trigger at least one new election among the new nodes.
	for i := 0; i < groupSize; i++ {
		log.Infof("adding node %d", i+groupSize)
		ch := cluster.nodes[i].ChangeGroupMembership(groupID, makeCommandID(),
			raftpb.ConfChangeAddNode,
			roachpb.ReplicaDescriptor{
				NodeID:    cluster.nodes[i+groupSize].nodeID,
				StoreID:   roachpb.StoreID(cluster.nodes[i+groupSize].nodeID),
				ReplicaID: roachpb.ReplicaID(cluster.nodes[i+groupSize].nodeID),
			}, nil)
		if err := <-ch; err != nil {
			t.Fatal(err)
		}

		log.Infof("removing node %d", i)
		ch = cluster.nodes[i].ChangeGroupMembership(groupID, makeCommandID(),
			raftpb.ConfChangeRemoveNode,
			roachpb.ReplicaDescriptor{
				NodeID:    cluster.nodes[i].nodeID,
				StoreID:   roachpb.StoreID(cluster.nodes[i].nodeID),
				ReplicaID: roachpb.ReplicaID(cluster.nodes[i].nodeID),
			}, nil)
		if err := <-ch; err != nil {
			t.Fatal(err)
		}
	}
}

func TestRapidMembershipChange(t *testing.T) {
	defer leaktest.AfterTest(t)
	stopper := stop.NewStopper()
	defer stopper.Stop()

	var wg sync.WaitGroup
	proposers := 5

	numCommit := int32(200)

	cluster := newTestCluster(nil, 1, stopper, t)
	groupID := roachpb.RangeID(1)

	cluster.createGroup(groupID, 0, 1 /* replicas */)
	startSeq := int32(0) // updated atomically from now on

	cmdIDFormat := "%0" + fmt.Sprintf("%d", CommandIDLen) + "d"
	teardown := make(chan struct{})

	proposerFn := func(i int) {
		defer wg.Done()

		var seq int32
		for {
			seq = atomic.AddInt32(&startSeq, 1)
			if seq > numCommit {
				break
			}
			cmdID := fmt.Sprintf(cmdIDFormat, seq)
		retry:
			for {
				if err := cluster.nodes[0].CreateGroup(groupID); err != nil {
					t.Fatal(err)
				}
				if log.V(1) {
					log.Infof("%-3d: try    %s", i, cmdID)
				}

				select {
				case err := <-cluster.nodes[0].SubmitCommand(groupID,
					cmdID, []byte("command")):
					if err == nil {
						log.Infof("%-3d: ok   %s", i, cmdID)
						break retry
					}
					log.Infof("%-3d: err  %s %s", i, cmdID, err)
				case <-teardown:
					return
				}
			}
			if err := cluster.nodes[0].RemoveGroup(groupID); err != nil {
				t.Fatal(err)
			}
		}
	}

	for i := 0; i < proposers; i++ {
		wg.Add(1)
		go proposerFn(i)
	}

	for e := range cluster.events[0].CommandCommitted {
		if log.V(1) {
			log.Infof("   : recv %s", e.CommandID)
		}
		if fmt.Sprintf(cmdIDFormat, numCommit) == e.CommandID {
			log.Infof("received everything we asked for, ending test")
			break
		}

	}
	close(teardown)
	// Because ending the test case is racy with the test itself, we wait until
	// all our goroutines have finished their work before we allow the test to
	// forcible terminate. This solves a race condition on `t`, which is
	// otherwise subject to concurrent access from our goroutine and the go
	// testing machinery.
	wg.Wait()
}

// TestConfigValidation verifies that the validation of a Config returns
// an error if tick-related config has a non-positive value.
func TestConfigValidation(t *testing.T) {
	defer leaktest.AfterTest(t)
	stopper := stop.NewStopper()
	defer stopper.Stop()
	transport := NewLocalRPCTransport(stopper)
	validConfig := Config{
		Transport:              transport,
		Storage:                nil,
		Ticker:                 nil,
		ElectionTimeoutTicks:   1,
		HeartbeatIntervalTicks: 2,
		TickInterval:           time.Hour,
	}
	if err := validConfig.validate(); err != nil {
		t.Error(err)
	}

	config := validConfig
	config.ElectionTimeoutTicks = -1
	if err := config.validate(); !testutils.IsError(err,
		"ElectionTimeoutTicks must be greater than zero") {
		t.Errorf("Unexpected error of validate: %s", err)
	}

	config = validConfig
	config.ElectionTimeoutTicks = 0
	if err := config.validate(); !testutils.IsError(err,
		"ElectionTimeoutTicks must be greater than zero") {
		t.Errorf("Unexpected error of validate: %s", err)
	}

	config = validConfig
	config.HeartbeatIntervalTicks = -1
	if err := config.validate(); !testutils.IsError(err,
		"HeartbeatIntervalTicks must be greater than zero") {
		t.Errorf("Unexpected error of validate: %s", err)
	}

	config = validConfig
	config.HeartbeatIntervalTicks = 0
	if err := config.validate(); !testutils.IsError(err,
		"HeartbeatIntervalTicks must be greater than zero") {
		t.Errorf("Unexpected error of validate: %s", err)
	}

	config = validConfig
	config.TickInterval = -1 * time.Hour
	if err := config.validate(); !testutils.IsError(err,
		"TickInterval must be greater than zero") {
		t.Errorf("Unexpected error of validate: %s", err)
	}

	config = validConfig
	config.TickInterval = 0
	if err := config.validate(); !testutils.IsError(err,
		"TickInterval must be greater than zero") {
		t.Errorf("Unexpected error of validate: %s", err)
	}
}
