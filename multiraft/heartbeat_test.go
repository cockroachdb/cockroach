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
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package multiraft

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/coreos/etcd/raft/raftpb"
)

// processEventsUntil reads and acknowledges messages from the given channel
// until either the given conditional returns true, the channel is closed or a
// read on the channel times out.
func processEventsUntil(ch <-chan *interceptMessage, stopper *util.Stopper, f func(*RaftMessageRequest) bool) {
	for {
		select {
		case e, ok := <-ch:
			if !ok {
				return
			}
			e.ack <- struct{}{}
			if f(e.args.(*RaftMessageRequest)) {
				return
			}
		case <-stopper.ShouldStop():
			return
		}
	}
}

// a heartbeatCondition is invoked when determining whether the intercepted
// stream should be let go. The heartbeatCountMap reflects the heartbeats
// intercepted up to and including the given *RaftMessageRequest.
type heartbeatCondition func(*RaftMessageRequest, heartbeatCountMap) bool

func alwaysFalse(r *RaftMessageRequest) bool {
	return false
}

// a heartbeatCountMap helps us keep track of the amount of heartbeats sent and
// received for a number of nodes, indexed by their NodeID.
type heartbeatCountMap map[uint64]heartbeatCount

func (hcm heartbeatCountMap) Sum() int {
	sum := 0
	for _, v := range hcm {
		sum += v.Sum()
	}
	return sum
}

func (hcm heartbeatCountMap) String() string {
	max := uint64(0)
	for k := range hcm {
		if k > max {
			max = k
		}
	}
	ret := "{ "
	for i := uint64(1); i <= max; i++ {
		ret += fmt.Sprintf("%d: %s, ", i, hcm[i])
	}
	return ret + "}"
}

type heartbeatCount struct {
	reqOut, reqIn, respOut, respIn int
}

func (hc heartbeatCount) Sum() int {
	return hc.reqOut + hc.reqIn + hc.respOut + hc.respIn
}

func (hc heartbeatCount) String() string {
	return fmt.Sprintf("[reqOut=%d reqIn=%d respOut=%d respIn=%d]",
		hc.reqOut, hc.reqIn, hc.respOut, hc.respIn)
}

// countHeartbeats reads intercepted messages from the channel until the given
// conditional returns true (or the channel closes or times out). The returned
// heartbeatCountMap will contain the count of heartbeat requests and responses
// for each nodeID observed in the message stream.
func countHeartbeats(ch <-chan *interceptMessage,
	cond heartbeatCondition) heartbeatCountMap {

	cnt := make(heartbeatCountMap)
	processEventsUntil(ch, nil, func(req *RaftMessageRequest) bool {
		from := cnt[req.Message.From]
		to := cnt[req.Message.To]
		switch req.Message.Type {
		case raftpb.MsgHeartbeat:
			from.reqOut++
			to.reqIn++
		case raftpb.MsgHeartbeatResp:
			from.respOut++
			to.respIn++
		default:
			// Don't evaluate cond() when this is
			// not a heartbeat.
			return false
		}
		cnt[req.Message.From] = from
		cnt[req.Message.To] = to
		return cond(req, cnt)
	})
	return cnt
}

// newBlockingCluster creates and returns a variant of testCluster
// which intercepts and acknowledges all inter-node traffic.
func newBlockingCluster(nodeCount int, stopper *util.Stopper, t *testing.T) *testCluster {
	transport := NewLocalInterceptableTransport(stopper)
	cluster := newTestCluster(transport, nodeCount, stopper, t)
	// Outgoing messages block the client until we acknowledge them, and
	// messages are sent synchronously.
	return cluster
}

// TestHeartbeatSingleGroup makes sure that in a single raft consensus group
// with a ticking master the correct heartbeats are sent and acknowledged.
func TestHeartbeatSingleGroup(t *testing.T) {
	defer leaktest.AfterTest(t)
	for _, nodeCount := range []int{2, 3, 5, 10} {
		for tickCount := range []int{0, 1, 2, 3, 8} {
			validateHeartbeatSingleGroup(nodeCount, tickCount, t)
		}
	}
}

// validateHeartbeatSingleGroup creates a raft group consisting of nodeCount
// nodes, with the first node being elected master. Once elected, the leader
// will tick tickCount times, and the first follower once. The remaining
// followers, if any, will not experience ticks. All heartbeats and responses
// sent by nodes in the system are intercepted and validated.
func validateHeartbeatSingleGroup(nodeCount, tickCount int, t *testing.T) {
	leaderIndex := 0 // first node is leader
	nc := nodeCount
	ltc := tickCount // leader tick count
	// Ticks of first follower. Hard coded to one in the test below, since any
	// higher value can lead to new elections which break the test.
	// Assigning this to a variable helps to make sense of the formulae below.
	ftc := 1

	expCnt := heartbeatCountMap{
		// The leader is the only node that receives responses.
		uint64(leaderIndex + 1): {reqOut: (ltc) * (nc - 1), reqIn: ftc, respOut: 0, respIn: ltc * (nc - 1)},
	}
	// The first follower ticks `ftc` times, but nobody responds.
	expCnt[2] = heartbeatCount{reqOut: (nc - 1) * ftc, reqIn: ltc, respOut: ltc, respIn: 0}
	// The remaining nodes follow the leader and don't tick.
	for i := 2; i < nodeCount; i++ {
		expCnt[uint64(i+1)] = heartbeatCount{reqOut: 0, reqIn: ltc + ftc, respOut: ltc, respIn: 0}
	}

	stopper := util.NewStopper()
	cluster := newBlockingCluster(nc, stopper, t)
	transport := cluster.transport.(*localInterceptableTransport)
	blocker := make(chan struct{})
	// Some more synchronization to prevent many heartbeat responses to be
	// triggered individually; this would lead to responses being optimized
	// away in the send loop and would complicate the count logic.
	readyForTick := make(chan struct{}, 100)
	readyForTick <- struct{}{} // queue initial tick
	ticks := 1                 // ticks queued so far

	go func() {
		// Create group, elect leader, then send ticks as we want them.
		cluster.createGroup(1, 0, nc)
		cluster.triggerElection(leaderIndex, 1)
		for i := 0; i < nodeCount; i++ {
			// Wait for the electon to resolve on all nodes, since fanout behavior
			// is affected by a node's belief about who the leader is.
			cluster.waitForElection(i)
		}
		cluster.tickers[leaderIndex+1].Tick() // Single tick for first follower.
		for i := 0; i < ltc; i++ {
			<-readyForTick
			cluster.tickers[leaderIndex].Tick()
		}
		close(blocker)
	}()

	// The main message processing loop.
	actCnt := countHeartbeats(transport.Events,
		func(req *RaftMessageRequest, cnt heartbeatCountMap) bool {
			// Whenever all followers have sent responses for all of the ticks,
			// we can send the next tick. The only reason for this fairly
			// complicated setup is to guarantee that no responses are
			// optimized away in handleRaftReady, which would make counting the
			// heartbeats trickier.
			tick := true
			for i := 2; i < nodeCount+1; i++ {
				if cnt[uint64(i)].respOut != ticks {
					tick = false
					break
				}
			}
			if tick {
				ticks++
				readyForTick <- struct{}{}
			}
			return cnt.Sum() >= expCnt.Sum()
		})
	// Once done counting, simply process messages.
	stopper.RunWorker(func() {
		processEventsUntil(transport.Events, stopper, alwaysFalse)
	})
	<-blocker
	if !reflect.DeepEqual(actCnt, expCnt) {
		t.Errorf("actual and expected heartbeat counts differ for %d nodes, "+
			"%d leader ticks:\n%v\n%v",
			nc, ltc, actCnt, expCnt)
	}
	stopper.Stop()
}

func TestHeartbeatMultipleGroupsJointLeader(t *testing.T) {
	defer leaktest.AfterTest(t)
	stopper := util.NewStopper()
	cluster := newBlockingCluster(6, stopper, t)
	transport := cluster.transport.(*localInterceptableTransport)
	done := make(chan struct{})
	firstPhase := make(chan struct{})
	secondPhase := make(chan struct{})
	go func() {
		// Create group, elect leader, then send ticks as we want them.
		cluster.createGroup(1, 0, 3)
		cluster.createGroup(2, 2, 3)
		// The node at index 2 (nodeID 3) is contained in both groups
		// Two requests to #1, #2, #4, #5 (from #3).
		cluster.triggerElection(2, 1)
		cluster.triggerElection(2, 2)
		// Wait until it winds up elected twice
		for i := 0; i < 2; i++ {
			if el := cluster.waitForElection(2); el.NodeID != 3 {
				t.Fatalf("wrong leader elected, wanted node 3 but got event %v", el)
			}
		}
		// Same on #4, but mostly to clean out the events channel; this node
		// will be leader later and we want a clean slate.
		if el := cluster.waitForElection(3); el.NodeID != 3 {
			t.Fatalf("wrong leader elected, wanted node 3 but got event %v", el)
		}
		// Request to #2, #3 but no response.
		cluster.tickers[0].Tick()
		// We don't tick node 2 to get some asymmetry.

		// Request to #1, #3 but no response.
		cluster.tickers[1].Tick()
		// Request to #1, #2, #4, #5 with response.
		cluster.tickers[2].Tick()
		// No request, no response (#6 not in any group).
		cluster.tickers[5].Tick()
		// Request to #3 and #4 but no response.
		cluster.tickers[4].Tick()
		// End the first phase. This is necessary to make sure all the pending
		// ticks are being processed before we elect a new leader for phase two.
		<-firstPhase
		// Elect a new leader for the second group.
		// Two requests to #3, #4
		cluster.triggerElection(3, 2)
		if el := cluster.waitForElection(3); el.NodeID != 4 {
			t.Fatalf("wrong leader elected, wanted node 4 but got event %v", el)
		}
		// Same on #3, but mostly to clean out the events channel; this node
		// will be leader later and we want a clean slate.
		if el := cluster.waitForElection(2); el.NodeID != 4 {
			t.Fatalf("wrong leader elected, wanted node 4 but got event %v", el)
		}
		// Requests to #2, #3 without responses.
		cluster.tickers[0].Tick()
		// Requests to #1, #2 with and #4, #5 without responses.
		cluster.tickers[2].Tick()
		// Requests to #3, #5 with responses.
		cluster.tickers[3].Tick()
		// Requests to #3, #4 without responses.
		cluster.tickers[4].Tick()
		<-secondPhase
		// Create the third group
		cluster.createGroup(3, 0, 3)
		// The node at index 2 (NodeID 3) will be leader for both group 1 and 3
		cluster.triggerElection(2, 3)
		if el := cluster.waitForElection(2); el.NodeID != 3 {
			t.Fatalf("wrong leader elected, wanted node 3 but got event %v", el)
		}
		// Requests to #1, #2 with and #4, #5 without responses.
		cluster.tickers[2].Tick()
		close(done)
	}()

	// The main message processing loop.
	expCntFirstPhase := heartbeatCountMap{
		1: {reqOut: 2, reqIn: 2, respOut: 1, respIn: 0},
		2: {reqOut: 2, reqIn: 2, respOut: 1, respIn: 0},
		3: {reqOut: 4, reqIn: 3, respOut: 0, respIn: 4},
		4: {reqOut: 0, reqIn: 2, respOut: 1, respIn: 0},
		5: {reqOut: 2, reqIn: 1, respOut: 1, respIn: 0},
		// NodeID 6 is not member of any Raft group, so it has no peers and
		// consequently must not even show up in the heartbeat count map.
	}
	actCnt := countHeartbeats(transport.Events,
		func(req *RaftMessageRequest, cnt heartbeatCountMap) bool {
			return cnt.Sum() >= expCntFirstPhase.Sum()
		})

	if !reflect.DeepEqual(actCnt, expCntFirstPhase) {
		t.Errorf("phase 1: expected and actual heartbeat counts differ:\n%v\n%v",
			expCntFirstPhase, actCnt)
	}
	close(firstPhase)
	expCntSecondPhase := heartbeatCountMap{
		1: {reqOut: 2, reqIn: 1, respOut: 1, respIn: 0},
		2: {reqOut: 0, reqIn: 2, respOut: 1, respIn: 0},
		3: {reqOut: 4, reqIn: 3, respOut: 1, respIn: 2},
		4: {reqOut: 2, reqIn: 2, respOut: 0, respIn: 2},
		5: {reqOut: 2, reqIn: 2, respOut: 1, respIn: 0},
	}
	actCnt = countHeartbeats(transport.Events,
		func(req *RaftMessageRequest, cnt heartbeatCountMap) bool {
			return cnt.Sum() >= expCntSecondPhase.Sum()
		})

	if !reflect.DeepEqual(actCnt, expCntSecondPhase) {
		t.Errorf("phase 2: expected and actual heartbeat counts differ:\n%v\n%v",
			expCntSecondPhase, actCnt)
	}
	close(secondPhase)
	expCntThirdPhase := heartbeatCountMap{
		1: {reqOut: 0, reqIn: 1, respOut: 1, respIn: 0},
		2: {reqOut: 0, reqIn: 1, respOut: 1, respIn: 0},
		3: {reqOut: 4, reqIn: 0, respOut: 0, respIn: 2},
		4: {reqOut: 0, reqIn: 1, respOut: 0, respIn: 0},
		5: {reqOut: 0, reqIn: 1, respOut: 0, respIn: 0},
	}
	actCnt = countHeartbeats(transport.Events,
		func(req *RaftMessageRequest, cnt heartbeatCountMap) bool {
			return cnt.Sum() >= expCntThirdPhase.Sum()
		})

	if !reflect.DeepEqual(actCnt, expCntThirdPhase) {
		t.Errorf("phase 3: expected and actual heartbeat counts differ:\n%v\n%v",
			expCntThirdPhase, actCnt)
	}
	// Keep processing without inspection and shutdown cluster.
	go processEventsUntil(transport.Events, stopper, alwaysFalse)
	<-done
	stopper.Stop()
}

// TestHeartbeatResponseFanout check 2 raft groups on the same node distribution,
// but each group has different Term, heartbeat response from each group should
// not disturb other group's Term or Leadership
func TestHeartbeatResponseFanout(t *testing.T) {
	defer leaktest.AfterTest(t)
	stopper := util.NewStopper()
	defer stopper.Stop()

	cluster := newTestCluster(nil, 3, stopper, t)
	groupID1 := proto.RaftID(1)
	cluster.createGroup(groupID1, 0, 3 /* replicas */)

	groupID2 := proto.RaftID(2)
	cluster.createGroup(groupID2, 0, 3 /* replicas */)

	leaderIndex := 0

	cluster.triggerElection(leaderIndex, groupID1)
	event := cluster.waitForElection(leaderIndex)
	// Drain off the election event from other nodes.
	_ = cluster.waitForElection((leaderIndex + 1) % 3)
	_ = cluster.waitForElection((leaderIndex + 2) % 3)

	if event.GroupID != groupID1 {
		t.Fatalf("election event had incorrect groupid %v", event.GroupID)
	}
	if event.NodeID != cluster.nodes[leaderIndex].nodeID {
		t.Fatalf("expected %v to win election, but was %v", cluster.nodes[leaderIndex].nodeID, event.NodeID)
	}
	// GroupID2 will have 3 round of election, so it will have different
	// term with groupID1, but both leader on the same node.
	for i := 2; i >= 0; i-- {
		leaderIndex = i
		cluster.triggerElection(leaderIndex, groupID2)
		event = cluster.waitForElection(leaderIndex)
		_ = cluster.waitForElection((leaderIndex + 1) % 3)
		_ = cluster.waitForElection((leaderIndex + 2) % 3)

		if event.GroupID != groupID2 {
			t.Fatalf("election event had incorrect groupid %v", event.GroupID)
		}
		if event.NodeID != cluster.nodes[leaderIndex].nodeID {
			t.Fatalf("expected %v to win election, but was %v", cluster.nodes[leaderIndex].nodeID, event.NodeID)
		}
	}
	// Send a coalesced heartbeat.
	// Heartbeat response from groupID2 will have a big term than which from groupID1.
	cluster.nodes[0].coalescedHeartbeat()
	// Start submit a command to see if groupID1's leader changed?
	cluster.nodes[0].SubmitCommand(groupID1, makeCommandID(), []byte("command"))

	select {
	case _ = <-cluster.events[0].CommandCommitted:
		log.Infof("SubmitCommand succeed after Heartbeat Response fanout")
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("No leader after Heartbeat Response fanout")
	}
}
