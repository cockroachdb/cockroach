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

	"github.com/cockroachdb/cockroach/util/log"
	"github.com/coreos/etcd/raft/raftpb"
)

// processEventsUntil reads and acknowledges messages from the given channel
// until either the given conditional returns true, the channel is closed or a
// read on the channel times out.
func processEventsUntil(ch <-chan *interceptMessage, f func(*RaftMessageRequest) bool) {
	t := time.After(500 * time.Millisecond)
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
		case <-t:
			log.Error("timeout when reading from intercept channel")
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
	processEventsUntil(ch, func(req *RaftMessageRequest) bool {
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

// a blockingCluster is a cluster in which we intercept and acknowledge all
// inter-node traffic.
func blockingCluster(nodeCount int, t *testing.T) *testCluster {
	transport := NewLocalInterceptableTransport()
	cluster := newTestCluster(transport, nodeCount, t)
	// Outgoing messages block the client until we acknowledge them, and
	// messages are sent synchronously.
	return cluster
}

// TestHeartbeatSingleGroup makes sure that in a single raft consensus group
// with a ticking master the correct heartbeats are sent and acknowledged.
func TestHeartbeatSingleGroup(t *testing.T) {
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
	leaderIndex := 0 // first node is leader.
	nc := nodeCount
	ltc := tickCount // leader tick count
	ec := 2          // extra ticks; due to election triggering.
	// Ticks of first follower. Hard coded to one in the test below, since any
	// higher value can lead to new elections which break the test.
	// Assigning this to a valuable helps to make sense of the formulae below.
	ftc := 1

	expCnt := heartbeatCountMap{
		// The leader is the only node that receives responses.
		uint64(leaderIndex + 1): {reqOut: (ec + ltc) * (nc - 1), reqIn: ftc, respOut: 0, respIn: ltc * (nc - 1)},
	}
	// The first follower ticks `ftc` times, but nobody responds.
	expCnt[2] = heartbeatCount{reqOut: (nc - 1) * ftc, reqIn: ec + ltc, respOut: ltc, respIn: 0}
	// The remaining nodes follow the leader and don't tick.
	for i := 2; i < nodeCount; i++ {
		expCnt[uint64(i+1)] = heartbeatCount{reqOut: 0, reqIn: ec + ltc + ftc, respOut: ltc, respIn: 0}
	}

	cluster := blockingCluster(nc, t)
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
		cluster.waitForElection(leaderIndex)
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
	go processEventsUntil(transport.Events, alwaysFalse)
	<-blocker
	if !reflect.DeepEqual(actCnt, expCnt) {
		t.Errorf("actual and expected heartbeat counts differ for %d nodes, "+
			"%d leader ticks:\n%v\n%v",
			nc, ltc, actCnt, expCnt)
	}
	cluster.stop()
}
