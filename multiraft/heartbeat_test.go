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
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/util/log"
	"github.com/coreos/etcd/raft/raftpb"
)

func processEventsUntil(ch <-chan *interceptMessage, f func(*RaftMessageRequest) bool) {
	for e := range ch {
		e.ack <- struct{}{}
		if f(e.args.(*RaftMessageRequest)) {
			return
		}
	}
}

// validateHeartbeatCount receives messages from the given channel and compares
// the observed number of heartbeat requests and responses.
// Once the channel closes or enough heartbeat responses have been observed,
// the function returns.
func validateHeartbeatCount(ch <-chan *interceptMessage,
	heartbeatCount, heartbeatResponseCount int, t *testing.T) {
	// maps nodeID to heartbeats sent by this node minus heartbeat
	// responses received by this node.
	outInDelta := make(map[uint64]int)
	cntH := 0
	cntR := 0
	defer func() {
		for nodeID, delta := range outInDelta {
			// If one of the checks below fails, this information will be useful.
			log.Infof("node %v: %d heartbeats without response", nodeID, delta)
		}
		if cntH != heartbeatCount {
			t.Errorf("unexpected total number of heartbeats: %d (expected %d)",
				cntH, heartbeatCount)
		}
		if cntR != heartbeatResponseCount {
			t.Errorf("unexpected total number of heartbeat responses: %d (expected %d)",
				cntR, heartbeatResponseCount)
		}
	}()
	for iMsg := range ch {
		iMsg.ack <- struct{}{}
		req := iMsg.args.(*RaftMessageRequest)
		switch req.Message.Type {
		case raftpb.MsgHeartbeat:
			cntH++
			outInDelta[req.Message.From]++
		case raftpb.MsgHeartbeatResp:
			cntR++
			nodeID := req.Message.To
			outInDelta[nodeID]--
			if outInDelta[nodeID] < 0 {
				t.Errorf("node %v: more responses received than heartbeats sent",
					nodeID)
			}
		}
		// If we've already received all the heartbeat responses that
		// we have asked for, return. Without this, it's more awkward
		// to know when to shut down the cluster.
		if cntR >= heartbeatResponseCount {
			return
		}
	}
}

func TestHeartbeat(t *testing.T) {
	nodeCount := 3
	cluster := newTestCluster(nodeCount, t)
	// Outgoing messages block the client until we acknowledge them, and
	// messages are sent synchronously.
	cluster.transport.EnableEvents()

	cluster.start()
	leaderTickCount := 18
	// Since waitForElection() currently triggers two ticks, it also triggers
	// some heartbeats which remain unanswered (the sender isn't leader yet).
	unansweredHeartbeats := (nodeCount - 1) * 2 // 2 because of 2 election ticks.

	// How many heartbeats and heartbeat responses we expect in total.
	expHeartbeatCount := (nodeCount-1)*leaderTickCount + unansweredHeartbeats
	expHeartbeatRespCount := (nodeCount - 1) * leaderTickCount

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		// Run the actual checks, consuming from the Events channel.
		validateHeartbeatCount(cluster.transport.Events, expHeartbeatCount,
			expHeartbeatRespCount, t)
		// Once we're here, we may begin tearing down.
		wg.Done()
		// For the rest of this test, simply keep acknowledging messages
		// so that Raft makes progress. This mostly serves to avoid deadlock
		// in cases where there is unexpected behaviour.
		processEventsUntil(cluster.transport.Events, func(r *RaftMessageRequest) bool {
			return false
		})
	}()

	groupID := uint64(1)
	leaderIndex := 0
	cluster.createGroup(groupID, 3)
	election := cluster.waitForElection(leaderIndex)

	for i := 0; i < leaderTickCount; i++ {
		cluster.tickers[leaderIndex].Tick()
	}

	wg.Wait()

	cluster.stop()

	// No further elections should have happened in the meantime.
	for i := 0; i < nodeCount; i++ {
		for {
			depleted := false
			select {
			case ev := <-cluster.events[i].LeaderElection:
				if ev.Term != election.Term {
					t.Errorf("node %v: unexpected election event: %v (original election: %v)", i, ev, election)
				}
			case <-time.After(time.Millisecond):
				depleted = true
			}
			if depleted {
				break
			}
		}
	}
	close(cluster.transport.Events)
}
