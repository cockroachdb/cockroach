// This code has been modified from its original form by The Cockroach Authors.
// All modifications are Copyright 2024 The Cockroach Authors.
//
// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rafttest

import (
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/raft/raftstoreliveness"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
)

type node struct {
	id     raftpb.PeerID
	iface  iface
	stopc  chan struct{}
	pausec chan bool

	mu struct {
		sync.Mutex
		rn *raft.RawNode
	}

	// storage is the stable storage. Accessed exclusively by the Ready handling
	// loop in run().
	storage *raft.MemoryStorage
}

func startNode(id raftpb.PeerID, peers []raft.Peer, iface iface) *node {
	st := raft.NewMemoryStorage()
	c := &raft.Config{
		ID:                        id,
		ElectionTick:              50,
		HeartbeatTick:             1,
		Storage:                   st,
		MaxSizePerMsg:             1024 * 1024,
		MaxInflightMsgs:           256,
		MaxUncommittedEntriesSize: 1 << 30,
		StoreLiveness:             raftstoreliveness.AlwaysLive{},
		CRDBVersion:               cluster.MakeTestingClusterSettings().Version,
		Metrics:                   raft.NewMetrics(),
	}
	rn, err := raft.NewRawNode(c)
	if err != nil {
		panic(err)
	}
	if err := rn.Bootstrap(peers); err != nil {
		panic(err)
	}
	n := &node{
		id:      id,
		iface:   iface,
		pausec:  make(chan bool),
		storage: st,
	}
	n.mu.rn = rn
	n.start()
	return n
}

func (n *node) start() {
	n.stopc = make(chan struct{})

	hasReady := make(chan struct{}, 1)
	maybeSignalLocked := func() {
		if n.mu.rn.HasReady() {
			select {
			case hasReady <- struct{}{}:
			default:
			}
		}
	}

	tick := func() {
		n.mu.Lock()
		defer n.mu.Unlock()
		n.mu.rn.Tick()
		maybeSignalLocked()
	}
	step := func(messages ...raftpb.Message) {
		n.mu.Lock()
		defer n.mu.Unlock()
		for _, m := range messages {
			_ = n.mu.rn.Step(m)
		}
		maybeSignalLocked()
	}
	getReady := func() (raft.Ready, bool) {
		n.mu.Lock()
		defer n.mu.Unlock()
		if !n.mu.rn.HasReady() {
			return raft.Ready{}, false
		}
		return n.mu.rn.Ready(), true
	}
	handleReady := func(rd raft.Ready) {
		if !raft.IsEmptyHardState(rd.HardState) {
			_ = n.storage.SetHardState(rd.HardState)
		}
		_ = n.storage.Append(rd.Entries)
		// Simulate disk latency.
		time.Sleep(time.Millisecond)
		// Send messages, with a simulated latency.
		for _, m := range rd.Messages {
			m := m
			go func() {
				time.Sleep(time.Duration(rand.Int63n(10)) * time.Millisecond)
				n.iface.send(m)
			}()
		}
		func() {
			n.mu.Lock()
			defer n.mu.Unlock()
			n.mu.rn.Advance(rd)
			maybeSignalLocked()
		}()
	}

	// An independently running Ready handling loop.
	stopReady := make(chan struct{})
	go func() {
		for {
			select {
			case <-hasReady:
				if rd, ok := getReady(); ok {
					handleReady(rd)
				}
			case <-stopReady:
				close(stopReady)
				return
			}
		}
	}()
	// A loop that handles ticks, messages, and lifetime commands like pause/stop.
	ticker := time.NewTicker(5 * time.Millisecond).C
	go func() {
		for {
			select {
			case <-ticker:
				tick()
			case m := <-n.iface.recv():
				step(m)
			case <-n.stopc:
				log.Printf("raft.%d: stop", n.id)
				stopReady <- struct{}{}
				<-stopReady
				close(n.stopc)
				return
			case p := <-n.pausec:
				var queue []raftpb.Message
				for p {
					select {
					case m := <-n.iface.recv():
						queue = append(queue, m)
					case p = <-n.pausec:
					}
				}
				// Step all queued messages.
				step(queue...)
			}
		}
	}()
}

func (n *node) propose(data []byte) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.mu.rn.Propose(data)
}

func (n *node) status() raft.Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.mu.rn.Status()
}

// stop stops the node. stop a stopped node might panic.
// All in memory state of node is discarded.
// All stable MUST be unchanged.
func (n *node) stop() {
	n.iface.disconnect()
	n.stopc <- struct{}{}
	// wait for the shutdown
	<-n.stopc
}

// restart restarts the node. restart a started node
// blocks and might affect the future stop operation.
func (n *node) restart() {
	// wait for the shutdown
	<-n.stopc
	c := &raft.Config{
		ID:                        n.id,
		ElectionTick:              10,
		HeartbeatTick:             1,
		Storage:                   n.storage,
		MaxSizePerMsg:             1024 * 1024,
		MaxInflightMsgs:           256,
		MaxUncommittedEntriesSize: 1 << 30,
		StoreLiveness:             raftstoreliveness.AlwaysLive{},
		CRDBVersion:               cluster.MakeTestingClusterSettings().Version,
		Metrics:                   raft.NewMetrics(),
	}
	rn, err := raft.NewRawNode(c)
	if err != nil {
		panic(err)
	}
	n.mu.rn = rn
	n.start()
	n.iface.connect()
}

// pause pauses the node.
// The paused node buffers the received messages and replies
// all of them when it resumes.
func (n *node) pause() {
	n.pausec <- true
}

// resume resumes the paused node.
func (n *node) resume() {
	n.pausec <- false
}
