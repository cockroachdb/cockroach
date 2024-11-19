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
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/raft"
	pb "github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/stretchr/testify/require"
)

func TestBasicProgress(t *testing.T) {
	peers := []raft.Peer{{ID: 1, Context: nil}, {ID: 2, Context: nil}, {ID: 3, Context: nil}, {ID: 4, Context: nil}, {ID: 5, Context: nil}}
	nt := newRaftNetwork(1, 2, 3, 4, 5)

	nodes := make([]*node, 0)

	for i := 1; i <= 5; i++ {
		n := startNode(pb.PeerID(i), peers, nt.nodeNetwork(pb.PeerID(i)))
		nodes = append(nodes, n)
	}

	l := waitLeader(nodes)

	for i := 0; i < 100; i++ {
		require.NoError(t, nodes[l].propose([]byte("somedata")), i)
	}

	if !waitCommitConverge(nodes, 100) {
		t.Errorf("commits failed to converge!")
	}

	for _, n := range nodes {
		n.stop()
	}
}

func TestRestart(t *testing.T) {
	peers := []raft.Peer{{ID: 1, Context: nil}, {ID: 2, Context: nil}, {ID: 3, Context: nil}, {ID: 4, Context: nil}, {ID: 5, Context: nil}}
	nt := newRaftNetwork(1, 2, 3, 4, 5)

	nodes := make([]*node, 0)

	for i := 1; i <= 5; i++ {
		n := startNode(pb.PeerID(i), peers, nt.nodeNetwork(pb.PeerID(i)))
		nodes = append(nodes, n)
	}

	l := waitLeader(nodes)
	k1, k2 := (l+1)%5, (l+2)%5

	for i := 0; i < 30; i++ {
		require.NoError(t, nodes[l].propose([]byte("somedata")))
	}
	nodes[k1].stop()
	for i := 0; i < 30; i++ {
		require.NoError(t, nodes[l].propose([]byte("somedata")))
	}
	nodes[k2].stop()
	for i := 0; i < 30; i++ {
		require.NoError(t, nodes[l].propose([]byte("somedata")))
	}
	nodes[k2].restart()
	for i := 0; i < 30; i++ {
		require.NoError(t, nodes[l].propose([]byte("somedata")))
	}
	nodes[k1].restart()

	if !waitCommitConverge(nodes, 120) {
		t.Errorf("commits failed to converge!")
	}

	for _, n := range nodes {
		n.stop()
	}
}

func TestPause(t *testing.T) {
	peers := []raft.Peer{{ID: 1, Context: nil}, {ID: 2, Context: nil}, {ID: 3, Context: nil}, {ID: 4, Context: nil}, {ID: 5, Context: nil}}
	nt := newRaftNetwork(1, 2, 3, 4, 5)

	nodes := make([]*node, 0)

	for i := 1; i <= 5; i++ {
		n := startNode(pb.PeerID(i), peers, nt.nodeNetwork(pb.PeerID(i)))
		nodes = append(nodes, n)
	}

	l := waitLeader(nodes)

	for i := 0; i < 30; i++ {
		require.NoError(t, nodes[l].propose([]byte("somedata")))
	}
	nodes[(l+1)%5].pause()
	for i := 0; i < 30; i++ {
		require.NoError(t, nodes[l].propose([]byte("somedata")))
	}
	nodes[(l+2)%5].pause()
	for i := 0; i < 30; i++ {
		require.NoError(t, nodes[l].propose([]byte("somedata")))
	}
	nodes[(l+2)%5].resume()
	for i := 0; i < 30; i++ {
		require.NoError(t, nodes[l].propose([]byte("somedata")))
	}
	nodes[(l+1)%5].resume()

	if !waitCommitConverge(nodes, 120) {
		t.Errorf("commits failed to converge!")
	}

	for _, n := range nodes {
		n.stop()
	}
}

func waitLeader(ns []*node) int {
	l := make(map[pb.PeerID]struct{})
	for {
		clear(l)
		lindex := -1

		for i, n := range ns {
			lead := n.status().HardState.Lead
			if lead != raft.None {
				l[lead] = struct{}{}
				if n.id == lead {
					lindex = i
				}
			}
		}

		if len(l) == 1 && lindex != -1 {
			return lindex
		}
	}
}

func waitCommitConverge(ns []*node, target uint64) bool {
	var c map[uint64]struct{}

	for i := 0; i < 50; i++ {
		c = make(map[uint64]struct{})
		var good int

		for _, n := range ns {
			commit := n.status().HardState.Commit
			c[commit] = struct{}{}
			if commit > target {
				good++
			}
		}

		if len(c) == 1 && good == len(ns) {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}

	return false
}
