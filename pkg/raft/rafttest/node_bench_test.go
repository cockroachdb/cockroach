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
)

func BenchmarkProposal3Nodes(b *testing.B) {
	peers := []raft.Peer{{ID: 1, Context: nil}, {ID: 2, Context: nil}, {ID: 3, Context: nil}}
	nt := newRaftNetwork(1, 2, 3)

	nodes := make([]*node, 0)

	for i := 1; i <= 3; i++ {
		n := startNode(pb.PeerID(i), peers, nt.nodeNetwork(pb.PeerID(i)))
		nodes = append(nodes, n)
	}
	// get ready and warm up
	time.Sleep(50 * time.Millisecond)
	l := waitLeader(nodes)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nodes[l].propose([]byte("somedata"))
	}
	waitCommitConverge(nodes, uint64(b.N+4))
	b.StopTimer()

	for _, n := range nodes {
		n.stop()
	}
}
