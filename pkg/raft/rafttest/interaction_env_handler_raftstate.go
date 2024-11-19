// This code has been modified from its original form by The Cockroach Authors.
// All modifications are Copyright 2024 The Cockroach Authors.
//
// Copyright 2021 The etcd Authors
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
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/datadriven"
)

// isVoter checks whether node id is in the voter list within st.
func isVoter(id raftpb.PeerID, st raft.Status) bool {
	idMap := st.Config.Voters.IDs()
	for idx := range idMap {
		if id == idx {
			return true
		}
	}
	return false
}

// handleRaftState pretty-prints the raft state for all nodes to the output buffer.
// For each node, the information is based on its own configuration view.
func (env *InteractionEnv) handleRaftState() error {
	for _, n := range env.Nodes {
		st := n.Status()
		var voterStatus string
		if isVoter(st.ID, st) {
			voterStatus = "(Voter)"
		} else {
			voterStatus = "(Non-Voter)"
		}
		fmt.Fprintf(env.Output, "%d: %s %s Term:%d Lead:%d LeadEpoch:%d\n",
			st.ID, st.RaftState, voterStatus, st.Term, st.Lead, st.LeadEpoch)
	}
	return nil
}

// handlePrintFortificationState pretty-prints the support map being tracked by a raft
// peer.
func (env *InteractionEnv) handlePrintFortificationState(
	t *testing.T, d datadriven.TestData,
) error {
	idx := firstAsNodeIdx(t, d)
	fmt.Fprint(env.Output, env.Nodes[idx].TestingFortificationStateString())
	return nil
}
