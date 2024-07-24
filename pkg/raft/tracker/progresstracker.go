// This code has been modified from its original form by Cockroach Labs, Inc.
// All modifications are Copyright 2024 Cockroach Labs, Inc.
//
// Copyright 2019 The etcd Authors
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

package tracker

import (
	"slices"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/raft/quorum"
	pb "github.com/cockroachdb/cockroach/pkg/raft/raftpb"
)

// ProgressTracker tracks the progress made by each peer in the currently active
// configuration. In particular, it tracks the match index for each peer, which
// in-turn allows for reasoning about the committed index.
type ProgressTracker struct {
	config *quorum.Config

	Progress ProgressMap
}

// MakeProgressTracker initializes a ProgressTracker.
func MakeProgressTracker(config *quorum.Config, progressMap ProgressMap) ProgressTracker {
	p := ProgressTracker{
		config:   config,
		Progress: progressMap,
	}
	return p
}

type matchAckIndexer map[pb.PeerID]*Progress

var _ quorum.AckedIndexer = matchAckIndexer(nil)

// AckedIndex implements AckedIndexer interface.
func (l matchAckIndexer) AckedIndex(id pb.PeerID) (quorum.Index, bool) {
	pr, ok := l[id]
	if !ok {
		return 0, false
	}
	return quorum.Index(pr.Match), true
}

// Committed returns the largest log index known to be committed based on what
// the voting members of the group have acknowledged.
func (p *ProgressTracker) Committed() uint64 {
	return uint64(p.config.Voters.CommittedIndex(matchAckIndexer(p.Progress)))
}

// Visit invokes the supplied closure for all tracked progresses in stable order.
func (p *ProgressTracker) Visit(f func(id pb.PeerID, pr *Progress)) {
	n := len(p.Progress)
	// We need to sort the IDs and don't want to allocate since this is hot code.
	// The optimization here mirrors that in `(MajorityConfig).CommittedIndex`,
	// see there for details.
	var sl [7]pb.PeerID
	var ids []pb.PeerID
	if len(sl) >= n {
		ids = sl[:n]
	} else {
		ids = make([]pb.PeerID, n)
	}
	for id := range p.Progress {
		n--
		ids[n] = id
	}
	slices.Sort(ids)
	for _, id := range ids {
		f(id, p.Progress[id])
	}
}

// QuorumActive returns true if the quorum is active from the view of the local
// raft state machine. Otherwise, it returns false.
func (p *ProgressTracker) QuorumActive() bool {
	votes := map[pb.PeerID]bool{}
	p.Visit(func(id pb.PeerID, pr *Progress) {
		if pr.IsLearner {
			return
		}
		votes[id] = pr.RecentActive
	})

	return p.config.Voters.VoteResult(votes) == quorum.VoteWon
}

// VoterNodes returns a sorted slice of voters.
func (p *ProgressTracker) VoterNodes() []pb.PeerID {
	m := p.config.Voters.IDs()
	nodes := make([]pb.PeerID, 0, len(m))
	for id := range m {
		nodes = append(nodes, id)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i] < nodes[j] })
	return nodes
}

// LearnerNodes returns a sorted slice of learners.
func (p *ProgressTracker) LearnerNodes() []pb.PeerID {
	if len(p.config.Learners) == 0 {
		return nil
	}
	nodes := make([]pb.PeerID, 0, len(p.config.Learners))
	for id := range p.config.Learners {
		nodes = append(nodes, id)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i] < nodes[j] })
	return nodes
}
