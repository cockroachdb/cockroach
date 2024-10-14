// This code has been modified from its original form by The Cockroach Authors.
// All modifications are Copyright 2024 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/raft/quorum"
	pb "github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"golang.org/x/exp/maps"
)

// ProgressTracker tracks the progress made by each peer in the currently active
// configuration. In particular, it tracks the match index for each peer, which
// in-turn allows for reasoning about the committed index.
type ProgressTracker struct {
	config *quorum.Config

	progress ProgressMap
}

// MakeProgressTracker initializes a ProgressTracker.
func MakeProgressTracker(config *quorum.Config, progressMap ProgressMap) ProgressTracker {
	p := ProgressTracker{
		config:   config,
		progress: progressMap,
	}
	return p
}

// Progress returns the progress associated with the supplied ID.
func (p *ProgressTracker) Progress(id pb.PeerID) *Progress {
	return p.progress[id]
}

// MoveProgressMap transfers ownership of the progress map to the caller. It
// shouldn't be used by the progress tracker after this.
func (p *ProgressTracker) MoveProgressMap() ProgressMap {
	progress := p.progress
	p.progress = nil
	return progress
}

// Len returns the length of the progress map.
func (p *ProgressTracker) Len() int {
	return len(p.progress)
}

func (p *ProgressTracker) TestingSetProgress(id pb.PeerID, progress *Progress) {
	p.progress[id] = progress
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
	return uint64(p.config.Voters.CommittedIndex(matchAckIndexer(p.progress)))
}

// Visit invokes the supplied closure for all tracked progresses in stable order.
func (p *ProgressTracker) Visit(f func(id pb.PeerID, pr *Progress)) {
	n := len(p.progress)
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
	for id := range p.progress {
		n--
		ids[n] = id
	}
	slices.Sort(ids)
	for _, id := range ids {
		f(id, p.progress[id])
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
	nodes := maps.Keys(p.config.Voters.IDs())
	slices.Sort(nodes)
	return nodes
}

// LearnerNodes returns a sorted slice of learners.
func (p *ProgressTracker) LearnerNodes() []pb.PeerID {
	if len(p.config.Learners) == 0 {
		return nil
	}
	nodes := maps.Keys(p.config.Learners)
	slices.Sort(nodes)
	return nodes
}

// WithBasicProgress is a helper to introspect the BasicProgress for this node
// and its peers.
func (p *ProgressTracker) WithBasicProgress(visitor func(id pb.PeerID, pr BasicProgress)) {
	for id, p := range p.progress {
		visitor(id, BasicProgress{
			Match: p.Match,
			Next:  p.Next,
			State: p.State,
		})
	}
}
