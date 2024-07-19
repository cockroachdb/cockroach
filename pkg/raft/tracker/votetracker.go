// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tracker

import (
	"github.com/cockroachdb/cockroach/pkg/raft/quorum"
	pb "github.com/cockroachdb/cockroach/pkg/raft/raftpb"
)

// VoteTracker is used to track votes from the currently active configuration
// and determine election results.
type VoteTracker struct {
	config *quorum.Config

	votes map[pb.PeerID]bool
}

func MakeVoteTracker(config *quorum.Config) VoteTracker {
	return VoteTracker{
		config: config,
		votes:  map[pb.PeerID]bool{},
	}
}

// RecordVote records that the node with the given id voted for this Raft
// instance if v == true (and declined it otherwise).
func (v *VoteTracker) RecordVote(id pb.PeerID, vote bool) {
	_, ok := v.votes[id]
	if !ok {
		v.votes[id] = vote
	}
}

// TallyVotes returns the number of granted and rejected Votes, and whether the
// election outcome is known.
func (v *VoteTracker) TallyVotes(
	progress ProgressMap,
) (granted int, rejected int, _ quorum.VoteResult) {
	// Make sure to populate granted/rejected correctly even if the Votes slice
	// contains members no longer part of the configuration. This doesn't really
	// matter in the way the numbers are used (they're informational), but might
	// as well get it right.
	for id, pr := range progress {
		if pr.IsLearner {
			continue
		}
		v, voted := v.votes[id]
		if !voted {
			continue
		}
		if v {
			granted++
		} else {
			rejected++
		}
	}
	result := v.config.Voters.VoteResult(v.votes)
	return granted, rejected, result
}

// ResetVotes prepares for a new round of vote counting via recordVote.
func (v *VoteTracker) ResetVotes() {
	v.votes = map[pb.PeerID]bool{}
}

// TestingGetVotes exports the votes map for testing.
func (v *VoteTracker) TestingGetVotes() map[pb.PeerID]bool {
	return v.votes
}
