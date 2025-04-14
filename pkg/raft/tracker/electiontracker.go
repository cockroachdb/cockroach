// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tracker

import (
	"github.com/cockroachdb/cockroach/pkg/raft/quorum"
	pb "github.com/cockroachdb/cockroach/pkg/raft/raftpb"
)

// ElectionTracker is used to track votes from the currently active configuration
// and determine election results.
// Also used for storing hints which will benefit the first round of MsgApp if
// the candidate becomes leader.
type ElectionTracker struct {
	config       *quorum.Config
	votes        map[pb.PeerID]bool
	conflictInfo map[pb.PeerID]hintInfo
}

// hintInfo stores information needed for the new leader to decide where to send
// entryIDs from after winning leader election.
type hintInfo struct {
	// conflictIdx is the best guess on where the new leader should send to the
	// follower if the candidate wins the election.
	conflictIdx pb.Index
	// match indicates if the particular conflictIdx is a durable match for the
	// voter's log if the current candidate wins the election.
	match bool
}

func MakeElectionTracker(config *quorum.Config) ElectionTracker {
	return ElectionTracker{
		config:       config,
		votes:        map[pb.PeerID]bool{},
		conflictInfo: map[pb.PeerID]hintInfo{},
	}
}

// RecordVote records that the node with the given id voted for this Raft
// instance if vote == true (and declined it otherwise).
func (e *ElectionTracker) RecordVote(id pb.PeerID, vote bool, conflictIdx pb.Index, match bool) {
	_, ok := e.votes[id]
	if !ok {
		e.votes[id] = vote
		e.conflictInfo[id] = hintInfo{
			conflictIdx: conflictIdx,
			match:       match,
		}
	}
}

// TallyVotes returns the number of granted and rejected Votes, and whether the
// election outcome is known.
func (e *ElectionTracker) TallyVotes() (granted int, rejected int, _ quorum.VoteResult) {
	// Make sure to populate granted/rejected correctly even if the votes slice
	// contains members no longer part of the configuration. This doesn't really
	// matter in the way the numbers are used (they're informational), but might
	// as well get it right.
	for id, v := range e.votes {
		if _, isLearner := e.config.Learners[id]; isLearner {
			continue
		}
		if v {
			granted++
		} else {
			rejected++
		}
	}
	result := e.config.Voters.VoteResult(e.votes)
	return granted, rejected, result
}

// GetGuessMatchEntryID returns the conflictIdx and match status for the
// given peer ID.
// If match is true, it means an entryID at conflictIdx on the
// follower's log matches the entryID on the leader's log.
// If match is false, it means the entryID at conflictIdx is either a best guess
// on where to send to the follower or just r.raftLog.LastIndex().
func (e *ElectionTracker) GetGuessMatchEntryID(
	id pb.PeerID,
) (conflictID pb.Index, match bool, exist bool) {
	hint, ok := e.conflictInfo[id]
	return hint.conflictIdx, hint.match, ok
}

func (e *ElectionTracker) GetVote(id pb.PeerID) bool {
	vote := e.votes[id]
	return vote
}

// ResetVotes prepares for a new round of vote counting via recordVote.
func (e *ElectionTracker) ResetVotes() {
	clear(e.votes)
	clear(e.conflictInfo)
}

// TestingGetVotes exports the votes map for testing.
func (e *ElectionTracker) TestingGetVotes() map[pb.PeerID]bool {
	return e.votes
}
