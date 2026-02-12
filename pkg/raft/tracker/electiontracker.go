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
	config     *quorum.Config
	votes      map[pb.PeerID]bool
	matchGuess map[pb.PeerID]MatchGuess
}

// MatchGuess stores information needed for the new leader to decide where to
// send entryIDs from after winning leader election.
type MatchGuess struct {
	// index is the best guess on where the voter's log potentially matches the
	// candidate's. When elected, the leader will start replicating the log from
	// index + 1.
	Index uint64
	// match indicates whether the voter's log matches the candidate's at index.
	Match bool
}

func MakeElectionTracker(config *quorum.Config) ElectionTracker {
	return ElectionTracker{
		config:     config,
		votes:      map[pb.PeerID]bool{},
		matchGuess: map[pb.PeerID]MatchGuess{},
	}
}

// RecordVote records that the node with the given id voted for this Raft
// instance if vote == true (and declined it otherwise).
func (e *ElectionTracker) RecordVote(id pb.PeerID, vote bool, matchGuess MatchGuess) {
	_, ok := e.votes[id]
	if !ok {
		e.votes[id] = vote
		e.matchGuess[id] = matchGuess
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

// MatchGuess returns the matchGuess previously recorded for the given PeerID,
// if it exists.
func (e *ElectionTracker) MatchGuess(id pb.PeerID) (MatchGuess, bool) {
	matchGuess, ok := e.matchGuess[id]
	return matchGuess, ok
}

// ResetVotes prepares for a new round of vote counting via recordVote.
func (e *ElectionTracker) ResetVotes() {
	clear(e.votes)
	clear(e.matchGuess)
}

// TestingGetVotes exports the votes map for testing.
func (e *ElectionTracker) TestingGetVotes() map[pb.PeerID]bool {
	return e.votes
}
