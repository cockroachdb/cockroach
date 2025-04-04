// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tracker

import (
	"github.com/cockroachdb/cockroach/pkg/raft/quorum"
	pb "github.com/cockroachdb/cockroach/pkg/raft/raftpb"
)

// EntryID stores raft entryIDs, stores where to start sending for MsgApp after
// winning leader election.
// TODO(hakuuww): This is a repeated type that we can avoid.
type EntryID struct {
	Index uint64
	Term  uint64
}

// ElectionTracker is used to track votes from the currently active configuration
// and determine election results.
type ElectionTracker struct {
	config          *quorum.Config
	votes           map[pb.PeerID]bool
	conflictEntryID map[pb.PeerID]hintInfo
}

// hintInfo stores information needed for the to decide where to send to
// after winning leader election.
type hintInfo struct {
	conflictID EntryID
	// match indicates if the particular conflictID matches our own log.
	match bool
}

func MakeElectionTracker(config *quorum.Config) ElectionTracker {
	return ElectionTracker{
		config:          config,
		votes:           map[pb.PeerID]bool{},
		conflictEntryID: map[pb.PeerID]hintInfo{},
	}
}

// RecordVote records that the node with the given id voted for this Raft
// instance if vote == true (and declined it otherwise).
func (e *ElectionTracker) RecordVote(id pb.PeerID, vote bool, conflictID EntryID, match bool) {
	_, ok := e.votes[id]
	if !ok {
		e.votes[id] = vote
		e.conflictEntryID[id] = hintInfo{
			conflictID: conflictID,
			match:      match,
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

// GetGuessMatchEntryID returns the conflictID and match status for the
// given peer ID. If match is true, it means the conflictID matches an entryID
// in this candidate's log.
func (e *ElectionTracker) GetGuessMatchEntryID(
	id pb.PeerID,
) (conflictID EntryID, match bool, exist bool) {
	hint, ok := e.conflictEntryID[id]
	return hint.conflictID, hint.match, ok
}

func (e *ElectionTracker) GetVote(id pb.PeerID) bool {
	vote, _ := e.votes[id]
	return vote
}

// ResetVotes prepares for a new round of vote counting via recordVote.
func (e *ElectionTracker) ResetVotes() {
	clear(e.votes)
	clear(e.conflictEntryID)
}

// TestingGetVotes exports the votes map for testing.
func (e *ElectionTracker) TestingGetVotes() map[pb.PeerID]bool {
	return e.votes
}
