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

/*
This file contains tests which verify that the scenarios described
in the raft paper (https://raft.github.io/raft.pdf) are
handled by the raft implementation correctly. Each test focuses on
several sentences written in the paper. This could help us to prevent
most implementation bugs.

Each test is composed of three parts: init, test and check.
Init part uses simple and understandable way to simulate the init state.
Test part uses Step function to generate the scenario. Check part checks
outgoing messages and state.
*/
package raft

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/raft/raftlogger"
	pb "github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/raft/raftstoreliveness"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFollowerUpdateTermFromMessage(t *testing.T) {
	testUpdateTermFromMessage(t, pb.StateFollower)
}
func TestCandidateUpdateTermFromMessage(t *testing.T) {
	testUpdateTermFromMessage(t, pb.StateCandidate)
}
func TestLeaderUpdateTermFromMessage(t *testing.T) {
	testUpdateTermFromMessage(t, pb.StateLeader)
}

// testUpdateTermFromMessage tests that if one server’s current term is
// smaller than the other’s, then it updates its current term to the larger
// value. If a candidate or leader discovers that its term is out of date,
// it immediately reverts to follower state.
// Reference: section 5.1
func testUpdateTermFromMessage(t *testing.T, state pb.StateType) {
	r := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	switch state {
	case pb.StateFollower:
		r.becomeFollower(1, 2)
	case pb.StateCandidate:
		r.becomeCandidate()
	case pb.StateLeader:
		r.becomeCandidate()
		r.becomeLeader()
	}

	r.Step(pb.Message{Type: pb.MsgApp, Term: 2})

	assert.Equal(t, uint64(2), r.Term)
	assert.Equal(t, pb.StateFollower, r.state)
}

// TestRejectStaleTermMessage tests that if a server receives a request with
// a stale term number, it rejects the request.
// Our implementation ignores the request instead.
// Reference: section 5.1
func TestRejectStaleTermMessage(t *testing.T) {
	called := false
	fakeStep := func(r *raft, m pb.Message) error {
		called = true
		return nil
	}
	r := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	r.step = fakeStep
	r.loadState(pb.HardState{Term: 2})

	r.Step(pb.Message{Type: pb.MsgApp, Term: r.Term - 1})

	assert.False(t, called)
}

// TestStartAsFollower tests that when servers start up, they begin as followers.
// Reference: section 5.2
func TestStartAsFollower(t *testing.T) {
	r := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	assert.Equal(t, pb.StateFollower, r.state)
}

// TestLeaderBcastBeat tests that if the leader receives a heartbeat tick,
// it will send a MsgHeartbeat with m.Index = 0, m.LogTerm=0 and empty entries
// as heartbeat to all followers.
// Reference: section 5.2
// Note that if store liveness is enabled, the leader might send a MsgApp on
// every heartbeat interval, but it won't send a MsgHeartbeat.
func TestLeaderBcastBeat(t *testing.T) {
	// heartbeat interval
	hi := int64(3)

	testutils.RunTrueAndFalse(t, "store-liveness-enabled",
		func(t *testing.T, storeLivenessEnabled bool) {
			testOptions := emptyTestConfigModifierOpt()
			if !storeLivenessEnabled {
				testOptions = withStoreLiveness(raftstoreliveness.Disabled{})
			}

			r := newTestRaft(1, 10, hi,
				newTestMemoryStorage(withPeers(1, 2, 3)), testOptions)

			r.becomeCandidate()
			r.becomeLeader()

			for i := 0; i < 10; i++ {
				mustAppendEntry(r, pb.Entry{Index: uint64(i) + 1})
			}

			for i := int64(0); i < hi; i++ {
				require.Empty(t, r.readMessages())
				r.tick()
			}

			msgs := r.readMessages()
			slices.SortFunc(msgs, cmpMessages)
			if storeLivenessEnabled {
				assert.Equal(t, []pb.Message{
					{From: 1, To: 2, Term: 1, Type: pb.MsgApp, Entries: r.raftLog.allEntries()},
					{From: 1, To: 3, Term: 1, Type: pb.MsgApp, Entries: r.raftLog.allEntries()},
					{From: 1, To: 2, Term: 1, Type: pb.MsgFortifyLeader},
					{From: 1, To: 3, Term: 1, Type: pb.MsgFortifyLeader},
				}, msgs)
			} else {
				assert.Equal(t, []pb.Message{
					{From: 1, To: 2, Term: 1, Type: pb.MsgHeartbeat},
					{From: 1, To: 3, Term: 1, Type: pb.MsgHeartbeat},
				}, msgs)
			}
		})
}

func TestFollowerStartElection(t *testing.T) {
	testNonleaderStartElection(t, pb.StateFollower)
}
func TestCandidateStartNewElection(t *testing.T) {
	testNonleaderStartElection(t, pb.StateCandidate)
}

// testNonleaderStartElection tests that if a follower receives no communication
// over election timeout, it begins an election to choose a new leader. It
// increments its current term and transitions to candidate state. It then
// votes for itself and issues RequestVote RPCs in parallel to each of the
// other servers in the cluster.
// Reference: section 5.2
// Also if a candidate fails to obtain a majority, it will time out and
// start a new election by incrementing its term and initiating another
// round of RequestVote RPCs.
// Reference: section 5.2
func testNonleaderStartElection(t *testing.T, state pb.StateType) {
	// election timeout
	et := int64(10)
	r := newTestRaft(1, et, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	switch state {
	case pb.StateFollower:
		r.becomeFollower(1, 2)
	case pb.StateCandidate:
		r.becomeCandidate()
	}

	for i := int64(1); i < 2*et; i++ {
		r.tick()
	}
	r.advanceMessagesAfterAppend()

	assert.Equal(t, uint64(2), r.Term)
	assert.Equal(t, pb.StateCandidate, r.state)
	assert.True(t, r.electionTracker.TestingGetVotes()[r.id])

	msgs := r.readMessages()
	slices.SortFunc(msgs, cmpMessages)
	assert.Equal(t, []pb.Message{
		{From: 1, To: 2, Term: 2, Type: pb.MsgVote},
		{From: 1, To: 3, Term: 2, Type: pb.MsgVote},
	}, msgs)
}

// TestLeaderElectionInOneRoundRPC tests all cases that may happen in
// leader election during one round of RequestVote RPC:
// a) it wins the election
// b) it loses the election
// c) it is unclear about the result
// Reference: section 5.2
func TestLeaderElectionInOneRoundRPC(t *testing.T) {
	tests := []struct {
		size  int
		votes map[pb.PeerID]bool
		state pb.StateType
	}{
		// win the election when receiving votes from a majority of the servers
		{1, map[pb.PeerID]bool{}, pb.StateLeader},
		{3, map[pb.PeerID]bool{2: true, 3: true}, pb.StateLeader},
		{3, map[pb.PeerID]bool{2: true}, pb.StateLeader},
		{5, map[pb.PeerID]bool{2: true, 3: true, 4: true, 5: true}, pb.StateLeader},
		{5, map[pb.PeerID]bool{2: true, 3: true, 4: true}, pb.StateLeader},
		{5, map[pb.PeerID]bool{2: true, 3: true}, pb.StateLeader},

		// return to follower state if it receives vote denial from a majority
		{3, map[pb.PeerID]bool{2: false, 3: false}, pb.StateFollower},
		{5, map[pb.PeerID]bool{2: false, 3: false, 4: false, 5: false}, pb.StateFollower},
		{5, map[pb.PeerID]bool{2: true, 3: false, 4: false, 5: false}, pb.StateFollower},

		// stay in candidate if it does not obtain the majority
		{3, map[pb.PeerID]bool{}, pb.StateCandidate},
		{5, map[pb.PeerID]bool{2: true}, pb.StateCandidate},
		{5, map[pb.PeerID]bool{2: false, 3: false}, pb.StateCandidate},
		{5, map[pb.PeerID]bool{}, pb.StateCandidate},
	}
	for i, tt := range tests {
		r := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(idsBySize(tt.size)...)))

		r.Step(pb.Message{From: 1, To: 1, Type: pb.MsgHup})
		r.advanceMessagesAfterAppend()
		for id, vote := range tt.votes {
			r.Step(pb.Message{From: id, To: 1, Term: r.Term, Type: pb.MsgVoteResp, Reject: !vote})
		}

		assert.Equal(t, tt.state, r.state, "#%d", i)
		assert.Equal(t, uint64(1), r.Term, "#%d", i)
	}
}

// TestFollowerVote tests that each follower will vote for at most one
// candidate in a given term, on a first-come-first-served basis.
// Reference: section 5.2
func TestFollowerVote(t *testing.T) {
	tests := []struct {
		vote    pb.PeerID
		nvote   pb.PeerID
		wreject bool
	}{
		{None, 2, false},
		{None, 3, false},
		{2, 2, false},
		{3, 3, false},
		{2, 3, true},
		{3, 2, true},
	}
	for i, tt := range tests {
		r := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
		r.loadState(pb.HardState{Term: 1, Vote: tt.vote})

		r.Step(pb.Message{From: tt.nvote, To: 1, Term: 1, Type: pb.MsgVote})

		assert.Equal(t, []pb.Message{
			{From: 1, To: tt.nvote, Term: 1, Type: pb.MsgVoteResp, Reject: tt.wreject},
		}, r.msgsAfterAppend, "#%d", i)
	}
}

// TestCandidateFallback tests that while waiting for votes,
// if a candidate receives an AppendEntries RPC from another server claiming
// to be leader whose term is at least as large as the candidate's current term,
// it recognizes the leader as legitimate and returns to follower state.
// Reference: section 5.2
func TestCandidateFallback(t *testing.T) {
	tests := []pb.Message{
		{From: 2, To: 1, Term: 1, Type: pb.MsgApp},
		{From: 2, To: 1, Term: 2, Type: pb.MsgApp},
	}
	for i, tt := range tests {
		r := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
		r.Step(pb.Message{From: 1, To: 1, Type: pb.MsgHup})
		require.Equal(t, pb.StateCandidate, r.state, "#%d", i)

		r.Step(tt)

		assert.Equal(t, pb.StateFollower, r.state, "#%d", i)
		assert.Equal(t, tt.Term, r.Term, "#%d", i)
	}
}

func TestFollowerElectionTimeoutRandomized(t *testing.T) {
	raftlogger.SetLogger(raftlogger.DiscardLogger)
	defer raftlogger.SetLogger(raftlogger.DefaultRaftLogger)
	testNonleaderElectionTimeoutRandomized(t, pb.StateFollower)
}
func TestCandidateElectionTimeoutRandomized(t *testing.T) {
	raftlogger.SetLogger(raftlogger.DiscardLogger)
	defer raftlogger.SetLogger(raftlogger.DefaultRaftLogger)
	testNonleaderElectionTimeoutRandomized(t, pb.StateCandidate)
}

// testNonleaderElectionTimeoutRandomized tests that election timeout for
// follower or candidate is randomized.
// Reference: section 5.2
func testNonleaderElectionTimeoutRandomized(t *testing.T, state pb.StateType) {
	et := int64(10)
	r := newTestRaft(1, et, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	timeouts := make(map[int64]bool)
	for round := int64(0); round < 50*et; round++ {
		switch state {
		case pb.StateFollower:
			r.becomeFollower(r.Term+1, 2)
		case pb.StateCandidate:
			r.becomeCandidate()
		}

		time := int64(0)
		for len(r.readMessages()) == 0 {
			r.tick()
			time++
		}
		timeouts[time] = true
	}

	for d := et; d < 2*et; d++ {
		assert.True(t, timeouts[d], "timeout in %d ticks should happen", d)
	}
}

func TestFollowersElectionTimeoutNonconflict(t *testing.T) {
	raftlogger.SetLogger(raftlogger.DiscardLogger)
	defer raftlogger.SetLogger(raftlogger.DefaultRaftLogger)
	testNonleadersElectionTimeoutNonconflict(t, pb.StateFollower)
}
func TestCandidatesElectionTimeoutNonconflict(t *testing.T) {
	raftlogger.SetLogger(raftlogger.DiscardLogger)
	defer raftlogger.SetLogger(raftlogger.DefaultRaftLogger)
	testNonleadersElectionTimeoutNonconflict(t, pb.StateCandidate)
}

// testNonleadersElectionTimeoutNonconflict tests that in most cases only a
// single server(follower or candidate) will time out, which reduces the
// likelihood of split vote in the new election.
// Reference: section 5.2
func testNonleadersElectionTimeoutNonconflict(t *testing.T, state pb.StateType) {
	et := int64(10)
	size := 5
	rs := make([]*raft, size)
	ids := idsBySize(size)
	for k := range rs {
		rs[k] = newTestRaft(ids[k], et, 1, newTestMemoryStorage(withPeers(ids...)))
	}
	conflicts := 0
	for round := 0; round < 1000; round++ {
		for _, r := range rs {
			switch state {
			case pb.StateFollower:
				r.becomeFollower(r.Term+1, None)
			case pb.StateCandidate:
				r.becomeCandidate()
			}
		}

		timeoutNum := 0
		for timeoutNum == 0 {
			for _, r := range rs {
				r.tick()
				if len(r.readMessages()) > 0 {
					timeoutNum++
				}
			}
		}
		// several rafts time out at the same tick
		if timeoutNum > 1 {
			conflicts++
		}
	}

	assert.LessOrEqual(t, float64(conflicts)/1000, 0.3)
}

// TestLeaderStartReplication tests that when receiving client proposals,
// the leader appends the proposal to its log as a new entry, then issues
// AppendEntries RPCs in parallel to each of the other servers to replicate
// the entry. Also, when sending an AppendEntries RPC, the leader includes
// the index and term of the entry in its log that immediately precedes
// the new entries.
// Also, it writes the new entry into stable storage.
// Reference: section 5.3
func TestLeaderStartReplication(t *testing.T) {
	s := newTestMemoryStorage(withPeers(1, 2, 3))
	r := newTestRaft(1, 10, 1, s)
	r.becomeCandidate()
	r.becomeLeader()
	commitNoopEntry(r, s)
	li := r.raftLog.lastIndex()

	ents := []pb.Entry{{Data: []byte("some data")}}
	r.Step(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: ents})

	assert.Equal(t, li+1, r.raftLog.lastIndex())
	assert.Equal(t, li, r.raftLog.committed)
	msgs := r.readMessages()
	slices.SortFunc(msgs, cmpMessages)
	wents := []pb.Entry{{Index: li + 1, Term: 1, Data: []byte("some data")}}
	assert.Equal(t, []pb.Message{
		{From: 1, To: 2, Term: 1, Type: pb.MsgApp, Index: li, LogTerm: 1, Entries: wents, Commit: li, Match: li},
		{From: 1, To: 3, Term: 1, Type: pb.MsgApp, Index: li, LogTerm: 1, Entries: wents, Commit: li, Match: li},
	}, msgs)
	assert.Equal(t, []pb.Entry{
		{Index: li + 1, Term: 1, Data: []byte("some data")},
	}, r.raftLog.nextUnstableEnts())
}

// TestLeaderCommitEntry tests that when the entry has been safely replicated,
// the leader gives out the applied entries, which can be applied to its state
// machine.
// Also, the leader keeps track of the highest index it knows to be committed,
// and it includes that index in future AppendEntries RPCs so that the other
// servers eventually find out.
// Reference: section 5.3
func TestLeaderCommitEntry(t *testing.T) {
	s := newTestMemoryStorage(withPeers(1, 2, 3))
	r := newTestRaft(1, 10, 1, s)
	r.becomeCandidate()
	r.becomeLeader()
	commitNoopEntry(r, s)
	li := r.raftLog.lastIndex()
	r.Step(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("some data")}}})

	for _, m := range r.readMessages() {
		r.Step(acceptAndReply(m))
	}

	assert.Equal(t, li+1, r.raftLog.committed)
	assert.Equal(t, []pb.Entry{
		{Index: li + 1, Term: 1, Data: []byte("some data")},
	}, r.raftLog.nextCommittedEnts(true))
	msgs := r.readMessages()
	slices.SortFunc(msgs, cmpMessages)
	for i, m := range msgs {
		assert.Equal(t, pb.PeerID(i+2), m.To)
		assert.Equal(t, pb.MsgApp, m.Type)
		assert.Equal(t, li+1, m.Commit)
	}
}

// TestLeaderAcknowledgeCommit tests that a log entry is committed once the
// leader that created the entry has replicated it on a majority of the servers.
// Reference: section 5.3
func TestLeaderAcknowledgeCommit(t *testing.T) {
	tests := []struct {
		size               int
		nonLeaderAcceptors map[pb.PeerID]bool
		wack               bool
	}{
		{1, nil, true},
		{3, nil, false},
		{3, map[pb.PeerID]bool{2: true}, true},
		{3, map[pb.PeerID]bool{2: true, 3: true}, true},
		{5, nil, false},
		{5, map[pb.PeerID]bool{2: true}, false},
		{5, map[pb.PeerID]bool{2: true, 3: true}, true},
		{5, map[pb.PeerID]bool{2: true, 3: true, 4: true}, true},
		{5, map[pb.PeerID]bool{2: true, 3: true, 4: true, 5: true}, true},
	}
	for i, tt := range tests {
		s := newTestMemoryStorage(withPeers(idsBySize(tt.size)...))
		r := newTestRaft(1, 10, 1, s)
		r.becomeCandidate()
		r.becomeLeader()
		commitNoopEntry(r, s)
		li := r.raftLog.lastIndex()
		r.Step(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("some data")}}})
		r.advanceMessagesAfterAppend()
		for _, m := range r.msgs {
			if tt.nonLeaderAcceptors[m.To] {
				r.Step(acceptAndReply(m))
			}
		}

		assert.Equal(t, tt.wack, r.raftLog.committed > li, "#%d", i)
	}
}

// TestLeaderCommitPrecedingEntries tests that when leader commits a log entry,
// it also commits all preceding entries in the leader’s log, including
// entries created by previous leaders.
// Also, it applies the entry to its local state machine (in log order).
// Reference: section 5.3
func TestLeaderCommitPrecedingEntries(t *testing.T) {
	tests := [][]pb.Entry{
		{},
		{{Term: 2, Index: 1}},
		{{Term: 1, Index: 1}, {Term: 2, Index: 2}},
		{{Term: 1, Index: 1}},
	}
	for i, tt := range tests {
		storage := newTestMemoryStorage(withPeers(1, 2, 3))
		storage.Append(tt)
		r := newTestRaft(1, 10, 1, storage)
		r.loadState(pb.HardState{Term: 2})
		r.becomeCandidate()
		r.becomeLeader()
		r.Step(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("some data")}}})

		for _, m := range r.readMessages() {
			r.Step(acceptAndReply(m))
		}

		li := uint64(len(tt))
		assert.Equal(t, append(tt,
			pb.Entry{Term: 3, Index: li + 1},
			pb.Entry{Term: 3, Index: li + 2, Data: []byte("some data")},
		), r.raftLog.nextCommittedEnts(true), "#%d", i)
	}
}

// TestFollowerCommitEntry tests that once a follower learns that a log entry
// is committed, it applies the entry to its local state machine (in log order).
// Reference: section 5.3
func TestFollowerCommitEntry(t *testing.T) {
	tests := []struct {
		ents   []pb.Entry
		commit uint64
	}{
		{
			[]pb.Entry{
				{Term: 1, Index: 1, Data: []byte("some data")},
			},
			1,
		},
		{
			[]pb.Entry{
				{Term: 1, Index: 1, Data: []byte("some data")},
				{Term: 1, Index: 2, Data: []byte("some data2")},
			},
			2,
		},
		{
			[]pb.Entry{
				{Term: 1, Index: 1, Data: []byte("some data2")},
				{Term: 1, Index: 2, Data: []byte("some data")},
			},
			2,
		},
		{
			[]pb.Entry{
				{Term: 1, Index: 1, Data: []byte("some data")},
				{Term: 1, Index: 2, Data: []byte("some data2")},
			},
			1,
		},
	}
	for i, tt := range tests {
		r := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
		r.becomeFollower(1, 2)

		r.Step(pb.Message{From: 2, To: 1, Type: pb.MsgApp, Term: 1, Entries: tt.ents, Commit: tt.commit})

		assert.Equal(t, tt.commit, r.raftLog.committed, "#%d", i)
		assert.Equal(t, tt.ents[:int(tt.commit)], r.raftLog.nextCommittedEnts(true), "#%d", i)
	}
}

// TestFollowerCheckMsgApp tests that if the follower does not find an
// entry in its log with the same index and term as the one in AppendEntries RPC,
// then it refuses the new entries. Otherwise it replies that it accepts the
// append entries.
// Reference: section 5.3
func TestFollowerCheckMsgApp(t *testing.T) {
	ents := []pb.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}}
	tests := []struct {
		term        uint64
		index       uint64
		windex      uint64
		wreject     bool
		wrejectHint uint64
		wlogterm    uint64
	}{
		// match with committed entries
		{0, 0, 1, false, 0, 0},
		{ents[0].Term, ents[0].Index, 1, false, 0, 0},
		// match with uncommitted entries
		{ents[1].Term, ents[1].Index, 2, false, 0, 0},

		// unmatch with existing entry
		{ents[0].Term, ents[1].Index, ents[1].Index, true, 1, 1},
		// unexisting entry
		{ents[1].Term, ents[1].Index + 1, ents[1].Index + 1, true, 2, 2},
	}
	for i, tt := range tests {
		storage := newTestMemoryStorage(withPeers(1, 2, 3))
		storage.Append(ents)
		r := newTestRaft(1, 10, 1, storage)
		r.loadState(pb.HardState{Commit: 1})
		r.becomeFollower(2, 2)

		r.Step(pb.Message{From: 2, To: 1, Type: pb.MsgApp, Term: 2, LogTerm: tt.term, Index: tt.index})
		assert.Equal(t, []pb.Message{
			{From: 1, To: 2, Type: pb.MsgAppResp, Term: 2, Index: tt.windex, Commit: 1, Reject: tt.wreject, RejectHint: tt.wrejectHint, LogTerm: tt.wlogterm},
		}, r.readMessages(), "#%d", i)
	}
}

// TestFollowerAppendEntries tests that when AppendEntries RPC is valid,
// the follower will delete the existing conflict entry and all that follow it,
// and append any new entries not already in the log.
// Also, it writes the new entry into stable storage.
// Reference: section 5.3
func TestFollowerAppendEntries(t *testing.T) {
	tests := []struct {
		index, term uint64
		ents        []pb.Entry
		wents       []pb.Entry
		wunstable   []pb.Entry
	}{
		{
			2, 2,
			index(3).terms(3),
			index(1).terms(1, 2, 3),
			index(3).terms(3),
		},
		{
			1, 1,
			index(2).terms(3, 4),
			index(1).terms(1, 3, 4),
			index(2).terms(3, 4),
		},
		{
			0, 0,
			index(1).terms(1),
			index(1).terms(1, 2),
			nil,
		},
		{
			0, 0,
			index(1).terms(3),
			index(1).terms(3),
			index(1).terms(3),
		},
	}
	for i, tt := range tests {
		storage := newTestMemoryStorage(withPeers(1, 2, 3))
		storage.Append([]pb.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}})
		r := newTestRaft(1, 10, 1, storage)
		r.becomeFollower(2, 2)

		r.Step(pb.Message{From: 2, To: 1, Type: pb.MsgApp, Term: 4, LogTerm: tt.term, Index: tt.index, Entries: tt.ents})

		assert.Equal(t, tt.wents, r.raftLog.allEntries(), "#%d", i)
		assert.Equal(t, tt.wunstable, r.raftLog.nextUnstableEnts(), "#%d", i)
	}
}

// TestLeaderSyncFollowerLog tests that the leader could bring a follower's log
// into consistency with its own.
// Reference: section 5.3, figure 7
func TestLeaderSyncFollowerLog(t *testing.T) {
	ents := index(0).terms(0, 1, 1, 1, 4, 4, 5, 5, 6, 6, 6)
	term := uint64(8)
	for i, tt := range [][]pb.Entry{
		index(0).terms(0, 1, 1, 1, 4, 4, 5, 5, 6, 6),
		index(0).terms(0, 1, 1, 1, 4, 4),
		index(0).terms(0, 1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 6),
		index(0).terms(0, 1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 7, 7),
		index(0).terms(0, 1, 1, 1, 4, 4, 4, 4),
		index(0).terms(0, 1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3),
	} {
		leadStorage := newTestMemoryStorage(withPeers(1, 2, 3))
		leadStorage.Append(ents)
		lead := newTestRaft(1, 10, 1, leadStorage)
		lead.loadState(pb.HardState{Commit: lead.raftLog.lastIndex(), Term: term})
		followerStorage := newTestMemoryStorage(withPeers(1, 2, 3))
		followerStorage.Append(tt)
		follower := newTestRaft(2, 10, 1, followerStorage)
		follower.loadState(pb.HardState{Term: term - 1})
		// It is necessary to have a three-node cluster.
		// The second may have more up-to-date log than the first one, so the
		// first node needs the vote from the third node to become the leader.
		n := newNetwork(lead, follower, nopStepper)
		n.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})
		// The election occurs in the term after the one we loaded with
		// lead.loadState above.
		n.send(pb.Message{From: 3, To: 1, Type: pb.MsgVoteResp, Term: term + 1})

		n.send(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{}}})

		assert.Empty(t, diffu(ltoa(lead.raftLog), ltoa(follower.raftLog)), "#%d", i)
	}
}

// TestVoteRequest tests that the vote request includes information about the candidate’s log
// and are sent to all of the other nodes.
// Reference: section 5.4.1
func TestVoteRequest(t *testing.T) {
	tests := []struct {
		ents  []pb.Entry
		wterm uint64
	}{
		{index(1).terms(1), 2},
		{index(1).terms(1, 2), 3},
	}
	for j, tt := range tests {
		r := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
		r.Step(pb.Message{
			From: 2, To: 1, Type: pb.MsgApp, Term: tt.wterm - 1, LogTerm: 0, Index: 0, Entries: tt.ents,
		})
		r.readMessages()

		for i := int64(1); i < r.electionTimeout*2; i++ {
			r.tickElection()
		}

		msgs := r.readMessages()
		slices.SortFunc(msgs, cmpMessages)
		require.Len(t, msgs, 2, "#%d", j)
		for i, m := range msgs {
			assert.Equal(t, pb.MsgVote, m.Type, "#%d.%d", j, i)
			assert.Equal(t, pb.PeerID(i+2), m.To, "#%d.%d", j, i)
			assert.Equal(t, tt.wterm, m.Term, "#%d.%d", j, i)

			assert.Equal(t, tt.ents[len(tt.ents)-1].Index, m.Index, "#%d.%d", j, i)
			assert.Equal(t, tt.ents[len(tt.ents)-1].Term, m.LogTerm, "#%d.%d", j, i)
		}
	}
}

// TestVoter tests the voter denies its vote if its own log is more up-to-date
// than that of the candidate.
// Reference: section 5.4.1
func TestVoter(t *testing.T) {
	tests := []struct {
		ents    []pb.Entry
		logterm uint64
		index   uint64

		wreject bool
	}{
		// same logterm
		{index(1).terms(1), 1, 1, false},
		{index(1).terms(1), 1, 2, false},
		{index(1).terms(1, 1), 1, 1, true},
		// candidate higher logterm
		{index(1).terms(1), 2, 1, false},
		{index(1).terms(1), 2, 2, false},
		{[]pb.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}}, 2, 1, false},
		// voter higher logterm
		{index(1).terms(2), 1, 1, true},
		{index(1).terms(2), 1, 2, true},
		{index(1).terms(2, 2), 1, 1, true},
		{index(1).terms(1, 1), 1, 1, true},
	}
	for i, tt := range tests {
		storage := newTestMemoryStorage(withPeers(1, 2))
		storage.Append(tt.ents)
		r := newTestRaft(1, 10, 1, storage)

		r.Step(pb.Message{From: 2, To: 1, Type: pb.MsgVote, Term: 3, LogTerm: tt.logterm, Index: tt.index})

		msgs := r.readMessages()
		require.Len(t, msgs, 1, "#%d", i)
		m := msgs[0]
		assert.Equal(t, pb.MsgVoteResp, m.Type, "#%d", i)
		assert.Equal(t, tt.wreject, m.Reject, "#%d", i)
	}
}

// TestLeaderOnlyCommitsLogFromCurrentTerm tests that only log entries from the leader’s
// current term are committed by counting replicas.
// Reference: section 5.4.2
func TestLeaderOnlyCommitsLogFromCurrentTerm(t *testing.T) {
	ents := []pb.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}}
	tests := []struct {
		index   uint64
		wcommit uint64
	}{
		// do not commit log entries in previous terms
		{1, 0},
		{2, 0},
		// commit log in current term
		{3, 3},
	}
	for i, tt := range tests {
		storage := newTestMemoryStorage(withPeers(1, 2))
		storage.Append(ents)
		r := newTestRaft(1, 10, 1, storage)
		r.loadState(pb.HardState{Term: 2})
		// become leader at term 3
		r.becomeCandidate()
		r.becomeLeader()
		r.readMessages()
		// propose a entry to current term
		r.Step(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{}}})

		r.Step(pb.Message{From: 2, To: 1, Type: pb.MsgAppResp, Term: r.Term, Index: tt.index})
		r.advanceMessagesAfterAppend()
		assert.Equal(t, tt.wcommit, r.raftLog.committed, "#%d", i)
	}
}

func cmpMessages(a, b pb.Message) int {
	return strings.Compare(fmt.Sprint(a), fmt.Sprint(b))
}

func commitNoopEntry(r *raft, s *MemoryStorage) {
	if r.state != pb.StateLeader {
		panic("it should only be used when it is the leader")
	}
	r.bcastAppend()
	// simulate the response of MsgApp
	msgs := r.readMessages()
	for _, m := range msgs {
		if m.Type != pb.MsgApp || len(m.Entries) != 1 || m.Entries[0].Data != nil {
			panic("not a message to append noop entry")
		}
		r.Step(acceptAndReply(m))
	}
	// ignore further messages to refresh followers' commit index
	r.readMessages()
	s.Append(r.raftLog.nextUnstableEnts())
	r.raftLog.appliedTo(r.raftLog.committed, 0 /* size */)
	r.raftLog.stableTo(r.raftLog.unstable.mark())
}

func acceptAndReply(m pb.Message) pb.Message {
	if m.Type != pb.MsgApp {
		panic("type should be MsgApp")
	}
	return pb.Message{
		From:  m.To,
		To:    m.From,
		Term:  m.Term,
		Type:  pb.MsgAppResp,
		Index: m.Index + uint64(len(m.Entries)),
	}
}
