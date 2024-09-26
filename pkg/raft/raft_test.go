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

package raft

import (
	"fmt"
	"math"
	"math/rand"
	"strings"
	"testing"

	pb "github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/raft/tracker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// nextEnts returns the appliable entries and updates the applied index.
func nextEnts(r *raft, s *MemoryStorage) (ents []pb.Entry) {
	// Append unstable entries.
	s.Append(r.raftLog.nextUnstableEnts())
	r.raftLog.stableTo(r.raftLog.lastEntryID())

	// Run post-append steps.
	r.advanceMessagesAfterAppend()

	// Return committed entries.
	ents = r.raftLog.nextCommittedEnts(true)
	r.raftLog.appliedTo(r.raftLog.committed, 0 /* size */)
	return ents
}

func mustAppendEntry(r *raft, ents ...pb.Entry) {
	if !r.appendEntry(ents...) {
		panic("entry unexpectedly dropped")
	}
}

type stateMachine interface {
	Step(m pb.Message) error
	readMessages() []pb.Message
	advanceMessagesAfterAppend()
}

func (r *raft) readMessages() []pb.Message {
	r.advanceMessagesAfterAppend()
	msgs := r.msgs
	r.msgs = nil
	return msgs
}

func (r *raft) advanceMessagesAfterAppend() {
	for {
		msgs := r.takeMessagesAfterAppend()
		if len(msgs) == 0 {
			break
		}
		r.stepOrSend(msgs)
	}
}

func (r *raft) takeMessagesAfterAppend() []pb.Message {
	msgs := r.msgsAfterAppend
	r.msgsAfterAppend = nil
	return msgs
}

func (r *raft) stepOrSend(msgs []pb.Message) error {
	for _, m := range msgs {
		if m.To == r.id {
			if err := r.Step(m); err != nil {
				return err
			}
		} else {
			r.msgs = append(r.msgs, m)
		}
	}
	return nil
}

func TestProgressLeader(t *testing.T) {
	s := newTestMemoryStorage(withPeers(1, 2))
	r := newTestRaft(1, 5, 1, s)
	r.becomeCandidate()
	r.becomeLeader()
	r.trk.Progress[2].BecomeReplicate()

	// Send proposals to r1. The first 5 entries should be queued in the unstable log.
	propMsg := pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("foo")}}}
	for i := 0; i < 5; i++ {
		require.NoError(t, r.Step(propMsg), "#%d", i)
	}

	require.Zero(t, r.trk.Progress[1].Match)

	ents := r.raftLog.nextUnstableEnts()
	require.Len(t, ents, 6)
	require.Len(t, ents[0].Data, 0)
	require.Equal(t, "foo", string(ents[5].Data))

	r.advanceMessagesAfterAppend()

	require.Equal(t, uint64(6), r.trk.Progress[1].Match)
	require.Equal(t, uint64(7), r.trk.Progress[1].Next)
}

// TestProgressResumeByHeartbeatResp ensures raft.heartbeat reset progress.paused by heartbeat response.
func TestProgressResumeByHeartbeatResp(t *testing.T) {
	r := newTestRaft(1, 5, 1, newTestMemoryStorage(withPeers(1, 2)))
	r.becomeCandidate()
	r.becomeLeader()

	r.trk.Progress[2].MsgAppFlowPaused = true

	r.Step(pb.Message{From: 1, To: 1, Type: pb.MsgBeat})
	assert.True(t, r.trk.Progress[2].MsgAppFlowPaused)

	r.trk.Progress[2].BecomeReplicate()
	assert.False(t, r.trk.Progress[2].MsgAppFlowPaused)
	r.trk.Progress[2].MsgAppFlowPaused = true
	r.Step(pb.Message{From: 2, To: 1, Type: pb.MsgHeartbeatResp})
	assert.False(t, r.trk.Progress[2].MsgAppFlowPaused)
}

func TestProgressPaused(t *testing.T) {
	r := newTestRaft(1, 5, 1, newTestMemoryStorage(withPeers(1, 2)))
	r.becomeCandidate()
	r.becomeLeader()
	r.Step(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("somedata")}}})
	r.Step(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("somedata")}}})
	r.Step(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("somedata")}}})

	ms := r.readMessages()
	assert.Len(t, ms, 1)
}

func TestProgressFlowControl(t *testing.T) {
	cfg := newTestConfig(1, 5, 1, newTestMemoryStorage(withPeers(1, 2)))
	cfg.MaxInflightMsgs = 3
	cfg.MaxSizePerMsg = 2048
	cfg.MaxInflightBytes = 9000 // A little over MaxInflightMsgs * MaxSizePerMsg.
	r := newRaft(cfg)
	r.becomeCandidate()
	r.becomeLeader()

	// Throw away all the messages relating to the initial election.
	r.readMessages()

	// While node 2 is in probe state, propose a bunch of entries.
	r.trk.Progress[2].BecomeProbe()
	blob := []byte(strings.Repeat("a", 1000))
	large := []byte(strings.Repeat("b", 5000))
	for i := 0; i < 22; i++ {
		blob := blob
		if i >= 10 && i < 16 { // Temporarily send large messages.
			blob = large
		}
		r.Step(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: blob}}})
	}

	ms := r.readMessages()
	// First append has two entries: the empty entry to confirm the
	// election, and the first proposal (only one proposal gets sent
	// because we're in probe state).
	require.Len(t, ms, 1)
	require.Equal(t, pb.MsgApp, ms[0].Type)

	require.Len(t, ms[0].Entries, 2)

	require.Empty(t, ms[0].Entries[0].Data)
	require.Len(t, ms[0].Entries[1].Data, 1000)

	ackAndVerify := func(index uint64, expEntries ...int) uint64 {
		r.Step(pb.Message{From: 2, To: 1, Type: pb.MsgAppResp, Index: index})
		ms := r.readMessages()
		require.Equal(t, len(expEntries), len(ms))

		for i, m := range ms {
			assert.Equal(t, pb.MsgApp, m.Type, "#%d", i)
			assert.Len(t, m.Entries, expEntries[i], "#%d", i)
		}
		last := ms[len(ms)-1].Entries
		if len(last) == 0 {
			return index
		}
		return last[len(last)-1].Index
	}

	// When this append is acked, we change to replicate state and can
	// send multiple messages at once.
	index := ackAndVerify(ms[0].Entries[1].Index, 2, 2, 2)
	// Ack all three of those messages together and get another 3 messages. The
	// third message contains a single large entry, in contrast to 2 before.
	index = ackAndVerify(index, 2, 1, 1)
	// All subsequent messages contain one large entry, and we cap at 2 messages
	// because it overflows MaxInflightBytes.
	index = ackAndVerify(index, 1, 1)
	index = ackAndVerify(index, 1, 1)
	// Start getting small messages again.
	index = ackAndVerify(index, 1, 2, 2)
	ackAndVerify(index, 2)
}

func TestUncommittedEntryLimit(t *testing.T) {
	// Use a relatively large number of entries here to prevent regression of a
	// bug which computed the size before it was fixed. This test would fail
	// with the bug, either because we'd get dropped proposals earlier than we
	// expect them, or because the final tally ends up nonzero. (At the time of
	// writing, the former).
	const maxEntries = 1024
	testEntry := pb.Entry{Data: []byte("testdata")}
	maxEntrySize := maxEntries * payloadSize(testEntry)

	require.Zero(t, payloadSize(pb.Entry{Data: nil}))

	cfg := newTestConfig(1, 5, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	cfg.MaxUncommittedEntriesSize = uint64(maxEntrySize)
	cfg.MaxInflightMsgs = 2 * 1024 // avoid interference
	r := newRaft(cfg)
	r.becomeCandidate()
	r.becomeLeader()
	require.Zero(t, r.uncommittedSize)

	// Set the two followers to the replicate state. Commit to tail of log.
	const numFollowers = 2
	r.trk.Progress[2].BecomeReplicate()
	r.trk.Progress[3].BecomeReplicate()
	r.uncommittedSize = 0

	// Send proposals to r1. The first 5 entries should be appended to the log.
	propMsg := pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{testEntry}}
	propEnts := make([]pb.Entry, maxEntries)
	for i := 0; i < maxEntries; i++ {
		require.NoError(t, r.Step(propMsg), "#%d", i)
		propEnts[i] = testEntry
	}

	// Send one more proposal to r1. It should be rejected.
	require.Equal(t, ErrProposalDropped, r.Step(propMsg))

	// Read messages and reduce the uncommitted size as if we had committed
	// these entries.
	ms := r.readMessages()
	require.Len(t, ms, maxEntries*numFollowers)
	r.reduceUncommittedSize(payloadsSize(propEnts))
	require.Zero(t, r.uncommittedSize)

	// Send a single large proposal to r1. Should be accepted even though it
	// pushes us above the limit because we were beneath it before the proposal.
	propEnts = make([]pb.Entry, 2*maxEntries)
	for i := range propEnts {
		propEnts[i] = testEntry
	}
	propMsgLarge := pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: propEnts}
	require.NoError(t, r.Step(propMsgLarge))

	// Send one more proposal to r1. It should be rejected, again.
	require.Equal(t, ErrProposalDropped, r.Step(propMsg))

	// But we can always append an entry with no Data. This is used both for the
	// leader's first empty entry and for auto-transitioning out of joint config
	// states.
	require.NoError(t, r.Step(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{}}}))

	// Read messages and reduce the uncommitted size as if we had committed
	// these entries.
	ms = r.readMessages()
	require.Len(t, ms, 2*numFollowers)
	r.reduceUncommittedSize(payloadsSize(propEnts))
	require.Zero(t, r.uncommittedSize)
}

func TestLeaderElection(t *testing.T) {
	testLeaderElection(t, false)
}

func TestLeaderElectionPreVote(t *testing.T) {
	testLeaderElection(t, true)
}

func testLeaderElection(t *testing.T, preVote bool) {
	var cfg func(*Config)
	candState := StateCandidate
	candTerm := uint64(1)
	if preVote {
		cfg = preVoteConfig
		// In pre-vote mode, an election that fails to complete
		// leaves the node in pre-candidate state without advancing
		// the term.
		candState = StatePreCandidate
		candTerm = 0
	}
	tests := []struct {
		*network
		state   StateType
		expTerm uint64
	}{
		{newNetworkWithConfig(cfg, nil, nil, nil), StateLeader, 1},
		{newNetworkWithConfig(cfg, nil, nil, nopStepper), StateLeader, 1},
		{newNetworkWithConfig(cfg, nil, nopStepper, nopStepper), candState, candTerm},
		{newNetworkWithConfig(cfg, nil, nopStepper, nopStepper, nil), candState, candTerm},
		{newNetworkWithConfig(cfg, nil, nopStepper, nopStepper, nil, nil), StateLeader, 1},

		// three logs further along than 0, but in the same term so rejections
		// are returned instead of the votes being ignored.
		{newNetworkWithConfig(cfg,
			nil, entsWithConfig(cfg, 1), entsWithConfig(cfg, 1), entsWithConfig(cfg, 1, 1), nil),
			StateFollower, 1},
	}

	for i, tt := range tests {
		tt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})
		sm := tt.network.peers[1].(*raft)
		assert.Equal(t, tt.state, sm.state, "#%d", i)
		assert.Equal(t, tt.expTerm, sm.Term, "#%d", i)
	}
}

// TestLearnerElectionTimeout verfies that the leader should not start election even
// when times out.
func TestLearnerElectionTimeout(t *testing.T) {
	n1 := newTestLearnerRaft(1, 10, 1, newTestMemoryStorage(withPeers(1), withLearners(2)))
	n2 := newTestLearnerRaft(2, 10, 1, newTestMemoryStorage(withPeers(1), withLearners(2)))

	n1.becomeFollower(1, None)
	n2.becomeFollower(1, None)

	// n2 is learner. Learner should not start election even when times out.
	setRandomizedElectionTimeout(n2, n2.electionTimeout)
	for i := 0; i < n2.electionTimeout; i++ {
		n2.tick()
	}

	assert.Equal(t, StateFollower, n2.state)
}

// TestLearnerPromotion verifies that the learner should not election until
// it is promoted to a normal peer.
func TestLearnerPromotion(t *testing.T) {
	n1 := newTestLearnerRaft(1, 10, 1, newTestMemoryStorage(withPeers(1), withLearners(2)))
	n2 := newTestLearnerRaft(2, 10, 1, newTestMemoryStorage(withPeers(1), withLearners(2)))

	n1.becomeFollower(1, None)
	n2.becomeFollower(1, None)

	nt := newNetwork(n1, n2)

	assert.NotEqual(t, StateLeader, n1.state)

	// n1 should become leader
	setRandomizedElectionTimeout(n1, n1.electionTimeout)
	for i := 0; i < n1.electionTimeout; i++ {
		n1.tick()
	}
	n1.advanceMessagesAfterAppend()

	assert.Equal(t, StateLeader, n1.state)
	assert.Equal(t, StateFollower, n2.state)

	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgBeat})

	n1.applyConfChange(pb.ConfChange{NodeID: 2, Type: pb.ConfChangeAddNode}.AsV2())
	n2.applyConfChange(pb.ConfChange{NodeID: 2, Type: pb.ConfChangeAddNode}.AsV2())
	assert.False(t, n2.isLearner)

	// n2 start election, should become leader
	setRandomizedElectionTimeout(n2, n2.electionTimeout)
	for i := 0; i < n2.electionTimeout; i++ {
		n2.tick()
	}
	n2.advanceMessagesAfterAppend()

	nt.send(pb.Message{From: 2, To: 2, Type: pb.MsgBeat})

	assert.Equal(t, StateFollower, n1.state)
	assert.Equal(t, StateLeader, n2.state)
}

// TestLearnerCanVote checks that a learner can vote when it receives a valid Vote request.
// See (*raft).Step for why this is necessary and correct behavior.
func TestLearnerCanVote(t *testing.T) {
	n2 := newTestLearnerRaft(2, 10, 1, newTestMemoryStorage(withPeers(1), withLearners(2)))

	n2.becomeFollower(1, None)

	n2.Step(pb.Message{From: 1, To: 2, Term: 2, Type: pb.MsgVote, LogTerm: 11, Index: 11})

	msgs := n2.readMessages()
	require.Len(t, msgs, 1)
	require.Equal(t, msgs[0].Type, pb.MsgVoteResp)
	require.False(t, msgs[0].Reject, "expected learner to not reject vote")
}

func TestLeaderCycle(t *testing.T) {
	testLeaderCycle(t, false)
}

func TestLeaderCyclePreVote(t *testing.T) {
	testLeaderCycle(t, true)
}

// testLeaderCycle verifies that each node in a cluster can campaign
// and be elected in turn. This ensures that elections (including
// pre-vote) work when not starting from a clean slate (as they do in
// TestLeaderElection)
func testLeaderCycle(t *testing.T, preVote bool) {
	var cfg func(*Config)
	if preVote {
		cfg = preVoteConfig
	}
	n := newNetworkWithConfig(cfg, nil, nil, nil)
	for campaignerID := uint64(1); campaignerID <= 3; campaignerID++ {
		n.send(pb.Message{From: campaignerID, To: campaignerID, Type: pb.MsgHup})

		for _, peer := range n.peers {
			sm := peer.(*raft)
			if sm.id == campaignerID {
				assert.Equal(t, StateLeader, sm.state, "preVote=%v: campaigning node %d", preVote, sm.id)
			} else {
				assert.Equal(t, StateFollower, sm.state, "preVote=%v: campaigning node %d, current node %d", preVote, campaignerID, sm.id)
			}
		}
	}
}

// TestLeaderElectionOverwriteNewerLogs tests a scenario in which a
// newly-elected leader does *not* have the newest (i.e. highest term)
// log entries, and must overwrite higher-term log entries with
// lower-term ones.
func TestLeaderElectionOverwriteNewerLogs(t *testing.T) {
	testLeaderElectionOverwriteNewerLogs(t, false)
}

func TestLeaderElectionOverwriteNewerLogsPreVote(t *testing.T) {
	testLeaderElectionOverwriteNewerLogs(t, true)
}

func testLeaderElectionOverwriteNewerLogs(t *testing.T, preVote bool) {
	var cfg func(*Config)
	if preVote {
		cfg = preVoteConfig
	}
	// This network represents the results of the following sequence of
	// events:
	// - Node 1 won the election in term 1.
	// - Node 1 replicated a log entry to node 2 but died before sending
	//   it to other nodes.
	// - Node 3 won the second election in term 2.
	// - Node 3 wrote an entry to its logs but died without sending it
	//   to any other nodes.
	//
	// At this point, nodes 1, 2, and 3 all have uncommitted entries in
	// their logs and could win an election at term 3. The winner's log
	// entry overwrites the losers'. (TestLeaderSyncFollowerLog tests
	// the case where older log entries are overwritten, so this test
	// focuses on the case where the newer entries are lost).
	n := newNetworkWithConfig(cfg,
		entsWithConfig(cfg, 1),     // Node 1: Won first election
		entsWithConfig(cfg, 1),     // Node 2: Got logs from node 1
		entsWithConfig(cfg, 2),     // Node 3: Won second election
		votedWithConfig(cfg, 3, 2), // Node 4: Voted but didn't get logs
		votedWithConfig(cfg, 3, 2)) // Node 5: Voted but didn't get logs

	// Node 1 campaigns. The election fails because a quorum of nodes
	// know about the election that already happened at term 2. Node 1's
	// term is pushed ahead to 2.
	n.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})
	sm1 := n.peers[1].(*raft)
	assert.Equal(t, StateFollower, sm1.state)
	assert.Equal(t, uint64(2), sm1.Term)

	// Node 1 campaigns again with a higher term. This time it succeeds.
	n.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})
	assert.Equal(t, StateLeader, sm1.state)
	assert.Equal(t, uint64(3), sm1.Term)

	// Now all nodes agree on a log entry with term 1 at index 1 (and
	// term 3 at index 2).
	for i := range n.peers {
		sm := n.peers[i].(*raft)
		entries := sm.raftLog.allEntries()
		require.Len(t, entries, 2)
		assert.Equal(t, uint64(1), entries[0].Term)
		assert.Equal(t, uint64(3), entries[1].Term)
	}
}

func TestVoteFromAnyState(t *testing.T) {
	testVoteFromAnyState(t, pb.MsgVote)
}

func TestPreVoteFromAnyState(t *testing.T) {
	testVoteFromAnyState(t, pb.MsgPreVote)
}

func testVoteFromAnyState(t *testing.T, vt pb.MessageType) {
	for st := StateType(0); st < numStates; st++ {
		r := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
		r.Term = 1

		switch st {
		case StateFollower:
			r.becomeFollower(r.Term, 3)
		case StatePreCandidate:
			r.becomePreCandidate()
		case StateCandidate:
			r.becomeCandidate()
		case StateLeader:
			r.becomeCandidate()
			r.becomeLeader()
		}

		// Note that setting our state above may have advanced r.Term
		// past its initial value.
		origTerm := r.Term
		newTerm := r.Term + 1

		msg := pb.Message{
			From:    2,
			To:      1,
			Type:    vt,
			Term:    newTerm,
			LogTerm: newTerm,
			Index:   42,
		}
		assert.NoError(t, r.Step(msg), "%s,%s", vt, st)
		msgs := r.readMessages()
		if assert.Len(t, msgs, 1, "%s,%s", vt, st) {
			resp := msgs[0]
			assert.Equal(t, voteRespMsgType(vt), resp.Type, "%s,%s", vt, st)
			assert.False(t, resp.Reject, "%s,%s", vt, st)
		}

		// If this was a real vote, we reset our state and term.
		if vt == pb.MsgVote {
			assert.Equal(t, StateFollower, r.state, "%s,%s", vt, st)
			assert.Equal(t, newTerm, r.Term, "%s,%s", vt, st)
			assert.Equal(t, uint64(2), r.Vote, "%s,%s", vt, st)
		} else {
			// In a prevote, nothing changes.
			assert.Equal(t, st, r.state, "%s,%s", vt, st)
			assert.Equal(t, origTerm, r.Term, "%s,%s", vt, st)
			// if st == StateFollower or StatePreCandidate, r hasn't voted yet.
			// In StateCandidate or StateLeader, it's voted for itself.
			assert.True(t, r.Vote == None || r.Vote == 1, "%s,%s: vote %d, want %d or 1", vt, st, r.Vote, None)
		}
	}
}

func TestLogReplication(t *testing.T) {
	tests := []struct {
		*network
		msgs       []pb.Message
		wcommitted uint64
	}{
		{
			newNetwork(nil, nil, nil),
			[]pb.Message{
				{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("somedata")}}},
			},
			2,
		},
		{
			newNetwork(nil, nil, nil),
			[]pb.Message{
				{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("somedata")}}},
				{From: 1, To: 2, Type: pb.MsgHup},
				{From: 1, To: 2, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("somedata")}}},
			},
			4,
		},
	}

	for i, tt := range tests {
		tt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

		for _, m := range tt.msgs {
			tt.send(m)
		}

		for j, x := range tt.network.peers {
			sm := x.(*raft)

			assert.Equal(t, tt.wcommitted, sm.raftLog.committed, "#%d.%d", i, j)

			var ents []pb.Entry
			for _, e := range nextEnts(sm, tt.network.storage[j]) {
				if e.Data != nil {
					ents = append(ents, e)
				}
			}
			var props []pb.Message
			for _, m := range tt.msgs {
				if m.Type == pb.MsgProp {
					props = append(props, m)
				}
			}
			for k, m := range props {
				assert.Equal(t, m.Entries[0].Data, ents[k].Data, "#%d.%d", i, j)
			}
		}
	}
}

// TestLearnerLogReplication tests that a learner can receive entries from the leader.
func TestLearnerLogReplication(t *testing.T) {
	s1 := newTestMemoryStorage(withPeers(1), withLearners(2))
	n1 := newTestLearnerRaft(1, 10, 1, s1)
	n2 := newTestLearnerRaft(2, 10, 1, newTestMemoryStorage(withPeers(1), withLearners(2)))

	nt := newNetwork(n1, n2)
	nt.t = t

	n1.becomeFollower(1, None)
	n2.becomeFollower(1, None)

	setRandomizedElectionTimeout(n1, n1.electionTimeout)
	for i := 0; i < n1.electionTimeout; i++ {
		n1.tick()
	}
	n1.advanceMessagesAfterAppend()

	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgBeat})

	// n1 is leader and n2 is learner
	assert.Equal(t, StateLeader, n1.state)
	assert.True(t, n2.isLearner)

	nextCommitted := uint64(2)
	{
		nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("somedata")}}})
	}

	assert.Equal(t, nextCommitted, n1.raftLog.committed)
	assert.Equal(t, n1.raftLog.committed, n2.raftLog.committed)

	match := n1.trk.Progress[2].Match
	assert.Equal(t, n2.raftLog.committed, match)
}

func TestSingleNodeCommit(t *testing.T) {
	s := newTestMemoryStorage(withPeers(1))
	cfg := newTestConfig(1, 10, 1, s)
	r := newRaft(cfg)
	tt := newNetwork(r)
	tt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})
	tt.send(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("some data")}}})
	tt.send(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("some data")}}})

	sm := tt.peers[1].(*raft)
	assert.Equal(t, uint64(3), sm.raftLog.committed)
}

// TestCannotCommitWithoutNewTermEntry tests the entries cannot be committed
// when leader changes, no new proposal comes in and ChangeTerm proposal is
// filtered.
func TestCannotCommitWithoutNewTermEntry(t *testing.T) {
	tt := newNetwork(nil, nil, nil, nil, nil)
	tt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	// 0 cannot reach 2,3,4
	tt.cut(1, 3)
	tt.cut(1, 4)
	tt.cut(1, 5)

	tt.send(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("some data")}}})
	tt.send(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("some data")}}})

	sm := tt.peers[1].(*raft)
	assert.Equal(t, uint64(1), sm.raftLog.committed)

	// network recovery
	tt.recover()
	// avoid committing ChangeTerm proposal
	tt.ignore(pb.MsgApp)

	// elect 2 as the new leader with term 2
	tt.send(pb.Message{From: 2, To: 2, Type: pb.MsgHup})

	// no log entries from previous term should be committed
	sm = tt.peers[2].(*raft)
	assert.Equal(t, uint64(1), sm.raftLog.committed)

	tt.recover()
	// send heartbeat; reset wait
	tt.send(pb.Message{From: 2, To: 2, Type: pb.MsgBeat})
	// append an entry at current term
	tt.send(pb.Message{From: 2, To: 2, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("some data")}}})
	// expect the committed to be advanced
	assert.Equal(t, uint64(5), sm.raftLog.committed)
}

// TestCommitWithoutNewTermEntry tests the entries could be committed
// when leader changes, no new proposal comes in.
func TestCommitWithoutNewTermEntry(t *testing.T) {
	tt := newNetwork(nil, nil, nil, nil, nil)
	tt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	// 0 cannot reach 3,4,5
	tt.cut(1, 3)
	tt.cut(1, 4)
	tt.cut(1, 5)

	tt.send(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("some data")}}})
	tt.send(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("some data")}}})

	sm := tt.peers[1].(*raft)
	assert.Equal(t, uint64(1), sm.raftLog.committed)

	// network recovery
	tt.recover()

	// elect 2 as the new leader with term 2
	// after append a ChangeTerm entry from the current term, all entries
	// should be committed
	tt.send(pb.Message{From: 2, To: 2, Type: pb.MsgHup})

	assert.Equal(t, uint64(4), sm.raftLog.committed)
}

func TestDuelingCandidates(t *testing.T) {
	s1 := newTestMemoryStorage(withPeers(1, 2, 3))
	s2 := newTestMemoryStorage(withPeers(1, 2, 3))
	s3 := newTestMemoryStorage(withPeers(1, 2, 3))
	a := newTestRaft(1, 10, 1, s1)
	b := newTestRaft(2, 10, 1, s2)
	c := newTestRaft(3, 10, 1, s3)

	nt := newNetwork(a, b, c)
	nt.cut(1, 3)

	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})
	nt.send(pb.Message{From: 3, To: 3, Type: pb.MsgHup})

	// 1 becomes leader since it receives votes from 1 and 2
	sm := nt.peers[1].(*raft)
	assert.Equal(t, StateLeader, sm.state)

	// 3 stays as candidate since it receives a vote from 3 and a rejection from 2
	sm = nt.peers[3].(*raft)
	assert.Equal(t, StateCandidate, sm.state)

	nt.recover()

	// candidate 3 now increases its term and tries to vote again
	// we expect it to disrupt the leader 1 since it has a higher term
	// 3 will be follower again since both 1 and 2 rejects its vote request since 3 does not have a long enough log
	nt.send(pb.Message{From: 3, To: 3, Type: pb.MsgHup})
	assert.Equal(t, StateFollower, sm.state)

	tests := []struct {
		sm        *raft
		state     StateType
		term      uint64
		lastIndex uint64
	}{
		{a, StateFollower, 2, 1},
		{b, StateFollower, 2, 1},
		{c, StateFollower, 2, 0},
	}

	for i, tt := range tests {
		assert.Equal(t, tt.state, tt.sm.state, "#%d", i)
		assert.Equal(t, tt.term, tt.sm.Term, "#%d", i)
		assert.Equal(t, tt.lastIndex, tt.sm.raftLog.lastIndex(), "#%d", i)
	}
}

func TestDuelingPreCandidates(t *testing.T) {
	cfgA := newTestConfig(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	cfgB := newTestConfig(2, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	cfgC := newTestConfig(3, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	cfgA.PreVote = true
	cfgB.PreVote = true
	cfgC.PreVote = true
	a := newRaft(cfgA)
	b := newRaft(cfgB)
	c := newRaft(cfgC)

	nt := newNetwork(a, b, c)
	nt.t = t
	nt.cut(1, 3)

	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})
	nt.send(pb.Message{From: 3, To: 3, Type: pb.MsgHup})

	// 1 becomes leader since it receives votes from 1 and 2
	sm := nt.peers[1].(*raft)
	assert.Equal(t, StateLeader, sm.state)

	// 3 campaigns then reverts to follower when its PreVote is rejected
	sm = nt.peers[3].(*raft)
	assert.Equal(t, StateFollower, sm.state)

	nt.recover()

	// Candidate 3 now increases its term and tries to vote again.
	// With PreVote, it does not disrupt the leader.
	nt.send(pb.Message{From: 3, To: 3, Type: pb.MsgHup})

	tests := []struct {
		sm        *raft
		state     StateType
		term      uint64
		lastIndex uint64
	}{
		{a, StateLeader, 1, 1},
		{b, StateFollower, 1, 1},
		{c, StateFollower, 1, 0},
	}

	for i, tt := range tests {
		assert.Equal(t, tt.state, tt.sm.state, "#%d", i)
		assert.Equal(t, tt.term, tt.sm.Term, "#%d", i)
		assert.Equal(t, tt.lastIndex, tt.sm.raftLog.lastIndex(), "#%d", i)
	}
}

func TestCandidateConcede(t *testing.T) {
	tt := newNetwork(nil, nil, nil)
	tt.isolate(1)

	tt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})
	tt.send(pb.Message{From: 3, To: 3, Type: pb.MsgHup})

	// heal the partition
	tt.recover()
	// send heartbeat; reset wait
	tt.send(pb.Message{From: 3, To: 3, Type: pb.MsgBeat})

	data := []byte("force follower")
	// send a proposal to 3 to flush out a MsgApp to 1
	tt.send(pb.Message{From: 3, To: 3, Type: pb.MsgProp, Entries: []pb.Entry{{Data: data}}})
	// send heartbeat; flush out commit
	tt.send(pb.Message{From: 3, To: 3, Type: pb.MsgBeat})

	a := tt.peers[1].(*raft)
	assert.Equal(t, StateFollower, a.state)
	assert.Equal(t, uint64(1), a.Term)

	wantLog := ltoa(newLog(&MemoryStorage{
		ents: []pb.Entry{{}, {Data: nil, Term: 1, Index: 1}, {Term: 1, Index: 2, Data: data}},
	}, nil))
	for i, p := range tt.peers {
		if sm, ok := p.(*raft); ok {
			l := ltoa(sm.raftLog)
			assert.Empty(t, diffu(wantLog, l), "#%d", i)
		} else {
			t.Logf("#%d: empty log", i)
		}
	}
}

func TestSingleNodeCandidate(t *testing.T) {
	tt := newNetwork(nil)
	tt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	sm := tt.peers[1].(*raft)
	assert.Equal(t, StateLeader, sm.state)
}

func TestSingleNodePreCandidate(t *testing.T) {
	tt := newNetworkWithConfig(preVoteConfig, nil)
	tt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	sm := tt.peers[1].(*raft)
	assert.Equal(t, StateLeader, sm.state)
}

func TestOldMessages(t *testing.T) {
	tt := newNetwork(nil, nil, nil)
	// make 0 leader @ term 3
	tt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})
	tt.send(pb.Message{From: 2, To: 2, Type: pb.MsgHup})
	tt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})
	// pretend we're an old leader trying to make progress; this entry is expected to be ignored.
	tt.send(pb.Message{From: 2, To: 1, Type: pb.MsgApp, Term: 2, Entries: index(3).terms(2)})
	// commit a new entry
	tt.send(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("somedata")}}})

	ents := index(0).terms(0, 1, 2, 3, 3)
	ents[4].Data = []byte("somedata")
	ilog := newLog(&MemoryStorage{ents: ents}, nil)
	base := ltoa(ilog)
	for i, p := range tt.peers {
		if sm, ok := p.(*raft); ok {
			l := ltoa(sm.raftLog)
			assert.Empty(t, diffu(base, l), "#%d", i)
		} else {
			t.Logf("#%d: empty log", i)
		}
	}
}

// TestOldMessagesReply - optimization - reply with new term.

func TestProposal(t *testing.T) {
	tests := []struct {
		*network
		success bool
	}{
		{newNetwork(nil, nil, nil), true},
		{newNetwork(nil, nil, nopStepper), true},
		{newNetwork(nil, nopStepper, nopStepper), false},
		{newNetwork(nil, nopStepper, nopStepper, nil), false},
		{newNetwork(nil, nopStepper, nopStepper, nil, nil), true},
	}

	for j, tt := range tests {
		send := func(m pb.Message) {
			defer func() {
				// only recover if we expect it to panic (success==false)
				if !tt.success {
					e := recover()
					if e != nil {
						t.Logf("#%d: err: %s", j, e)
					}
				}
			}()
			tt.send(m)
		}

		data := []byte("somedata")

		// promote 1 to become leader
		send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})
		send(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: data}}})
		r := tt.network.peers[1].(*raft)

		wantLog := newLog(NewMemoryStorage(), raftLogger)
		if tt.success {
			wantLog = newLog(&MemoryStorage{
				ents: []pb.Entry{{}, {Data: nil, Term: 1, Index: 1}, {Term: 1, Index: 2, Data: data}},
			}, nil)
		}
		base := ltoa(wantLog)
		for i, p := range tt.peers {
			if sm, ok := p.(*raft); ok {
				l := ltoa(sm.raftLog)
				assert.Empty(t, diffu(base, l), "#%d, peer %d", j, i)
			} else {
				t.Logf("#%d: peer %d empty log", j, i)
			}
		}
		assert.Equal(t, uint64(1), r.Term, "#%d", j)
	}
}

func TestProposalByProxy(t *testing.T) {
	data := []byte("somedata")
	tests := []*network{
		newNetwork(nil, nil, nil),
		newNetwork(nil, nil, nopStepper),
	}

	for j, tt := range tests {
		// promote 0 the leader
		tt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

		// propose via follower
		tt.send(pb.Message{From: 2, To: 2, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("somedata")}}})

		wantLog := newLog(&MemoryStorage{
			ents: []pb.Entry{{}, {Data: nil, Term: 1, Index: 1}, {Term: 1, Data: data, Index: 2}},
		}, nil)
		base := ltoa(wantLog)
		for i, p := range tt.peers {
			if sm, ok := p.(*raft); ok {
				l := ltoa(sm.raftLog)
				assert.Empty(t, diffu(base, l), "#%d.%d", j, i)
			} else {
				t.Logf("#%d: peer %d empty log", j, i)
			}
		}
		sm := tt.peers[1].(*raft)
		assert.Equal(t, uint64(1), sm.Term, "#%d", j)
	}
}

func TestCommit(t *testing.T) {
	tests := []struct {
		matches []uint64
		logs    []pb.Entry
		smTerm  uint64
		w       uint64
	}{
		// single
		{[]uint64{1}, index(1).terms(1), 1, 1},
		{[]uint64{1}, index(1).terms(1), 2, 0},
		{[]uint64{2}, index(1).terms(1, 2), 2, 2},
		{[]uint64{1}, index(1).terms(2), 2, 1},

		// odd
		{[]uint64{2, 1, 1}, index(1).terms(1, 2), 1, 1},
		{[]uint64{2, 1, 1}, index(1).terms(1, 1), 2, 0},
		{[]uint64{2, 1, 2}, index(1).terms(1, 2), 2, 2},
		{[]uint64{2, 1, 2}, index(1).terms(1, 1), 2, 0},

		// even
		{[]uint64{2, 1, 1, 1}, index(1).terms(1, 2), 1, 1},
		{[]uint64{2, 1, 1, 1}, index(1).terms(1, 1), 2, 0},
		{[]uint64{2, 1, 1, 2}, index(1).terms(1, 2), 1, 1},
		{[]uint64{2, 1, 1, 2}, index(1).terms(1, 1), 2, 0},
		{[]uint64{2, 1, 2, 2}, index(1).terms(1, 2), 2, 2},
		{[]uint64{2, 1, 2, 2}, index(1).terms(1, 1), 2, 0},
	}

	for i, tt := range tests {
		storage := newTestMemoryStorage(withPeers(1))
		storage.Append(tt.logs)
		storage.hardState = pb.HardState{Term: tt.smTerm}

		sm := newTestRaft(1, 10, 2, storage)
		for j := 0; j < len(tt.matches); j++ {
			id := uint64(j) + 1
			if id > 1 {
				sm.applyConfChange(pb.ConfChange{Type: pb.ConfChangeAddNode, NodeID: id}.AsV2())
			}
			pr := sm.trk.Progress[id]
			pr.Match, pr.Next = tt.matches[j], tt.matches[j]+1
		}
		sm.maybeCommit()
		assert.Equal(t, tt.w, sm.raftLog.committed, "#%d", i)
	}
}

func TestPastElectionTimeout(t *testing.T) {
	tests := []struct {
		elapse       int
		wprobability float64
		round        bool
	}{
		{5, 0, false},
		{10, 0.1, true},
		{13, 0.4, true},
		{15, 0.6, true},
		{18, 0.9, true},
		{20, 1, false},
	}

	for i, tt := range tests {
		sm := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1)))
		sm.electionElapsed = tt.elapse
		c := 0
		for j := 0; j < 10000; j++ {
			sm.resetRandomizedElectionTimeout()
			if sm.pastElectionTimeout() {
				c++
			}
		}
		got := float64(c) / 10000.0
		if tt.round {
			got = math.Floor(got*10+0.5) / 10.0
		}
		assert.Equal(t, tt.wprobability, got, "#%d", i)
	}
}

// TestStepIgnoreOldTermMsg to ensure that the Step function ignores the message
// from old term and does not pass it to the actual stepX function.
func TestStepIgnoreOldTermMsg(t *testing.T) {
	called := false
	fakeStep := func(r *raft, m pb.Message) error {
		called = true
		return nil
	}
	sm := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1)))
	sm.step = fakeStep
	sm.Term = 2
	sm.Step(pb.Message{Type: pb.MsgApp, Term: sm.Term - 1})
	assert.False(t, called)
}

// TestHandleMsgApp ensures:
//  1. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm.
//  2. If an existing entry conflicts with a new one (same index but different terms),
//     delete the existing entry and all that follow it; append any new entries not already in the log.
//  3. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry).
func TestHandleMsgApp(t *testing.T) {
	tests := []struct {
		m       pb.Message
		wIndex  uint64
		wCommit uint64
		wReject bool
	}{
		// Ensure 1
		{pb.Message{Type: pb.MsgApp, Term: 2, LogTerm: 3, Index: 2, Commit: 3}, 2, 0, true}, // previous log mismatch
		{pb.Message{Type: pb.MsgApp, Term: 2, LogTerm: 3, Index: 3, Commit: 3}, 2, 0, true}, // previous log non-exist

		// Ensure 2
		{pb.Message{Type: pb.MsgApp, Term: 2, LogTerm: 1, Index: 1, Commit: 1}, 2, 1, false},
		{pb.Message{Type: pb.MsgApp, Term: 2, LogTerm: 0, Index: 0, Commit: 1, Entries: []pb.Entry{{Index: 1, Term: 2}}}, 1, 1, false},
		{pb.Message{Type: pb.MsgApp, Term: 2, LogTerm: 2, Index: 2, Commit: 3, Entries: []pb.Entry{{Index: 3, Term: 2}, {Index: 4, Term: 2}}}, 4, 3, false},
		{pb.Message{Type: pb.MsgApp, Term: 2, LogTerm: 2, Index: 2, Commit: 4, Entries: []pb.Entry{{Index: 3, Term: 2}}}, 3, 3, false},
		{pb.Message{Type: pb.MsgApp, Term: 2, LogTerm: 1, Index: 1, Commit: 4, Entries: []pb.Entry{{Index: 2, Term: 2}}}, 2, 2, false},

		// Ensure 3
		{pb.Message{Type: pb.MsgApp, Term: 1, LogTerm: 1, Index: 1, Commit: 3}, 2, 1, false},                                           // match entry 1, commit up to last new entry 1
		{pb.Message{Type: pb.MsgApp, Term: 1, LogTerm: 1, Index: 1, Commit: 3, Entries: []pb.Entry{{Index: 2, Term: 2}}}, 2, 2, false}, // match entry 1, commit up to last new entry 2
		{pb.Message{Type: pb.MsgApp, Term: 2, LogTerm: 2, Index: 2, Commit: 3}, 2, 2, false},                                           // match entry 2, commit up to last new entry 2
		{pb.Message{Type: pb.MsgApp, Term: 2, LogTerm: 2, Index: 2, Commit: 4}, 2, 2, false},                                           // commit up to log.last()
	}

	for i, tt := range tests {
		storage := newTestMemoryStorage(withPeers(1))
		require.NoError(t, storage.Append(index(1).terms(1, 2)))
		sm := newTestRaft(1, 10, 1, storage)
		sm.becomeFollower(2, None)

		sm.handleAppendEntries(tt.m)
		assert.Equal(t, tt.wIndex, sm.raftLog.lastIndex(), "#%d", i)
		assert.Equal(t, tt.wCommit, sm.raftLog.committed, "#%d", i)
		m := sm.readMessages()
		require.Len(t, m, 1, "#%d", i)
		assert.Equal(t, tt.wReject, m[0].Reject, "#%d", i)
	}
}

// TestHandleHeartbeat ensures that the follower commits to the commit in the message.
func TestHandleHeartbeat(t *testing.T) {
	commit := uint64(2)
	tests := []struct {
		m       pb.Message
		wCommit uint64
	}{
		{pb.Message{From: 2, To: 1, Type: pb.MsgHeartbeat, Term: 2, Commit: commit + 1}, commit + 1},
		{pb.Message{From: 2, To: 1, Type: pb.MsgHeartbeat, Term: 2, Commit: commit - 1}, commit}, // do not decrease commit
	}

	for i, tt := range tests {
		storage := newTestMemoryStorage(withPeers(1, 2))
		require.NoError(t, storage.Append(index(1).terms(1, 2, 3)))
		sm := newTestRaft(1, 5, 1, storage)
		sm.becomeFollower(2, 2)
		sm.raftLog.commitTo(commit)
		sm.handleHeartbeat(tt.m)
		assert.Equal(t, tt.wCommit, sm.raftLog.committed, "#%d", i)
		m := sm.readMessages()
		require.Len(t, m, 1, "#%d", i)
		assert.Equal(t, pb.MsgHeartbeatResp, m[0].Type, "#%d", i)
	}
}

// TestHandleHeartbeatResp ensures that we re-send log entries when we get a heartbeat response.
func TestHandleHeartbeatResp(t *testing.T) {
	storage := newTestMemoryStorage(withPeers(1, 2))
	require.NoError(t, storage.Append(index(1).terms(1, 2, 3)))
	sm := newTestRaft(1, 5, 1, storage)
	sm.becomeCandidate()
	sm.becomeLeader()
	sm.raftLog.commitTo(sm.raftLog.lastIndex())

	// A heartbeat response from a node that is behind; re-send MsgApp
	sm.Step(pb.Message{From: 2, Type: pb.MsgHeartbeatResp})
	msgs := sm.readMessages()
	require.Len(t, msgs, 1)
	assert.Equal(t, pb.MsgApp, msgs[0].Type)

	// A second heartbeat response generates another MsgApp re-send
	sm.Step(pb.Message{From: 2, Type: pb.MsgHeartbeatResp})
	msgs = sm.readMessages()
	require.Len(t, msgs, 1)
	assert.Equal(t, pb.MsgApp, msgs[0].Type)

	// Once we have an MsgAppResp, heartbeats no longer send MsgApp.
	sm.Step(pb.Message{
		From:  2,
		Type:  pb.MsgAppResp,
		Index: msgs[0].Index + uint64(len(msgs[0].Entries)),
	})
	// Consume the message sent in response to MsgAppResp
	sm.readMessages()

	sm.Step(pb.Message{From: 2, Type: pb.MsgHeartbeatResp})
	msgs = sm.readMessages()
	require.Empty(t, msgs)
}

// TestRaftFreesReadOnlyMem ensures raft will free read request from
// readOnly readIndexQueue and pendingReadIndex map.
// related issue: https://github.com/etcd-io/etcd/issues/7571
func TestRaftFreesReadOnlyMem(t *testing.T) {
	sm := newTestRaft(1, 5, 1, newTestMemoryStorage(withPeers(1, 2)))
	sm.becomeCandidate()
	sm.becomeLeader()
	sm.raftLog.commitTo(sm.raftLog.lastIndex())

	ctx := []byte("ctx")

	// leader starts linearizable read request.
	// more info: raft dissertation 6.4, step 2.
	sm.Step(pb.Message{From: 2, Type: pb.MsgReadIndex, Entries: []pb.Entry{{Data: ctx}}})
	msgs := sm.readMessages()
	require.Len(t, msgs, 1)
	require.Equal(t, pb.MsgHeartbeat, msgs[0].Type)
	require.Equal(t, ctx, msgs[0].Context)
	require.Len(t, sm.readOnly.readIndexQueue, 1)
	require.Len(t, sm.readOnly.pendingReadIndex, 1)
	_, ok := sm.readOnly.pendingReadIndex[string(ctx)]
	require.True(t, ok)

	// heartbeat responses from majority of followers (1 in this case)
	// acknowledge the authority of the leader.
	// more info: raft dissertation 6.4, step 3.
	sm.Step(pb.Message{From: 2, Type: pb.MsgHeartbeatResp, Context: ctx})
	require.Empty(t, sm.readOnly.readIndexQueue)
	require.Empty(t, sm.readOnly.pendingReadIndex)
	_, ok = sm.readOnly.pendingReadIndex[string(ctx)]
	require.False(t, ok)
}

// TestMsgAppRespWaitReset verifies the resume behavior of a leader
// MsgAppResp.
func TestMsgAppRespWaitReset(t *testing.T) {
	s := newTestMemoryStorage(withPeers(1, 2, 3))
	sm := newTestRaft(1, 5, 1, s)
	sm.becomeCandidate()
	sm.becomeLeader()

	// Run n1 which includes sending a message like the below
	// one to n2, but also appending to its own log.
	nextEnts(sm, s)

	// Node 2 acks the first entry, making it committed.
	sm.Step(pb.Message{
		From:  2,
		Type:  pb.MsgAppResp,
		Index: 1,
	})
	require.Equal(t, uint64(1), sm.raftLog.committed)

	// Also consume the MsgApp messages that update Commit on the followers.
	sm.readMessages()

	// A new command is now proposed on node 1.
	sm.Step(pb.Message{
		From:    1,
		Type:    pb.MsgProp,
		Entries: []pb.Entry{{}},
	})

	// The command is broadcast to all nodes not in the wait state.
	// Node 2 left the wait state due to its MsgAppResp, but node 3 is still waiting.
	msgs := sm.readMessages()
	require.Len(t, msgs, 1)
	assert.Equal(t, pb.MsgApp, msgs[0].Type)
	assert.Equal(t, uint64(2), msgs[0].To)
	assert.Len(t, msgs[0].Entries, 1)
	assert.Equal(t, uint64(2), msgs[0].Entries[0].Index)

	// Now Node 3 acks the first entry. This releases the wait and entry 2 is sent.
	sm.Step(pb.Message{
		From:  3,
		Type:  pb.MsgAppResp,
		Index: 1,
	})
	msgs = sm.readMessages()
	require.Len(t, msgs, 1)
	assert.Equal(t, pb.MsgApp, msgs[0].Type)
	assert.Equal(t, uint64(3), msgs[0].To)
	assert.Len(t, msgs[0].Entries, 1)
	assert.Equal(t, uint64(2), msgs[0].Entries[0].Index)
}

func TestRecvMsgVote(t *testing.T) {
	testRecvMsgVote(t, pb.MsgVote)
}

func TestRecvMsgPreVote(t *testing.T) {
	testRecvMsgVote(t, pb.MsgPreVote)
}

func testRecvMsgVote(t *testing.T, msgType pb.MessageType) {
	tests := []struct {
		state          StateType
		index, logTerm uint64
		voteFor        uint64
		wreject        bool
	}{
		{StateFollower, 0, 0, None, true},
		{StateFollower, 0, 1, None, true},
		{StateFollower, 0, 2, None, true},
		{StateFollower, 0, 3, None, false},

		{StateFollower, 1, 0, None, true},
		{StateFollower, 1, 1, None, true},
		{StateFollower, 1, 2, None, true},
		{StateFollower, 1, 3, None, false},

		{StateFollower, 2, 0, None, true},
		{StateFollower, 2, 1, None, true},
		{StateFollower, 2, 2, None, false},
		{StateFollower, 2, 3, None, false},

		{StateFollower, 3, 0, None, true},
		{StateFollower, 3, 1, None, true},
		{StateFollower, 3, 2, None, false},
		{StateFollower, 3, 3, None, false},

		{StateFollower, 3, 2, 2, false},
		{StateFollower, 3, 2, 1, true},

		{StateLeader, 3, 3, 1, true},
		{StatePreCandidate, 3, 3, 1, true},
		{StateCandidate, 3, 3, 1, true},
	}

	for i, tt := range tests {
		sm := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1)))
		sm.state = tt.state
		switch tt.state {
		case StateFollower:
			sm.step = stepFollower
		case StateCandidate, StatePreCandidate:
			sm.step = stepCandidate
		case StateLeader:
			sm.step = stepLeader
		}
		sm.Vote = tt.voteFor
		sm.raftLog = newLog(&MemoryStorage{ents: index(0).terms(0, 2, 2)}, nil)

		// raft.Term is greater than or equal to raft.raftLog.lastTerm. In this
		// test we're only testing MsgVote responses when the campaigning node
		// has a different raft log compared to the recipient node.
		// Additionally we're verifying behaviour when the recipient node has
		// already given out its vote for its current term. We're not testing
		// what the recipient node does when receiving a message with a
		// different term number, so we simply initialize both term numbers to
		// be the same.
		term := max(sm.raftLog.lastEntryID().term, tt.logTerm)
		sm.Term = term
		sm.Step(pb.Message{Type: msgType, Term: term, From: 2, Index: tt.index, LogTerm: tt.logTerm})

		msgs := sm.readMessages()
		require.Len(t, msgs, 1, "#%d", i)
		assert.Equal(t, voteRespMsgType(msgType), msgs[0].Type, "#%d", i)
		assert.Equal(t, tt.wreject, msgs[0].Reject, "#%d", i)
	}
}

func TestStateTransition(t *testing.T) {
	tests := []struct {
		from   StateType
		to     StateType
		wallow bool
		wterm  uint64
		wlead  uint64
	}{
		{StateFollower, StateFollower, true, 1, None},
		{StateFollower, StatePreCandidate, true, 0, None},
		{StateFollower, StateCandidate, true, 1, None},
		{StateFollower, StateLeader, false, 0, None},

		{StatePreCandidate, StateFollower, true, 0, None},
		{StatePreCandidate, StatePreCandidate, true, 0, None},
		{StatePreCandidate, StateCandidate, true, 1, None},
		{StatePreCandidate, StateLeader, true, 0, 1},

		{StateCandidate, StateFollower, true, 0, None},
		{StateCandidate, StatePreCandidate, true, 0, None},
		{StateCandidate, StateCandidate, true, 1, None},
		{StateCandidate, StateLeader, true, 0, 1},

		{StateLeader, StateFollower, true, 1, None},
		{StateLeader, StatePreCandidate, false, 0, None},
		{StateLeader, StateCandidate, false, 1, None},
		{StateLeader, StateLeader, true, 0, 1},
	}

	for i, tt := range tests {
		func() {
			defer func() {
				if r := recover(); r != nil {
					assert.False(t, tt.wallow, "#%d", i)
				}
			}()

			sm := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1)))
			sm.state = tt.from

			switch tt.to {
			case StateFollower:
				sm.becomeFollower(tt.wterm, tt.wlead)
			case StatePreCandidate:
				sm.becomePreCandidate()
			case StateCandidate:
				sm.becomeCandidate()
			case StateLeader:
				sm.becomeLeader()
			}

			assert.Equal(t, tt.wterm, sm.Term, "#%d", i)
			assert.Equal(t, tt.wlead, sm.lead, "#%d", i)
		}()
	}
}

func TestAllServerStepdown(t *testing.T) {
	tests := []struct {
		state StateType

		wstate StateType
		wterm  uint64
		windex uint64
	}{
		{StateFollower, StateFollower, 3, 0},
		{StatePreCandidate, StateFollower, 3, 0},
		{StateCandidate, StateFollower, 3, 0},
		{StateLeader, StateFollower, 3, 1},
	}

	tmsgTypes := [...]pb.MessageType{pb.MsgVote, pb.MsgApp}
	tterm := uint64(3)

	for i, tt := range tests {
		sm := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
		switch tt.state {
		case StateFollower:
			sm.becomeFollower(1, None)
		case StatePreCandidate:
			sm.becomePreCandidate()
		case StateCandidate:
			sm.becomeCandidate()
		case StateLeader:
			sm.becomeCandidate()
			sm.becomeLeader()
		}

		for j, msgType := range tmsgTypes {
			sm.Step(pb.Message{From: 2, Type: msgType, Term: tterm, LogTerm: tterm})

			assert.Equal(t, tt.wstate, sm.state, "#%d.%d", i, j)
			assert.Equal(t, tt.wterm, sm.Term, "#%d.%d", i, j)
			assert.Equal(t, tt.windex, sm.raftLog.lastIndex(), "#%d.%d", i, j)
			assert.Len(t, sm.raftLog.allEntries(), int(tt.windex), "#%d.%d", i, j)

			wlead := uint64(2)
			if msgType == pb.MsgVote {
				wlead = None
			}
			assert.Equal(t, wlead, sm.lead, "#%d.%d", i, j)
		}
	}
}

func TestCandidateResetTermMsgHeartbeat(t *testing.T) {
	testCandidateResetTerm(t, pb.MsgHeartbeat)
}

func TestCandidateResetTermMsgApp(t *testing.T) {
	testCandidateResetTerm(t, pb.MsgApp)
}

// testCandidateResetTerm tests when a candidate receives a
// MsgHeartbeat or MsgApp from leader, "Step" resets the term
// with leader's and reverts back to follower.
func testCandidateResetTerm(t *testing.T, mt pb.MessageType) {
	a := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	b := newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	c := newTestRaft(3, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))

	nt := newNetwork(a, b, c)

	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	assert.Equal(t, StateLeader, a.state)
	assert.Equal(t, StateFollower, b.state)
	assert.Equal(t, StateFollower, c.state)

	// isolate 3 and increase term in rest
	nt.isolate(3)

	nt.send(pb.Message{From: 2, To: 2, Type: pb.MsgHup})
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	assert.Equal(t, StateLeader, a.state)
	assert.Equal(t, StateFollower, b.state)

	// trigger campaign in isolated c
	c.resetRandomizedElectionTimeout()
	for i := 0; i < c.randomizedElectionTimeout; i++ {
		c.tick()
	}
	c.advanceMessagesAfterAppend()

	assert.Equal(t, StateCandidate, c.state)

	nt.recover()

	// leader sends to isolated candidate
	// and expects candidate to revert to follower
	nt.send(pb.Message{From: 1, To: 3, Term: a.Term, Type: mt})

	assert.Equal(t, StateFollower, c.state)
	// follower c term is reset with leader's
	assert.Equal(t, a.Term, c.Term)

}

// The following three tests exercise the behavior of a (pre-)candidate when its
// own self-vote is delivered back to itself after the peer has already learned
// that it has lost the election. The self-vote should be ignored in these cases.

func TestCandidateSelfVoteAfterLostElection(t *testing.T) {
	testCandidateSelfVoteAfterLostElection(t, false)
}

func TestCandidateSelfVoteAfterLostElectionPreVote(t *testing.T) {
	testCandidateSelfVoteAfterLostElection(t, true)
}

func testCandidateSelfVoteAfterLostElection(t *testing.T, preVote bool) {
	sm := newTestRaft(1, 5, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	sm.preVote = preVote

	// n1 calls an election.
	sm.Step(pb.Message{From: 1, To: 1, Type: pb.MsgHup})
	steps := sm.takeMessagesAfterAppend()

	// n1 hears that n2 already won the election before it has had a
	// change to sync its vote to disk and account for its self-vote.
	// Becomes a follower.
	sm.Step(pb.Message{From: 2, To: 1, Term: sm.Term, Type: pb.MsgHeartbeat})
	assert.Equal(t, StateFollower, sm.state)

	// n1 remains a follower even after its self-vote is delivered.
	sm.stepOrSend(steps)
	assert.Equal(t, StateFollower, sm.state)

	// Its self-vote does not make its way to its ProgressTracker.
	granted, _, _ := sm.trk.TallyVotes()
	assert.Zero(t, granted)

}

func TestCandidateDeliversPreCandidateSelfVoteAfterBecomingCandidate(t *testing.T) {
	sm := newTestRaft(1, 5, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	sm.preVote = true

	// n1 calls an election.
	sm.Step(pb.Message{From: 1, To: 1, Type: pb.MsgHup})
	assert.Equal(t, StatePreCandidate, sm.state)
	steps := sm.takeMessagesAfterAppend()

	// n1 receives pre-candidate votes from both other peers before
	// voting for itself. n1 becomes a candidate.
	// NB: pre-vote messages carry the local term + 1.
	sm.Step(pb.Message{From: 2, To: 1, Term: sm.Term + 1, Type: pb.MsgPreVoteResp})
	sm.Step(pb.Message{From: 3, To: 1, Term: sm.Term + 1, Type: pb.MsgPreVoteResp})
	assert.Equal(t, StateCandidate, sm.state)

	// n1 remains a candidate even after its delayed pre-vote self-vote is
	// delivered.
	sm.stepOrSend(steps)
	assert.Equal(t, StateCandidate, sm.state)

	steps = sm.takeMessagesAfterAppend()

	// Its pre-vote self-vote does not make its way to its ProgressTracker.
	granted, _, _ := sm.trk.TallyVotes()
	assert.Zero(t, granted)

	// A single vote from n2 does not move n1 to the leader.
	sm.Step(pb.Message{From: 2, To: 1, Term: sm.Term, Type: pb.MsgVoteResp})
	assert.Equal(t, StateCandidate, sm.state)

	// n1 becomes the leader once its self-vote is received because now
	// quorum is reached.
	sm.stepOrSend(steps)
	assert.Equal(t, StateLeader, sm.state)
}

func TestLeaderMsgAppSelfAckAfterTermChange(t *testing.T) {
	sm := newTestRaft(1, 5, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	sm.becomeCandidate()
	sm.becomeLeader()

	// n1 proposes a write.
	sm.Step(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("somedata")}}})
	steps := sm.takeMessagesAfterAppend()

	// n1 hears that n2 is the new leader.
	sm.Step(pb.Message{From: 2, To: 1, Term: sm.Term + 1, Type: pb.MsgHeartbeat})
	assert.Equal(t, StateFollower, sm.state)

	// n1 advances, ignoring its earlier self-ack of its MsgApp. The
	// corresponding MsgAppResp is ignored because it carries an earlier term.
	sm.stepOrSend(steps)
	assert.Equal(t, StateFollower, sm.state)
}

func TestLeaderStepdownWhenQuorumActive(t *testing.T) {
	sm := newTestRaft(1, 5, 1, newTestMemoryStorage(withPeers(1, 2, 3)))

	sm.checkQuorum = true

	sm.becomeCandidate()
	sm.becomeLeader()

	for i := 0; i < sm.electionTimeout+1; i++ {
		sm.Step(pb.Message{From: 2, Type: pb.MsgHeartbeatResp, Term: sm.Term})
		sm.tick()
	}

	assert.Equal(t, StateLeader, sm.state)
}

func TestLeaderStepdownWhenQuorumLost(t *testing.T) {
	sm := newTestRaft(1, 5, 1, newTestMemoryStorage(withPeers(1, 2, 3)))

	sm.checkQuorum = true

	sm.becomeCandidate()
	sm.becomeLeader()

	for i := 0; i < sm.electionTimeout+1; i++ {
		sm.tick()
	}

	assert.Equal(t, StateFollower, sm.state)
}

func TestLeaderSupersedingWithCheckQuorum(t *testing.T) {
	a := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	b := newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	c := newTestRaft(3, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))

	a.checkQuorum = true
	b.checkQuorum = true
	c.checkQuorum = true

	nt := newNetwork(a, b, c)
	setRandomizedElectionTimeout(b, b.electionTimeout+1)

	for i := 0; i < b.electionTimeout; i++ {
		b.tick()
	}
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	assert.Equal(t, StateLeader, a.state)
	assert.Equal(t, StateFollower, c.state)

	nt.send(pb.Message{From: 3, To: 3, Type: pb.MsgHup})

	// Peer b rejected c's vote since its electionElapsed had not reached to electionTimeout
	assert.Equal(t, StateCandidate, c.state)

	// Letting b's electionElapsed reach to electionTimeout
	for i := 0; i < b.electionTimeout; i++ {
		b.tick()
	}
	nt.send(pb.Message{From: 3, To: 3, Type: pb.MsgHup})

	assert.Equal(t, StateLeader, c.state)
}

func TestLeaderElectionWithCheckQuorum(t *testing.T) {
	a := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	b := newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	c := newTestRaft(3, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))

	a.checkQuorum = true
	b.checkQuorum = true
	c.checkQuorum = true

	nt := newNetwork(a, b, c)
	setRandomizedElectionTimeout(a, a.electionTimeout+1)
	setRandomizedElectionTimeout(b, b.electionTimeout+2)

	// Immediately after creation, votes are cast regardless of the
	// election timeout.
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	assert.Equal(t, StateLeader, a.state)
	assert.Equal(t, StateFollower, c.state)

	// need to reset randomizedElectionTimeout larger than electionTimeout again,
	// because the value might be reset to electionTimeout since the last state changes
	setRandomizedElectionTimeout(a, a.electionTimeout+1)
	setRandomizedElectionTimeout(b, b.electionTimeout+2)
	for i := 0; i < a.electionTimeout; i++ {
		a.tick()
	}
	for i := 0; i < b.electionTimeout; i++ {
		b.tick()
	}
	nt.send(pb.Message{From: 3, To: 3, Type: pb.MsgHup})

	assert.Equal(t, StateFollower, a.state)
	assert.Equal(t, StateLeader, c.state)
}

// TestFreeStuckCandidateWithCheckQuorum ensures that a candidate with a higher term
// can disrupt the leader even if the leader still "officially" holds the lease, The
// leader is expected to step down and adopt the candidate's term
func TestFreeStuckCandidateWithCheckQuorum(t *testing.T) {
	a := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	b := newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	c := newTestRaft(3, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))

	a.checkQuorum = true
	b.checkQuorum = true
	c.checkQuorum = true

	nt := newNetwork(a, b, c)
	setRandomizedElectionTimeout(b, b.electionTimeout+1)

	for i := 0; i < b.electionTimeout; i++ {
		b.tick()
	}
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	nt.isolate(1)
	nt.send(pb.Message{From: 3, To: 3, Type: pb.MsgHup})

	assert.Equal(t, StateFollower, b.state)
	assert.Equal(t, StateCandidate, c.state)
	assert.Equal(t, b.Term+1, c.Term)

	// Vote again for safety
	nt.send(pb.Message{From: 3, To: 3, Type: pb.MsgHup})

	assert.Equal(t, StateFollower, b.state)
	assert.Equal(t, StateCandidate, c.state)
	assert.Equal(t, b.Term+2, c.Term)

	nt.recover()
	nt.send(pb.Message{From: 1, To: 3, Type: pb.MsgHeartbeat, Term: a.Term})

	// Disrupt the leader so that the stuck peer is freed
	assert.Equal(t, StateFollower, a.state)
	assert.Equal(t, a.Term, c.Term)

	// Vote again, should become leader this time
	nt.send(pb.Message{From: 3, To: 3, Type: pb.MsgHup})

	assert.Equal(t, StateLeader, c.state)
}

func TestNonPromotableVoterWithCheckQuorum(t *testing.T) {
	a := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2)))
	b := newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1)))

	a.checkQuorum = true
	b.checkQuorum = true

	nt := newNetwork(a, b)
	setRandomizedElectionTimeout(b, b.electionTimeout+1)
	// Need to remove 2 again to make it a non-promotable node since newNetwork overwritten some internal states
	b.applyConfChange(pb.ConfChange{Type: pb.ConfChangeRemoveNode, NodeID: 2}.AsV2())

	require.False(t, b.promotable())

	for i := 0; i < b.electionTimeout; i++ {
		b.tick()
	}
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	assert.Equal(t, StateLeader, a.state)
	assert.Equal(t, StateFollower, b.state)
	assert.Equal(t, uint64(1), b.lead)
}

// TestDisruptiveFollower tests isolated follower,
// with slow network incoming from leader, election times out
// to become a candidate with an increased term. Then, the
// candiate's response to late leader heartbeat forces the leader
// to step down.
func TestDisruptiveFollower(t *testing.T) {
	n1 := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	n2 := newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	n3 := newTestRaft(3, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))

	n1.checkQuorum = true
	n2.checkQuorum = true
	n3.checkQuorum = true

	n1.becomeFollower(1, None)
	n2.becomeFollower(1, None)
	n3.becomeFollower(1, None)

	nt := newNetwork(n1, n2, n3)

	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	// check state
	require.Equal(t, StateLeader, n1.state)
	require.Equal(t, StateFollower, n2.state)
	require.Equal(t, StateFollower, n3.state)

	// etcd server "advanceTicksForElection" on restart;
	// this is to expedite campaign trigger when given larger
	// election timeouts (e.g. multi-datacenter deploy)
	// Or leader messages are being delayed while ticks elapse
	setRandomizedElectionTimeout(n3, n3.electionTimeout+2)
	for i := 0; i < n3.randomizedElectionTimeout-1; i++ {
		n3.tick()
	}

	// ideally, before last election tick elapses,
	// the follower n3 receives "pb.MsgApp" or "pb.MsgHeartbeat"
	// from leader n1, and then resets its "electionElapsed"
	// however, last tick may elapse before receiving any
	// messages from leader, thus triggering campaign
	n3.tick()

	// n1 is still leader yet
	// while its heartbeat to candidate n3 is being delayed

	// check state
	require.Equal(t, StateLeader, n1.state)
	require.Equal(t, StateFollower, n2.state)
	require.Equal(t, StateCandidate, n3.state)

	// check term
	require.Equal(t, uint64(2), n1.Term)
	require.Equal(t, uint64(2), n2.Term)
	require.Equal(t, uint64(3), n3.Term)

	// while outgoing vote requests are still queued in n3,
	// leader heartbeat finally arrives at candidate n3
	// however, due to delayed network from leader, leader
	// heartbeat was sent with lower term than candidate's
	nt.send(pb.Message{From: 1, To: 3, Term: n1.Term, Type: pb.MsgHeartbeat})

	// then candidate n3 responds with "pb.MsgAppResp" of higher term
	// and leader steps down from a message with higher term
	// this is to disrupt the current leader, so that candidate
	// with higher term can be freed with following election

	// check state
	require.Equal(t, StateFollower, n1.state)
	require.Equal(t, StateFollower, n2.state)
	require.Equal(t, StateCandidate, n3.state)

	// check term
	require.Equal(t, uint64(3), n1.Term)
	require.Equal(t, uint64(2), n2.Term)
	require.Equal(t, uint64(3), n3.Term)
}

// TestDisruptiveFollowerPreVote tests isolated follower,
// with slow network incoming from leader, election times out
// to become a pre-candidate with less log than current leader.
// Then pre-vote phase prevents this isolated node from forcing
// current leader to step down, thus less disruptions.
func TestDisruptiveFollowerPreVote(t *testing.T) {
	n1 := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	n2 := newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	n3 := newTestRaft(3, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))

	n1.checkQuorum = true
	n2.checkQuorum = true
	n3.checkQuorum = true

	n1.becomeFollower(1, None)
	n2.becomeFollower(1, None)
	n3.becomeFollower(1, None)

	nt := newNetwork(n1, n2, n3)

	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	// check state
	require.Equal(t, StateLeader, n1.state)
	require.Equal(t, StateFollower, n2.state)
	require.Equal(t, StateFollower, n3.state)

	nt.isolate(3)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("somedata")}}})
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("somedata")}}})
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("somedata")}}})
	n1.preVote = true
	n2.preVote = true
	n3.preVote = true
	nt.recover()
	nt.send(pb.Message{From: 3, To: 3, Type: pb.MsgHup})

	// check state
	require.Equal(t, StateLeader, n1.state)
	require.Equal(t, StateFollower, n2.state)
	require.Equal(t, StatePreCandidate, n3.state)

	// check term
	require.Equal(t, uint64(2), n1.Term)
	require.Equal(t, uint64(2), n2.Term)
	require.Equal(t, uint64(2), n3.Term)

	// delayed leader heartbeat does not force current leader to step down
	nt.send(pb.Message{From: 1, To: 3, Term: n1.Term, Type: pb.MsgHeartbeat})
	require.Equal(t, StateLeader, n1.state)
}

func TestReadOnlyOptionSafe(t *testing.T) {
	a := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	b := newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	c := newTestRaft(3, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))

	nt := newNetwork(a, b, c)
	setRandomizedElectionTimeout(b, b.electionTimeout+1)

	for i := 0; i < b.electionTimeout; i++ {
		b.tick()
	}
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	require.Equal(t, StateLeader, a.state)

	tests := []struct {
		sm        *raft
		proposals int
		wri       uint64
		wctx      []byte
	}{
		{a, 10, 11, []byte("ctx1")},
		{b, 10, 21, []byte("ctx2")},
		{c, 10, 31, []byte("ctx3")},
		{a, 10, 41, []byte("ctx4")},
		{b, 10, 51, []byte("ctx5")},
		{c, 10, 61, []byte("ctx6")},
	}

	for i, tt := range tests {
		for j := 0; j < tt.proposals; j++ {
			nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{}}})
		}

		nt.send(pb.Message{From: tt.sm.id, To: tt.sm.id, Type: pb.MsgReadIndex, Entries: []pb.Entry{{Data: tt.wctx}}})

		r := tt.sm
		assert.NotEmpty(t, r.readStates, "#%d", i)
		rs := r.readStates[0]
		assert.Equal(t, tt.wri, rs.Index, "#%d", i)
		assert.Equal(t, tt.wctx, rs.RequestCtx, "#%d", i)
		r.readStates = nil
	}
}

func TestReadOnlyWithLearner(t *testing.T) {
	s := newTestMemoryStorage(withPeers(1), withLearners(2))
	a := newTestLearnerRaft(1, 10, 1, s)
	b := newTestLearnerRaft(2, 10, 1, newTestMemoryStorage(withPeers(1), withLearners(2)))

	nt := newNetwork(a, b)
	setRandomizedElectionTimeout(b, b.electionTimeout+1)

	for i := 0; i < b.electionTimeout; i++ {
		b.tick()
	}
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	require.Equal(t, StateLeader, a.state)

	tests := []struct {
		sm        *raft
		proposals int
		wri       uint64
		wctx      []byte
	}{
		{a, 10, 11, []byte("ctx1")},
		{b, 10, 21, []byte("ctx2")},
		{a, 10, 31, []byte("ctx3")},
		{b, 10, 41, []byte("ctx4")},
	}

	for i, tt := range tests {
		for j := 0; j < tt.proposals; j++ {
			nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{}}})
			nextEnts(a, s) // append the entries on the leader
		}

		nt.send(pb.Message{From: tt.sm.id, To: tt.sm.id, Type: pb.MsgReadIndex, Entries: []pb.Entry{{Data: tt.wctx}}})

		r := tt.sm
		require.NotEmpty(t, r.readStates, "#%d", i)
		rs := r.readStates[0]
		assert.Equal(t, tt.wri, rs.Index, "#%d", i)
		assert.Equal(t, tt.wctx, rs.RequestCtx, "#%d", i)
		r.readStates = nil
	}
}

func TestReadOnlyOptionLease(t *testing.T) {
	a := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	b := newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	c := newTestRaft(3, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	a.readOnly.option = ReadOnlyLeaseBased
	b.readOnly.option = ReadOnlyLeaseBased
	c.readOnly.option = ReadOnlyLeaseBased
	a.checkQuorum = true
	b.checkQuorum = true
	c.checkQuorum = true

	nt := newNetwork(a, b, c)
	setRandomizedElectionTimeout(b, b.electionTimeout+1)

	for i := 0; i < b.electionTimeout; i++ {
		b.tick()
	}
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	require.Equal(t, StateLeader, a.state)

	tests := []struct {
		sm        *raft
		proposals int
		wri       uint64
		wctx      []byte
	}{
		{a, 10, 11, []byte("ctx1")},
		{b, 10, 21, []byte("ctx2")},
		{c, 10, 31, []byte("ctx3")},
		{a, 10, 41, []byte("ctx4")},
		{b, 10, 51, []byte("ctx5")},
		{c, 10, 61, []byte("ctx6")},
	}

	for i, tt := range tests {
		for j := 0; j < tt.proposals; j++ {
			nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{}}})
		}

		nt.send(pb.Message{From: tt.sm.id, To: tt.sm.id, Type: pb.MsgReadIndex, Entries: []pb.Entry{{Data: tt.wctx}}})

		r := tt.sm
		rs := r.readStates[0]
		assert.Equal(t, tt.wri, rs.Index, "#%d", i)
		assert.Equal(t, tt.wctx, rs.RequestCtx, "#%d", i)
		r.readStates = nil
	}
}

// TestReadOnlyForNewLeader ensures that a leader only accepts MsgReadIndex message
// when it commits at least one log entry at it term.
func TestReadOnlyForNewLeader(t *testing.T) {
	nodeConfigs := []struct {
		id           uint64
		committed    uint64
		applied      uint64
		compactIndex uint64
	}{
		{1, 1, 1, 0},
		{2, 2, 2, 2},
		{3, 2, 2, 2},
	}
	peers := make([]stateMachine, 0)
	for _, c := range nodeConfigs {
		storage := newTestMemoryStorage(withPeers(1, 2, 3))
		require.NoError(t, storage.Append(index(1).terms(1, 1)))
		storage.SetHardState(pb.HardState{Term: 1, Commit: c.committed})
		if c.compactIndex != 0 {
			storage.Compact(c.compactIndex)
		}
		cfg := newTestConfig(c.id, 10, 1, storage)
		cfg.Applied = c.applied
		raft := newRaft(cfg)
		peers = append(peers, raft)
	}
	nt := newNetwork(peers...)

	// Drop MsgApp to forbid peer a to commit any log entry at its term after it becomes leader.
	nt.ignore(pb.MsgApp)
	// Force peer a to become leader.
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	sm := nt.peers[1].(*raft)
	require.Equal(t, StateLeader, sm.state)

	// Ensure peer a drops read only request.
	var windex uint64 = 4
	wctx := []byte("ctx")
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgReadIndex, Entries: []pb.Entry{{Data: wctx}}})
	require.Empty(t, sm.readStates)

	nt.recover()

	// Force peer a to commit a log entry at its term
	for i := 0; i < sm.heartbeatTimeout; i++ {
		sm.tick()
	}
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{}}})
	require.Equal(t, uint64(4), sm.raftLog.committed)
	lastLogTerm := sm.raftLog.zeroTermOnOutOfBounds(sm.raftLog.term(sm.raftLog.committed))
	require.Equal(t, sm.Term, lastLogTerm)

	// Ensure peer a processed postponed read only request after it committed an entry at its term.
	require.Len(t, sm.readStates, 1)
	rs := sm.readStates[0]
	require.Equal(t, windex, rs.Index)
	require.Equal(t, wctx, rs.RequestCtx)

	// Ensure peer a accepts read only request after it committed an entry at its term.
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgReadIndex, Entries: []pb.Entry{{Data: wctx}}})
	require.Len(t, sm.readStates, 2)
	rs = sm.readStates[1]
	require.Equal(t, windex, rs.Index)
	require.Equal(t, wctx, rs.RequestCtx)
}

func TestLeaderAppResp(t *testing.T) {
	// initial progress: match = 0; next = 3
	tests := []struct {
		index  uint64
		reject bool
		// progress
		wmatch uint64
		wnext  uint64
		// message
		wmsgNum    int
		windex     uint64
		wcommitted uint64
	}{
		{3, true, 0, 3, 0, 0, 0},  // stale resp; no replies
		{2, true, 0, 2, 1, 1, 0},  // denied resp; leader does not commit; decrease next and send probing msg
		{2, false, 2, 4, 2, 2, 2}, // accept resp; leader commits; broadcast with commit index
		// Follower is StateProbing at 0, it sends MsgAppResp for 0 (which
		// matches the pr.Match) so it is moved to StateReplicate and as many
		// entries as possible are sent to it (1, 2, and 3). Correspondingly the
		// Next is then 4 (an Entry at 4 does not exist, indicating the follower
		// will be up to date should it process the emitted MsgApp).
		{0, false, 0, 4, 1, 0, 0},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			// sm term is 1 after it becomes the leader.
			// thus the last log term must be 1 to be committed.
			sm := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
			sm.raftLog = newLog(&MemoryStorage{ents: index(0).terms(0, 1, 1)}, nil)
			sm.becomeCandidate()
			sm.becomeLeader()
			sm.readMessages()
			require.NoError(t, sm.Step(
				pb.Message{
					From:       2,
					Type:       pb.MsgAppResp,
					Index:      tt.index,
					Term:       sm.Term,
					Reject:     tt.reject,
					RejectHint: tt.index,
				},
			))

			p := sm.trk.Progress[2]
			require.Equal(t, tt.wmatch, p.Match)
			require.Equal(t, tt.wnext, p.Next)

			msgs := sm.readMessages()

			require.Len(t, msgs, tt.wmsgNum)
			for _, msg := range msgs {
				require.Equal(t, tt.windex, msg.Index, "%v", DescribeMessage(msg, nil))
				require.Equal(t, tt.wcommitted, msg.Commit, "%v", DescribeMessage(msg, nil))
			}
		})
	}
}

// TestBcastBeat is when the leader receives a heartbeat tick, it should
// send a MsgHeartbeat with m.Index = 0, m.LogTerm=0 and empty entries.
func TestBcastBeat(t *testing.T) {
	offset := uint64(1000)
	// make a state machine with log.offset = 1000
	s := pb.Snapshot{
		Metadata: pb.SnapshotMetadata{
			Index:     offset,
			Term:      1,
			ConfState: pb.ConfState{Voters: []uint64{1, 2, 3}},
		},
	}
	storage := NewMemoryStorage()
	storage.ApplySnapshot(s)
	sm := newTestRaft(1, 10, 1, storage)
	sm.Term = 1

	sm.becomeCandidate()
	sm.becomeLeader()
	for i := 0; i < 10; i++ {
		mustAppendEntry(sm, pb.Entry{Index: uint64(i) + 1})
	}
	sm.advanceMessagesAfterAppend()

	// slow follower
	sm.trk.Progress[2].Match, sm.trk.Progress[2].Next = 5, 6
	// normal follower
	sm.trk.Progress[3].Match, sm.trk.Progress[3].Next = sm.raftLog.lastIndex(), sm.raftLog.lastIndex()+1

	sm.Step(pb.Message{Type: pb.MsgBeat})
	msgs := sm.readMessages()
	require.Len(t, msgs, 2)

	wantCommitMap := map[uint64]uint64{
		2: min(sm.raftLog.committed, sm.trk.Progress[2].Match),
		3: min(sm.raftLog.committed, sm.trk.Progress[3].Match),
	}
	for i, m := range msgs {
		require.Equal(t, pb.MsgHeartbeat, m.Type, "#%d", i)
		require.Zero(t, m.Index, "#%d", i)
		require.Zero(t, m.LogTerm, "#%d", i)

		commit, ok := wantCommitMap[m.To]
		require.True(t, ok, "#%d", i)
		require.Equal(t, commit, m.Commit, "#%d", i)
		delete(wantCommitMap, m.To)

		require.Empty(t, m.Entries, "#%d", i)
	}
}

// TestRecvMsgBeat tests the output of the state machine when receiving MsgBeat
func TestRecvMsgBeat(t *testing.T) {
	tests := []struct {
		state StateType
		wMsg  int
	}{
		{StateLeader, 2},
		// candidate and follower should ignore MsgBeat
		{StateCandidate, 0},
		{StateFollower, 0},
	}

	for i, tt := range tests {
		sm := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
		sm.raftLog = newLog(&MemoryStorage{ents: index(0).terms(0, 1, 1)}, nil)
		sm.Term = 1
		sm.state = tt.state
		switch tt.state {
		case StateFollower:
			sm.step = stepFollower
		case StateCandidate:
			sm.step = stepCandidate
		case StateLeader:
			sm.step = stepLeader
		}
		sm.Step(pb.Message{From: 1, To: 1, Type: pb.MsgBeat})

		msgs := sm.readMessages()
		assert.Len(t, msgs, tt.wMsg, "#%d", i)
		for _, m := range msgs {
			assert.Equal(t, pb.MsgHeartbeat, m.Type, "#%d", i)
		}
	}
}

func TestLeaderIncreaseNext(t *testing.T) {
	previousEnts := index(1).terms(1, 2, 3)
	tests := []struct {
		// progress
		state tracker.StateType
		next  uint64

		wnext uint64
	}{
		// state replicate, optimistically increase next
		// previous entries + noop entry + propose + 1
		{tracker.StateReplicate, 2, uint64(len(previousEnts) + 1 + 1 + 1)},
		// state probe, not optimistically increase next
		{tracker.StateProbe, 2, 2},
	}

	for i, tt := range tests {
		sm := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2)))
		sm.raftLog.append(previousEnts...)
		sm.becomeCandidate()
		sm.becomeLeader()
		sm.trk.Progress[2].State = tt.state
		sm.trk.Progress[2].Next = tt.next
		sm.Step(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("somedata")}}})

		p := sm.trk.Progress[2]
		assert.Equal(t, tt.wnext, p.Next, "#%d", i)
	}
}

func TestSendAppendForProgressProbe(t *testing.T) {
	r := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2)))
	r.becomeCandidate()
	r.becomeLeader()
	r.readMessages()
	r.trk.Progress[2].BecomeProbe()

	// each round is a heartbeat
	for i := 0; i < 3; i++ {
		if i == 0 {
			// we expect that raft will only send out one msgAPP on the first
			// loop. After that, the follower is paused until a heartbeat response is
			// received.
			mustAppendEntry(r, pb.Entry{Data: []byte("somedata")})
			r.sendAppend(2)
			msg := r.readMessages()
			assert.Len(t, msg, 1)
			assert.Zero(t, msg[0].Index)
		}

		assert.True(t, r.trk.Progress[2].MsgAppFlowPaused)
		for j := 0; j < 10; j++ {
			mustAppendEntry(r, pb.Entry{Data: []byte("somedata")})
			r.sendAppend(2)
			assert.Empty(t, r.readMessages())
		}

		// do a heartbeat
		for j := 0; j < r.heartbeatTimeout; j++ {
			r.Step(pb.Message{From: 1, To: 1, Type: pb.MsgBeat})
		}
		assert.True(t, r.trk.Progress[2].MsgAppFlowPaused)

		// consume the heartbeat
		msg := r.readMessages()
		assert.Len(t, msg, 1)
		assert.Equal(t, pb.MsgHeartbeat, msg[0].Type)
	}

	// a heartbeat response will allow another message to be sent
	r.Step(pb.Message{From: 2, To: 1, Type: pb.MsgHeartbeatResp})
	msg := r.readMessages()
	assert.Len(t, msg, 1)
	assert.Zero(t, msg[0].Index)
	assert.True(t, r.trk.Progress[2].MsgAppFlowPaused)
}

func TestSendAppendForProgressReplicate(t *testing.T) {
	r := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2)))
	r.becomeCandidate()
	r.becomeLeader()
	r.readMessages()
	r.trk.Progress[2].BecomeReplicate()

	for i := 0; i < 10; i++ {
		mustAppendEntry(r, pb.Entry{Data: []byte("somedata")})
		r.sendAppend(2)
		msgs := r.readMessages()
		assert.Len(t, msgs, 1, "#%d", i)
	}
}

func TestSendAppendForProgressSnapshot(t *testing.T) {
	r := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2)))
	r.becomeCandidate()
	r.becomeLeader()
	r.readMessages()
	r.trk.Progress[2].BecomeSnapshot(10)

	for i := 0; i < 10; i++ {
		mustAppendEntry(r, pb.Entry{Data: []byte("somedata")})
		r.sendAppend(2)
		msgs := r.readMessages()
		assert.Empty(t, msgs, "#%d", i)
	}
}

func TestRecvMsgUnreachable(t *testing.T) {
	previousEnts := index(1).terms(1, 2, 3)
	s := newTestMemoryStorage(withPeers(1, 2))
	s.Append(previousEnts)
	r := newTestRaft(1, 10, 1, s)
	r.becomeCandidate()
	r.becomeLeader()
	r.readMessages()
	// set node 2 to state replicate
	r.trk.Progress[2].Match = 3
	r.trk.Progress[2].BecomeReplicate()
	r.trk.Progress[2].Next = 6

	r.Step(pb.Message{From: 2, To: 1, Type: pb.MsgUnreachable})

	assert.Equal(t, tracker.StateProbe, r.trk.Progress[2].State)
	wnext := r.trk.Progress[2].Match + 1
	assert.Equal(t, wnext, r.trk.Progress[2].Next)
}

func TestRestore(t *testing.T) {
	s := pb.Snapshot{
		Metadata: pb.SnapshotMetadata{
			Index:     11, // magic number
			Term:      11, // magic number
			ConfState: pb.ConfState{Voters: []uint64{1, 2, 3}},
		},
	}

	storage := newTestMemoryStorage(withPeers(1, 2))
	sm := newTestRaft(1, 10, 1, storage)
	require.True(t, sm.restore(s))

	assert.Equal(t, s.Metadata.Index, sm.raftLog.lastIndex())
	assert.Equal(t, s.Metadata.Term, mustTerm(sm.raftLog.term(s.Metadata.Index)))
	assert.Equal(t, s.Metadata.ConfState.Voters, sm.trk.VoterNodes())

	require.False(t, sm.restore(s))
	for i := 0; i < sm.randomizedElectionTimeout; i++ {
		sm.tick()
	}
	assert.Equal(t, StateFollower, sm.state)
}

// TestRestoreWithLearner restores a snapshot which contains learners.
func TestRestoreWithLearner(t *testing.T) {
	s := pb.Snapshot{
		Metadata: pb.SnapshotMetadata{
			Index:     11, // magic number
			Term:      11, // magic number
			ConfState: pb.ConfState{Voters: []uint64{1, 2}, Learners: []uint64{3}},
		},
	}

	storage := newTestMemoryStorage(withPeers(1, 2), withLearners(3))
	sm := newTestLearnerRaft(3, 8, 2, storage)
	assert.True(t, sm.restore(s))

	assert.Equal(t, s.Metadata.Index, sm.raftLog.lastIndex())
	assert.Equal(t, s.Metadata.Term, mustTerm(sm.raftLog.term(s.Metadata.Index)))

	sg := sm.trk.VoterNodes()
	assert.Len(t, sg, len(s.Metadata.ConfState.Voters))

	lns := sm.trk.LearnerNodes()
	assert.Len(t, lns, len(s.Metadata.ConfState.Learners))

	for _, n := range s.Metadata.ConfState.Voters {
		assert.False(t, sm.trk.Progress[n].IsLearner)
	}
	for _, n := range s.Metadata.ConfState.Learners {
		assert.True(t, sm.trk.Progress[n].IsLearner)
	}

	assert.False(t, sm.restore(s))
}

// TestRestoreWithVotersOutgoing tests if outgoing voter can receive and apply snapshot correctly.
func TestRestoreWithVotersOutgoing(t *testing.T) {
	s := pb.Snapshot{
		Metadata: pb.SnapshotMetadata{
			Index:     11, // magic number
			Term:      11, // magic number
			ConfState: pb.ConfState{Voters: []uint64{2, 3, 4}, VotersOutgoing: []uint64{1, 2, 3}},
		},
	}

	storage := newTestMemoryStorage(withPeers(1, 2))
	sm := newTestRaft(1, 10, 1, storage)
	require.True(t, sm.restore(s))

	assert.Equal(t, s.Metadata.Index, sm.raftLog.lastIndex())
	assert.Equal(t, mustTerm(sm.raftLog.term(s.Metadata.Index)), s.Metadata.Term)

	sg := sm.trk.VoterNodes()
	assert.Equal(t, []uint64{1, 2, 3, 4}, sg)

	require.False(t, sm.restore(s))

	// It should not campaign before actually applying data.
	for i := 0; i < sm.randomizedElectionTimeout; i++ {
		sm.tick()
	}
	assert.Equal(t, StateFollower, sm.state)
}

// TestRestoreVoterToLearner verifies that a normal peer can be downgraded to a
// learner through a snapshot. At the time of writing, we don't allow
// configuration changes to do this directly, but note that the snapshot may
// compress multiple changes to the configuration into one: the voter could have
// been removed, then readded as a learner and the snapshot reflects both
// changes. In that case, a voter receives a snapshot telling it that it is now
// a learner. In fact, the node has to accept that snapshot, or it is
// permanently cut off from the Raft log.
func TestRestoreVoterToLearner(t *testing.T) {
	s := pb.Snapshot{
		Metadata: pb.SnapshotMetadata{
			Index:     11, // magic number
			Term:      11, // magic number
			ConfState: pb.ConfState{Voters: []uint64{1, 2}, Learners: []uint64{3}},
		},
	}

	storage := newTestMemoryStorage(withPeers(1, 2, 3))
	sm := newTestRaft(3, 10, 1, storage)

	assert.False(t, sm.isLearner)
	assert.True(t, sm.restore(s))
}

// TestRestoreLearnerPromotion checks that a learner can become to a follower after
// restoring snapshot.
func TestRestoreLearnerPromotion(t *testing.T) {
	s := pb.Snapshot{
		Metadata: pb.SnapshotMetadata{
			Index:     11, // magic number
			Term:      11, // magic number
			ConfState: pb.ConfState{Voters: []uint64{1, 2, 3}},
		},
	}

	storage := newTestMemoryStorage(withPeers(1, 2), withLearners(3))
	sm := newTestLearnerRaft(3, 10, 1, storage)

	assert.True(t, sm.isLearner)
	assert.True(t, sm.restore(s))
	assert.False(t, sm.isLearner)
}

// TestLearnerReceiveSnapshot tests that a learner can receive a snpahost from leader
func TestLearnerReceiveSnapshot(t *testing.T) {
	// restore the state machine from a snapshot so it has a compacted log and a snapshot
	s := pb.Snapshot{
		Metadata: pb.SnapshotMetadata{
			Index:     11, // magic number
			Term:      11, // magic number
			ConfState: pb.ConfState{Voters: []uint64{1}, Learners: []uint64{2}},
		},
	}

	store := newTestMemoryStorage(withPeers(1), withLearners(2))
	n1 := newTestLearnerRaft(1, 10, 1, store)
	n2 := newTestLearnerRaft(2, 10, 1, newTestMemoryStorage(withPeers(1), withLearners(2)))

	n1.restore(s)
	snap := n1.raftLog.nextUnstableSnapshot()
	store.ApplySnapshot(*snap)
	n1.appliedSnap(snap)

	nt := newNetwork(n1, n2)

	setRandomizedElectionTimeout(n1, n1.electionTimeout)
	for i := 0; i < n1.electionTimeout; i++ {
		n1.tick()
	}

	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgBeat})

	assert.Equal(t, n1.raftLog.committed, n2.raftLog.committed)
}

func TestRestoreIgnoreSnapshot(t *testing.T) {
	previousEnts := index(1).terms(1, 1, 1)
	commit := uint64(1)
	storage := newTestMemoryStorage(withPeers(1, 2))
	sm := newTestRaft(1, 10, 1, storage)
	sm.raftLog.append(previousEnts...)
	sm.raftLog.commitTo(commit)

	s := pb.Snapshot{
		Metadata: pb.SnapshotMetadata{
			Index:     commit,
			Term:      1,
			ConfState: pb.ConfState{Voters: []uint64{1, 2}},
		},
	}

	// ignore snapshot
	assert.False(t, sm.restore(s))
	assert.Equal(t, sm.raftLog.committed, commit)

	// ignore snapshot and fast forward commit
	s.Metadata.Index = commit + 1
	assert.False(t, sm.restore(s))
	assert.Equal(t, sm.raftLog.committed, commit+1)
}

func TestProvideSnap(t *testing.T) {
	// restore the state machine from a snapshot so it has a compacted log and a snapshot
	s := pb.Snapshot{
		Metadata: pb.SnapshotMetadata{
			Index:     11, // magic number
			Term:      11, // magic number
			ConfState: pb.ConfState{Voters: []uint64{1, 2}},
		},
	}
	storage := newTestMemoryStorage(withPeers(1))
	sm := newTestRaft(1, 10, 1, storage)
	sm.restore(s)

	sm.becomeCandidate()
	sm.becomeLeader()

	// force set the next of node 2, so that node 2 needs a snapshot
	sm.trk.Progress[2].Next = sm.raftLog.firstIndex()
	sm.Step(pb.Message{From: 2, To: 1, Type: pb.MsgAppResp, Index: sm.trk.Progress[2].Next - 1, Reject: true})

	msgs := sm.readMessages()
	require.Len(t, msgs, 1)
	m := msgs[0]
	assert.Equal(t, m.Type, pb.MsgSnap)
}

func TestIgnoreProvidingSnap(t *testing.T) {
	// restore the state machine from a snapshot so it has a compacted log and a snapshot
	s := pb.Snapshot{
		Metadata: pb.SnapshotMetadata{
			Index:     11, // magic number
			Term:      11, // magic number
			ConfState: pb.ConfState{Voters: []uint64{1, 2}},
		},
	}
	storage := newTestMemoryStorage(withPeers(1))
	sm := newTestRaft(1, 10, 1, storage)
	sm.restore(s)

	sm.becomeCandidate()
	sm.becomeLeader()

	// force set the next of node 2, so that node 2 needs a snapshot
	// change node 2 to be inactive, expect node 1 ignore sending snapshot to 2
	sm.trk.Progress[2].Next = sm.raftLog.firstIndex() - 1
	sm.trk.Progress[2].RecentActive = false

	sm.Step(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("somedata")}}})

	msgs := sm.readMessages()
	assert.Empty(t, msgs)
}

func TestRestoreFromSnapMsg(t *testing.T) {
	s := &pb.Snapshot{
		Metadata: pb.SnapshotMetadata{
			Index:     11, // magic number
			Term:      11, // magic number
			ConfState: pb.ConfState{Voters: []uint64{1, 2}},
		},
	}
	m := pb.Message{Type: pb.MsgSnap, From: 1, Term: 2, Snapshot: s}

	sm := newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1, 2)))
	sm.Step(m)

	assert.Equal(t, uint64(1), sm.lead)
	// TODO(bdarnell): what should this test?
}

func TestSlowNodeRestore(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	nt.isolate(3)
	for j := 0; j <= 100; j++ {
		nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{}}})
	}
	lead := nt.peers[1].(*raft)
	nextEnts(lead, nt.storage[1])
	nt.storage[1].CreateSnapshot(lead.raftLog.applied, &pb.ConfState{Voters: lead.trk.VoterNodes()}, nil)
	nt.storage[1].Compact(lead.raftLog.applied)

	nt.recover()
	// send heartbeats so that the leader can learn everyone is active.
	// node 3 will only be considered as active when node 1 receives a reply from it.
	for {
		nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgBeat})
		if lead.trk.Progress[3].RecentActive {
			break
		}
	}

	// trigger a snapshot
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{}}})

	follower := nt.peers[3].(*raft)

	// trigger a commit
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{}}})
	assert.Equal(t, lead.raftLog.committed, follower.raftLog.committed)
}

// TestStepConfig tests that when raft step msgProp in EntryConfChange type,
// it appends the entry to log and sets pendingConf to be true.
func TestStepConfig(t *testing.T) {
	// a raft that cannot make progress
	r := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2)))
	r.becomeCandidate()
	r.becomeLeader()
	index := r.raftLog.lastIndex()
	r.Step(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Type: pb.EntryConfChange}}})
	assert.Equal(t, index+1, r.raftLog.lastIndex())
	assert.Equal(t, index+1, r.pendingConfIndex)
}

// TestStepIgnoreConfig tests that if raft step the second msgProp in
// EntryConfChange type when the first one is uncommitted, the node will set
// the proposal to noop and keep its original state.
func TestStepIgnoreConfig(t *testing.T) {
	// a raft that cannot make progress
	r := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2)))
	r.becomeCandidate()
	r.becomeLeader()
	r.Step(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Type: pb.EntryConfChange}}})
	index := r.raftLog.lastIndex()
	pendingConfIndex := r.pendingConfIndex
	r.Step(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Type: pb.EntryConfChange}}})
	wents := []pb.Entry{{Type: pb.EntryNormal, Term: 1, Index: 3, Data: nil}}
	ents, err := r.raftLog.entries(index+1, noLimit)
	require.NoError(t, err)
	assert.Equal(t, wents, ents)
	assert.Equal(t, pendingConfIndex, r.pendingConfIndex)
}

// TestNewLeaderPendingConfig tests that new leader sets its pendingConfigIndex
// based on uncommitted entries.
func TestNewLeaderPendingConfig(t *testing.T) {
	tests := []struct {
		addEntry      bool
		wpendingIndex uint64
	}{
		{false, 0},
		{true, 1},
	}
	for i, tt := range tests {
		r := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2)))
		if tt.addEntry {
			mustAppendEntry(r, pb.Entry{Type: pb.EntryNormal})
		}
		r.becomeCandidate()
		r.becomeLeader()
		assert.Equal(t, tt.wpendingIndex, r.pendingConfIndex, "#%d", i)
	}
}

// TestAddNode tests that addNode could update nodes correctly.
func TestAddNode(t *testing.T) {
	r := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1)))
	r.applyConfChange(pb.ConfChange{NodeID: 2, Type: pb.ConfChangeAddNode}.AsV2())
	nodes := r.trk.VoterNodes()
	assert.Equal(t, []uint64{1, 2}, nodes)
}

// TestAddLearner tests that addLearner could update nodes correctly.
func TestAddLearner(t *testing.T) {
	r := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1)))
	// Add new learner peer.
	r.applyConfChange(pb.ConfChange{NodeID: 2, Type: pb.ConfChangeAddLearnerNode}.AsV2())
	require.False(t, r.isLearner)
	nodes := r.trk.LearnerNodes()
	assert.Equal(t, []uint64{2}, nodes)
	require.True(t, r.trk.Progress[2].IsLearner)

	// Promote peer to voter.
	r.applyConfChange(pb.ConfChange{NodeID: 2, Type: pb.ConfChangeAddNode}.AsV2())
	require.False(t, r.trk.Progress[2].IsLearner)

	// Demote r.
	r.applyConfChange(pb.ConfChange{NodeID: 1, Type: pb.ConfChangeAddLearnerNode}.AsV2())
	require.True(t, r.trk.Progress[1].IsLearner)
	require.True(t, r.isLearner)

	// Promote r again.
	r.applyConfChange(pb.ConfChange{NodeID: 1, Type: pb.ConfChangeAddNode}.AsV2())
	require.False(t, r.trk.Progress[1].IsLearner)
	require.False(t, r.isLearner)
}

// TestAddNodeCheckQuorum tests that addNode does not trigger a leader election
// immediately when checkQuorum is set.
func TestAddNodeCheckQuorum(t *testing.T) {
	r := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1)))
	r.checkQuorum = true

	r.becomeCandidate()
	r.becomeLeader()

	for i := 0; i < r.electionTimeout-1; i++ {
		r.tick()
	}

	r.applyConfChange(pb.ConfChange{NodeID: 2, Type: pb.ConfChangeAddNode}.AsV2())

	// This tick will reach electionTimeout, which triggers a quorum check.
	r.tick()

	// Node 1 should still be the leader after a single tick.
	assert.Equal(t, StateLeader, r.state)

	// After another electionTimeout ticks without hearing from node 2,
	// node 1 should step down.
	for i := 0; i < r.electionTimeout; i++ {
		r.tick()
	}

	assert.Equal(t, StateFollower, r.state)
}

// TestRemoveNode tests that removeNode could update nodes and
// removed list correctly.
func TestRemoveNode(t *testing.T) {
	r := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2)))
	r.applyConfChange(pb.ConfChange{NodeID: 2, Type: pb.ConfChangeRemoveNode}.AsV2())
	w := []uint64{1}
	assert.Equal(t, w, r.trk.VoterNodes())

	// Removing the remaining voter will panic.
	defer func() {
		assert.NotNil(t, recover(), "did not panic")
	}()
	r.applyConfChange(pb.ConfChange{NodeID: 1, Type: pb.ConfChangeRemoveNode}.AsV2())
}

// TestRemoveLearner tests that removeNode could update nodes and
// removed list correctly.
func TestRemoveLearner(t *testing.T) {
	r := newTestLearnerRaft(1, 10, 1, newTestMemoryStorage(withPeers(1), withLearners(2)))
	r.applyConfChange(pb.ConfChange{NodeID: 2, Type: pb.ConfChangeRemoveNode}.AsV2())
	w := []uint64{1}
	assert.Equal(t, w, r.trk.VoterNodes())

	w = nil
	assert.Equal(t, w, r.trk.LearnerNodes())

	// Removing the remaining voter will panic.
	defer func() {
		assert.NotNil(t, recover(), "did not panic")
	}()
	r.applyConfChange(pb.ConfChange{NodeID: 1, Type: pb.ConfChangeRemoveNode}.AsV2())
}

func TestPromotable(t *testing.T) {
	id := uint64(1)
	tests := []struct {
		peers []uint64
		wp    bool
	}{
		{[]uint64{1}, true},
		{[]uint64{1, 2, 3}, true},
		{[]uint64{}, false},
		{[]uint64{2, 3}, false},
	}
	for i, tt := range tests {
		r := newTestRaft(id, 5, 1, newTestMemoryStorage(withPeers(tt.peers...)))
		assert.Equal(t, tt.wp, r.promotable(), "#%d", i)
	}
}

func TestRaftNodes(t *testing.T) {
	tests := []struct {
		ids  []uint64
		wids []uint64
	}{
		{
			[]uint64{1, 2, 3},
			[]uint64{1, 2, 3},
		},
		{
			[]uint64{3, 2, 1},
			[]uint64{1, 2, 3},
		},
	}
	for i, tt := range tests {
		r := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(tt.ids...)))
		assert.Equal(t, tt.wids, r.trk.VoterNodes(), "#%d", i)
	}
}

func TestCampaignWhileLeader(t *testing.T) {
	testCampaignWhileLeader(t, false)
}

func TestPreCampaignWhileLeader(t *testing.T) {
	testCampaignWhileLeader(t, true)
}

func testCampaignWhileLeader(t *testing.T, preVote bool) {
	cfg := newTestConfig(1, 5, 1, newTestMemoryStorage(withPeers(1)))
	cfg.PreVote = preVote
	r := newRaft(cfg)
	assert.Equal(t, StateFollower, r.state)
	// We don't call campaign() directly because it comes after the check
	// for our current state.
	r.Step(pb.Message{From: 1, To: 1, Type: pb.MsgHup})
	r.advanceMessagesAfterAppend()
	assert.Equal(t, StateLeader, r.state)

	term := r.Term
	r.Step(pb.Message{From: 1, To: 1, Type: pb.MsgHup})
	r.advanceMessagesAfterAppend()
	assert.Equal(t, StateLeader, r.state)
	assert.Equal(t, term, r.Term)
}

// TestCommitAfterRemoveNode verifies that pending commands can become
// committed when a config change reduces the quorum requirements.
func TestCommitAfterRemoveNode(t *testing.T) {
	// Create a cluster with two nodes.
	s := newTestMemoryStorage(withPeers(1, 2))
	r := newTestRaft(1, 5, 1, s)
	r.becomeCandidate()
	r.becomeLeader()

	// Begin to remove the second node.
	cc := pb.ConfChange{
		Type:   pb.ConfChangeRemoveNode,
		NodeID: 2,
	}
	ccData, err := cc.Marshal()
	require.NoError(t, err)
	r.Step(pb.Message{
		Type: pb.MsgProp,
		Entries: []pb.Entry{
			{Type: pb.EntryConfChange, Data: ccData},
		},
	})

	// Stabilize the log and make sure nothing is committed yet.
	require.Empty(t, nextEnts(r, s))
	ccIndex := r.raftLog.lastIndex()

	// While the config change is pending, make another proposal.
	r.Step(pb.Message{
		Type: pb.MsgProp,
		Entries: []pb.Entry{
			{Type: pb.EntryNormal, Data: []byte("hello")},
		},
	})

	// Node 2 acknowledges the config change, committing it.
	r.Step(pb.Message{
		Type:  pb.MsgAppResp,
		From:  2,
		Index: ccIndex,
	})
	ents := nextEnts(r, s)
	require.Len(t, ents, 2)
	require.Equal(t, pb.EntryNormal, ents[0].Type)
	require.Nil(t, ents[0].Data)
	require.Equal(t, pb.EntryConfChange, ents[1].Type)

	// Apply the config change. This reduces quorum requirements so the
	// pending command can now commit.
	r.applyConfChange(cc.AsV2())
	ents = nextEnts(r, s)
	require.Len(t, ents, 1)
	require.Equal(t, pb.EntryNormal, ents[0].Type)
	require.Equal(t, []byte("hello"), ents[0].Data)
}

// TestLeaderTransferToUpToDateNode verifies transferring should succeed
// if the transferee has the most up-to-date log entries when transfer starts.
func TestLeaderTransferToUpToDateNode(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	lead := nt.peers[1].(*raft)

	require.Equal(t, uint64(1), lead.lead)

	// Transfer leadership to 2.
	nt.send(pb.Message{From: 2, To: 1, Type: pb.MsgTransferLeader})

	checkLeaderTransferState(t, lead, StateFollower, 2)

	// After some log replication, transfer leadership back to 1.
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{}}})

	nt.send(pb.Message{From: 1, To: 2, Type: pb.MsgTransferLeader})

	checkLeaderTransferState(t, lead, StateLeader, 1)
}

// TestLeaderTransferToUpToDateNodeFromFollower verifies transferring should succeed
// if the transferee has the most up-to-date log entries when transfer starts.
// Not like TestLeaderTransferToUpToDateNode, where the leader transfer message
// is sent to the leader, in this test case every leader transfer message is sent
// to the follower.
func TestLeaderTransferToUpToDateNodeFromFollower(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	lead := nt.peers[1].(*raft)

	require.Equal(t, uint64(1), lead.lead)

	// Transfer leadership to 2.
	nt.send(pb.Message{From: 2, To: 2, Type: pb.MsgTransferLeader})

	checkLeaderTransferState(t, lead, StateFollower, 2)

	// After some log replication, transfer leadership back to 1.
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{}}})

	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgTransferLeader})

	checkLeaderTransferState(t, lead, StateLeader, 1)
}

// TestLeaderTransferWithCheckQuorum ensures transferring leader still works
// even the current leader is still under its leader lease
func TestLeaderTransferWithCheckQuorum(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	for i := 1; i < 4; i++ {
		r := nt.peers[uint64(i)].(*raft)
		r.checkQuorum = true
		setRandomizedElectionTimeout(r, r.electionTimeout+i)
	}

	// Letting peer 2 electionElapsed reach to timeout so that it can vote for peer 1
	f := nt.peers[2].(*raft)
	for i := 0; i < f.electionTimeout; i++ {
		f.tick()
	}

	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	lead := nt.peers[1].(*raft)

	require.Equal(t, StateLeader, lead.state)

	// Transfer leadership to 2.
	nt.send(pb.Message{From: 2, To: 1, Type: pb.MsgTransferLeader})

	checkLeaderTransferState(t, lead, StateFollower, 2)

	// After some log replication, transfer leadership back to 1.
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{}}})

	nt.send(pb.Message{From: 1, To: 2, Type: pb.MsgTransferLeader})

	checkLeaderTransferState(t, lead, StateLeader, 1)
}

func TestLeaderTransferToSlowFollower(t *testing.T) {
	defaultLogger.EnableDebug()
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	nt.isolate(3)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{}}})

	nt.recover()
	lead := nt.peers[1].(*raft)
	require.Equal(t, uint64(1), lead.trk.Progress[3].Match)

	// Transfer leadership to 3 when node 3 is lack of log.
	nt.send(pb.Message{From: 3, To: 1, Type: pb.MsgTransferLeader})

	checkLeaderTransferState(t, lead, StateFollower, 3)
}

func TestLeaderTransferAfterSnapshot(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	nt.isolate(3)

	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{}}})
	lead := nt.peers[1].(*raft)
	nextEnts(lead, nt.storage[1])
	nt.storage[1].CreateSnapshot(lead.raftLog.applied, &pb.ConfState{Voters: lead.trk.VoterNodes()}, nil)
	nt.storage[1].Compact(lead.raftLog.applied)

	nt.recover()
	require.Equal(t, uint64(1), lead.trk.Progress[3].Match)

	filtered := pb.Message{}
	// Snapshot needs to be applied before sending MsgAppResp
	nt.msgHook = func(m pb.Message) bool {
		if m.Type != pb.MsgAppResp || m.From != 3 || m.Reject {
			return true
		}
		filtered = m
		return false
	}
	// Transfer leadership to 3 when node 3 is lack of snapshot.
	nt.send(pb.Message{From: 3, To: 1, Type: pb.MsgTransferLeader})
	require.Equal(t, StateLeader, lead.state)
	require.NotEqual(t, pb.Message{}, filtered)

	// Apply snapshot and resume progress
	follower := nt.peers[3].(*raft)
	snap := follower.raftLog.nextUnstableSnapshot()
	nt.storage[3].ApplySnapshot(*snap)
	follower.appliedSnap(snap)
	nt.msgHook = nil
	nt.send(filtered)

	checkLeaderTransferState(t, lead, StateFollower, 3)
}

func TestLeaderTransferToSelf(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	lead := nt.peers[1].(*raft)

	// Transfer leadership to self, there will be noop.
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgTransferLeader})
	checkLeaderTransferState(t, lead, StateLeader, 1)
}

func TestLeaderTransferToNonExistingNode(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	lead := nt.peers[1].(*raft)
	// Transfer leadership to non-existing node, there will be noop.
	nt.send(pb.Message{From: 4, To: 1, Type: pb.MsgTransferLeader})
	checkLeaderTransferState(t, lead, StateLeader, 1)
}

func TestLeaderTransferTimeout(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	nt.isolate(3)

	lead := nt.peers[1].(*raft)

	// Transfer leadership to isolated node, wait for timeout.
	nt.send(pb.Message{From: 3, To: 1, Type: pb.MsgTransferLeader})
	require.Equal(t, uint64(3), lead.leadTransferee)

	for i := 0; i < lead.heartbeatTimeout; i++ {
		lead.tick()
	}
	require.Equal(t, uint64(3), lead.leadTransferee)

	for i := 0; i < lead.electionTimeout-lead.heartbeatTimeout; i++ {
		lead.tick()
	}

	checkLeaderTransferState(t, lead, StateLeader, 1)
}

func TestLeaderTransferIgnoreProposal(t *testing.T) {
	s := newTestMemoryStorage(withPeers(1, 2, 3))
	r := newTestRaft(1, 10, 1, s)
	nt := newNetwork(r, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	nt.isolate(3)

	lead := nt.peers[1].(*raft)

	nextEnts(r, s) // handle empty entry

	// Transfer leadership to isolated node to let transfer pending, then send proposal.
	nt.send(pb.Message{From: 3, To: 1, Type: pb.MsgTransferLeader})
	require.Equal(t, uint64(3), lead.leadTransferee)

	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{}}})
	err := lead.Step(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{}}})
	require.Equal(t, ErrProposalDropped, err)

	require.Equal(t, uint64(1), lead.trk.Progress[1].Match)
}

func TestLeaderTransferReceiveHigherTermVote(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	nt.isolate(3)

	lead := nt.peers[1].(*raft)

	// Transfer leadership to isolated node to let transfer pending.
	nt.send(pb.Message{From: 3, To: 1, Type: pb.MsgTransferLeader})
	require.Equal(t, uint64(3), lead.leadTransferee)

	nt.send(pb.Message{From: 2, To: 2, Type: pb.MsgHup, Index: 1, Term: 2})

	checkLeaderTransferState(t, lead, StateFollower, 2)
}

func TestLeaderTransferRemoveNode(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	nt.ignore(pb.MsgTimeoutNow)

	lead := nt.peers[1].(*raft)

	// The leadTransferee is removed when leadship transferring.
	nt.send(pb.Message{From: 3, To: 1, Type: pb.MsgTransferLeader})
	require.Equal(t, uint64(3), lead.leadTransferee)

	lead.applyConfChange(pb.ConfChange{NodeID: 3, Type: pb.ConfChangeRemoveNode}.AsV2())

	checkLeaderTransferState(t, lead, StateLeader, 1)
}

func TestLeaderTransferDemoteNode(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	nt.ignore(pb.MsgTimeoutNow)

	lead := nt.peers[1].(*raft)

	// The leadTransferee is demoted when leadship transferring.
	nt.send(pb.Message{From: 3, To: 1, Type: pb.MsgTransferLeader})
	require.Equal(t, uint64(3), lead.leadTransferee)

	lead.applyConfChange(pb.ConfChangeV2{
		Changes: []pb.ConfChangeSingle{
			{
				Type:   pb.ConfChangeRemoveNode,
				NodeID: 3,
			},
			{
				Type:   pb.ConfChangeAddLearnerNode,
				NodeID: 3,
			},
		},
	})

	// Make the Raft group commit the LeaveJoint entry.
	lead.applyConfChange(pb.ConfChangeV2{})
	checkLeaderTransferState(t, lead, StateLeader, 1)
}

// TestLeaderTransferBack verifies leadership can transfer back to self when last transfer is pending.
func TestLeaderTransferBack(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	nt.isolate(3)

	lead := nt.peers[1].(*raft)

	nt.send(pb.Message{From: 3, To: 1, Type: pb.MsgTransferLeader})
	require.Equal(t, uint64(3), lead.leadTransferee)

	// Transfer leadership back to self.
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgTransferLeader})

	checkLeaderTransferState(t, lead, StateLeader, 1)
}

// TestLeaderTransferSecondTransferToAnotherNode verifies leader can transfer to another node
// when last transfer is pending.
func TestLeaderTransferSecondTransferToAnotherNode(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	nt.isolate(3)

	lead := nt.peers[1].(*raft)

	nt.send(pb.Message{From: 3, To: 1, Type: pb.MsgTransferLeader})
	require.Equal(t, uint64(3), lead.leadTransferee)

	// Transfer leadership to another node.
	nt.send(pb.Message{From: 2, To: 1, Type: pb.MsgTransferLeader})

	checkLeaderTransferState(t, lead, StateFollower, 2)
}

// TestLeaderTransferSecondTransferToSameNode verifies second transfer leader request
// to the same node should not extend the timeout while the first one is pending.
func TestLeaderTransferSecondTransferToSameNode(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	nt.isolate(3)

	lead := nt.peers[1].(*raft)

	nt.send(pb.Message{From: 3, To: 1, Type: pb.MsgTransferLeader})
	require.Equal(t, uint64(3), lead.leadTransferee)

	for i := 0; i < lead.heartbeatTimeout; i++ {
		lead.tick()
	}
	// Second transfer leadership request to the same node.
	nt.send(pb.Message{From: 3, To: 1, Type: pb.MsgTransferLeader})

	for i := 0; i < lead.electionTimeout-lead.heartbeatTimeout; i++ {
		lead.tick()
	}

	checkLeaderTransferState(t, lead, StateLeader, 1)
}

func checkLeaderTransferState(t *testing.T, r *raft, state StateType, lead uint64) {
	require.Equal(t, state, r.state)
	require.Equal(t, lead, r.lead)
	require.Equal(t, None, r.leadTransferee)
}

// TestTransferNonMember verifies that when a MsgTimeoutNow arrives at
// a node that has been removed from the group, nothing happens.
// (previously, if the node also got votes, it would panic as it
// transitioned to StateLeader)
func TestTransferNonMember(t *testing.T) {
	r := newTestRaft(1, 5, 1, newTestMemoryStorage(withPeers(2, 3, 4)))
	r.Step(pb.Message{From: 2, To: 1, Type: pb.MsgTimeoutNow})

	r.Step(pb.Message{From: 2, To: 1, Type: pb.MsgVoteResp})
	r.Step(pb.Message{From: 3, To: 1, Type: pb.MsgVoteResp})
	require.Equal(t, StateFollower, r.state)
}

// TestNodeWithSmallerTermCanCompleteElection tests the scenario where a node
// that has been partitioned away (and fallen behind) rejoins the cluster at
// about the same time the leader node gets partitioned away.
// Previously the cluster would come to a standstill when run with PreVote
// enabled.
func TestNodeWithSmallerTermCanCompleteElection(t *testing.T) {
	n1 := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	n2 := newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	n3 := newTestRaft(3, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))

	n1.becomeFollower(1, None)
	n2.becomeFollower(1, None)
	n3.becomeFollower(1, None)

	n1.preVote = true
	n2.preVote = true
	n3.preVote = true

	// cause a network partition to isolate node 3
	nt := newNetwork(n1, n2, n3)
	nt.cut(1, 3)
	nt.cut(2, 3)

	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	sm := nt.peers[1].(*raft)
	assert.Equal(t, StateLeader, sm.state)

	sm = nt.peers[2].(*raft)
	assert.Equal(t, StateFollower, sm.state)

	nt.send(pb.Message{From: 3, To: 3, Type: pb.MsgHup})
	sm = nt.peers[3].(*raft)
	assert.Equal(t, StatePreCandidate, sm.state)

	nt.send(pb.Message{From: 2, To: 2, Type: pb.MsgHup})

	// check whether the term values are expected
	sm = nt.peers[1].(*raft)
	assert.Equal(t, uint64(3), sm.Term)
	sm = nt.peers[2].(*raft)
	assert.Equal(t, uint64(3), sm.Term)
	sm = nt.peers[3].(*raft)
	assert.Equal(t, uint64(1), sm.Term)

	// check state
	sm = nt.peers[1].(*raft)
	assert.Equal(t, StateFollower, sm.state)
	sm = nt.peers[2].(*raft)
	assert.Equal(t, StateLeader, sm.state)
	sm = nt.peers[3].(*raft)
	assert.Equal(t, StatePreCandidate, sm.state)

	sm.logger.Infof("going to bring back peer 3 and kill peer 2")
	// recover the network then immediately isolate b which is currently
	// the leader, this is to emulate the crash of b.
	nt.recover()
	nt.cut(2, 1)
	nt.cut(2, 3)

	// call for election
	nt.send(pb.Message{From: 3, To: 3, Type: pb.MsgHup})
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	// do we have a leader?
	sma := nt.peers[1].(*raft)
	smb := nt.peers[3].(*raft)
	assert.True(t, sma.state == StateLeader || smb.state == StateLeader)
}

// TestPreVoteWithSplitVote verifies that after split vote, cluster can complete
// election in next round.
func TestPreVoteWithSplitVote(t *testing.T) {
	n1 := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	n2 := newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	n3 := newTestRaft(3, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))

	n1.becomeFollower(1, None)
	n2.becomeFollower(1, None)
	n3.becomeFollower(1, None)

	n1.preVote = true
	n2.preVote = true
	n3.preVote = true

	nt := newNetwork(n1, n2, n3)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	// simulate leader down. followers start split vote.
	nt.isolate(1)
	nt.send([]pb.Message{
		{From: 2, To: 2, Type: pb.MsgHup},
		{From: 3, To: 3, Type: pb.MsgHup},
	}...)

	// check whether the term values are expected
	sm := nt.peers[2].(*raft)
	assert.Equal(t, uint64(3), sm.Term)
	sm = nt.peers[3].(*raft)
	assert.Equal(t, uint64(3), sm.Term)

	// check state
	sm = nt.peers[2].(*raft)
	assert.Equal(t, StateCandidate, sm.state)
	sm = nt.peers[3].(*raft)
	assert.Equal(t, StateCandidate, sm.state)

	// node 2 election timeout first
	nt.send(pb.Message{From: 2, To: 2, Type: pb.MsgHup})

	// check whether the term values are expected
	sm = nt.peers[2].(*raft)
	assert.Equal(t, uint64(4), sm.Term)
	sm = nt.peers[3].(*raft)
	assert.Equal(t, uint64(4), sm.Term)

	// check state
	sm = nt.peers[2].(*raft)
	assert.Equal(t, StateLeader, sm.state)
	sm = nt.peers[3].(*raft)
	assert.Equal(t, StateFollower, sm.state)
}

// TestPreVoteWithCheckQuorum ensures that after a node become pre-candidate,
// it will checkQuorum correctly.
func TestPreVoteWithCheckQuorum(t *testing.T) {
	n1 := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	n2 := newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	n3 := newTestRaft(3, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))

	n1.becomeFollower(1, None)
	n2.becomeFollower(1, None)
	n3.becomeFollower(1, None)

	n1.preVote = true
	n2.preVote = true
	n3.preVote = true

	n1.checkQuorum = true
	n2.checkQuorum = true
	n3.checkQuorum = true

	nt := newNetwork(n1, n2, n3)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	// isolate node 1. node 2 and node 3 have leader info
	nt.isolate(1)

	// check state
	sm := nt.peers[1].(*raft)
	require.Equal(t, StateLeader, sm.state)

	sm = nt.peers[2].(*raft)
	require.Equal(t, StateFollower, sm.state)

	sm = nt.peers[3].(*raft)
	require.Equal(t, StateFollower, sm.state)

	// node 2 will ignore node 3's PreVote
	nt.send(pb.Message{From: 3, To: 3, Type: pb.MsgHup})
	nt.send(pb.Message{From: 2, To: 2, Type: pb.MsgHup})

	// Do we have a leader?
	assert.True(t, n2.state == StateLeader || n3.state == StateFollower)
}

// TestLearnerCampaign verifies that a learner won't campaign even if it receives
// a MsgHup or MsgTimeoutNow.
func TestLearnerCampaign(t *testing.T) {
	n1 := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1)))
	n1.applyConfChange(pb.ConfChange{NodeID: 2, Type: pb.ConfChangeAddLearnerNode}.AsV2())
	n2 := newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1)))
	n2.applyConfChange(pb.ConfChange{NodeID: 2, Type: pb.ConfChangeAddLearnerNode}.AsV2())
	nt := newNetwork(n1, n2)
	nt.send(pb.Message{From: 2, To: 2, Type: pb.MsgHup})

	require.True(t, n2.isLearner)

	require.Equal(t, StateFollower, n2.state)

	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})
	require.True(t, n1.state == StateLeader && n1.lead == 1)

	// NB: TransferLeader already checks that the recipient is not a learner, but
	// the check could have happened by the time the recipient becomes a learner,
	// in which case it will receive MsgTimeoutNow as in this test case and we
	// verify that it's ignored.
	nt.send(pb.Message{From: 1, To: 2, Type: pb.MsgTimeoutNow})
	require.Equal(t, StateFollower, n2.state)
}

// simulate rolling update a cluster for Pre-Vote. cluster has 3 nodes [n1, n2, n3].
// n1 is leader with term 2
// n2 is follower with term 2
// n3 is partitioned, with term 4 and less log, state is candidate
func newPreVoteMigrationCluster(t *testing.T) *network {
	n1 := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	n2 := newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	n3 := newTestRaft(3, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))

	n1.becomeFollower(1, None)
	n2.becomeFollower(1, None)
	n3.becomeFollower(1, None)

	n1.preVote = true
	n2.preVote = true
	// We intentionally do not enable PreVote for n3, this is done so in order
	// to simulate a rolling restart process where it's possible to have a mixed
	// version cluster with replicas with PreVote enabled, and replicas without.

	nt := newNetwork(n1, n2, n3)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	// Cause a network partition to isolate n3.
	nt.isolate(3)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("some data")}}})
	nt.send(pb.Message{From: 3, To: 3, Type: pb.MsgHup})
	nt.send(pb.Message{From: 3, To: 3, Type: pb.MsgHup})

	// check state
	require.Equal(t, StateLeader, n1.state)
	require.Equal(t, StateFollower, n2.state)
	require.Equal(t, StateCandidate, n3.state)

	// check term
	require.Equal(t, uint64(2), n1.Term)
	require.Equal(t, uint64(2), n2.Term)
	require.Equal(t, uint64(4), n3.Term)

	// Enable prevote on n3, then recover the network
	n3.preVote = true
	nt.recover()

	return nt
}

func TestPreVoteMigrationCanCompleteElection(t *testing.T) {
	nt := newPreVoteMigrationCluster(t)

	// n1 is leader with term 2
	// n2 is follower with term 2
	// n3 is pre-candidate with term 4, and less log
	n2 := nt.peers[2].(*raft)
	n3 := nt.peers[3].(*raft)

	// simulate leader down
	nt.isolate(1)

	// Call for elections from both n2 and n3.
	nt.send(pb.Message{From: 3, To: 3, Type: pb.MsgHup})
	nt.send(pb.Message{From: 2, To: 2, Type: pb.MsgHup})

	// check state
	assert.Equal(t, StateFollower, n2.state)
	assert.Equal(t, StatePreCandidate, n3.state)

	nt.send(pb.Message{From: 3, To: 3, Type: pb.MsgHup})
	nt.send(pb.Message{From: 2, To: 2, Type: pb.MsgHup})

	// Do we have a leader?
	assert.True(t, n2.state == StateLeader || n3.state == StateFollower)
}

func TestPreVoteMigrationWithFreeStuckPreCandidate(t *testing.T) {
	nt := newPreVoteMigrationCluster(t)

	// n1 is leader with term 2
	// n2 is follower with term 2
	// n3 is pre-candidate with term 4, and less log
	n1 := nt.peers[1].(*raft)
	n2 := nt.peers[2].(*raft)
	n3 := nt.peers[3].(*raft)

	nt.send(pb.Message{From: 3, To: 3, Type: pb.MsgHup})

	assert.Equal(t, StateLeader, n1.state)
	assert.Equal(t, StateFollower, n2.state)
	assert.Equal(t, StatePreCandidate, n3.state)

	// Pre-Vote again for safety
	nt.send(pb.Message{From: 3, To: 3, Type: pb.MsgHup})

	assert.Equal(t, StateLeader, n1.state)
	assert.Equal(t, StateFollower, n2.state)
	assert.Equal(t, StatePreCandidate, n3.state)

	nt.send(pb.Message{From: 1, To: 3, Type: pb.MsgHeartbeat, Term: n1.Term})

	// Disrupt the leader so that the stuck peer is freed
	assert.Equal(t, StateFollower, n1.state)
	assert.Equal(t, n1.Term, n3.Term)

}

func testConfChangeCheckBeforeCampaign(t *testing.T, v2 bool) {
	nt := newNetwork(nil, nil, nil)
	n1 := nt.peers[1].(*raft)
	n2 := nt.peers[2].(*raft)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})
	assert.Equal(t, StateLeader, n1.state)

	// Begin to remove the third node.
	cc := pb.ConfChange{
		Type:   pb.ConfChangeRemoveNode,
		NodeID: 2,
	}
	var ccData []byte
	var err error
	var ty pb.EntryType
	if v2 {
		ccv2 := cc.AsV2()
		ccData, err = ccv2.Marshal()
		ty = pb.EntryConfChangeV2
	} else {
		ccData, err = cc.Marshal()
		ty = pb.EntryConfChange
	}
	require.NoError(t, err)
	nt.send(pb.Message{
		From: 1,
		To:   1,
		Type: pb.MsgProp,
		Entries: []pb.Entry{
			{Type: ty, Data: ccData},
		},
	})

	// Trigger campaign in node 2
	for i := 0; i < n2.randomizedElectionTimeout; i++ {
		n2.tick()
	}
	// It's still follower because committed conf change is not applied.
	assert.Equal(t, StateFollower, n2.state)

	// Transfer leadership to peer 2.
	nt.send(pb.Message{From: 2, To: 1, Type: pb.MsgTransferLeader})
	assert.Equal(t, StateLeader, n1.state)
	// It's still follower because committed conf change is not applied.
	assert.Equal(t, StateFollower, n2.state)

	// Abort transfer leader
	for i := 0; i < n1.electionTimeout; i++ {
		n1.tick()
	}

	// Advance apply
	nextEnts(n2, nt.storage[2])

	// Transfer leadership to peer 2 again.
	nt.send(pb.Message{From: 2, To: 1, Type: pb.MsgTransferLeader})
	assert.Equal(t, StateFollower, n1.state)
	assert.Equal(t, StateLeader, n2.state)

	nextEnts(n1, nt.storage[1])
	// Trigger campaign in node 2
	for i := 0; i < n1.randomizedElectionTimeout; i++ {
		n1.tick()
	}
	assert.Equal(t, StateCandidate, n1.state)
}

// TestConfChangeCheckBeforeCampaign tests if unapplied ConfChange is checked before campaign.
func TestConfChangeCheckBeforeCampaign(t *testing.T) {
	testConfChangeCheckBeforeCampaign(t, false)
}

// TestConfChangeV2CheckBeforeCampaign tests if unapplied ConfChangeV2 is checked before campaign.
func TestConfChangeV2CheckBeforeCampaign(t *testing.T) {
	testConfChangeCheckBeforeCampaign(t, true)
}

func TestFastLogRejection(t *testing.T) {
	tests := []struct {
		leaderLog       []pb.Entry // Logs on the leader
		followerLog     []pb.Entry // Logs on the follower
		followerCompact uint64     // Index at which the follower log is compacted.
		rejectHintTerm  uint64     // Expected term included in rejected MsgAppResp.
		rejectHintIndex uint64     // Expected index included in rejected MsgAppResp.
		nextAppendTerm  uint64     // Expected term when leader appends after rejected.
		nextAppendIndex uint64     // Expected index when leader appends after rejected.
	}{
		// This case tests that leader can find the conflict index quickly.
		// Firstly leader appends (type=MsgApp,index=7,logTerm=4, entries=...);
		// After rejected leader appends (type=MsgApp,index=3,logTerm=2).
		{
			leaderLog:       index(1).terms(1, 2, 2, 4, 4, 4, 4),
			followerLog:     index(1).terms(1, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3),
			rejectHintTerm:  3,
			rejectHintIndex: 7,
			nextAppendTerm:  2,
			nextAppendIndex: 3,
		},
		// This case tests that leader can find the conflict index quickly.
		// Firstly leader appends (type=MsgApp,index=8,logTerm=5, entries=...);
		// After rejected leader appends (type=MsgApp,index=4,logTerm=3).
		{
			leaderLog:       index(1).terms(1, 2, 2, 3, 4, 4, 4, 5),
			followerLog:     index(1).terms(1, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3),
			rejectHintTerm:  3,
			rejectHintIndex: 8,
			nextAppendTerm:  3,
			nextAppendIndex: 4,
		},
		// This case tests that follower can find the conflict index quickly.
		// Firstly leader appends (type=MsgApp,index=4,logTerm=1, entries=...);
		// After rejected leader appends (type=MsgApp,index=1,logTerm=1).
		{
			leaderLog:       index(1).terms(1, 1, 1, 1),
			followerLog:     index(1).terms(1, 2, 2, 4),
			rejectHintTerm:  1,
			rejectHintIndex: 1,
			nextAppendTerm:  1,
			nextAppendIndex: 1,
		},
		// This case is similar to the previous case. However, this time, the
		// leader has a longer uncommitted log tail than the follower.
		// Firstly leader appends (type=MsgApp,index=6,logTerm=1, entries=...);
		// After rejected leader appends (type=MsgApp,index=1,logTerm=1).
		{
			leaderLog:       index(1).terms(1, 1, 1, 1, 1, 1),
			followerLog:     index(1).terms(1, 2, 2, 4),
			rejectHintTerm:  1,
			rejectHintIndex: 1,
			nextAppendTerm:  1,
			nextAppendIndex: 1,
		},
		// This case is similar to the previous case. However, this time, the
		// follower has a longer uncommitted log tail than the leader.
		// Firstly leader appends (type=MsgApp,index=4,logTerm=1, entries=...);
		// After rejected leader appends (type=MsgApp,index=1,logTerm=1).
		{
			leaderLog:       index(1).terms(1, 1, 1, 1),
			followerLog:     index(1).terms(1, 2, 2, 4, 4, 4),
			rejectHintTerm:  1,
			rejectHintIndex: 1,
			nextAppendTerm:  1,
			nextAppendIndex: 1,
		},
		// An normal case that there are no log conflicts.
		// Firstly leader appends (type=MsgApp,index=5,logTerm=5, entries=...);
		// After rejected leader appends (type=MsgApp,index=4,logTerm=4).
		{
			leaderLog:       index(1).terms(1, 1, 1, 4, 5),
			followerLog:     index(1).terms(1, 1, 1, 4),
			rejectHintTerm:  4,
			rejectHintIndex: 4,
			nextAppendTerm:  4,
			nextAppendIndex: 4,
		},
		// Test case from example comment in stepLeader (on leader).
		{
			leaderLog:       index(1).terms(2, 5, 5, 5, 5, 5, 5, 5, 5),
			followerLog:     index(1).terms(2, 4, 4, 4, 4, 4),
			rejectHintTerm:  4,
			rejectHintIndex: 6,
			nextAppendTerm:  2,
			nextAppendIndex: 1,
		},
		// Test case from example comment in handleAppendEntries (on follower).
		{
			leaderLog:       index(1).terms(2, 2, 2, 2, 2),
			followerLog:     index(1).terms(2, 4, 4, 4, 4, 4, 4, 4),
			rejectHintTerm:  2,
			rejectHintIndex: 1,
			nextAppendTerm:  2,
			nextAppendIndex: 1,
		},
		// A case when a stale MsgApp from leader arrives after the corresponding
		// log index got compacted.
		// A stale (type=MsgApp,index=3,logTerm=3,entries=[(term=3,index=4)]) is
		// delivered to a follower who has already compacted beyond log index 3. The
		// MsgAppResp rejection will return same index=3, with logTerm=0. The leader
		// will rollback by one entry, and send MsgApp with index=2,logTerm=1.
		{
			leaderLog:       index(1).terms(1, 1, 3),
			followerLog:     index(1).terms(1, 1, 3, 3, 3),
			followerCompact: 5, // entries <= index 5 are compacted
			rejectHintTerm:  0,
			rejectHintIndex: 3,
			nextAppendTerm:  1,
			nextAppendIndex: 2,
		},
	}

	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			s1 := NewMemoryStorage()
			s1.snapshot.Metadata.ConfState = pb.ConfState{Voters: []uint64{1, 2, 3}}
			s1.Append(test.leaderLog)
			last := test.leaderLog[len(test.leaderLog)-1]
			s1.SetHardState(pb.HardState{
				Term:   last.Term - 1,
				Commit: last.Index,
			})
			n1 := newTestRaft(1, 10, 1, s1)
			n1.becomeCandidate() // bumps Term to last.Term
			n1.becomeLeader()

			s2 := NewMemoryStorage()
			s2.snapshot.Metadata.ConfState = pb.ConfState{Voters: []uint64{1, 2, 3}}
			s2.Append(test.followerLog)
			s2.SetHardState(pb.HardState{
				Term:   last.Term,
				Vote:   1,
				Commit: 0,
			})
			n2 := newTestRaft(2, 10, 1, s2)
			if test.followerCompact != 0 {
				s2.Compact(test.followerCompact)
				// NB: the state of n2 after this compaction isn't realistic because the
				// commit index is still at 0. We do this to exercise a "doesn't happen"
				// edge case behaviour, in case it still does happen in some other way.
			}

			require.NoError(t, n2.Step(pb.Message{From: 1, To: 2, Type: pb.MsgHeartbeat}))
			msgs := n2.readMessages()
			require.Len(t, msgs, 1, "can't read 1 message from peer 2")
			require.Equal(t, pb.MsgHeartbeatResp, msgs[0].Type)

			require.NoError(t, n1.Step(msgs[0]))
			msgs = n1.readMessages()
			require.Len(t, msgs, 1, "can't read 1 message from peer 1")
			require.Equal(t, pb.MsgApp, msgs[0].Type)

			require.NoError(t, n2.Step(msgs[0]), "peer 2 step append fail")
			msgs = n2.readMessages()
			require.Len(t, msgs, 1, "can't read 1 message from peer 2")
			require.Equal(t, pb.MsgAppResp, msgs[0].Type)
			require.True(t, msgs[0].Reject, "expected rejected append response from peer 2")
			require.Equal(t, test.rejectHintTerm, msgs[0].LogTerm, "hint log term mismatch")
			require.Equal(t, test.rejectHintIndex, msgs[0].RejectHint, "hint log index mismatch")

			require.NoError(t, n1.Step(msgs[0]), "peer 1 step append fail")
			msgs = n1.readMessages()
			require.Equal(t, test.nextAppendTerm, msgs[0].LogTerm)
			require.Equal(t, test.nextAppendIndex, msgs[0].Index)
		})
	}
}

func entsWithConfig(configFunc func(*Config), terms ...uint64) *raft {
	storage := NewMemoryStorage()
	for i, term := range terms {
		storage.Append([]pb.Entry{{Index: uint64(i + 1), Term: term}})
	}
	cfg := newTestConfig(1, 5, 1, storage)
	if configFunc != nil {
		configFunc(cfg)
	}
	sm := newRaft(cfg)
	sm.reset(terms[len(terms)-1])
	return sm
}

// votedWithConfig creates a raft state machine with Vote and Term set
// to the given value but no log entries (indicating that it voted in
// the given term but has not received any logs).
func votedWithConfig(configFunc func(*Config), vote, term uint64) *raft {
	storage := NewMemoryStorage()
	storage.SetHardState(pb.HardState{Vote: vote, Term: term})
	cfg := newTestConfig(1, 5, 1, storage)
	if configFunc != nil {
		configFunc(cfg)
	}
	sm := newRaft(cfg)
	sm.reset(term)
	return sm
}

func TestLogReplicationWithReorderedMessage(t *testing.T) {
	r1 := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2)))
	r1.becomeCandidate()
	r1.becomeLeader()
	r1.readMessages()
	r1.trk.Progress[2].BecomeReplicate()

	r2 := newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1, 2)))

	// r1 sends 2 MsgApp messages to r2.
	mustAppendEntry(r1, pb.Entry{Data: []byte("somedata")})
	r1.sendAppend(2)
	req1 := expectOneMessage(t, r1)
	mustAppendEntry(r1, pb.Entry{Data: []byte("somedata")})
	r1.sendAppend(2)
	req2 := expectOneMessage(t, r1)

	// r2 receives the second MsgApp first due to reordering.
	r2.Step(req2)
	resp2 := expectOneMessage(t, r2)
	// r2 rejects req2
	require.True(t, resp2.Reject)
	require.Zero(t, resp2.RejectHint)
	require.Equal(t, uint64(2), resp2.Index)

	// r2 handles the first MsgApp and responses to r1.
	// And r1 updates match index accordingly.
	r2.Step(req1)
	m := expectOneMessage(t, r2)
	require.False(t, m.Reject)
	require.Equal(t, uint64(2), m.Index)
	r1.Step(m)
	m = expectOneMessage(t, r1)
	require.Equal(t, uint64(2), r1.trk.Progress[2].Match)

	// r1 observes a transient network issue to r2, hence transits to probe state.
	r1.Step(pb.Message{From: 2, To: 1, Type: pb.MsgUnreachable})
	require.Equal(t, tracker.StateProbe, r1.trk.Progress[2].State)

	// now r1 receives the delayed resp2.
	r1.Step(resp2)
	m = expectOneMessage(t, r1)
	// r1 shall re-send MsgApp from match index even if resp2's reject hint is less than matching index.
	require.Equal(t, r1.trk.Progress[2].Match, m.Index)
}

func expectOneMessage(t *testing.T, r *raft) pb.Message {
	msgs := r.readMessages()
	require.Len(t, msgs, 1, "expect one message")
	return msgs[0]
}

type network struct {
	t *testing.T // optional

	peers   map[uint64]stateMachine
	storage map[uint64]*MemoryStorage
	dropm   map[connem]float64
	ignorem map[pb.MessageType]bool

	// msgHook is called for each message sent. It may inspect the
	// message and return true to send it or false to drop it.
	msgHook func(pb.Message) bool
}

// newNetwork initializes a network from peers.
// A nil node will be replaced with a new *stateMachine.
// A *stateMachine will get its k, id.
// When using stateMachine, the address list is always [1, n].
func newNetwork(peers ...stateMachine) *network {
	return newNetworkWithConfig(nil, peers...)
}

// newNetworkWithConfig is like newNetwork but calls the given func to
// modify the configuration of any state machines it creates.
func newNetworkWithConfig(configFunc func(*Config), peers ...stateMachine) *network {
	size := len(peers)
	peerAddrs := idsBySize(size)

	npeers := make(map[uint64]stateMachine, size)
	nstorage := make(map[uint64]*MemoryStorage, size)

	for j, p := range peers {
		id := peerAddrs[j]
		switch v := p.(type) {
		case nil:
			nstorage[id] = newTestMemoryStorage(withPeers(peerAddrs...))
			cfg := newTestConfig(id, 10, 1, nstorage[id])
			if configFunc != nil {
				configFunc(cfg)
			}
			sm := newRaft(cfg)
			npeers[id] = sm
		case *raft:
			// TODO(tbg): this is all pretty confused. Clean this up.
			learners := make(map[uint64]bool, len(v.trk.Learners))
			for i := range v.trk.Learners {
				learners[i] = true
			}
			v.id = id
			v.trk = tracker.MakeProgressTracker(v.trk.MaxInflight, v.trk.MaxInflightBytes)
			if len(learners) > 0 {
				v.trk.Learners = map[uint64]struct{}{}
			}
			for i := 0; i < size; i++ {
				pr := &tracker.Progress{}
				if _, ok := learners[peerAddrs[i]]; ok {
					pr.IsLearner = true
					v.trk.Learners[peerAddrs[i]] = struct{}{}
				} else {
					v.trk.Voters[0][peerAddrs[i]] = struct{}{}
				}
				v.trk.Progress[peerAddrs[i]] = pr
			}
			v.reset(v.Term)
			npeers[id] = v
		case *blackHole:
			npeers[id] = v
		default:
			panic(fmt.Sprintf("unexpected state machine type: %T", p))
		}
	}
	return &network{
		peers:   npeers,
		storage: nstorage,
		dropm:   make(map[connem]float64),
		ignorem: make(map[pb.MessageType]bool),
	}
}

func preVoteConfig(c *Config) {
	c.PreVote = true
}

func (nw *network) send(msgs ...pb.Message) {
	for len(msgs) > 0 {
		m := msgs[0]
		p := nw.peers[m.To]
		if nw.t != nil {
			nw.t.Log(DescribeMessage(m, nil))
		}
		_ = p.Step(m)
		p.advanceMessagesAfterAppend()
		msgs = append(msgs[1:], nw.filter(p.readMessages())...)
	}
}

func (nw *network) drop(from, to uint64, perc float64) {
	nw.dropm[connem{from, to}] = perc
}

func (nw *network) cut(one, other uint64) {
	nw.drop(one, other, 2.0) // always drop
	nw.drop(other, one, 2.0) // always drop
}

func (nw *network) isolate(id uint64) {
	for i := 0; i < len(nw.peers); i++ {
		nid := uint64(i) + 1
		if nid != id {
			nw.drop(id, nid, 1.0) // always drop
			nw.drop(nid, id, 1.0) // always drop
		}
	}
}

func (nw *network) ignore(t pb.MessageType) {
	nw.ignorem[t] = true
}

func (nw *network) recover() {
	nw.dropm = make(map[connem]float64)
	nw.ignorem = make(map[pb.MessageType]bool)
}

func (nw *network) filter(msgs []pb.Message) []pb.Message {
	var mm []pb.Message
	for _, m := range msgs {
		if nw.ignorem[m.Type] {
			continue
		}
		switch m.Type {
		case pb.MsgHup:
			// hups never go over the network, so don't drop them but panic
			panic("unexpected msgHup")
		default:
			perc := nw.dropm[connem{m.From, m.To}]
			if n := rand.Float64(); n < perc {
				continue
			}
		}
		if nw.msgHook != nil {
			if !nw.msgHook(m) {
				continue
			}
		}
		mm = append(mm, m)
	}
	return mm
}

type connem struct {
	from, to uint64
}

type blackHole struct{}

func (blackHole) Step(pb.Message) error       { return nil }
func (blackHole) readMessages() []pb.Message  { return nil }
func (blackHole) advanceMessagesAfterAppend() {}

var nopStepper = &blackHole{}

func idsBySize(size int) []uint64 {
	ids := make([]uint64, size)
	for i := 0; i < size; i++ {
		ids[i] = 1 + uint64(i)
	}
	return ids
}

// setRandomizedElectionTimeout set up the value by caller instead of choosing
// by system, in some test scenario we need to fill in some expected value to
// ensure the certainty
func setRandomizedElectionTimeout(r *raft, v int) {
	r.randomizedElectionTimeout = v
}

// SetRandomizedElectionTimeout is like setRandomizedElectionTimeout, but
// exported for use by tests that are not in the raft package, using RawNode.
func SetRandomizedElectionTimeout(r *RawNode, v int) {
	setRandomizedElectionTimeout(r.raft, v)
}

func newTestConfig(id uint64, election, heartbeat int, storage Storage) *Config {
	return &Config{
		ID:              id,
		ElectionTick:    election,
		HeartbeatTick:   heartbeat,
		Storage:         storage,
		MaxSizePerMsg:   noLimit,
		MaxInflightMsgs: 256,
	}
}

type testMemoryStorageOptions func(*MemoryStorage)

func withPeers(peers ...uint64) testMemoryStorageOptions {
	return func(ms *MemoryStorage) {
		ms.snapshot.Metadata.ConfState.Voters = peers
	}
}

func withLearners(learners ...uint64) testMemoryStorageOptions {
	return func(ms *MemoryStorage) {
		ms.snapshot.Metadata.ConfState.Learners = learners
	}
}

func newTestMemoryStorage(opts ...testMemoryStorageOptions) *MemoryStorage {
	ms := NewMemoryStorage()
	for _, o := range opts {
		o(ms)
	}
	return ms
}

func newTestRaft(id uint64, election, heartbeat int, storage Storage) *raft {
	return newRaft(newTestConfig(id, election, heartbeat, storage))
}

func newTestLearnerRaft(id uint64, election, heartbeat int, storage Storage) *raft {
	cfg := newTestConfig(id, election, heartbeat, storage)
	return newRaft(cfg)
}

// newTestRawNode sets up a RawNode with the given peers. The configuration will
// not be reflected in the Storage.
func newTestRawNode(id uint64, election, heartbeat int, storage Storage) *RawNode {
	cfg := newTestConfig(id, election, heartbeat, storage)
	rn, err := NewRawNode(cfg)
	if err != nil {
		panic(err)
	}
	return rn
}
