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

	"github.com/cockroachdb/cockroach/pkg/raft/raftlogger"
	pb "github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/raft/raftstoreliveness"
	"github.com/cockroachdb/cockroach/pkg/raft/tracker"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// nextEnts returns the appliable entries and updates the applied index.
func nextEnts(r *raft, s *MemoryStorage) (ents []pb.Entry) {
	// Append unstable entries.
	s.Append(r.raftLog.nextUnstableEnts())
	r.raftLog.stableTo(r.raftLog.unstable.mark())

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
	r.trk.Progress(2).BecomeReplicate()

	// Send proposals to r1. The first 5 entries should be queued in the unstable log.
	propMsg := pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("foo")}}}
	for i := 0; i < 5; i++ {
		require.NoError(t, r.Step(propMsg), "#%d", i)
	}

	require.Zero(t, r.trk.Progress(1).Match)

	ents := r.raftLog.nextUnstableEnts()
	require.Len(t, ents, 6)
	require.Len(t, ents[0].Data, 0)
	require.Equal(t, "foo", string(ents[5].Data))

	r.advanceMessagesAfterAppend()

	require.Equal(t, uint64(6), r.trk.Progress(1).Match)
	require.Equal(t, uint64(7), r.trk.Progress(1).Next)
}

// TestProgressResumeByHeartbeatResp ensures raft.heartbeat reset progress.paused by heartbeat response.
func TestProgressResumeByHeartbeatResp(t *testing.T) {
	r := newTestRaft(1, 5, 1, newTestMemoryStorage(withPeers(1, 2)))
	r.becomeCandidate()
	r.becomeLeader()

	r.trk.Progress(2).MsgAppProbesPaused = true

	r.Step(pb.Message{From: 1, To: 1, Type: pb.MsgBeat})
	assert.True(t, r.trk.Progress(2).MsgAppProbesPaused)

	r.trk.Progress(2).BecomeReplicate()
	assert.False(t, r.trk.Progress(2).MsgAppProbesPaused)
	r.trk.Progress(2).MsgAppProbesPaused = true
	r.Step(pb.Message{From: 2, To: 1, Type: pb.MsgHeartbeatResp})
	assert.True(t, r.trk.Progress(2).MsgAppProbesPaused)
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
	r.trk.Progress(2).BecomeProbe()
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
	r.trk.Progress(2).BecomeReplicate()
	r.trk.Progress(3).BecomeReplicate()
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
	candState := pb.StateCandidate
	candTerm := uint64(1)
	if preVote {
		cfg = preVoteConfig
		// In pre-vote mode, an election that fails to complete
		// leaves the node in pre-candidate state without advancing
		// the term.
		candState = pb.StatePreCandidate
		candTerm = 0
	}
	tests := []struct {
		*network
		state   pb.StateType
		expTerm uint64
	}{
		{newNetworkWithConfig(cfg, nil, nil, nil), pb.StateLeader, 1},
		{newNetworkWithConfig(cfg, nil, nil, nopStepper), pb.StateLeader, 1},
		{newNetworkWithConfig(cfg, nil, nopStepper, nopStepper), candState, candTerm},
		{newNetworkWithConfig(cfg, nil, nopStepper, nopStepper, nil), candState, candTerm},
		{newNetworkWithConfig(cfg, nil, nopStepper, nopStepper, nil, nil), pb.StateLeader, 1},

		// three logs further along than 0, but in the same term so rejections
		// are returned instead of the votes being ignored.
		{newNetworkWithConfig(cfg,
			nil, entsWithConfig(cfg, 1), entsWithConfig(cfg, 1), entsWithConfig(cfg, 1, 1), nil),
			pb.StateFollower, 1},
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
	for i := int64(0); i < n2.electionTimeout; i++ {
		n2.tick()
	}

	assert.Equal(t, pb.StateFollower, n2.state)
}

// TestLearnerPromotion verifies that the learner should not election until
// it is promoted to a normal peer.
func TestLearnerPromotion(t *testing.T) {
	testutils.RunTrueAndFalse(t, "store-liveness-enabled",
		func(t *testing.T, storeLivenessEnabled bool) {
			testLearnerPromotion(t, storeLivenessEnabled)
		})
}

func testLearnerPromotion(t *testing.T, storeLivenessEnabled bool) {
	var fabric *raftstoreliveness.LivenessFabric
	var n1, n2 *raft

	if storeLivenessEnabled {
		fabric = raftstoreliveness.NewLivenessFabricWithPeers(1, 2, 3)
		n1 = newTestLearnerRaft(1, 10, 1, newTestMemoryStorage(withPeers(1), withLearners(2)),
			withStoreLiveness(fabric.GetStoreLiveness(1)))
		n2 = newTestLearnerRaft(2, 10, 1, newTestMemoryStorage(withPeers(1), withLearners(2)),
			withStoreLiveness(fabric.GetStoreLiveness(2)))
	} else {
		n1 = newTestLearnerRaft(1, 10, 1, newTestMemoryStorage(withPeers(1), withLearners(2)),
			withStoreLiveness(raftstoreliveness.Disabled{}))
		n2 = newTestLearnerRaft(2, 10, 1, newTestMemoryStorage(withPeers(1), withLearners(2)),
			withStoreLiveness(raftstoreliveness.Disabled{}))
	}

	n1.becomeFollower(1, None)
	n2.becomeFollower(1, None)

	nt := newNetworkWithConfigAndLivenessFabric(nil, fabric, n1, n2)

	assert.NotEqual(t, pb.StateLeader, n1.state)

	// n1 should become leader
	setRandomizedElectionTimeout(n1, n1.electionTimeout)
	for i := int64(0); i < n1.electionTimeout; i++ {
		n1.tick()
	}
	n1.advanceMessagesAfterAppend()

	assert.Equal(t, pb.StateLeader, n1.state)
	assert.Equal(t, pb.StateFollower, n2.state)

	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgBeat})

	n1.applyConfChange(pb.ConfChange{NodeID: 2, Type: pb.ConfChangeAddNode}.AsV2())
	n2.applyConfChange(pb.ConfChange{NodeID: 2, Type: pb.ConfChangeAddNode}.AsV2())
	assert.False(t, n2.isLearner)

	if storeLivenessEnabled {
		// We need to withdraw support of 1 to allow 2 to campaign and get elected.
		nt.livenessFabric.WithdrawSupportForPeerFromAllPeers(1)
	}

	// n2 start election, should become leader
	setRandomizedElectionTimeout(n2, n2.electionTimeout)
	for i := int64(0); i < n2.electionTimeout; i++ {
		n2.tick()
	}
	n2.advanceMessagesAfterAppend()

	nt.send(pb.Message{From: 2, To: 2, Type: pb.MsgBeat})

	assert.Equal(t, pb.StateFollower, n1.state)
	assert.Equal(t, pb.StateLeader, n2.state)
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
	testutils.RunTrueAndFalse(t, "store-liveness-enabled",
		func(t *testing.T, storeLivenessEnabled bool) {
			testLeaderCycle(t, false, storeLivenessEnabled)
		})
}

func TestLeaderCyclePreVote(t *testing.T) {
	testutils.RunTrueAndFalse(t, "store-liveness-enabled",
		func(t *testing.T, storeLivenessEnabled bool) {
			testLeaderCycle(t, true, storeLivenessEnabled)
		})
}

// testLeaderCycle verifies that each node in a cluster can campaign
// and be elected in turn. This ensures that elections (including
// pre-vote) work when not starting from a clean slate (as they do in
// TestLeaderElection)
func testLeaderCycle(t *testing.T, preVote bool, storeLivenessEnabled bool) {
	var cfg func(c *Config) = nil

	if preVote {
		cfg = preVoteConfigWithFortificationDisabled
	}

	if preVote && storeLivenessEnabled {
		cfg = preVoteConfig
	} else if preVote && !storeLivenessEnabled {
		cfg = preVoteConfigWithFortificationDisabled
	} else if !preVote && storeLivenessEnabled {
		// The default configuration satisfies this condition.
	} else if !preVote && !storeLivenessEnabled {
		cfg = fortificationDisabledConfig
	}

	n := newNetworkWithConfig(cfg, nil, nil, nil)
	n.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})
	curLeader := pb.PeerID(1)

	for campaignerID := pb.PeerID(1); campaignerID <= 3; campaignerID++ {
		if storeLivenessEnabled {
			// We need to withdraw support of the current leader to allow the new peer
			// to campaign and get elected.
			n.livenessFabric.WithdrawSupportForPeerFromAllPeers(curLeader)
		}

		n.send(pb.Message{From: campaignerID, To: campaignerID, Type: pb.MsgHup})

		if storeLivenessEnabled {
			// Restore the support state.
			n.livenessFabric.GrantSupportForPeerFromAllPeers(curLeader)
		}

		// Update the current leader to prep for the next iteration.
		curLeader = campaignerID

		for _, peer := range n.peers {
			sm := peer.(*raft)
			if sm.id == campaignerID {
				assert.Equal(t, pb.StateLeader, sm.state, "preVote=%v: campaigning node %d", preVote, sm.id)
			} else {
				assert.Equal(t, pb.StateFollower, sm.state, "preVote=%v: campaigning node %d, current node %d", preVote, campaignerID, sm.id)
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
	assert.Equal(t, pb.StateFollower, sm1.state)
	assert.Equal(t, uint64(2), sm1.Term)

	// Node 1 campaigns again with a higher term. This time it succeeds.
	n.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})
	assert.Equal(t, pb.StateLeader, sm1.state)
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
	for st := pb.StateType(0); st < pb.NumStates; st++ {
		r := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
		r.Term = 1

		switch st {
		case pb.StateFollower:
			r.becomeFollower(r.Term, 3)
		case pb.StatePreCandidate:
			r.becomePreCandidate()
		case pb.StateCandidate:
			r.becomeCandidate()
		case pb.StateLeader:
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
			assert.Equal(t, pb.StateFollower, r.state, "%s,%s", vt, st)
			assert.Equal(t, newTerm, r.Term, "%s,%s", vt, st)
			assert.Equal(t, pb.PeerID(2), r.Vote, "%s,%s", vt, st)
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

// TestLogReplication tests that the normal replication flow works.
func TestLogReplication(t *testing.T) {
	testutils.RunTrueAndFalse(t, "store-liveness-enabled",
		func(t *testing.T, storeLivenessEnabled bool) {
			testLogReplication(t, storeLivenessEnabled)
		})
}

func testLogReplication(t *testing.T, storeLivenessEnabled bool) {
	var cfg func(c *Config) = nil
	if !storeLivenessEnabled {
		cfg = fortificationDisabledConfig
	}

	tests := []struct {
		*network
		msgs       []pb.Message
		wcommitted uint64
	}{
		{
			newNetworkWithConfig(cfg, nil, nil, nil),
			[]pb.Message{
				{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("somedata")}}},
			},
			2,
		},
		{
			newNetworkWithConfig(cfg, nil, nil, nil),
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
			if m.Type == pb.MsgHup && storeLivenessEnabled {
				// We need to withdraw support of the current leader to allow the new peer
				// to campaign and get elected.
				tt.livenessFabric.WithdrawSupportForPeerFromAllPeers(1)
			}

			tt.send(m)

			if m.Type == pb.MsgHup && storeLivenessEnabled {
				// Restore the support state.
				tt.livenessFabric.GrantSupportForPeerFromAllPeers(1)
			}
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
	for i := int64(0); i < n1.electionTimeout; i++ {
		n1.tick()
	}
	n1.advanceMessagesAfterAppend()

	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgBeat})

	// n1 is leader and n2 is learner
	assert.Equal(t, pb.StateLeader, n1.state)
	assert.True(t, n2.isLearner)

	nextCommitted := uint64(2)
	{
		nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("somedata")}}})
	}

	assert.Equal(t, nextCommitted, n1.raftLog.committed)
	assert.Equal(t, n1.raftLog.committed, n2.raftLog.committed)

	match := n1.trk.Progress(2).Match
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
	testutils.RunTrueAndFalse(t, "store-liveness-enabled",
		func(t *testing.T, storeLivenessEnabled bool) {
			testCannotCommitWithoutNewTermEntry(t, storeLivenessEnabled)
		})
}

func testCannotCommitWithoutNewTermEntry(t *testing.T, storeLivenessEnabled bool) {
	var cfg func(c *Config) = nil
	if !storeLivenessEnabled {
		cfg = fortificationDisabledConfig
	}

	tt := newNetworkWithConfig(cfg, nil, nil, nil, nil, nil)
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

	// Elect 2 as the new leader with term 2.
	if storeLivenessEnabled {
		// We need to withdraw support of the current leader to allow the new peer
		// to campaign and get elected.
		tt.livenessFabric.WithdrawSupportForPeerFromAllPeers(1)
	}

	tt.send(pb.Message{From: 2, To: 2, Type: pb.MsgHup})

	if storeLivenessEnabled {
		// Restore the support state.
		tt.livenessFabric.GrantSupportForPeerFromAllPeers(1)
	}

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

// TestCommitWithoutNewTermEntry tests the entries could be committed when
// leader changes, no new proposal comes in.
func TestCommitWithoutNewTermEntry(t *testing.T) {
	testutils.RunTrueAndFalse(t, "store-liveness-enabled",
		func(t *testing.T, storeLivenessEnabled bool) {
			testCommitWithoutNewTermEntry(t, storeLivenessEnabled)
		})
}

func testCommitWithoutNewTermEntry(t *testing.T, storeLivenessEnabled bool) {
	var cfg func(c *Config) = nil
	if !storeLivenessEnabled {
		cfg = fortificationDisabledConfig
	}

	tt := newNetworkWithConfig(cfg, nil, nil, nil, nil, nil)
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

	if storeLivenessEnabled {
		// We need to withdraw support of the current leader to allow the new peer
		// to campaign and get elected.
		tt.livenessFabric.WithdrawSupportForPeerFromAllPeers(1)
	}

	tt.send(pb.Message{From: 2, To: 2, Type: pb.MsgHup})

	if storeLivenessEnabled {
		// Restore the support state.
		tt.livenessFabric.GrantSupportForPeerFromAllPeers(1)
	}

	assert.Equal(t, uint64(4), sm.raftLog.committed)
}

func TestDuelingCandidates(t *testing.T) {
	testutils.RunTrueAndFalse(t, "store-liveness-enabled",
		func(t *testing.T, storeLivenessEnabled bool) {
			testDuelingCandidates(t, storeLivenessEnabled)
		})
}

func testDuelingCandidates(t *testing.T, storeLivenessEnabled bool) {
	var fabric *raftstoreliveness.LivenessFabric
	var a, b, c *raft

	s1 := newTestMemoryStorage(withPeers(1, 2, 3))
	s2 := newTestMemoryStorage(withPeers(1, 2, 3))
	s3 := newTestMemoryStorage(withPeers(1, 2, 3))

	if storeLivenessEnabled {
		fabric = raftstoreliveness.NewLivenessFabricWithPeers(1, 2, 3)
		a = newTestRaft(1, 10, 1, s1, withStoreLiveness(fabric.GetStoreLiveness(1)))
		b = newTestRaft(2, 10, 1, s2, withStoreLiveness(fabric.GetStoreLiveness(2)))
		c = newTestRaft(3, 10, 1, s3, withStoreLiveness(fabric.GetStoreLiveness(3)))
	} else {
		a = newTestRaft(1, 10, 1, s1, withStoreLiveness(raftstoreliveness.Disabled{}))
		b = newTestRaft(2, 10, 1, s2, withStoreLiveness(raftstoreliveness.Disabled{}))
		c = newTestRaft(3, 10, 1, s3, withStoreLiveness(raftstoreliveness.Disabled{}))
	}

	nt := newNetworkWithConfigAndLivenessFabric(nil, fabric, a, b, c)

	nt.cut(1, 3)
	if storeLivenessEnabled {
		// We need to withdraw support for and from 1 and 3 to simulate a partition.
		nt.livenessFabric.WithdrawSupport(1, 3)
		nt.livenessFabric.WithdrawSupport(3, 1)
	}

	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})
	nt.send(pb.Message{From: 3, To: 3, Type: pb.MsgHup})

	// 1 becomes leader since it receives votes from 1 and 2
	sm := nt.peers[1].(*raft)
	assert.Equal(t, pb.StateLeader, sm.state)

	// 3 stays as candidate since it receives a vote from 3 and a rejection from 2
	sm = nt.peers[3].(*raft)
	assert.Equal(t, pb.StateCandidate, sm.state)

	nt.recover()
	if storeLivenessEnabled {
		// Fix the network at the store liveness layer.
		nt.livenessFabric.GrantSupport(1, 3)
		nt.livenessFabric.GrantSupport(3, 1)
	}

	// candidate 3 now increases its term and tries to vote again
	// we expect it to disrupt the leader 1 since it has a higher term
	// 3 will be follower again since both 1 and 2 rejects its vote request since 3 does not have a long enough log

	if storeLivenessEnabled {
		// We need to withdraw support from 1 so 3 can be elected as leader.
		nt.livenessFabric.WithdrawSupportForPeerFromAllPeers(1)
	}
	nt.send(pb.Message{From: 3, To: 3, Type: pb.MsgHup})
	assert.Equal(t, pb.StateFollower, sm.state)

	tests := []struct {
		sm        *raft
		state     pb.StateType
		term      uint64
		lastIndex uint64
	}{
		{a, pb.StateFollower, 2, 1},
		{b, pb.StateFollower, 2, 1},
		{c, pb.StateFollower, 2, 0},
	}

	for i, tt := range tests {
		assert.Equal(t, tt.state, tt.sm.state, "#%d", i)
		assert.Equal(t, tt.term, tt.sm.Term, "#%d", i)
		assert.Equal(t, tt.lastIndex, tt.sm.raftLog.lastIndex(), "#%d", i)
	}
}

func TestDuelingPreCandidates(t *testing.T) {
	testutils.RunTrueAndFalse(t, "store-liveness-enabled",
		func(t *testing.T, storeLivenessEnabled bool) {
			testDuelingPreCandidates(t, storeLivenessEnabled)
		})
}

func testDuelingPreCandidates(t *testing.T, storeLivenessEnabled bool) {
	var fabric *raftstoreliveness.LivenessFabric
	var cfgA, cfgB, cfgC *Config

	if storeLivenessEnabled {
		fabric = raftstoreliveness.NewLivenessFabricWithPeers(1, 2, 3)
		cfgA = newTestConfig(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(fabric.GetStoreLiveness(1)))
		cfgB = newTestConfig(2, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(fabric.GetStoreLiveness(2)))
		cfgC = newTestConfig(3, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(fabric.GetStoreLiveness(3)))
	} else {
		cfgA = newTestConfig(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(raftstoreliveness.Disabled{}))
		cfgB = newTestConfig(2, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(raftstoreliveness.Disabled{}))
		cfgC = newTestConfig(3, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(raftstoreliveness.Disabled{}))
	}

	cfgA.PreVote = true
	cfgB.PreVote = true
	cfgC.PreVote = true

	a := newRaft(cfgA)
	b := newRaft(cfgB)
	c := newRaft(cfgC)

	nt := newNetworkWithConfigAndLivenessFabric(nil, fabric, a, b, c)
	nt.t = t

	nt.cut(1, 3)
	if storeLivenessEnabled {
		// Withdraw the support between 1 and 3 to simulate a network partition.
		nt.livenessFabric.WithdrawSupport(1, 3)
		nt.livenessFabric.WithdrawSupport(3, 1)
	}

	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})
	nt.send(pb.Message{From: 3, To: 3, Type: pb.MsgHup})

	// 1 becomes leader since it receives votes from 1 and 2
	sm := nt.peers[1].(*raft)
	assert.Equal(t, pb.StateLeader, sm.state)

	// 3 campaigns then reverts to follower when its PreVote is rejected
	sm = nt.peers[3].(*raft)
	assert.Equal(t, pb.StateFollower, sm.state)

	nt.recover()

	// Candidate 3 now increases its term and tries to vote again.
	// With PreVote, it does not disrupt the leader.

	if storeLivenessEnabled {
		// We need to withdraw support from 1 so 3 can campaign and not get rejected
		// because of store liveness support.
		nt.livenessFabric.WithdrawSupportForPeerFromAllPeers(1)
	}
	nt.send(pb.Message{From: 3, To: 3, Type: pb.MsgHup})

	tests := []struct {
		sm        *raft
		state     pb.StateType
		term      uint64
		lastIndex uint64
	}{
		{a, pb.StateLeader, 1, 1},
		{b, pb.StateFollower, 1, 1},
		{c, pb.StateFollower, 1, 0},
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
	p := tt.peers[pb.PeerID(3)].(*raft)
	for ticks := p.heartbeatTimeout; ticks > 0; ticks-- {
		tt.tick(p)
	}

	data := []byte("force follower")
	// send a proposal to 3 to flush out a MsgApp to 1
	tt.send(pb.Message{From: 3, To: 3, Type: pb.MsgProp, Entries: []pb.Entry{{Data: data}}})
	// send heartbeat; flush out commit
	for ticks := p.heartbeatTimeout; ticks > 0; ticks-- {
		tt.tick(p)
	}

	a := tt.peers[1].(*raft)
	assert.Equal(t, pb.StateFollower, a.state)
	assert.Equal(t, uint64(1), a.Term)

	wantLog := ltoa(newLog(&MemoryStorage{ls: LogSlice{
		term:    1,
		entries: []pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1, Data: data}},
	}}, nil))
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
	assert.Equal(t, pb.StateLeader, sm.state)
}

func TestSingleNodePreCandidate(t *testing.T) {
	tt := newNetworkWithConfig(preVoteConfig, nil)
	tt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	sm := tt.peers[1].(*raft)
	assert.Equal(t, pb.StateLeader, sm.state)
}

func TestOldMessages(t *testing.T) {
	testutils.RunTrueAndFalse(t, "store-liveness-enabled",
		func(t *testing.T, storeLivenessEnabled bool) {
			testOldMessages(t, storeLivenessEnabled)
		})
}

func testOldMessages(t *testing.T, storeLivenessEnabled bool) {
	var cfg func(c *Config) = nil
	if !storeLivenessEnabled {
		cfg = fortificationDisabledConfig
	}

	tt := newNetworkWithConfig(cfg, nil, nil, nil)
	// make 0 leader @ term 3
	tt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	if storeLivenessEnabled {
		// We need to withdraw support of the current leader to allow the new peer
		// to campaign and get elected.
		tt.livenessFabric.WithdrawSupportForPeerFromAllPeers(1)
		tt.send(pb.Message{From: 2, To: 2, Type: pb.MsgHup})
		tt.livenessFabric.GrantSupportForPeerFromAllPeers(1)

		tt.livenessFabric.WithdrawSupportForPeerFromAllPeers(2)
		tt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})
		tt.livenessFabric.GrantSupportForPeerFromAllPeers(2)
	} else {
		tt.send(pb.Message{From: 2, To: 2, Type: pb.MsgHup})
		tt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})
	}

	// pretend we're an old leader trying to make progress; this entry is expected to be ignored.
	tt.send(pb.Message{From: 2, To: 1, Type: pb.MsgApp, Term: 2, Entries: index(3).terms(2)})
	// commit a new entry
	tt.send(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("somedata")}}})

	ents := index(1).terms(1, 2, 3, 3)
	ents[3].Data = []byte("somedata")
	ilog := newLog(&MemoryStorage{ls: LogSlice{
		term:    3,
		entries: ents,
	}}, nil)
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

		wantLog := newLog(NewMemoryStorage(), raftlogger.RaftLogger)
		if tt.success {
			wantLog = newLog(&MemoryStorage{ls: LogSlice{
				term:    2,
				entries: []pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1, Data: data}},
			}}, nil)
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

		wantLog := newLog(&MemoryStorage{ls: LogSlice{
			term:    1,
			entries: []pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1, Data: data}},
		}}, nil)
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
	m := func(indices ...uint64) []uint64 { return indices }
	for _, tt := range []struct {
		term  uint64     // term before becoming leader
		log   []pb.Entry // log before becoming leader
		app   []pb.Entry // appended entries after becoming leader
		match []uint64   // match indices for all peers
		want  uint64     // expected commit index
	}{
		// single node
		{term: 0, match: m(0), want: 0},
		{term: 0, match: m(1), want: 1},
		{term: 1, log: index(1).terms(1), match: m(1), want: 0},
		{term: 1, log: index(1).terms(1), match: m(2), want: 2},
		{term: 1, log: index(1).terms(1), app: index(3).terms(2), match: m(1), want: 0},
		{term: 1, log: index(1).terms(1), app: index(3).terms(2), match: m(2), want: 2},
		{term: 1, log: index(1).terms(1), app: index(3).terms(2), match: m(3), want: 3},

		// odd number of nodes
		{term: 1, log: index(1).terms(1), match: m(1, 1, 1), want: 0},
		{term: 1, log: index(1).terms(1), match: m(2, 1, 1), want: 0},
		{term: 1, log: index(1).terms(1), match: m(2, 1, 2), want: 2},
		{term: 1, log: index(1).terms(1), match: m(2, 2, 2), want: 2},
		{term: 1, log: index(1).terms(1, 1), app: index(4).terms(2, 2), match: m(2, 2, 2), want: 0},
		{term: 1, log: index(1).terms(1, 1), app: index(4).terms(2, 2), match: m(3, 3, 2), want: 3},
		{term: 1, log: index(1).terms(1, 1), app: index(4).terms(2, 2), match: m(4, 4, 5), want: 4},
		{term: 1, log: index(1).terms(1, 1), app: index(4).terms(2, 2), match: m(5, 4, 5), want: 5},

		// even number of nodes
		{term: 1, log: index(1).terms(1), match: m(1, 1), want: 0},
		{term: 1, log: index(1).terms(1), match: m(2, 1, 1, 1), want: 0},
		{term: 1, log: index(1).terms(1), match: m(2, 1, 2, 1), want: 0},
		{term: 1, log: index(1).terms(1), match: m(2, 1, 2, 2), want: 2},
		{term: 1, log: index(1).terms(1, 1), app: index(4).terms(2, 2), match: m(2, 2, 2, 1), want: 0},
		{term: 1, log: index(1).terms(1, 1), app: index(4).terms(2, 2), match: m(3, 2, 2, 3), want: 0},
		{term: 1, log: index(1).terms(1, 1), app: index(4).terms(2, 2), match: m(3, 3, 1, 3), want: 3},
		{term: 1, log: index(1).terms(1, 1), app: index(4).terms(2, 2), match: m(4, 4, 4, 5), want: 4},
		{term: 1, log: index(1).terms(1, 1), app: index(4).terms(2, 2), match: m(5, 4, 5, 5), want: 5},
	} {
		t.Run("", func(t *testing.T) {
			storage := newTestMemoryStorage(withPeers(1))
			require.NoError(t, storage.Append(tt.log))
			require.NoError(t, storage.SetHardState(pb.HardState{Term: tt.term}))
			sm := newTestRaft(1, 10, 2, storage)
			sm.becomeCandidate()
			sm.becomeLeader()
			require.Equal(t, tt.term+1, sm.Term)
			require.True(t, sm.appendEntry(tt.app...))

			for i, match := range tt.match {
				id := pb.PeerID(i + 1)
				if id > 1 {
					sm.applyConfChange(pb.ConfChange{Type: pb.ConfChangeAddNode, NodeID: id}.AsV2())
				}
				require.LessOrEqual(t, match, sm.raftLog.lastIndex())
				pr := sm.trk.Progress(id)
				pr.MaybeUpdate(match)
			}
			sm.maybeCommit()
			assert.Equal(t, tt.want, sm.raftLog.committed)
		})
	}
}

// TestAtRandomizedElectionTimeout tests that the followers who call
// atRandomizedElectionTimeout() will campaign uniformly randomly between the
// range of [electionTimeout, 2 * electionTimeout - 1].
func TestAtRandomizedElectionTimeout(t *testing.T) {
	tests := []struct {
		electionElapsed int64
		// wprobability is the expected probability of an election at
		// the given electionElapsed.
		wprobability float64
		round        bool
	}{
		// randomizedElectionTimeout = [10,20).
		// electionElapsed less than the electionTimeout should never campaign.
		{0, 0, false},
		{5, 0, false},
		{9, 0, false},

		// Since there are 10 possible values for randomizedElectionTimeout, we
		// expect the probability to be 1/10 for each value.
		{10, 0.1, true},
		{13, 0.1, true},
		{15, 0.1, true},
		{18, 0.1, true},
		{20, 0.1, true},

		// No possible value of randomizedElectionTimeout [10,20) would cause an
		// election at electionElapsed = 21.
		{21, 0, false},

		// Only one out of ten values of randomizedElectionTimeout (11) leads to
		// election at electionElapsed = 22.
		{22, 0.1, true},

		// Two out of ten values of randomizedElectionTimeout (10, 11) would lead
		// to election at electionElapsed = 120.
		{110, 0.2, true},
	}

	for i, tt := range tests {
		sm := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1)))
		sm.electionElapsed = tt.electionElapsed
		c := 0
		for j := 0; j < 10000; j++ {
			sm.resetRandomizedElectionTimeout()
			if sm.atRandomizedElectionTimeout() {
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
		{pb.Message{Type: pb.MsgApp, Term: 3, LogTerm: 3, Index: 2, Commit: 3}, 2, 0, true}, // previous log mismatch
		{pb.Message{Type: pb.MsgApp, Term: 3, LogTerm: 3, Index: 3, Commit: 3}, 2, 0, true}, // previous log non-exist

		// Ensure 2
		{pb.Message{Type: pb.MsgApp, Term: 2, LogTerm: 1, Index: 1, Commit: 1}, 2, 1, false},
		{pb.Message{Type: pb.MsgApp, Term: 3, LogTerm: 0, Index: 0, Commit: 1, Entries: []pb.Entry{{Index: 1, Term: 3}}}, 1, 1, false},
		{pb.Message{Type: pb.MsgApp, Term: 2, LogTerm: 2, Index: 2, Commit: 3, Entries: []pb.Entry{{Index: 3, Term: 2}, {Index: 4, Term: 2}}}, 4, 3, false},
		{pb.Message{Type: pb.MsgApp, Term: 2, LogTerm: 2, Index: 2, Commit: 4, Entries: []pb.Entry{{Index: 3, Term: 2}}}, 3, 3, false},
		{pb.Message{Type: pb.MsgApp, Term: 2, LogTerm: 1, Index: 1, Commit: 4, Entries: []pb.Entry{{Index: 2, Term: 2}}}, 2, 2, false},

		// Ensure 3
		{pb.Message{Type: pb.MsgApp, Term: 1, LogTerm: 1, Index: 1, Commit: 3}, 2, 1, false},                                           // match entry 1, commit up to last new entry 1
		{pb.Message{Type: pb.MsgApp, Term: 2, LogTerm: 1, Index: 1, Commit: 3, Entries: []pb.Entry{{Index: 2, Term: 2}}}, 2, 2, false}, // match entry 1, commit up to last new entry 2
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

// TestHandleHeartbeat ensures that the follower handles heartbeats properly.
func TestHandleHeartbeat(t *testing.T) {
	commit := uint64(2)
	tests := []struct {
		m       pb.Message
		accTerm uint64
		wCommit uint64
	}{
		{pb.Message{From: 2, To: 1, Type: pb.MsgHeartbeat, Term: 2, Commit: commit + 1}, 2, commit + 1},
		{pb.Message{From: 2, To: 1, Type: pb.MsgHeartbeat, Term: 2, Commit: commit - 1}, 2, commit}, // do not decrease commit
		{pb.Message{From: 2, To: 1, Type: pb.MsgHeartbeat, Term: 2, Commit: commit - 1}, 1, commit},

		// Do not increase the commit index if the log is not guaranteed to be a
		// prefix of the leader's log.
		{pb.Message{From: 2, To: 1, Type: pb.MsgHeartbeat, Term: 2, Commit: commit + 1}, 1, commit},
		// Do not increase the commit index beyond our log size.
		{pb.Message{From: 2, To: 1, Type: pb.MsgHeartbeat, Term: 2, Commit: commit + 10}, 2, commit + 1}, // do not decrease commit
	}

	for i, tt := range tests {
		storage := newTestMemoryStorage(withPeers(1, 2))
		init := entryID{}.append(1, 1, tt.accTerm)
		require.NoError(t, storage.Append(init.entries))
		sm := newTestRaft(1, 5, 1, storage)
		sm.becomeFollower(init.term, 2)
		sm.raftLog.commitTo(LogMark{Term: init.term, Index: commit})
		sm.handleHeartbeat(tt.m)
		m := sm.readMessages()
		require.Len(t, m, 1, "#%d", i)
		assert.Equal(t, pb.MsgHeartbeatResp, m[0].Type, "#%d", i)
	}
}

// TestHandleHeartbeatRespStoreLivenessDisabled ensures that we re-send log
// entries when we get a heartbeat response.
func TestHandleHeartbeatRespStoreLivenessDisabled(t *testing.T) {
	storage := newTestMemoryStorage(withPeers(1, 2))
	require.NoError(t, storage.SetHardState(pb.HardState{Term: 3}))
	require.NoError(t, storage.Append(index(1).terms(1, 2, 3)))
	sm := newTestRaft(1, 5, 1, storage, withStoreLiveness(raftstoreliveness.Disabled{}))
	sm.becomeCandidate()
	sm.becomeLeader()
	sm.raftLog.commitTo(sm.raftLog.unstable.mark())

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
		From:   2,
		Type:   pb.MsgAppResp,
		Index:  msgs[0].Index + uint64(len(msgs[0].Entries)),
		Commit: sm.raftLog.lastIndex(),
	})
	// Consume the message sent in response to MsgAppResp
	sm.readMessages()

	sm.Step(pb.Message{From: 2, Type: pb.MsgHeartbeatResp})
	msgs = sm.readMessages()
	require.Empty(t, msgs)
}

// TestHandleHeatbeatTimeoutStoreLivenessEnabled ensures that we re-send log
// entries on heartbeat timeouts only if we need to.
func TestHandleHeatbeatTimeoutStoreLivenessEnabled(t *testing.T) {
	storage := newTestMemoryStorage(withPeers(1, 2))
	require.NoError(t, storage.SetHardState(pb.HardState{Term: 3}))
	require.NoError(t, storage.Append(index(1).terms(1, 2, 3)))
	sm := newTestRaft(1, 5, 1, storage)
	sm.becomeCandidate()
	sm.becomeLeader()
	sm.fortificationTracker.RecordFortification(pb.PeerID(2), 1)
	sm.fortificationTracker.RecordFortification(pb.PeerID(3), 1)
	sm.raftLog.commitTo(sm.raftLog.unstable.mark())

	// On heartbeat timeout, the leader sends a MsgApp.
	for ticks := sm.heartbeatTimeout; ticks > 0; ticks-- {
		sm.tick()
	}

	msgs := sm.readMessages()
	require.Len(t, msgs, 1)
	assert.Equal(t, pb.MsgApp, msgs[0].Type)

	// On another heartbeat timeout, the leader sends a MsgApp.
	for ticks := sm.heartbeatTimeout; ticks > 0; ticks-- {
		sm.tick()
	}
	msgs = sm.readMessages()
	require.Len(t, msgs, 1)
	assert.Equal(t, pb.MsgApp, msgs[0].Type)

	// Once the leader receives a MsgAppResp, it doesn't send MsgApp.
	sm.Step(pb.Message{
		From:   2,
		Type:   pb.MsgAppResp,
		Index:  msgs[0].Index + uint64(len(msgs[0].Entries)),
		Commit: sm.raftLog.lastIndex(),
	})

	// Consume the message sent in response to MsgAppResp
	sm.readMessages()

	// On heartbeat timeout, the leader doesn't send a MsgApp because the follower
	// is up-to-date.
	for ticks := sm.heartbeatTimeout; ticks > 0; ticks-- {
		sm.tick()
	}
	msgs = sm.readMessages()
	require.Len(t, msgs, 0)
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
	assert.Equal(t, pb.PeerID(2), msgs[0].To)
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
	assert.Equal(t, pb.PeerID(3), msgs[0].To)
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
		state          pb.StateType
		index, logTerm uint64
		voteFor        pb.PeerID
		wreject        bool
	}{
		{pb.StateFollower, 0, 0, None, true},
		{pb.StateFollower, 0, 1, None, true},
		{pb.StateFollower, 0, 2, None, true},
		{pb.StateFollower, 0, 3, None, false},

		{pb.StateFollower, 1, 0, None, true},
		{pb.StateFollower, 1, 1, None, true},
		{pb.StateFollower, 1, 2, None, true},
		{pb.StateFollower, 1, 3, None, false},

		{pb.StateFollower, 2, 0, None, true},
		{pb.StateFollower, 2, 1, None, true},
		{pb.StateFollower, 2, 2, None, false},
		{pb.StateFollower, 2, 3, None, false},

		{pb.StateFollower, 3, 0, None, true},
		{pb.StateFollower, 3, 1, None, true},
		{pb.StateFollower, 3, 2, None, false},
		{pb.StateFollower, 3, 3, None, false},

		{pb.StateFollower, 3, 2, 2, false},
		{pb.StateFollower, 3, 2, 1, true},

		{pb.StateLeader, 3, 3, 1, true},
		{pb.StatePreCandidate, 3, 3, 1, true},
		{pb.StateCandidate, 3, 3, 1, true},
	}

	for i, tt := range tests {
		sm := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1)))
		sm.state = tt.state
		switch tt.state {
		case pb.StateFollower:
			sm.step = stepFollower
		case pb.StateCandidate, pb.StatePreCandidate:
			sm.step = stepCandidate
		case pb.StateLeader:
			sm.step = stepLeader
		}
		sm.Vote = tt.voteFor
		sm.raftLog = newLog(&MemoryStorage{ls: LogSlice{
			term:    2,
			entries: index(1).terms(2, 2),
		}}, nil)

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
		from   pb.StateType
		to     pb.StateType
		wallow bool
		wterm  uint64
		wlead  pb.PeerID
	}{
		{pb.StateFollower, pb.StateFollower, true, 1, None},
		{pb.StateFollower, pb.StatePreCandidate, true, 0, None},
		{pb.StateFollower, pb.StateCandidate, true, 1, None},
		{pb.StateFollower, pb.StateLeader, false, 0, None},

		{pb.StatePreCandidate, pb.StateFollower, true, 0, None},
		{pb.StatePreCandidate, pb.StatePreCandidate, true, 0, None},
		{pb.StatePreCandidate, pb.StateCandidate, true, 1, None},
		{pb.StatePreCandidate, pb.StateLeader, true, 0, 1},

		{pb.StateCandidate, pb.StateFollower, true, 0, None},
		{pb.StateCandidate, pb.StatePreCandidate, true, 0, None},
		{pb.StateCandidate, pb.StateCandidate, true, 1, None},
		{pb.StateCandidate, pb.StateLeader, true, 0, 1},

		{pb.StateLeader, pb.StateFollower, true, 1, None},
		{pb.StateLeader, pb.StatePreCandidate, false, 0, None},
		{pb.StateLeader, pb.StateCandidate, false, 1, None},
		{pb.StateLeader, pb.StateLeader, true, 0, 1},
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
			case pb.StateFollower:
				sm.becomeFollower(tt.wterm, tt.wlead)
			case pb.StatePreCandidate:
				sm.becomePreCandidate()
			case pb.StateCandidate:
				sm.becomeCandidate()
			case pb.StateLeader:
				sm.becomeLeader()
			}

			assert.Equal(t, tt.wterm, sm.Term, "#%d", i)
			assert.Equal(t, tt.wlead, sm.lead, "#%d", i)
		}()
	}
}

func TestAllServerStepdown(t *testing.T) {
	tests := []struct {
		state pb.StateType

		wstate pb.StateType
		wterm  uint64
		windex uint64
	}{
		{pb.StateFollower, pb.StateFollower, 3, 0},
		{pb.StatePreCandidate, pb.StateFollower, 3, 0},
		{pb.StateCandidate, pb.StateFollower, 3, 0},
		{pb.StateLeader, pb.StateFollower, 3, 1},
	}

	tmsgTypes := [...]pb.MessageType{pb.MsgVote, pb.MsgApp}
	tterm := uint64(3)

	for i, tt := range tests {
		sm := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
		switch tt.state {
		case pb.StateFollower:
			sm.becomeFollower(1, None)
		case pb.StatePreCandidate:
			sm.becomePreCandidate()
		case pb.StateCandidate:
			sm.becomeCandidate()
		case pb.StateLeader:
			sm.becomeCandidate()
			sm.becomeLeader()
		}

		for j, msgType := range tmsgTypes {
			sm.Step(pb.Message{From: 2, Type: msgType, Term: tterm, LogTerm: tterm})

			assert.Equal(t, tt.wstate, sm.state, "#%d.%d", i, j)
			assert.Equal(t, tt.wterm, sm.Term, "#%d.%d", i, j)
			assert.Equal(t, tt.windex, sm.raftLog.lastIndex(), "#%d.%d", i, j)
			assert.Len(t, sm.raftLog.allEntries(), int(tt.windex), "#%d.%d", i, j)

			wlead := pb.PeerID(2)
			if msgType == pb.MsgVote {
				wlead = None
			}
			assert.Equal(t, wlead, sm.lead, "#%d.%d", i, j)
		}
	}
}

func TestCandidateResetTermMsgHeartbeat(t *testing.T) {
	testutils.RunTrueAndFalse(t, "store-liveness-enabled",
		func(t *testing.T, storeLivenessEnabled bool) {
			testCandidateResetTerm(t, pb.MsgHeartbeat, storeLivenessEnabled)
		})
}

func TestCandidateResetTermMsgApp(t *testing.T) {
	testutils.RunTrueAndFalse(t, "store-liveness-enabled",
		func(t *testing.T, storeLivenessEnabled bool) {
			testCandidateResetTerm(t, pb.MsgApp, storeLivenessEnabled)
		})
}

// testCandidateResetTerm tests when a candidate receives a
// MsgHeartbeat or MsgApp from leader, "Step" resets the term
// with leader's and reverts back to follower.
func testCandidateResetTerm(t *testing.T, mt pb.MessageType, storeLivenessEnabled bool) {
	var fabric *raftstoreliveness.LivenessFabric
	var a, b, c *raft

	if storeLivenessEnabled {
		fabric = raftstoreliveness.NewLivenessFabricWithPeers(1, 2, 3)
		a = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(fabric.GetStoreLiveness(1)))
		b = newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(fabric.GetStoreLiveness(2)))
		c = newTestRaft(3, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(fabric.GetStoreLiveness(3)))
	} else {
		a = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(raftstoreliveness.Disabled{}))
		b = newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(raftstoreliveness.Disabled{}))
		c = newTestRaft(3, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(raftstoreliveness.Disabled{}))
	}

	nt := newNetworkWithConfigAndLivenessFabric(nil, fabric, a, b, c)

	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	assert.Equal(t, pb.StateLeader, a.state)
	assert.Equal(t, pb.StateFollower, b.state)
	assert.Equal(t, pb.StateFollower, c.state)

	// isolate 3 and increase term in rest
	nt.isolate(3)

	if storeLivenessEnabled {
		// We need to withdraw from 1 to allow 2 to campaign and get elected.
		nt.livenessFabric.WithdrawSupportForPeerFromAllPeers(1)
	}

	nt.send(pb.Message{From: 2, To: 2, Type: pb.MsgHup})

	if storeLivenessEnabled {
		// We need to grant support to 1, and withdraw it from 2 (the current
		// leader) to allow 1 to campaign and get elected.
		nt.livenessFabric.GrantSupportForPeerFromAllPeers(1)
		nt.livenessFabric.WithdrawSupportForPeerFromAllPeers(2)
	}

	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	assert.Equal(t, pb.StateLeader, a.state)
	assert.Equal(t, pb.StateFollower, b.state)

	if storeLivenessEnabled {
		// We need to withdraw support from 1 to allow 3 to campaign.
		nt.livenessFabric.WithdrawSupportForPeerFromAllPeers(1)
	}

	// trigger campaign in isolated c
	c.resetRandomizedElectionTimeout()
	for i := int64(0); i < c.randomizedElectionTimeout; i++ {
		c.tick()
	}
	c.advanceMessagesAfterAppend()

	assert.Equal(t, pb.StateCandidate, c.state)

	nt.recover()

	// leader sends to isolated candidate
	// and expects candidate to revert to follower
	nt.send(pb.Message{From: 1, To: 3, Term: a.Term, Type: mt})

	assert.Equal(t, pb.StateFollower, c.state)
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
	assert.Equal(t, pb.StateFollower, sm.state)

	// n1 remains a follower even after its self-vote is delivered.
	sm.stepOrSend(steps)
	assert.Equal(t, pb.StateFollower, sm.state)

	// Its self-vote does not make its way to its ProgressTracker.
	granted, _, _ := sm.electionTracker.TallyVotes()
	assert.Zero(t, granted)

}

func TestCandidateDeliversPreCandidateSelfVoteAfterBecomingCandidate(t *testing.T) {
	sm := newTestRaft(1, 5, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	sm.preVote = true

	// n1 calls an election.
	sm.Step(pb.Message{From: 1, To: 1, Type: pb.MsgHup})
	assert.Equal(t, pb.StatePreCandidate, sm.state)
	steps := sm.takeMessagesAfterAppend()

	// n1 receives pre-candidate votes from both other peers before
	// voting for itself. n1 becomes a candidate.
	// NB: pre-vote messages carry the local term + 1.
	sm.Step(pb.Message{From: 2, To: 1, Term: sm.Term + 1, Type: pb.MsgPreVoteResp})
	sm.Step(pb.Message{From: 3, To: 1, Term: sm.Term + 1, Type: pb.MsgPreVoteResp})
	assert.Equal(t, pb.StateCandidate, sm.state)

	// n1 remains a candidate even after its delayed pre-vote self-vote is
	// delivered.
	sm.stepOrSend(steps)
	assert.Equal(t, pb.StateCandidate, sm.state)

	steps = sm.takeMessagesAfterAppend()

	// Its pre-vote self-vote does not make its way to its ProgressTracker.
	granted, _, _ := sm.electionTracker.TallyVotes()
	assert.Zero(t, granted)

	// A single vote from n2 does not move n1 to the leader.
	sm.Step(pb.Message{From: 2, To: 1, Term: sm.Term, Type: pb.MsgVoteResp})
	assert.Equal(t, pb.StateCandidate, sm.state)

	// n1 becomes the leader once its self-vote is received because now
	// quorum is reached.
	sm.stepOrSend(steps)
	assert.Equal(t, pb.StateLeader, sm.state)
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
	assert.Equal(t, pb.StateFollower, sm.state)

	// n1 advances, ignoring its earlier self-ack of its MsgApp. The
	// corresponding MsgAppResp is ignored because it carries an earlier term.
	sm.stepOrSend(steps)
	assert.Equal(t, pb.StateFollower, sm.state)
}

func TestLeaderStepdownWhenQuorumActive(t *testing.T) {
	sm := newTestRaft(1, 5, 1, newTestMemoryStorage(withPeers(1, 2, 3)))

	sm.checkQuorum = true

	sm.becomeCandidate()
	sm.becomeLeader()

	for i := int64(0); i < sm.electionTimeout+1; i++ {
		sm.Step(pb.Message{From: 2, Type: pb.MsgHeartbeatResp, Term: sm.Term})
		sm.tick()
	}

	assert.Equal(t, pb.StateLeader, sm.state)
}

func TestLeaderStepdownWhenQuorumLost(t *testing.T) {
	testutils.RunTrueAndFalse(t, "store-liveness-enabled",
		func(t *testing.T, storeLivenessEnabled bool) {
			testLeaderStepdownWhenQuorumLost(t, storeLivenessEnabled)
		})
}

func testLeaderStepdownWhenQuorumLost(t *testing.T, storeLivenessEnabled bool) {
	var fabric *raftstoreliveness.LivenessFabric
	var sm *raft

	if storeLivenessEnabled {
		fabric = raftstoreliveness.NewLivenessFabricWithPeers(1, 2, 3)
		sm = newTestRaft(1, 5, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(fabric.GetStoreLiveness(1)))
	} else {
		sm = newTestRaft(1, 5, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(raftstoreliveness.Disabled{}))
	}

	sm.checkQuorum = true

	sm.becomeCandidate()
	sm.becomeLeader()
	assert.Equal(t, pb.StateLeader, sm.state)

	for i := int64(0); i < sm.electionTimeout; i++ {
		sm.tick()
	}

	assert.Equal(t, pb.StateFollower, sm.state)
}

func TestLeaderSupersedingWithCheckQuorum(t *testing.T) {
	testutils.RunTrueAndFalse(t, "store-liveness-enabled",
		func(t *testing.T, storeLivenessEnabled bool) {
			testLeaderSupersedingWithCheckQuorum(t, storeLivenessEnabled)
		})
}
func testLeaderSupersedingWithCheckQuorum(t *testing.T, storeLivenessEnabled bool) {
	var fabric *raftstoreliveness.LivenessFabric
	var n1, n2, n3 *raft

	if storeLivenessEnabled {
		fabric = raftstoreliveness.NewLivenessFabricWithPeers(1, 2, 3)
		n1 = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(fabric.GetStoreLiveness(1)))
		n2 = newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(fabric.GetStoreLiveness(2)))
		n3 = newTestRaft(3, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(fabric.GetStoreLiveness(3)))
	} else {
		n1 = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(raftstoreliveness.Disabled{}))
		n2 = newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(raftstoreliveness.Disabled{}))
		n3 = newTestRaft(3, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(raftstoreliveness.Disabled{}))
	}

	n1.checkQuorum = true
	n2.checkQuorum = true
	n3.checkQuorum = true

	nt := newNetworkWithConfigAndLivenessFabric(nil, fabric, n1, n2, n3)
	setRandomizedElectionTimeout(n2, n2.electionTimeout+1)

	for i := int64(0); i < n2.electionTimeout; i++ {
		n2.tick()
	}
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	assert.Equal(t, pb.StateLeader, n1.state)
	assert.Equal(t, pb.StateFollower, n3.state)

	if storeLivenessEnabled {
		// We need to withdraw support from 1 so 3 can campaign and not get rejected
		// because other followers support 1.
		nt.livenessFabric.WithdrawSupportForPeerFromAllPeers(1)
	}

	nt.send(pb.Message{From: 3, To: 3, Type: pb.MsgHup})

	if storeLivenessEnabled {
		// 2 voted for 3 since its support for 1 was withdrawn.
		assert.Equal(t, pb.StateLeader, n3.state)
	} else {
		// 2 rejected 3's vote request since its electionElapsed had not reached to
		// electionTimeout.
		assert.Equal(t, pb.StateCandidate, n3.state)
	}

	// Letting b's electionElapsed reach to electionTimeout
	for i := int64(0); i < n2.electionTimeout; i++ {
		n2.tick()
	}
	nt.send(pb.Message{From: 3, To: 3, Type: pb.MsgHup})

	assert.Equal(t, pb.StateLeader, n3.state)
}

func TestLeaderElectionWithCheckQuorum(t *testing.T) {
	testutils.RunTrueAndFalse(t, "store-liveness-enabled",
		func(t *testing.T, storeLivenessEnabled bool) {
			testLeaderElectionWithCheckQuorum(t, storeLivenessEnabled)
		})
}

func testLeaderElectionWithCheckQuorum(t *testing.T, storeLivenessEnabled bool) {
	var fabric *raftstoreliveness.LivenessFabric
	var a, b, c *raft

	if storeLivenessEnabled {
		fabric = raftstoreliveness.NewLivenessFabricWithPeers(1, 2, 3)
		a = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(fabric.GetStoreLiveness(1)))
		b = newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(fabric.GetStoreLiveness(2)))
		c = newTestRaft(3, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(fabric.GetStoreLiveness(3)))
	} else {
		a = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(raftstoreliveness.Disabled{}))
		b = newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(raftstoreliveness.Disabled{}))
		c = newTestRaft(3, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(raftstoreliveness.Disabled{}))
	}

	a.checkQuorum = true
	b.checkQuorum = true
	c.checkQuorum = true

	nt := newNetworkWithConfigAndLivenessFabric(nil, fabric, a, b, c)
	setRandomizedElectionTimeout(a, a.electionTimeout+1)
	setRandomizedElectionTimeout(b, b.electionTimeout+2)

	// Immediately after creation, votes are cast regardless of the
	// election timeout.
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	assert.Equal(t, pb.StateLeader, a.state)
	assert.Equal(t, pb.StateFollower, b.state)
	assert.Equal(t, pb.StateFollower, c.state)

	// need to reset randomizedElectionTimeout larger than electionTimeout again,
	// because the value might be reset to electionTimeout since the last state changes
	setRandomizedElectionTimeout(a, a.electionTimeout+1)
	setRandomizedElectionTimeout(b, b.electionTimeout+2)

	if storeLivenessEnabled {
		// We need to withdraw from 1 to allow 3 to campaign and get elected.
		nt.livenessFabric.WithdrawSupportForPeerFromAllPeers(1)
	}

	for i := int64(0); i < a.electionTimeout; i++ {
		a.tick()
	}

	// Increment electionElapsed to electionTimeout. This will allow b to vote for
	// c when it campaigns.
	if storeLivenessEnabled {
		// Tick b once. This will allow it to realize that it no longer supports a
		// leader and will forward its electionElapsed to electionTimeout.
		b.tick()
	} else {
		for i := int64(0); i < b.electionTimeout; i++ {
			b.tick()
		}
	}

	nt.send(pb.Message{From: 3, To: 3, Type: pb.MsgHup})

	assert.Equal(t, pb.StateFollower, a.state)
	assert.Equal(t, pb.StateLeader, c.state)
}

// TestFreeStuckCandidateWithCheckQuorum ensures that a candidate with a higher term
// can disrupt the leader even if the leader still "officially" holds the lease, The
// leader is expected to step down and adopt the candidate's term.
func TestFreeStuckCandidateWithCheckQuorum(t *testing.T) {
	testutils.RunTrueAndFalse(t, "store-liveness-enabled",
		func(t *testing.T, storeLivenessEnabled bool) {
			testFreeStuckCandidateWithCheckQuorum(t, storeLivenessEnabled)
		})
}

func testFreeStuckCandidateWithCheckQuorum(t *testing.T, storeLivenessEnabled bool) {
	var fabric *raftstoreliveness.LivenessFabric
	var a, b, c *raft

	if storeLivenessEnabled {
		fabric = raftstoreliveness.NewLivenessFabricWithPeers(1, 2, 3)
		a = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(fabric.GetStoreLiveness(1)))
		b = newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(fabric.GetStoreLiveness(2)))
		c = newTestRaft(3, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(fabric.GetStoreLiveness(3)))
	} else {
		a = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(raftstoreliveness.Disabled{}))
		b = newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(raftstoreliveness.Disabled{}))
		c = newTestRaft(3, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(raftstoreliveness.Disabled{}))
	}

	a.checkQuorum = true
	b.checkQuorum = true
	c.checkQuorum = true

	nt := newNetworkWithConfigAndLivenessFabric(nil, fabric, a, b, c)

	// Elect node 1 as leader.
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})
	assert.Equal(t, pb.StateLeader, a.state)
	if storeLivenessEnabled {
		assert.Equal(t, hlc.MaxTimestamp, getBasicStatus(a).LeadSupportUntil)
	}

	nt.isolate(1)
	if storeLivenessEnabled {
		// For the purposes of this test, we want 3 to campaign and get rejected.
		// However, if we withdraw the support between 2 and 1, 2 will vote for 3
		// when it campaigns. Therefore, we only withdraw the support between
		// 1 and 3.
		nt.livenessFabric.WithdrawSupport(1, 3)
		nt.livenessFabric.WithdrawSupport(3, 1)
	}

	nt.send(pb.Message{From: 3, To: 3, Type: pb.MsgHup})

	assert.Equal(t, pb.StateFollower, b.state)
	assert.Equal(t, pb.StateCandidate, c.state)
	assert.Equal(t, b.Term+1, c.Term)

	// Vote again for safety.
	nt.send(pb.Message{From: 3, To: 3, Type: pb.MsgHup})

	assert.Equal(t, pb.StateFollower, b.state)
	assert.Equal(t, pb.StateCandidate, c.state)
	assert.Equal(t, b.Term+2, c.Term)

	nt.recover()
	if storeLivenessEnabled {
		// Recover the store liveness layer as well.
		nt.livenessFabric.GrantSupportForPeerFromAllPeers(1)
	}

	// If the stuck candidate were to talk to the follower, it may be ignored,
	// depending on whether the follower is fortified by the leader.
	nt.send(pb.Message{From: 3, To: 2, Type: pb.MsgAppResp, Term: c.Term})
	if storeLivenessEnabled {
		assert.Equal(t, c.Term-2, b.Term)
	} else {
		assert.Equal(t, c.Term, b.Term)
	}

	// Disrupt the leader so that the stuck peer is freed. The leader steps down
	// immediately, but only changes its term if it was not fortified. If it was,
	// it waits for defortification.
	hbType := pb.MsgHeartbeat
	if storeLivenessEnabled {
		hbType = pb.MsgFortifyLeader
	}
	nt.send(pb.Message{From: 1, To: 3, Type: hbType, Term: a.Term})

	if storeLivenessEnabled {
		// Expect that we are still the leader since it's still not safe to step
		// down, however, the step-down intent is recorded.
		assert.Equal(t, pb.StateLeader, a.state)
		assert.Equal(t, true, a.fortificationTracker.SteppingDown())
		assert.Equal(t, c.Term, a.fortificationTracker.SteppingDownTerm())

		// The leader hasn't defortified yet, so 3 can't win an election.
		nt.send(pb.Message{From: 3, To: 3, Type: pb.MsgHup})
		assert.Equal(t, pb.StateCandidate, c.state)
		assert.Equal(t, pb.StateLeader, a.state)
		assert.Equal(t, c.Term-3, a.Term)
		assert.Equal(t, a.id, a.lead)

		// Expire the support, and tick it once. It should step down.
		nt.livenessFabric.SetSupportExpired(1, true)
		a.tick()
	}
	assert.Equal(t, pb.StateFollower, a.state)

	// Node 1 doesn't remember that it was the leader.
	assert.Equal(t, None, a.lead)

	if storeLivenessEnabled {
		// Since node 3 campaigned one extra time above, it will have a term that is
		// higher than node 1 by one.
		assert.Equal(t, c.Term-1, a.Term)
	} else {
		assert.Equal(t, c.Term, a.Term)
	}

	// Vote again, should become leader this time.
	nt.send(pb.Message{From: 3, To: 3, Type: pb.MsgHup})
	assert.Equal(t, pb.StateLeader, c.state)
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

	for i := int64(0); i < b.electionTimeout; i++ {
		b.tick()
	}
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	assert.Equal(t, pb.StateLeader, a.state)
	assert.Equal(t, pb.StateFollower, b.state)
	assert.Equal(t, pb.PeerID(1), b.lead)
}

// TestDisruptiveFollower tests isolated follower,
// with slow network incoming from leader, election times out
// to become a candidate with an increased term. Then, the
// candiate's response to late leader heartbeat forces the leader
// to step down.
func TestDisruptiveFollower(t *testing.T) {
	testutils.RunTrueAndFalse(t, "store-liveness-enabled",
		func(t *testing.T, storeLivenessEnabled bool) {
			testDisruptiveFollower(t, storeLivenessEnabled)
		})
}

func testDisruptiveFollower(t *testing.T, storeLivenessEnabled bool) {
	var fabric *raftstoreliveness.LivenessFabric
	var n1, n2, n3 *raft

	if storeLivenessEnabled {
		fabric = raftstoreliveness.NewLivenessFabricWithPeers(1, 2, 3)
		n1 = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(fabric.GetStoreLiveness(1)))
		n2 = newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(fabric.GetStoreLiveness(2)))
		n3 = newTestRaft(3, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(fabric.GetStoreLiveness(3)))
	} else {
		n1 = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(raftstoreliveness.Disabled{}))
		n2 = newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(raftstoreliveness.Disabled{}))
		n3 = newTestRaft(3, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(raftstoreliveness.Disabled{}))
	}

	n1.checkQuorum = true
	n2.checkQuorum = true
	n3.checkQuorum = true

	n1.becomeFollower(1, None)
	n2.becomeFollower(1, None)
	n3.becomeFollower(1, None)

	nt := newNetworkWithConfigAndLivenessFabric(nil, fabric, n1, n2, n3)

	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	// check state
	require.Equal(t, pb.StateLeader, n1.state)
	require.Equal(t, pb.StateFollower, n2.state)
	require.Equal(t, pb.StateFollower, n3.state)

	if storeLivenessEnabled {
		// We need to withdraw support from 1 so 3 can campaign and not get rejected
		// because of store liveness support.
		nt.livenessFabric.WithdrawSupportForPeerFromAllPeers(1)
	}

	// etcd server "advanceTicksForElection" on restart;
	// this is to expedite campaign trigger when given larger
	// election timeouts (e.g. multi-datacenter deploy)
	// Or leader messages are being delayed while ticks elapse
	setRandomizedElectionTimeout(n3, n3.electionTimeout+2)
	for i := int64(0); i < n3.randomizedElectionTimeout-1; i++ {
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
	require.Equal(t, pb.StateLeader, n1.state)
	require.Equal(t, pb.StateFollower, n2.state)
	require.Equal(t, pb.StateCandidate, n3.state)

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
	require.Equal(t, pb.StateFollower, n1.state)
	require.Equal(t, pb.StateFollower, n2.state)
	if storeLivenessEnabled {
		// Since the support for 1 was withdrawn, the inFortifyLease is no longer
		// valid, and n3 will receive votes and become a leader.
		require.Equal(t, pb.StateLeader, n3.state)
		require.Equal(t, uint64(3), n1.Term)
		require.Equal(t, uint64(3), n2.Term)
		require.Equal(t, uint64(3), n3.Term)
	} else {
		// Since other peers still hold a valid inHeartbeatLease, n3 will not
		// receive enough votes to become a leader.
		require.Equal(t, pb.StateCandidate, n3.state)
		require.Equal(t, uint64(3), n1.Term)
		require.Equal(t, uint64(2), n2.Term)
		require.Equal(t, uint64(3), n3.Term)
	}
}

// TestDisruptiveFollowerPreVote tests isolated follower,
// with slow network incoming from leader, election times out
// to become a pre-candidate with less log than current leader.
// Then pre-vote phase prevents this isolated node from forcing
// current leader to step down, thus less disruptions.
func TestDisruptiveFollowerPreVote(t *testing.T) {
	testutils.RunTrueAndFalse(t, "store-liveness-enabled",
		func(t *testing.T, storeLivenessEnabled bool) {
			testDisruptiveFollowerPreVote(t, storeLivenessEnabled)
		})
}

func testDisruptiveFollowerPreVote(t *testing.T, storeLivenessEnabled bool) {
	var fabric *raftstoreliveness.LivenessFabric
	var n1, n2, n3 *raft

	if storeLivenessEnabled {
		fabric = raftstoreliveness.NewLivenessFabricWithPeers(1, 2, 3)
		n1 = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(fabric.GetStoreLiveness(1)))
		n2 = newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(fabric.GetStoreLiveness(2)))
		n3 = newTestRaft(3, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(fabric.GetStoreLiveness(3)))
	} else {
		n1 = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(raftstoreliveness.Disabled{}))
		n2 = newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(raftstoreliveness.Disabled{}))
		n3 = newTestRaft(3, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(raftstoreliveness.Disabled{}))
	}

	n1.checkQuorum = true
	n2.checkQuorum = true
	n3.checkQuorum = true

	n1.becomeFollower(1, None)
	n2.becomeFollower(1, None)
	n3.becomeFollower(1, None)

	nt := newNetworkWithConfigAndLivenessFabric(nil, fabric, n1, n2, n3)

	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	// check state
	require.Equal(t, pb.StateLeader, n1.state)
	require.Equal(t, pb.StateFollower, n2.state)
	require.Equal(t, pb.StateFollower, n3.state)

	nt.isolate(3)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("somedata")}}})
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("somedata")}}})
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("somedata")}}})
	n1.preVote = true
	n2.preVote = true
	n3.preVote = true
	nt.recover()

	if storeLivenessEnabled {
		// We need to withdraw support from 1 so 3 can campaign and not get rejected
		// because of store liveness support.
		nt.livenessFabric.WithdrawSupportForPeerFromAllPeers(1)
	}

	nt.send(pb.Message{From: 3, To: 3, Type: pb.MsgHup})

	// check state
	require.Equal(t, pb.StateLeader, n1.state)
	require.Equal(t, pb.StateFollower, n2.state)
	if storeLivenessEnabled {
		// Since the peers no longer hold a valid inFortifyLease, 3 will receive
		// rejection votes and become a follower again.
		require.Equal(t, pb.StateFollower, n3.state)
	} else {
		// Peers will just ignore the MsgVoteRequest due to the inHeartbeatLease and
		// 3 will remain a preCandidate.
		require.Equal(t, pb.StatePreCandidate, n3.state)
	}

	// check term
	require.Equal(t, uint64(2), n1.Term)
	require.Equal(t, uint64(2), n2.Term)
	require.Equal(t, uint64(2), n3.Term)

	// delayed leader heartbeat does not force current leader to step down
	nt.send(pb.Message{From: 1, To: 3, Term: n1.Term, Type: pb.MsgHeartbeat})
	require.Equal(t, pb.StateLeader, n1.state)
}

// TestPreCandidateIgnoresDefortification tests that a pre-candidate ignores
// MsgDefortifyLeader and doesn't become a follower again.
func TestPreCandidateIgnoresDefortification(t *testing.T) {
	var fabric *raftstoreliveness.LivenessFabric
	var n1, n2 *raft

	fabric = raftstoreliveness.NewLivenessFabricWithPeers(1, 2)
	n1 = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2)),
		withStoreLiveness(fabric.GetStoreLiveness(1)))
	n2 = newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1, 2)),
		withStoreLiveness(fabric.GetStoreLiveness(2)))

	n1.checkQuorum = true
	n2.checkQuorum = true
	n1.preVote = true
	n2.preVote = true

	n1.becomeFollower(1, None)
	n2.becomeFollower(1, None)

	nt := newNetworkWithConfigAndLivenessFabric(nil, fabric, n1, n2)

	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	// Check raft states.
	require.Equal(t, pb.StateLeader, n1.state)
	require.Equal(t, pb.StateFollower, n2.state)

	// The term is 2 for both nodes.
	require.Equal(t, uint64(2), n1.Term)
	require.Equal(t, uint64(2), n2.Term)

	// Withdraw 2's support for 1. This allows 2 to pre-campaign since it's not
	// supporting a fortified leader.
	nt.livenessFabric.WithdrawSupportFor(2, 1)

	// Isolate 1 so that it doesn't receive the MsgVoteRequest from 2, and
	// therefore it doesn't vote for it.
	nt.isolate(1)
	nt.send(pb.Message{From: 2, To: 2, Type: pb.MsgHup})

	// 2 is now a pre-candidate.
	require.Equal(t, pb.StatePreCandidate, n2.state)

	// 2 should remain a PreCandidate even if it receives a MsgDefortifyLeader.
	nt.send(pb.Message{From: 1, To: 2, Term: 2, Type: pb.MsgDeFortifyLeader})
	require.Equal(t, pb.StatePreCandidate, n2.state)

	// However, receiving another message from a leader would cause 2 to become
	// follower again.
	nt.send(pb.Message{From: 1, To: 2, Term: 2, Type: pb.MsgApp})
	require.Equal(t, pb.StateFollower, n2.state)
}

func TestLeaderAppResp(t *testing.T) {
	// The test creates a leader node at term 2, with raft log [1 1 1 2 2 2].
	// Initial progress: match = 0, next = 4.
	for _, tt := range []struct {
		index  uint64
		reject bool
		// progress
		wmatch uint64
		// log index of the next entry to send to this follower
		wnext uint64
		// number of messages the leader sends out
		wmsgNum int
		// prevLogIndex in MsgApp from leader to followers
		windex uint64
		// leader's commit index
		wcommitted uint64
		// storage access counts for getting term number
		ctStgTerm int
	}{
		// stale resp; no replies
		{2, true, 0, 4, 0, 0, 0, 1},
		// stale resp; no replies
		{6, true, 0, 4, 0, 0, 0, 1},

		// denied resp; leader does not commit; decrease next and send probing msg
		// An additional term storage access is involved for an entry
		// that's already persisted since we are probing backwards.
		{3, true, 0, 3, 1, 2, 0, 2},

		// Follower 2 responds to leader, indicating log index 2 is replicated.
		// Leader tries to commit, but commit index doesn't advance since the index
		// is from a previous term.
		// We hit maybeCommit() and do term check comparison by using the invariant
		// raft.idxPreLeading.
		// There is no storage access for term in the maybeCommit() code path
		{2, false, 2, 7, 1, 2, 0, 2},

		// NB: For the following tests, we are skipping the MsgAppResp for the first
		// 3 entries, by directly processing MsgAppResp for later entries.
		//
		// Follower 2 is StateProbing at 4, it sends MsgAppResp for 4, and is moved
		// to StateReplicate and as many entries as possible are sent to it (5, 6).
		// Correspondingly the Next is then 7 (entry 7 does not exist, indicating
		// the follower will be up to date should it process the emitted MsgApp).
		// accept resp; leader commits; respond with commit index
		{4, false, 4, 7, 1, 4, 4, 1},

		// Follower 2 says term2, index5 is already replicated.
		// The leader responds with the updated commit index to follower 2.
		{5, false, 5, 7, 1, 5, 5, 1},
		// Follower 2 says term2, index6 is already replicated.
		// The leader responds with the updated commit index to follower 2.
		{6, false, 6, 7, 1, 6, 6, 1},
	} {
		t.Run("", func(t *testing.T) {
			storage := newTestMemoryStorage(withPeers(1, 2, 3))
			require.NoError(t, storage.Append(index(1).terms(1, 1, 1)))
			require.NoError(t, storage.SetHardState(pb.HardState{Term: 1}))
			sm := newTestRaft(1, 10, 1, storage)
			sm.becomeCandidate()
			require.Equal(t, uint64(2), sm.Term)
			require.Equal(t, uint64(3), sm.raftLog.lastIndex())
			sm.becomeLeader()
			require.Equal(t, uint64(4), sm.raftLog.lastIndex()) // appended a dummy
			sm.appendEntry(index(5).terms(2, 2)...)
			require.Equal(t, uint64(0), sm.raftLog.committed)
			sm.bcastAppend()
			sm.readMessages()

			require.NoError(t, sm.Step(pb.Message{
				From:       2,
				Type:       pb.MsgAppResp,
				Index:      tt.index,
				Term:       sm.Term,
				Reject:     tt.reject,
				RejectHint: tt.index,
			}))

			p := sm.trk.Progress(2)
			require.Equal(t, tt.wmatch, p.Match)
			require.Equal(t, tt.wnext, p.Next)

			msgs := sm.readMessages()
			require.Len(t, msgs, tt.wmsgNum)
			for _, msg := range msgs {
				require.Equal(t, tt.windex, msg.Index, "%v", DescribeMessage(msg, nil))
				require.Equal(t, tt.wcommitted, msg.Commit, "%v", DescribeMessage(msg, nil))
			}

			assert.Equal(t, tt.ctStgTerm, storage.callStats.term)
		})
	}
}

// TestBcastBeat is when the leader receives a heartbeat tick, it should
// send a MsgHeartbeat with m.Index = 0, m.LogTerm=0 and empty entries if
// store liveness is disabled. On the other hand, if store liveness is enabled,
// the leader doesn't send a MsgHeartbeat but sends a MsgApp if the follower
// needs it to catch up.
func TestBcastBeat(t *testing.T) {
	testutils.RunTrueAndFalse(t, "store-liveness-enabled",
		func(t *testing.T, storeLivenessEnabled bool) {
			offset := uint64(1000)
			// make a state machine with log.offset = 1000
			s := pb.Snapshot{
				Metadata: pb.SnapshotMetadata{
					Index:     offset,
					Term:      1,
					ConfState: pb.ConfState{Voters: []pb.PeerID{1, 2, 3}},
				},
			}
			storage := NewMemoryStorage()
			storage.ApplySnapshot(s)

			testOptions := emptyTestConfigModifierOpt()
			if !storeLivenessEnabled {
				testOptions = withStoreLiveness(raftstoreliveness.Disabled{})
			}

			sm := newTestRaft(1, 10, 1, storage, testOptions)

			sm.Term = 1

			sm.becomeCandidate()
			sm.becomeLeader()

			for i := 0; i < 10; i++ {
				mustAppendEntry(sm, pb.Entry{Index: uint64(i) + 1})
			}
			sm.advanceMessagesAfterAppend()

			// slow follower
			sm.trk.Progress(2).Match, sm.trk.Progress(2).Next = 5, 6
			// normal follower
			sm.trk.Progress(3).Match, sm.trk.Progress(3).Next = sm.raftLog.lastIndex(),
				sm.raftLog.lastIndex()+1

			// TODO(ibrahim): Create a test helper function that takes the number of
			// ticks and calls tick() that many times. Then we can refactor a lot of
			// tests that have this pattern.
			for ticks := sm.heartbeatTimeout; ticks > 0; ticks-- {
				sm.tick()
			}
			msgs := sm.readMessages()
			// If storeliveness is enabled, the heartbeat timeout will send a MsgApp
			// if it needs to. In this case since follower 2 is slow, we will send a
			// MsgApp to it.
			if storeLivenessEnabled {
				require.Len(t, msgs, 3)
				assert.Equal(t, []pb.Message{
					{From: 1, To: 2, Term: 2, Type: pb.MsgFortifyLeader},
					{From: 1, To: 3, Term: 2, Type: pb.MsgFortifyLeader},
					{From: 1, To: 3, Term: 2, Type: pb.MsgApp, LogTerm: 2, Index: 1011, Commit: 1000,
						Match: 1011},
				}, msgs)
			} else {
				require.Len(t, msgs, 2)
				assert.Equal(t, []pb.Message{
					{From: 1, To: 2, Term: 2, Type: pb.MsgHeartbeat, Match: 5},
					{From: 1, To: 3, Term: 2, Type: pb.MsgHeartbeat, Match: 1011},
				}, msgs)

				// Make sure that the heartbeat messages contain the expected fields.
				for i, m := range msgs {
					require.Equal(t, pb.MsgHeartbeat, m.Type, "#%d", i)
					require.Zero(t, m.Index, "#%d", i)
					require.Zero(t, m.LogTerm, "#%d", i)
					require.Empty(t, m.Entries, "#%d", i)
				}
			}
		})
}

// TestRecvMsgBeat tests the output of the state machine when receiving MsgBeat
func TestRecvMsgBeat(t *testing.T) {
	tests := []struct {
		state pb.StateType
		wMsg  int
	}{
		{pb.StateLeader, 2},
		// candidate and follower should ignore MsgBeat
		{pb.StateCandidate, 0},
		{pb.StateFollower, 0},
	}

	for i, tt := range tests {
		sm := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
		sm.raftLog = newLog(&MemoryStorage{ls: LogSlice{
			term:    1,
			entries: index(1).terms(1, 1),
		}}, nil)
		sm.Term = 1
		sm.state = tt.state
		switch tt.state {
		case pb.StateFollower:
			sm.step = stepFollower
		case pb.StateCandidate:
			sm.step = stepCandidate
		case pb.StateLeader:
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
	init := entryID{}.append(1, 2, 3)
	tests := []struct {
		// progress
		state tracker.StateType
		next  uint64

		wnext uint64
	}{
		// state replicate, optimistically increase next
		// previous entries + noop entry + propose + 1
		{tracker.StateReplicate, 2, uint64(len(init.entries) + 1 + 1 + 1)},
		// state probe, not optimistically increase next
		{tracker.StateProbe, 2, 2},
	}

	for i, tt := range tests {
		sm := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2)))
		sm.becomeFollower(init.term, None)
		require.True(t, sm.raftLog.append(init))
		sm.becomeCandidate()
		sm.becomeLeader()
		sm.trk.Progress(2).State = tt.state
		sm.trk.Progress(2).Next = tt.next
		sm.Step(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("somedata")}}})

		p := sm.trk.Progress(2)
		assert.Equal(t, tt.wnext, p.Next, "#%d", i)
	}
}

func TestSendAppendForProgressProbeStoreLivenessDisabled(t *testing.T) {
	r := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2)),
		withStoreLiveness(raftstoreliveness.Disabled{}))

	r.becomeCandidate()
	r.becomeLeader()

	// Initialize the log with some data.
	mustAppendEntry(r, pb.Entry{Data: []byte("init")})

	// Force set the match index to 1. This will make the leader use the index 1
	// when sending the MsgApp.
	r.trk.Progress(2).Match = 1
	r.trk.Progress(2).BecomeProbe()

	// each round is a heartbeat
	for i := 0; i < 3; i++ {
		if i == 0 {
			// We expect that raft will only send out one MsgApp on the first loop.
			// After that, the follower is paused until a heartbeat response is
			// received.
			mustAppendEntry(r, pb.Entry{Data: []byte("somedata")})
			r.maybeSendAppend(2)
			msg := r.readMessages()
			assert.Len(t, msg, 1)
			assert.Equal(t, pb.MsgApp, msg[0].Type)
			assert.Equal(t, msg[0].Index, uint64(1))
		}

		assert.True(t, r.trk.Progress(2).MsgAppProbesPaused)
		for j := 0; j < 10; j++ {
			mustAppendEntry(r, pb.Entry{Data: []byte("somedata")})
			r.maybeSendAppend(2)
			assert.Empty(t, r.readMessages())
		}

		// do a heartbeat
		for j := int64(0); j < r.heartbeatTimeout; j++ {
			r.tick()
		}
		assert.True(t, r.trk.Progress(2).MsgAppProbesPaused)

		// No MsgApp gets sent since we haven't received a MsgHeartbeatResp.
		msg := r.readMessages()
		assert.Len(t, msg, 1)
		assert.Equal(t, pb.MsgHeartbeat, msg[0].Type)
	}

	// a MsgHeartbeatResp will allow another message to be sent
	r.Step(pb.Message{From: 2, To: 1, Type: pb.MsgHeartbeatResp})
	msg := r.readMessages()
	assert.Len(t, msg, 1)
	assert.Equal(t, msg[0].Type, pb.MsgApp)
	assert.Equal(t, msg[0].Index, uint64(1))
	assert.True(t, r.trk.Progress(2).MsgAppProbesPaused)
}

func TestSendAppendForProgressProbeStoreLivenessEnabled(t *testing.T) {
	r := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2)))

	r.becomeCandidate()
	r.becomeLeader()

	// Initialize the log with some data.
	mustAppendEntry(r, pb.Entry{Data: []byte("init")})

	// Force set the match index to 1. This will make the leader use the index 1
	// when sending the probe MsgApp.
	r.trk.Progress(2).Match = 1
	r.trk.Progress(2).BecomeProbe()

	r.readMessages()
	r.trk.Progress(2).BecomeProbe()

	// each round is a heartbeat
	for i := 0; i < 3; i++ {
		if i == 0 {
			// We expect that raft will only send out one MsgApp on the first loop.
			// After that, the follower is paused until the next heartbeat timeout.
			mustAppendEntry(r, pb.Entry{Data: []byte("somedata")})
			r.maybeSendAppend(2)
			msg := r.readMessages()
			assert.Len(t, msg, 1)
			assert.Equal(t, pb.MsgApp, msg[0].Type)
			assert.Equal(t, msg[0].Index, uint64(1))
		}

		assert.True(t, r.trk.Progress(2).MsgAppProbesPaused)
		for j := 0; j < 10; j++ {
			mustAppendEntry(r, pb.Entry{Data: []byte("somedata")})
			r.maybeSendAppend(2)
			assert.Empty(t, r.readMessages())
		}

		// The next heartbeat timeout will allow another message to be sent.
		for j := int64(0); j < r.heartbeatTimeout; j++ {
			r.tick()
		}
		assert.True(t, r.trk.Progress(2).MsgAppProbesPaused)

		msg := r.readMessages()
		assert.Len(t, msg, 2)
		assert.Equal(t, pb.MsgFortifyLeader, msg[0].Type)
		assert.Equal(t, pb.MsgApp, msg[1].Type)
		assert.Equal(t, msg[1].Index, uint64(1))
		assert.True(t, r.trk.Progress(2).MsgAppProbesPaused)
	}
}

func TestSendAppendForProgressReplicate(t *testing.T) {
	r := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2)))
	r.becomeCandidate()
	r.becomeLeader()
	r.readMessages()
	r.trk.Progress(2).BecomeReplicate()

	for i := 0; i < 10; i++ {
		mustAppendEntry(r, pb.Entry{Data: []byte("somedata")})
		r.maybeSendAppend(2)
		msgs := r.readMessages()
		assert.Len(t, msgs, 1, "#%d", i)
	}
}

func TestSendAppendForProgressSnapshot(t *testing.T) {
	r := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2)))
	r.becomeCandidate()
	r.becomeLeader()
	r.readMessages()
	r.trk.Progress(2).BecomeSnapshot(10)

	for i := 0; i < 10; i++ {
		mustAppendEntry(r, pb.Entry{Data: []byte("somedata")})
		r.maybeSendAppend(2)
		msgs := r.readMessages()
		assert.Empty(t, msgs, "#%d", i)
	}
}

func TestRecvMsgUnreachable(t *testing.T) {
	previousEnts := index(1).terms(1, 2, 3)
	s := newTestMemoryStorage(withPeers(1, 2))
	s.SetHardState(pb.HardState{Term: 3})
	s.Append(previousEnts)
	r := newTestRaft(1, 10, 1, s)
	r.becomeCandidate()
	r.becomeLeader()
	r.readMessages()
	// set node 2 to state replicate
	r.trk.Progress(2).Match = 3
	r.trk.Progress(2).BecomeReplicate()
	r.trk.Progress(2).Next = 6

	r.Step(pb.Message{From: 2, To: 1, Type: pb.MsgUnreachable})

	assert.Equal(t, tracker.StateProbe, r.trk.Progress(2).State)
	wnext := r.trk.Progress(2).Match + 1
	assert.Equal(t, wnext, r.trk.Progress(2).Next)
}

func TestRestore(t *testing.T) {
	s := snapshot{
		term: 11,
		snap: pb.Snapshot{Metadata: pb.SnapshotMetadata{
			Index:     11, // magic number
			Term:      11, // magic number
			ConfState: pb.ConfState{Voters: []pb.PeerID{1, 2, 3}},
		}},
	}

	storage := newTestMemoryStorage(withPeers(1, 2))
	sm := newTestRaft(1, 10, 1, storage)
	require.True(t, sm.restore(s))

	assert.Equal(t, s.lastEntryID(), sm.raftLog.lastEntryID())
	assert.Equal(t, s.snap.Metadata.ConfState.Voters, sm.trk.VoterNodes())

	require.False(t, sm.restore(s))
	for i := int64(0); i < sm.randomizedElectionTimeout; i++ {
		sm.tick()
	}
	assert.Equal(t, pb.StateFollower, sm.state)
}

// TestRestoreWithLearner restores a snapshot which contains learners.
func TestRestoreWithLearner(t *testing.T) {
	s := snapshot{
		term: 11,
		snap: pb.Snapshot{Metadata: pb.SnapshotMetadata{
			Index:     11, // magic number
			Term:      11, // magic number
			ConfState: pb.ConfState{Voters: []pb.PeerID{1, 2}, Learners: []pb.PeerID{3}},
		}},
	}

	storage := newTestMemoryStorage(withPeers(1, 2), withLearners(3))
	sm := newTestLearnerRaft(3, 8, 2, storage)
	assert.True(t, sm.restore(s))

	assert.Equal(t, s.lastEntryID(), sm.raftLog.lastEntryID())

	sg := sm.trk.VoterNodes()
	assert.Len(t, sg, len(s.snap.Metadata.ConfState.Voters))

	lns := sm.trk.LearnerNodes()
	assert.Len(t, lns, len(s.snap.Metadata.ConfState.Learners))

	for _, n := range s.snap.Metadata.ConfState.Voters {
		assert.False(t, sm.trk.Progress(n).IsLearner)
	}
	for _, n := range s.snap.Metadata.ConfState.Learners {
		assert.True(t, sm.trk.Progress(n).IsLearner)
	}

	assert.False(t, sm.restore(s))
}

// TestRestoreWithVotersOutgoing tests if outgoing voter can receive and apply snapshot correctly.
func TestRestoreWithVotersOutgoing(t *testing.T) {
	s := snapshot{
		term: 11,
		snap: pb.Snapshot{Metadata: pb.SnapshotMetadata{
			Index:     11, // magic number
			Term:      11, // magic number
			ConfState: pb.ConfState{Voters: []pb.PeerID{2, 3, 4}, VotersOutgoing: []pb.PeerID{1, 2, 3}},
		}},
	}

	storage := newTestMemoryStorage(withPeers(1, 2))
	sm := newTestRaft(1, 10, 1, storage)
	require.True(t, sm.restore(s))

	assert.Equal(t, s.lastEntryID(), sm.raftLog.lastEntryID())

	sg := sm.trk.VoterNodes()
	assert.Equal(t, []pb.PeerID{1, 2, 3, 4}, sg)

	require.False(t, sm.restore(s))

	// It should not campaign before actually applying data.
	for i := int64(0); i < sm.randomizedElectionTimeout; i++ {
		sm.tick()
	}
	assert.Equal(t, pb.StateFollower, sm.state)
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
	s := snapshot{
		term: 11,
		snap: pb.Snapshot{Metadata: pb.SnapshotMetadata{
			Index:     11, // magic number
			Term:      11, // magic number
			ConfState: pb.ConfState{Voters: []pb.PeerID{1, 2}, Learners: []pb.PeerID{3}},
		}},
	}

	storage := newTestMemoryStorage(withPeers(1, 2, 3))
	sm := newTestRaft(3, 10, 1, storage)

	assert.False(t, sm.isLearner)
	assert.True(t, sm.restore(s))
}

// TestRestoreLearnerPromotion checks that a learner can become to a follower after
// restoring snapshot.
func TestRestoreLearnerPromotion(t *testing.T) {
	s := snapshot{
		term: 11,
		snap: pb.Snapshot{Metadata: pb.SnapshotMetadata{
			Index:     11, // magic number
			Term:      11, // magic number
			ConfState: pb.ConfState{Voters: []pb.PeerID{1, 2, 3}},
		}},
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
	s := snapshot{
		term: 11,
		snap: pb.Snapshot{Metadata: pb.SnapshotMetadata{
			Index:     11, // magic number
			Term:      11, // magic number
			ConfState: pb.ConfState{Voters: []pb.PeerID{1}, Learners: []pb.PeerID{2}},
		}},
	}

	store := newTestMemoryStorage(withPeers(1), withLearners(2))
	store.SetHardState(pb.HardState{Term: 11})
	n1 := newTestLearnerRaft(1, 10, 1, store)
	n2 := newTestLearnerRaft(2, 10, 1, newTestMemoryStorage(withPeers(1), withLearners(2)))

	n1.restore(s)
	snap := n1.raftLog.nextUnstableSnapshot()
	store.ApplySnapshot(*snap)
	n1.appliedSnap(snap)

	nt := newNetwork(n1, n2)

	setRandomizedElectionTimeout(n1, n1.electionTimeout)
	for i := int64(0); i < n1.electionTimeout; i++ {
		n1.tick()
	}

	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgBeat})

	assert.Equal(t, n1.raftLog.committed, n2.raftLog.committed)
}

func TestRestoreIgnoreSnapshot(t *testing.T) {
	init := entryID{}.append(1, 1, 1)
	commit := uint64(1)
	storage := newTestMemoryStorage(withPeers(1, 2))
	sm := newTestRaft(1, 10, 1, storage)
	require.True(t, sm.raftLog.append(init))
	sm.raftLog.commitTo(LogMark{Term: init.term, Index: commit})

	s := snapshot{
		term: 1,
		snap: pb.Snapshot{Metadata: pb.SnapshotMetadata{
			Index:     commit,
			Term:      1,
			ConfState: pb.ConfState{Voters: []pb.PeerID{1, 2}},
		}},
	}

	// ignore snapshot
	assert.False(t, sm.restore(s))
	assert.Equal(t, sm.raftLog.committed, commit)

	// ignore snapshot and fast forward commit
	s.snap.Metadata.Index = commit + 1
	assert.False(t, sm.restore(s))
	assert.Equal(t, sm.raftLog.committed, commit+1)
}

func TestProvideSnap(t *testing.T) {
	// restore the state machine from a snapshot so it has a compacted log and a snapshot
	s := snapshot{
		term: 11,
		snap: pb.Snapshot{Metadata: pb.SnapshotMetadata{
			Index:     11, // magic number
			Term:      11, // magic number
			ConfState: pb.ConfState{Voters: []pb.PeerID{1, 2}},
		}},
	}
	storage := newTestMemoryStorage(withPeers(1))
	sm := newTestRaft(1, 10, 1, storage)
	sm.becomeFollower(s.term, None)
	sm.restore(s)

	sm.becomeCandidate()
	sm.becomeLeader()

	// force set the next of node 2, so that node 2 needs a snapshot
	sm.trk.Progress(2).Next = sm.raftLog.firstIndex()
	sm.Step(pb.Message{From: 2, To: 1, Type: pb.MsgAppResp, Index: sm.trk.Progress(2).Next - 1, Reject: true})

	msgs := sm.readMessages()
	require.Len(t, msgs, 1)
	m := msgs[0]
	assert.Equal(t, m.Type, pb.MsgSnap)
}

func TestIgnoreProvidingSnap(t *testing.T) {
	// restore the state machine from a snapshot so it has a compacted log and a snapshot
	s := snapshot{
		term: 11,
		snap: pb.Snapshot{Metadata: pb.SnapshotMetadata{
			Index:     11, // magic number
			Term:      11, // magic number
			ConfState: pb.ConfState{Voters: []pb.PeerID{1, 2}},
		}},
	}
	storage := newTestMemoryStorage(withPeers(1))
	sm := newTestRaft(1, 10, 1, storage)
	sm.becomeFollower(s.term, None)
	sm.restore(s)

	sm.becomeCandidate()
	sm.becomeLeader()

	// force set the next of node 2, so that node 2 needs a snapshot
	// change node 2 to be inactive, expect node 1 ignore sending snapshot to 2
	sm.trk.Progress(2).Next = sm.raftLog.firstIndex() - 1
	sm.trk.Progress(2).RecentActive = false

	sm.Step(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("somedata")}}})

	msgs := sm.readMessages()
	assert.Empty(t, msgs)
}

func TestRestoreFromSnapMsg(t *testing.T) {
	s := snapshot{
		term: 11,
		snap: pb.Snapshot{Metadata: pb.SnapshotMetadata{
			Index:     11, // magic number
			Term:      11, // magic number
			ConfState: pb.ConfState{Voters: []pb.PeerID{1, 2}},
		}},
	}
	m := pb.Message{Type: pb.MsgSnap, From: 1, Term: 2, Snapshot: &s.snap}

	sm := newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1, 2)))
	sm.Step(m)

	assert.Equal(t, None, sm.lead)
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
		if lead.trk.Progress(3).RecentActive {
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
	ents, err := r.raftLog.entries(index, noLimit)
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
	assert.Equal(t, []pb.PeerID{1, 2}, nodes)
}

// TestAddLearner tests that addLearner could update nodes correctly.
func TestAddLearner(t *testing.T) {
	r := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1)))
	// Add new learner peer.
	r.applyConfChange(pb.ConfChange{NodeID: 2, Type: pb.ConfChangeAddLearnerNode}.AsV2())
	require.False(t, r.isLearner)
	nodes := r.trk.LearnerNodes()
	assert.Equal(t, []pb.PeerID{2}, nodes)
	require.True(t, r.trk.Progress(2).IsLearner)

	// Promote peer to voter.
	r.applyConfChange(pb.ConfChange{NodeID: 2, Type: pb.ConfChangeAddNode}.AsV2())
	require.False(t, r.trk.Progress(2).IsLearner)

	// Demote r.
	r.applyConfChange(pb.ConfChange{NodeID: 1, Type: pb.ConfChangeAddLearnerNode}.AsV2())
	require.True(t, r.trk.Progress(1).IsLearner)
	require.True(t, r.isLearner)

	// Promote r again.
	r.applyConfChange(pb.ConfChange{NodeID: 1, Type: pb.ConfChangeAddNode}.AsV2())
	require.False(t, r.trk.Progress(1).IsLearner)
	require.False(t, r.isLearner)
}

// TestAddNodeCheckQuorum tests that addNode does not trigger a leader election
// immediately when checkQuorum is set.
func TestAddNodeCheckQuorum(t *testing.T) {
	testutils.RunTrueAndFalse(t, "store-liveness-enabled",
		func(t *testing.T, storeLivenessEnabled bool) {
			testAddNodeCheckQuorum(t, storeLivenessEnabled)
		})
}

func testAddNodeCheckQuorum(t *testing.T, storeLivenessEnabled bool) {
	var fabric *raftstoreliveness.LivenessFabric
	var r *raft

	if storeLivenessEnabled {
		fabric = raftstoreliveness.NewLivenessFabricWithPeers(1, 2)
		r = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1)),
			withStoreLiveness(fabric.GetStoreLiveness(1)))
	} else {
		r = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1)),
			withStoreLiveness(raftstoreliveness.Disabled{}))
	}

	r.checkQuorum = true

	r.becomeCandidate()
	r.becomeLeader()

	for i := int64(0); i < r.electionTimeout-1; i++ {
		r.tick()
	}

	r.applyConfChange(pb.ConfChange{NodeID: 2, Type: pb.ConfChangeAddNode}.AsV2())

	// This tick will reach electionTimeout, which triggers a quorum check.
	r.tick()

	// Node 1 should still be the leader after a single tick.
	assert.Equal(t, pb.StateLeader, r.state)

	// After another electionTimeout ticks without hearing from node 2,
	// node 1 should step down.
	for i := int64(0); i < r.electionTimeout; i++ {
		r.tick()
	}

	assert.Equal(t, pb.StateFollower, r.state)
}

// TestRemoveNode tests that removeNode could update nodes and
// removed list correctly.
func TestRemoveNode(t *testing.T) {
	r := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2)))
	r.applyConfChange(pb.ConfChange{NodeID: 2, Type: pb.ConfChangeRemoveNode}.AsV2())
	w := []pb.PeerID{1}
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
	w := []pb.PeerID{1}
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
	id := pb.PeerID(1)
	tests := []struct {
		peers []pb.PeerID
		wp    bool
	}{
		{[]pb.PeerID{1}, true},
		{[]pb.PeerID{1, 2, 3}, true},
		{[]pb.PeerID{}, false},
		{[]pb.PeerID{2, 3}, false},
	}
	for i, tt := range tests {
		r := newTestRaft(id, 5, 1, newTestMemoryStorage(withPeers(tt.peers...)))
		assert.Equal(t, tt.wp, r.promotable(), "#%d", i)
	}
}

func TestRaftNodes(t *testing.T) {
	tests := []struct {
		ids  []pb.PeerID
		wids []pb.PeerID
	}{
		{
			[]pb.PeerID{1, 2, 3},
			[]pb.PeerID{1, 2, 3},
		},
		{
			[]pb.PeerID{3, 2, 1},
			[]pb.PeerID{1, 2, 3},
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
	assert.Equal(t, pb.StateFollower, r.state)
	// We don't call campaign() directly because it comes after the check
	// for our current state.
	r.Step(pb.Message{From: 1, To: 1, Type: pb.MsgHup})
	r.advanceMessagesAfterAppend()
	assert.Equal(t, pb.StateLeader, r.state)

	term := r.Term
	r.Step(pb.Message{From: 1, To: 1, Type: pb.MsgHup})
	r.advanceMessagesAfterAppend()
	assert.Equal(t, pb.StateLeader, r.state)
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

	// Begin to demote the second node by entering a joint config.
	cc := pb.ConfChangeV2{
		Changes: []pb.ConfChangeSingle{
			{Type: pb.ConfChangeRemoveNode, NodeID: 2},
			{Type: pb.ConfChangeAddLearnerNode, NodeID: 2},
		},
	}
	ccData, err := cc.Marshal()
	require.NoError(t, err)
	r.Step(pb.Message{
		Type: pb.MsgProp,
		Entries: []pb.Entry{
			{Type: pb.EntryConfChangeV2, Data: ccData},
		},
	})

	// Stabilize the log and make sure nothing is committed yet.
	require.Empty(t, nextEnts(r, s))
	ccIndex := r.raftLog.lastIndex()

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
	require.Equal(t, pb.EntryConfChangeV2, ents[1].Type)

	// Apply the config changes. This enters a joint config. At this point the
	// quorum requirement is 2, because node 2 remains a voter on the outgoing
	// side of the joint config.
	r.applyConfChange(cc.AsV2())

	// Immediately exit the joint config.
	cc = pb.ConfChangeV2{}
	ccData, err = cc.Marshal()
	require.NoError(t, err)
	r.Step(pb.Message{
		Type: pb.MsgProp,
		Entries: []pb.Entry{
			{Type: pb.EntryConfChangeV2, Data: ccData},
		},
	})

	// Stabilize the log and make sure nothing is committed yet.
	require.Empty(t, nextEnts(r, s))
	ccIndex = r.raftLog.lastIndex()

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
	ents = nextEnts(r, s)
	require.Len(t, ents, 1)
	require.Equal(t, pb.EntryConfChangeV2, ents[0].Type)

	// Apply the config changes to exit the joint config. This reduces quorum
	// requirements so the pending command can now commit.
	r.applyConfChange(cc.AsV2())
	ents = nextEnts(r, s)
	require.Len(t, ents, 1)
	require.Equal(t, pb.EntryNormal, ents[0].Type)
	require.Equal(t, []byte("hello"), ents[0].Data)
}

// TestLeaderTransferToUpToDateNode verifies transferring should start
// immediately if the transferee has the most up-to-date log entries when
// transfer is requested.
func TestLeaderTransferToUpToDateNode(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	lead := nt.peers[1].(*raft)

	require.Equal(t, pb.PeerID(1), lead.lead)

	// Transfer leadership to 2.
	nt.send(pb.Message{From: 2, To: 1, Type: pb.MsgTransferLeader})

	checkLeaderTransferState(t, lead, pb.StateFollower, 2)

	// After some log replication, transfer leadership back to 1.
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{}}})

	nt.send(pb.Message{From: 1, To: 2, Type: pb.MsgTransferLeader})

	checkLeaderTransferState(t, lead, pb.StateLeader, 1)
}

// TestLeaderTransferToUpToDateNodeFromFollower verifies transferring should
// start immediately if the transferee has the most up-to-date log entries when
// transfer starts. Unlike TestLeaderTransferToUpToDateNode, where the leader
// transfer message is sent to the leader, in this test case every leader
// transfer message is sent to the follower and is redirected to the leader.
func TestLeaderTransferToUpToDateNodeFromFollower(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	lead := nt.peers[1].(*raft)

	require.Equal(t, pb.PeerID(1), lead.lead)

	// Transfer leadership to 2.
	nt.send(pb.Message{From: 2, To: 2, Type: pb.MsgTransferLeader})

	checkLeaderTransferState(t, lead, pb.StateFollower, 2)

	// After some log replication, transfer leadership back to 1.
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{}}})

	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgTransferLeader})

	checkLeaderTransferState(t, lead, pb.StateLeader, 1)
}

// TestLeaderTransferLeaderStepsDownImmediately verifies that the outgoing
// leader steps down to a follower as soon as it sends a MsgTimeoutNow to the
// transfer target, even before (and regardless of if) the target receives the
// MsgTimeoutNow and campaigns.
func TestLeaderTransferLeaderStepsDownImmediately(t *testing.T) {
	testutils.RunTrueAndFalse(t, "store-liveness-enabled",
		func(t *testing.T, storeLivenessEnabled bool) {
			testLeaderTransferLeaderStepsDownImmediately(t, storeLivenessEnabled)
		})
}

func testLeaderTransferLeaderStepsDownImmediately(t *testing.T, storeLivenessEnabled bool) {
	var fabric *raftstoreliveness.LivenessFabric
	var n1, n2, n3 *raft

	if storeLivenessEnabled {
		fabric = raftstoreliveness.NewLivenessFabricWithPeers(1, 2, 3)
		n1 = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(fabric.GetStoreLiveness(1)))
		n2 = newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(fabric.GetStoreLiveness(2)))
		n3 = newTestRaft(3, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(fabric.GetStoreLiveness(3)))
	} else {
		n1 = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(raftstoreliveness.Disabled{}))
		n2 = newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(raftstoreliveness.Disabled{}))
		n3 = newTestRaft(3, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(raftstoreliveness.Disabled{}))
	}

	nt := newNetworkWithConfigAndLivenessFabric(nil, fabric, n1, n2, n3)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	// Isolate node 3. It is up-to-date, so the leadership transfer will be
	// initiated immediately, but node 3 will never receive the MsgTimeoutNow and
	// call an election.
	nt.isolate(3)

	lead := nt.peers[1].(*raft)
	require.Equal(t, uint64(1), lead.Term)
	require.Equal(t, pb.PeerID(1), lead.lead)

	// Transfer leadership to 3. The leader steps down immediately in the same
	// term, waiting for the transfer target to call an election.
	nt.send(pb.Message{From: 3, To: 1, Type: pb.MsgTransferLeader})

	require.Equal(t, uint64(1), lead.Term)
	checkLeaderTransferState(t, lead, pb.StateFollower, None)

	// With leader leases, the ex-leader would send a MsgDefortifyLeader to
	// its followers when the support is expired.
	if storeLivenessEnabled {
		nt.livenessFabric.SetSupportExpired(1, true)
		lead.tick()
		nt.send(lead.readMessages()...)
		nt.livenessFabric.SetSupportExpired(1, false)
	}

	// Eventually, the previous leader gives up on waiting and calls an election
	// to reestablish leadership at the next term.
	for i := int64(0); i < lead.randomizedElectionTimeout; i++ {
		lead.tick()
	}
	nt.send(lead.readMessages()...)

	require.Equal(t, uint64(2), lead.Term)
	checkLeaderTransferState(t, lead, pb.StateLeader, 1)
}

// TestLeaderTransferWithCheckQuorum ensures transferring leader still works
// even the current leader is still under its leader lease.
func TestLeaderTransferWithCheckQuorum(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	for i := int64(1); i < 4; i++ {
		r := nt.peers[pb.PeerID(i)].(*raft)
		r.checkQuorum = true
		setRandomizedElectionTimeout(r, r.electionTimeout+i)
	}

	// Letting peer 2 electionElapsed reach to timeout so that it can vote for peer 1
	f := nt.peers[2].(*raft)
	for i := int64(0); i < f.electionTimeout; i++ {
		f.tick()
	}

	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	lead := nt.peers[1].(*raft)

	require.Equal(t, pb.StateLeader, lead.state)

	// Transfer leadership to 2.
	nt.send(pb.Message{From: 2, To: 1, Type: pb.MsgTransferLeader})

	checkLeaderTransferState(t, lead, pb.StateFollower, 2)

	// After some log replication, transfer leadership back to 1.
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{}}})

	nt.send(pb.Message{From: 1, To: 2, Type: pb.MsgTransferLeader})

	checkLeaderTransferState(t, lead, pb.StateLeader, 1)
}

func TestLeaderTransferToSlowFollower(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	// Isolate node 3 and propose an entry on 1. This will cause node 3 to fall
	// behind on its log, so that the leadership transfer won't be initiated
	// immediately.
	nt.isolate(3)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{}}})

	lead := nt.peers[1].(*raft)
	require.Equal(t, uint64(2), lead.trk.Progress(1).Match)
	require.Equal(t, uint64(1), lead.trk.Progress(3).Match)

	// Reconnect node 3 and initiate a transfer of leadership from node 1 to node
	// 3. The leader (node 1) will catch it up on log entries using MsgApps before
	// transferring it leadership using MsgTimeoutNow.
	nt.recover()
	nt.send(pb.Message{From: 3, To: 1, Type: pb.MsgTransferLeader})

	checkLeaderTransferState(t, lead, pb.StateFollower, 3)
}

func TestLeaderTransferToCandidate(t *testing.T) {
	nt := newNetworkWithConfig(preVoteConfigWithFortificationDisabled, nil, nil, nil)
	n3 := nt.peers[3].(*raft)

	// Elect node 1 as the leader of term 1.
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})
	require.Equal(t, uint64(1), n3.Term)

	// Isolate node 3 so that it decides to become a pre-candidate.
	nt.isolate(3)
	for i := int64(0); i < n3.randomizedElectionTimeout; i++ {
		nt.tick(n3)
	}
	require.Equal(t, pb.StatePreCandidate, n3.state)
	require.Equal(t, uint64(1), n3.Term)

	// Reconnect node 3 and initiate a transfer of leadership from node 1 to node
	// 3, all before node 3 steps back to a follower. This will instruct node 3 to
	// call an election at the next term, which it can and does win.
	nt.recover()
	nt.send(pb.Message{From: 3, To: 1, Type: pb.MsgTransferLeader})
	require.Equal(t, pb.StateLeader, n3.state)
	require.Equal(t, uint64(2), n3.Term)
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
	require.Equal(t, uint64(1), lead.trk.Progress(3).Match)

	filtered := pb.Message{}
	// Snapshot needs to be applied before sending MsgAppResp.
	nt.msgHook = func(m pb.Message) bool {
		if m.Type != pb.MsgAppResp || m.From != 3 || m.Reject {
			return true
		}
		filtered = m
		return false
	}
	// Transfer leadership to 3 when node 3 is missing a snapshot.
	nt.send(pb.Message{From: 3, To: 1, Type: pb.MsgTransferLeader})
	require.Equal(t, pb.StateLeader, lead.state)
	require.NotEqual(t, pb.Message{}, filtered)

	// Apply snapshot and resume progress.
	follower := nt.peers[3].(*raft)
	snap := follower.raftLog.nextUnstableSnapshot()
	nt.storage[3].ApplySnapshot(*snap)
	follower.appliedSnap(snap)
	nt.msgHook = nil
	nt.send(filtered)

	checkLeaderTransferState(t, lead, pb.StateFollower, 3)
}

func TestLeaderTransferToSelf(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	lead := nt.peers[1].(*raft)

	// Transfer leadership to self, there will be noop.
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgTransferLeader})
	checkLeaderTransferState(t, lead, pb.StateLeader, 1)
}

func TestLeaderTransferToNonExistingNode(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	lead := nt.peers[1].(*raft)
	// Transfer leadership to non-existing node, there will be noop.
	nt.send(pb.Message{From: 4, To: 1, Type: pb.MsgTransferLeader})
	checkLeaderTransferState(t, lead, pb.StateLeader, 1)
}

func TestLeaderTransferTimeout(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	// Isolate node 3 and propose an entry on 1. This will cause node 3 to fall
	// behind on its log, so that the leadership transfer won't be initiated
	// immediately. If it were, we couldn't test the timeout.
	nt.isolate(3)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{}}})

	lead := nt.peers[1].(*raft)

	// Transfer leadership to isolated node, wait for timeout.
	nt.send(pb.Message{From: 3, To: 1, Type: pb.MsgTransferLeader})
	require.Equal(t, pb.PeerID(3), lead.leadTransferee)

	for i := int64(0); i < lead.heartbeatTimeout; i++ {
		lead.tick()
	}
	require.Equal(t, pb.PeerID(3), lead.leadTransferee)

	for i := int64(0); i < lead.electionTimeout-lead.heartbeatTimeout; i++ {
		lead.tick()
	}

	checkLeaderTransferState(t, lead, pb.StateLeader, 1)
}

func TestLeaderTransferIgnoreProposal(t *testing.T) {
	s := newTestMemoryStorage(withPeers(1, 2, 3))
	r := newTestRaft(1, 10, 1, s)
	nt := newNetwork(r, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	// Isolate node 3 and propose an entry on 1. This will cause node 3 to fall
	// behind on its log, so that the leadership transfer won't be initiated
	// immediately.
	nt.isolate(3)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{}}})

	// Transfer leadership to the isolated, behind node. This will leave the
	// transfer in a pending state as the leader tries to catch up the target.
	nt.send(pb.Message{From: 3, To: 1, Type: pb.MsgTransferLeader})
	lead := nt.peers[1].(*raft)
	require.Equal(t, pb.PeerID(3), lead.leadTransferee)

	// Then send proposal. This should be dropped.
	err := lead.Step(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{}}})
	require.Equal(t, ErrProposalDropped, err)
	require.Equal(t, pb.PeerID(3), lead.leadTransferee)

	require.Equal(t, uint64(2), lead.trk.Progress(1).Match)
}

func TestLeaderTransferReceiveHigherTermVote(t *testing.T) {
	testutils.RunTrueAndFalse(t, "store-liveness-enabled",
		func(t *testing.T, storeLivenessEnabled bool) {
			testLeaderTransferReceiveHigherTermVote(t, storeLivenessEnabled)
		})
}

func testLeaderTransferReceiveHigherTermVote(t *testing.T, storeLivenessEnabled bool) {
	var cfg func(c *Config) = nil
	if !storeLivenessEnabled {
		cfg = fortificationDisabledConfig
	}

	nt := newNetworkWithConfig(cfg, nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	// Isolate node 3 and propose an entry on 1. This will cause node 3 to fall
	// behind on its log, so that the leadership transfer won't be initiated
	// immediately.
	nt.isolate(3)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{}}})

	lead := nt.peers[1].(*raft)

	// Transfer leadership to the isolated, behind node. This will leave the
	// transfer in a pending state as the leader tries to catch up the target.
	nt.send(pb.Message{From: 3, To: 1, Type: pb.MsgTransferLeader})
	require.Equal(t, pb.PeerID(3), lead.leadTransferee)

	if storeLivenessEnabled {
		// We need to withdraw support of the current leader to allow the new peer
		// to campaign and get elected.
		nt.livenessFabric.WithdrawSupportForPeerFromAllPeers(1)
	}

	nt.send(pb.Message{From: 2, To: 2, Type: pb.MsgHup, Index: 1, Term: 2})

	checkLeaderTransferState(t, lead, pb.StateFollower, 2)
}

func TestLeaderTransferRemoveNode(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	// Isolate node 3 and propose an entry on 1. This will cause node 3 to fall
	// behind on its log, so that the leadership transfer won't be initiated
	// immediately.
	nt.isolate(3)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{}}})

	lead := nt.peers[1].(*raft)

	// The leadTransferee is removed with leadership transfer in progress.
	nt.send(pb.Message{From: 3, To: 1, Type: pb.MsgTransferLeader})
	require.Equal(t, pb.PeerID(3), lead.leadTransferee)

	lead.applyConfChange(pb.ConfChange{NodeID: 3, Type: pb.ConfChangeRemoveNode}.AsV2())

	checkLeaderTransferState(t, lead, pb.StateLeader, 1)
}

func TestLeaderTransferDemoteNode(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	// Isolate node 3 and propose an entry on 1. This will cause node 3 to fall
	// behind on its log, so that the leadership transfer won't be initiated
	// immediately.
	nt.isolate(3)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{}}})

	lead := nt.peers[1].(*raft)

	// The leadTransferee is demoted with leadership transfer in progress.
	nt.send(pb.Message{From: 3, To: 1, Type: pb.MsgTransferLeader})
	require.Equal(t, pb.PeerID(3), lead.leadTransferee)

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
	checkLeaderTransferState(t, lead, pb.StateLeader, 1)
}

// TestLeaderTransferBack verifies leadership can transfer back to self when
// last transfer is pending, which cancels the transfer attempt.
func TestLeaderTransferBack(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	// Isolate node 3 and propose an entry on 1. This will cause node 3 to fall
	// behind on its log, so that the leadership transfer won't be initiated
	// immediately.
	nt.isolate(3)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{}}})

	lead := nt.peers[1].(*raft)

	nt.send(pb.Message{From: 3, To: 1, Type: pb.MsgTransferLeader})
	require.Equal(t, pb.PeerID(3), lead.leadTransferee)

	// Transfer leadership back to self.
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgTransferLeader})

	checkLeaderTransferState(t, lead, pb.StateLeader, 1)
}

// TestLeaderTransferSecondTransferToAnotherNode verifies leader can transfer to
// another node when last transfer is pending, which cancels the previous
// transfer attempt and starts a new one.
func TestLeaderTransferSecondTransferToAnotherNode(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	// Isolate node 3 and propose an entry on 1. This will cause node 3 to fall
	// behind on its log, so that the leadership transfer won't be initiated
	// immediately.
	nt.isolate(3)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{}}})

	lead := nt.peers[1].(*raft)

	nt.send(pb.Message{From: 3, To: 1, Type: pb.MsgTransferLeader})
	require.Equal(t, pb.PeerID(3), lead.leadTransferee)

	// Transfer leadership to another node.
	nt.send(pb.Message{From: 2, To: 1, Type: pb.MsgTransferLeader})

	checkLeaderTransferState(t, lead, pb.StateFollower, 2)
}

// TestLeaderTransferSecondTransferToSameNode verifies second transfer leader
// request to the same node should not extend the timeout while the first one is
// pending.
func TestLeaderTransferSecondTransferToSameNode(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	// Isolate node 3 and propose an entry on 1. This will cause node 3 to fall
	// behind on its log, so that the leadership transfer won't be initiated
	// immediately.
	nt.isolate(3)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{}}})

	lead := nt.peers[1].(*raft)

	nt.send(pb.Message{From: 3, To: 1, Type: pb.MsgTransferLeader})
	require.Equal(t, pb.PeerID(3), lead.leadTransferee)

	for i := int64(0); i < lead.heartbeatTimeout; i++ {
		lead.tick()
	}
	// Second transfer leadership request to the same node.
	nt.send(pb.Message{From: 3, To: 1, Type: pb.MsgTransferLeader})

	for i := int64(0); i < lead.electionTimeout-lead.heartbeatTimeout; i++ {
		lead.tick()
	}

	checkLeaderTransferState(t, lead, pb.StateLeader, 1)
}

func checkLeaderTransferState(t *testing.T, r *raft, state pb.StateType, lead pb.PeerID) {
	require.Equal(t, state, r.state)
	require.Equal(t, lead, r.lead)
	require.Equal(t, None, r.leadTransferee)
}

// TestLeaderTransferNonMember verifies that when a MsgTimeoutNow arrives at a
// node that has been removed from the group, nothing happens. (previously, if
// the node also got votes, it would panic as it transitioned to StateLeader).
func TestLeaderTransferNonMember(t *testing.T) {
	r := newTestRaft(1, 5, 1, newTestMemoryStorage(withPeers(2, 3, 4)))
	r.Step(pb.Message{From: 2, To: 1, Type: pb.MsgTimeoutNow})

	r.Step(pb.Message{From: 2, To: 1, Type: pb.MsgVoteResp})
	r.Step(pb.Message{From: 3, To: 1, Type: pb.MsgVoteResp})
	require.Equal(t, pb.StateFollower, r.state)
}

// TestLeaderTransferDifferentTerms verifies that a MsgTimeoutNow will only be
// respected if it is from the current term or from a new term.
func TestLeaderTransferDifferentTerms(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	// Transfer leadership to node 2, then 3, to drive up the term.
	nt.send(pb.Message{From: 2, To: 1, Type: pb.MsgTransferLeader})
	nt.send(pb.Message{From: 3, To: 2, Type: pb.MsgTransferLeader})
	for i, p := range nt.peers {
		r := p.(*raft)
		expState := pb.StateFollower
		if i == 3 {
			expState = pb.StateLeader
		}
		require.Equal(t, expState, r.state)
		require.Equal(t, uint64(3), r.Term)
	}

	// Send a MsgTimeoutNow to node 1 from an old term. This should be ignored.
	// This is important, as a MsgTimeoutNow allows a follower to call a "force"
	// election, which bypasses pre-vote and leader support safeguards. We don't
	// want a stale MsgTimeoutNow sent from an old leader giving a follower
	// permission to overthrow a newer leader.
	nt.send(pb.Message{From: 2, To: 1, Term: 2, Type: pb.MsgTimeoutNow})
	n1 := nt.peers[1].(*raft)
	require.Equal(t, pb.StateFollower, n1.state)
	require.Equal(t, uint64(3), n1.Term)

	// Send a MsgTimeoutNow to node 1 from the current term. This should cause it
	// to call an election for the _next_ term, which it will win.
	nt.send(pb.Message{From: 3, To: 1, Term: 3, Type: pb.MsgTimeoutNow})
	require.Equal(t, pb.StateLeader, n1.state)
	require.Equal(t, uint64(4), n1.Term)

	// Send a MsgTimeoutNow to node 2 from a new term. This should advance the
	// term on node 2 and cause it to call an election for the _next_ term, which
	// it will win.
	nt.send(pb.Message{From: 1, To: 2, Term: 5, Type: pb.MsgTimeoutNow})
	n2 := nt.peers[2].(*raft)
	require.Equal(t, pb.StateLeader, n2.state)
	require.Equal(t, uint64(6), n2.Term)
}

// TestLeaderTransferStaleFollower verifies that a MsgTimeoutNow received by a
// stale follower (a follower still at an earlier term) will cause the follower
// to call an election which it can not win.
func TestLeaderTransferStaleFollower(t *testing.T) {
	testutils.RunTrueAndFalse(t, "store-liveness-enabled",
		func(t *testing.T, storeLivenessEnabled bool) {
			testLeaderTransferStaleFollower(t, storeLivenessEnabled)
		})
}

func testLeaderTransferStaleFollower(t *testing.T, storeLivenessEnabled bool) {
	var fabric *raftstoreliveness.LivenessFabric
	var n1, n2, n3 *raft

	if storeLivenessEnabled {
		fabric = raftstoreliveness.NewLivenessFabricWithPeers(1, 2, 3)
		n1 = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(fabric.GetStoreLiveness(1)))
		n2 = newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(fabric.GetStoreLiveness(2)))
		n3 = newTestRaft(3, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(fabric.GetStoreLiveness(3)))
	} else {
		n1 = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(raftstoreliveness.Disabled{}))
		n2 = newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(raftstoreliveness.Disabled{}))
		n3 = newTestRaft(3, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(raftstoreliveness.Disabled{}))
	}

	nt := newNetworkWithConfigAndLivenessFabric(nil, fabric, n1, n2, n3)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})
	nodes := []*raft{n1, n2, n3}

	// Attempt to transfer leadership to node 3. The MsgTimeoutNow is sent
	// immediately and node 1 steps down as leader, but node 3 does not receive
	// the message due to a network partition.
	nt.isolate(3)

	nt.send(pb.Message{From: 3, To: 1, Type: pb.MsgTransferLeader})
	for _, n := range nodes {
		require.Equal(t, pb.StateFollower, n.state)
		require.Equal(t, uint64(1), n.Term)
	}

	// With leader leases, the ex-leader would send a MsgDefortifyLeader to
	// its followers when the support is expired.
	if storeLivenessEnabled {
		nt.livenessFabric.SetSupportExpired(1, true)
		n1.tick()
		nt.send(nt.filter(n1.readMessages())...)
		nt.livenessFabric.SetSupportExpired(1, false)
	}

	// Eventually, the previous leader gives up on waiting and calls an election
	// to reestablish leadership at the next term. Node 3 does not hear about this
	// either.
	for i := int64(0); i < n1.randomizedElectionTimeout; i++ {
		n1.tick()
	}
	nt.send(nt.filter(n1.readMessages())...)
	for _, n := range nodes {
		expState := pb.StateFollower
		if n == n1 {
			expState = pb.StateLeader
		}
		expTerm := uint64(2)
		if n == n3 {
			expTerm = 1
		}
		require.Equal(t, expState, n.state)
		require.Equal(t, expTerm, n.Term)
	}

	// The network partition heals and n3 receives the lost MsgTimeoutNow that n1
	// had previously tried to send to it back in term 1. It calls an unsuccessful
	// election, through which it learns about the new leadership term.
	nt.recover()
	nt.send(pb.Message{From: 1, To: 3, Term: 1, Type: pb.MsgTimeoutNow})
	for _, n := range nodes {
		expState := pb.StateFollower
		if n == n1 {
			expState = pb.StateLeader
		}
		require.Equal(t, expState, n.state)
		require.Equal(t, uint64(2), n.Term)
	}
}

// TestNodeWithSmallerTermCanCompleteElection tests the scenario where a node
// that has been partitioned away (and fallen behind) rejoins the cluster at
// about the same time the leader node gets partitioned away.
// Previously the cluster would come to a standstill when run with PreVote
// enabled.
func TestNodeWithSmallerTermCanCompleteElection(t *testing.T) {
	testutils.RunTrueAndFalse(t, "store-liveness-enabled",
		func(t *testing.T, storeLivenessEnabled bool) {
			testNodeWithSmallerTermCanCompleteElection(t, storeLivenessEnabled)
		})
}

func testNodeWithSmallerTermCanCompleteElection(t *testing.T, storeLivenessEnabled bool) {
	var fabric *raftstoreliveness.LivenessFabric
	var n1, n2, n3 *raft

	if storeLivenessEnabled {
		fabric = raftstoreliveness.NewLivenessFabricWithPeers(1, 2, 3)
		n1 = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(fabric.GetStoreLiveness(1)))
		n2 = newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(fabric.GetStoreLiveness(2)))
		n3 = newTestRaft(3, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(fabric.GetStoreLiveness(3)))
	} else {
		n1 = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(raftstoreliveness.Disabled{}))
		n2 = newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(raftstoreliveness.Disabled{}))
		n3 = newTestRaft(3, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(raftstoreliveness.Disabled{}))
	}

	n1.becomeFollower(1, None)
	n2.becomeFollower(1, None)
	n3.becomeFollower(1, None)

	n1.preVote = true
	n2.preVote = true
	n3.preVote = true

	// cause a network partition to isolate node 3
	nt := newNetworkWithConfigAndLivenessFabric(nil, fabric, n1, n2, n3)
	nt.cut(1, 3)
	nt.cut(2, 3)
	if storeLivenessEnabled {
		// We need to isolate node 3 in the store liveness layer as well.
		nt.livenessFabric.Isolate(3)
	}

	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	sm := nt.peers[1].(*raft)
	assert.Equal(t, pb.StateLeader, sm.state)

	sm = nt.peers[2].(*raft)
	assert.Equal(t, pb.StateFollower, sm.state)

	nt.send(pb.Message{From: 3, To: 3, Type: pb.MsgHup})
	sm = nt.peers[3].(*raft)
	if storeLivenessEnabled {
		// Since 3 isn't supported by a majority, it won't pre-campaign,
		assert.Equal(t, pb.StateFollower, sm.state)
	} else {
		assert.Equal(t, pb.StatePreCandidate, sm.state)
	}

	if storeLivenessEnabled {
		// Withdraw support from 1 so 2 can campaign and get elected.
		nt.livenessFabric.WithdrawSupportForPeerFromAllPeers(1)
	}
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
	assert.Equal(t, pb.StateFollower, sm.state)
	sm = nt.peers[2].(*raft)
	assert.Equal(t, pb.StateLeader, sm.state)
	sm = nt.peers[3].(*raft)
	if storeLivenessEnabled {
		// Since 3 wasn't supported by a majority, it didn't pre-campaign.
		assert.Equal(t, pb.StateFollower, sm.state)
	} else {
		assert.Equal(t, pb.StatePreCandidate, sm.state)
	}

	sm.logger.Infof("going to bring back peer 3 and kill peer 2")
	// recover the network then immediately isolate b which is currently
	// the leader, this is to emulate the crash of b.
	nt.recover()
	nt.cut(2, 1)
	nt.cut(2, 3)

	if storeLivenessEnabled {
		// Un-isolate 3 in store liveness as well.
		nt.livenessFabric.UnIsolate(3)

		// Re-grant support for 3 from all peers. This will allow 1 to campaign.
		nt.livenessFabric.GrantSupportForPeerFromAllPeers(1)

		// Isolate node 2 in store liveness as well.
		nt.livenessFabric.Isolate(2)
	}

	// call for election
	nt.send(pb.Message{From: 3, To: 3, Type: pb.MsgHup})
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	// do we have a leader?
	sma := nt.peers[1].(*raft)
	smb := nt.peers[3].(*raft)
	assert.True(t, sma.state == pb.StateLeader || smb.state == pb.StateLeader)
}

// TestPreVoteWithSplitVote verifies that after split vote, cluster can complete
// election in next round.
func TestPreVoteWithSplitVote(t *testing.T) {
	testutils.RunTrueAndFalse(t, "store-liveness-enabled",
		func(t *testing.T, storeLivenessEnabled bool) {
			testPreVoteWithSplitVote(t, storeLivenessEnabled)
		})
}

func testPreVoteWithSplitVote(t *testing.T, storeLivenessEnabled bool) {
	var fabric *raftstoreliveness.LivenessFabric
	var n1, n2, n3 *raft

	if storeLivenessEnabled {
		fabric = raftstoreliveness.NewLivenessFabricWithPeers(1, 2, 3)
		n1 = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(fabric.GetStoreLiveness(1)))
		n2 = newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(fabric.GetStoreLiveness(2)))
		n3 = newTestRaft(3, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(fabric.GetStoreLiveness(3)))
	} else {
		n1 = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(raftstoreliveness.Disabled{}))
		n2 = newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(raftstoreliveness.Disabled{}))
		n3 = newTestRaft(3, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(raftstoreliveness.Disabled{}))
	}

	n1.becomeFollower(1, None)
	n2.becomeFollower(1, None)
	n3.becomeFollower(1, None)

	n1.preVote = true
	n2.preVote = true
	n3.preVote = true

	nt := newNetworkWithConfigAndLivenessFabric(nil, fabric, n1, n2, n3)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	// simulate leader down. followers start split vote.
	nt.isolate(1)
	if storeLivenessEnabled {
		// We need to isolate 1 in the store liveness layer as well.
		nt.livenessFabric.Isolate(1)
	}

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
	assert.Equal(t, pb.StateCandidate, sm.state)
	sm = nt.peers[3].(*raft)
	assert.Equal(t, pb.StateCandidate, sm.state)

	// node 2 election timeout first
	nt.send(pb.Message{From: 2, To: 2, Type: pb.MsgHup})

	// check whether the term values are expected
	sm = nt.peers[2].(*raft)
	assert.Equal(t, uint64(4), sm.Term)
	sm = nt.peers[3].(*raft)
	assert.Equal(t, uint64(4), sm.Term)

	// check state
	sm = nt.peers[2].(*raft)
	assert.Equal(t, pb.StateLeader, sm.state)
	sm = nt.peers[3].(*raft)
	assert.Equal(t, pb.StateFollower, sm.state)
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
	require.Equal(t, pb.StateLeader, sm.state)

	sm = nt.peers[2].(*raft)
	require.Equal(t, pb.StateFollower, sm.state)

	sm = nt.peers[3].(*raft)
	require.Equal(t, pb.StateFollower, sm.state)

	// node 2 will ignore node 3's PreVote
	nt.send(pb.Message{From: 3, To: 3, Type: pb.MsgHup})
	nt.send(pb.Message{From: 2, To: 2, Type: pb.MsgHup})

	// Do we have a leader?
	assert.True(t, n2.state == pb.StateLeader || n3.state == pb.StateFollower)
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

	require.Equal(t, pb.StateFollower, n2.state)

	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})
	require.True(t, n1.state == pb.StateLeader && n1.lead == 1)

	// NB: TransferLeader already checks that the recipient is not a learner, but
	// the check could have happened by the time the recipient becomes a learner,
	// in which case it will receive MsgTimeoutNow as in this test case and we
	// verify that it's ignored.
	nt.send(pb.Message{From: 1, To: 2, Type: pb.MsgTimeoutNow})
	require.Equal(t, pb.StateFollower, n2.state)
}

// simulate rolling update a cluster for Pre-Vote. cluster has 3 nodes [n1, n2, n3].
// n1 is leader with term 2
// n2 is follower with term 2
// n3 is partitioned, with term 4 and less log, state is candidate
func newPreVoteMigrationCluster(
	t *testing.T, storeLivenessEnabled bool, fabric *raftstoreliveness.LivenessFabric,
) *network {
	var n1, n2, n3 *raft

	if storeLivenessEnabled {
		n1 = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(fabric.GetStoreLiveness(1)))
		n2 = newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(fabric.GetStoreLiveness(2)))
		n3 = newTestRaft(3, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(fabric.GetStoreLiveness(3)))
	} else {
		n1 = newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(raftstoreliveness.Disabled{}))
		n2 = newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(raftstoreliveness.Disabled{}))
		n3 = newTestRaft(3, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)),
			withStoreLiveness(raftstoreliveness.Disabled{}))
	}

	n1.becomeFollower(1, None)
	n2.becomeFollower(1, None)
	n3.becomeFollower(1, None)

	n1.preVote = true
	n2.preVote = true
	// We intentionally do not enable PreVote for n3, this is done so in order
	// to simulate a rolling restart process where it's possible to have a mixed
	// version cluster with replicas with PreVote enabled, and replicas without.

	nt := newNetworkWithConfigAndLivenessFabric(nil, fabric, n1, n2, n3)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	require.Equal(t, pb.StateLeader, n1.state)
	require.Equal(t, pb.StateFollower, n2.state)
	require.Equal(t, pb.StateFollower, n3.state)

	// Cause a network partition to isolate n3.
	nt.isolate(3)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("some data")}}})

	if storeLivenessEnabled {
		// We need to withdraw support from 1 before 3 can campaign and get elected.
		nt.livenessFabric.WithdrawSupportForPeerFromAllPeers(1)
	}

	nt.send(pb.Message{From: 3, To: 3, Type: pb.MsgHup})
	nt.send(pb.Message{From: 3, To: 3, Type: pb.MsgHup})

	// check state
	require.Equal(t, pb.StateLeader, n1.state)
	require.Equal(t, pb.StateFollower, n2.state)
	require.Equal(t, pb.StateCandidate, n3.state)

	// check term
	require.Equal(t, uint64(2), n1.Term)
	require.Equal(t, uint64(2), n2.Term)
	require.Equal(t, uint64(4), n3.Term)

	if storeLivenessEnabled {
		// Restore the liveness support state to return a working cluster with all
		// nodes having support.
		nt.livenessFabric.GrantSupportForPeerFromAllPeers(1)
	}

	// Enable prevote on n3, then recover the network
	n3.preVote = true
	nt.recover()

	return nt
}

func TestPreVoteMigrationCanCompleteElection(t *testing.T) {
	testutils.RunTrueAndFalse(t, "store-liveness-enabled",
		func(t *testing.T, storeLivenessEnabled bool) {
			testPreVoteMigrationCanCompleteElection(t, storeLivenessEnabled)
		})
}

func testPreVoteMigrationCanCompleteElection(t *testing.T, storeLivenessEnabled bool) {
	var fabric *raftstoreliveness.LivenessFabric

	if storeLivenessEnabled {
		fabric = raftstoreliveness.NewLivenessFabricWithPeers(1, 2, 3)
	}

	nt := newPreVoteMigrationCluster(t, storeLivenessEnabled, fabric)

	// n1 is leader with term 2
	// n2 is follower with term 2
	// n3 is pre-candidate with term 4, and less log
	n2 := nt.peers[2].(*raft)
	n3 := nt.peers[3].(*raft)

	// simulate leader down
	nt.isolate(1)

	if storeLivenessEnabled {
		// We need to withdraw support from 1 so 3 can campaign and get elected.
		nt.livenessFabric.WithdrawSupportForPeerFromAllPeers(1)
	}

	// Call for elections from both n2 and n3.
	nt.send(pb.Message{From: 3, To: 3, Type: pb.MsgHup})

	if storeLivenessEnabled {
		// We need to withdraw support from 3 so 2 can campaign and get elected.
		nt.livenessFabric.WithdrawSupportForPeerFromAllPeers(3)
	}

	nt.send(pb.Message{From: 2, To: 2, Type: pb.MsgHup})

	// check state
	assert.Equal(t, pb.StateFollower, n2.state)
	assert.Equal(t, pb.StatePreCandidate, n3.state)

	nt.send(pb.Message{From: 3, To: 3, Type: pb.MsgHup})
	nt.send(pb.Message{From: 2, To: 2, Type: pb.MsgHup})

	// Do we have a leader?
	assert.True(t, n2.state == pb.StateLeader || n3.state == pb.StateFollower)
}

func TestPreVoteMigrationWithFreeStuckPreCandidate(t *testing.T) {
	testutils.RunTrueAndFalse(t, "store-liveness-enabled",
		func(t *testing.T, storeLivenessEnabled bool) {
			testPreVoteMigrationWithFreeStuckPreCandidate(t, storeLivenessEnabled)
		})
}

func testPreVoteMigrationWithFreeStuckPreCandidate(t *testing.T, storeLivenessEnabled bool) {
	var fabric *raftstoreliveness.LivenessFabric

	if storeLivenessEnabled {
		fabric = raftstoreliveness.NewLivenessFabricWithPeers(1, 2, 3)
	}

	nt := newPreVoteMigrationCluster(t, storeLivenessEnabled, fabric)

	// n1 is leader with term 2
	// n2 is follower with term 2
	// n3 is pre-candidate with term 4, and less log
	n1 := nt.peers[1].(*raft)
	n2 := nt.peers[2].(*raft)
	n3 := nt.peers[3].(*raft)

	assert.Equal(t, pb.StateLeader, n1.state)
	if storeLivenessEnabled {
		assert.Equal(t, hlc.MaxTimestamp, getBasicStatus(n1).LeadSupportUntil)
	}

	if storeLivenessEnabled {
		// 1 needs to withdraw support for 3 before it can become a preCandidate.
		nt.livenessFabric.WithdrawSupport(1, 3)
	}

	nt.send(pb.Message{From: 3, To: 3, Type: pb.MsgHup})

	assert.Equal(t, pb.StateLeader, n1.state)
	assert.Equal(t, pb.StateFollower, n2.state)
	assert.Equal(t, pb.StatePreCandidate, n3.state)

	// Pre-Vote again for safety.
	nt.send(pb.Message{From: 3, To: 3, Type: pb.MsgHup})

	assert.Equal(t, pb.StateLeader, n1.state)
	assert.Equal(t, pb.StateFollower, n2.state)
	assert.Equal(t, pb.StatePreCandidate, n3.state)

	// If the stuck candidate were to talk to the follower, it may be ignored,
	// depending on whether the follower is fortified by the leader.
	nt.send(pb.Message{From: 3, To: 2, Type: pb.MsgAppResp, Term: n3.Term})
	if storeLivenessEnabled {
		assert.Equal(t, n3.Term-2, n2.Term)
	} else {
		assert.Equal(t, n3.Term, n2.Term)
	}

	// Disrupt the leader so that the stuck peer is freed. The leader steps down
	// immediately if it's not fortified. However, if it was fortified, it will
	// only step down when a quorum stops supporting it.
	hbType := pb.MsgHeartbeat
	if storeLivenessEnabled {
		hbType = pb.MsgFortifyLeader
	}
	nt.send(pb.Message{From: 1, To: 3, Type: hbType, Term: n1.Term})

	if storeLivenessEnabled {
		// Expect that we are still the leader since it's still not safe to step
		// down, however, the step-down intent is recorded.
		assert.Equal(t, pb.StateLeader, n1.state)
		assert.Equal(t, true, n1.fortificationTracker.SteppingDown())

		// The leader still hasn't defortified, so the stranded peer still can't
		// win an election.
		nt.send(pb.Message{From: 3, To: 3, Type: pb.MsgHup})
		assert.Equal(t, pb.StatePreCandidate, n3.state)
		assert.Equal(t, pb.StateLeader, n1.state)
		assert.Equal(t, n3.Term-2, n1.Term)
		assert.Equal(t, n1.id, n1.lead)

		// Expire the support, and tick it once. It should step down.
		nt.livenessFabric.SetSupportExpired(1, true)
		n1.tick()
	}

	assert.Equal(t, pb.StateFollower, n1.state)

	// Node 1 doesn't remember that it was the leader.
	assert.Equal(t, None, n1.lead)
	assert.Equal(t, n3.Term, n1.Term)

	// Return the support back to node 1 so that it can call an election.
	if storeLivenessEnabled {
		fabric.SetSupportExpired(1, false)
	}

	// The ex-leader calls an election, which it wins.
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})
	assert.Equal(t, pb.StateLeader, n1.state)
}

func testConfChangeCheckBeforeCampaign(t *testing.T, v2 bool, storeLivenessEnabled bool) {
	var cfg func(c *Config) = nil
	if !storeLivenessEnabled {
		cfg = fortificationDisabledConfig
	}

	nt := newNetworkWithConfig(cfg, nil, nil, nil)
	n1 := nt.peers[1].(*raft)
	n2 := nt.peers[2].(*raft)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})
	assert.Equal(t, pb.StateLeader, n1.state)

	// Begin to add a fourth node.
	cc := pb.ConfChange{
		Type:   pb.ConfChangeAddNode,
		NodeID: 4,
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

	if storeLivenessEnabled {
		// We need to withdraw support of the current leader to allow the new peer
		// to campaign and get elected.
		nt.livenessFabric.WithdrawSupportForPeerFromAllPeers(1)
	}

	// Trigger campaign in node 2
	for i := int64(0); i < n2.randomizedElectionTimeout; i++ {
		n2.tick()
	}
	// It's still follower because committed conf change is not applied.
	assert.Equal(t, pb.StateFollower, n2.state)

	// Transfer leadership to peer 2.
	nt.send(pb.Message{From: 2, To: 1, Type: pb.MsgTransferLeader})
	// The outgoing leader steps down immediately.
	assert.Equal(t, pb.StateFollower, n1.state)
	// The transfer target does not campaign immediately because the committed
	// conf change is not applied.
	assert.Equal(t, pb.StateFollower, n2.state)

	if storeLivenessEnabled {
		// Restore the support state.
		nt.livenessFabric.GrantSupportForPeerFromAllPeers(1)
	}

	// Advance apply on node 1 and re-establish leadership.
	nextEnts(n1, nt.storage[1])
	for i := int64(0); i < n1.randomizedElectionTimeout; i++ {
		n1.tick()
	}
	nt.send(n1.readMessages()...)
	assert.Equal(t, pb.StateLeader, n1.state)

	// Advance apply on node 2.
	nextEnts(n2, nt.storage[2])

	// Transfer leadership to peer 2 again.
	nt.send(pb.Message{From: 2, To: 1, Type: pb.MsgTransferLeader})

	// The outgoing leader steps down immediately.
	assert.Equal(t, pb.StateFollower, n1.state)
	// The transfer target campaigns immediately now that the committed conf
	// change is applied.
	assert.Equal(t, pb.StateLeader, n2.state)
}

// TestConfChangeCheckBeforeCampaign tests if unapplied ConfChange is checked before campaign.
func TestConfChangeCheckBeforeCampaign(t *testing.T) {
	testutils.RunTrueAndFalse(t, "store-liveness-enabled",
		func(t *testing.T, storeLivenessEnabled bool) {
			testConfChangeCheckBeforeCampaign(t, false, storeLivenessEnabled)
		})
}

// TestConfChangeV2CheckBeforeCampaign tests if unapplied ConfChangeV2 is checked before campaign.
func TestConfChangeV2CheckBeforeCampaign(t *testing.T) {
	testutils.RunTrueAndFalse(t, "store-liveness-enabled",
		func(t *testing.T, storeLivenessEnabled bool) {
			testConfChangeCheckBeforeCampaign(t, true, storeLivenessEnabled)
		})
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
			s1.snapshot.Metadata.ConfState = pb.ConfState{Voters: []pb.PeerID{1, 2, 3}}
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
			s2.snapshot.Metadata.ConfState = pb.ConfState{Voters: []pb.PeerID{1, 2, 3}}
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
func votedWithConfig(configFunc func(*Config), vote pb.PeerID, term uint64) *raft {
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
	r1.trk.Progress(2).BecomeReplicate()

	r2 := newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1, 2)))

	// r1 sends 2 MsgApp messages to r2.
	mustAppendEntry(r1, pb.Entry{Data: []byte("somedata")})
	r1.maybeSendAppend(2)
	req1 := expectOneMessage(t, r1)
	mustAppendEntry(r1, pb.Entry{Data: []byte("somedata")})
	r1.maybeSendAppend(2)
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
	require.Equal(t, uint64(2), r1.trk.Progress(2).Match)

	// r1 observes a transient network issue to r2, hence transits to probe state.
	r1.Step(pb.Message{From: 2, To: 1, Type: pb.MsgUnreachable})
	require.Equal(t, tracker.StateProbe, r1.trk.Progress(2).State)

	// now r1 receives the delayed resp2.
	r1.Step(resp2)
	m = expectOneMessage(t, r1)
	// r1 shall re-send MsgApp from match index even if resp2's reject hint is less than matching index.
	require.Equal(t, r1.trk.Progress(2).Match, m.Index)
}

func TestFortificationMetrics(t *testing.T) {
	fabric := raftstoreliveness.NewLivenessFabricWithPeers(1, 2, 3, 4)
	n1 := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3, 4)),
		withStoreLiveness(fabric.GetStoreLiveness(1)))
	n2 := newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3, 4)),
		withStoreLiveness(fabric.GetStoreLiveness(2)))
	n3 := newTestRaft(3, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3, 4)),
		withStoreLiveness(fabric.GetStoreLiveness(3)))
	n4 := newTestRaft(3, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3, 4)),
		withStoreLiveness(fabric.GetStoreLiveness(4)))

	nt := newNetworkWithConfigAndLivenessFabric(nil, fabric, n1, n2, n3, n4)

	// Withdraw 2's SupportFor() 1. This should cause 2 to reject the
	// fortification request.
	nt.livenessFabric.WithdrawSupportFor(2, 1)

	// Withdraw 1's SupportFrom() 3. This should cause 1 to skip sending the
	// fortification message to 3.
	nt.livenessFabric.WithdrawSupportFrom(1, 3)

	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	// The leader should receive an accepted MsgFortifyResp from itself and 4.
	require.Equal(t, int64(2), n1.metrics.AcceptedFortificationResponses.Count())
	// The leader should receive a rejected MsgFortifyResp from 2.
	require.Equal(t, int64(1), n1.metrics.RejectedFortificationResponses.Count())
	// The leader should skip sending a MsgFortify to 3.
	require.Equal(t, int64(1), n1.metrics.SkippedFortificationDueToLackOfSupport.Count())
}

func expectOneMessage(t *testing.T, r *raft) pb.Message {
	msgs := r.readMessages()
	require.Len(t, msgs, 1, "expect one message")
	return msgs[0]
}

type network struct {
	t *testing.T // optional

	peers   map[pb.PeerID]stateMachine
	storage map[pb.PeerID]*MemoryStorage
	dropm   map[connem]float64
	ignorem map[pb.MessageType]bool

	// msgHook is called for each message sent. It may inspect the
	// message and return true to send it or false to drop it.
	msgHook        func(pb.Message) bool
	livenessFabric *raftstoreliveness.LivenessFabric
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
	return newNetworkWithConfigAndLivenessFabric(configFunc, nil /* fabric */, peers...)
}

// newNetworkWithConfig is like newNetwork but calls the given func to
// modify the configuration of any state machines it creates and uses the store
// liveness fabric if provided.
func newNetworkWithConfigAndLivenessFabric(
	configFunc func(*Config), fabric *raftstoreliveness.LivenessFabric, peers ...stateMachine,
) *network {
	size := len(peers)
	peerAddrs := idsBySize(size)

	npeers := make(map[pb.PeerID]stateMachine, size)
	nstorage := make(map[pb.PeerID]*MemoryStorage, size)

	createNewFabric := fabric == nil

	if createNewFabric {
		fabric = raftstoreliveness.NewLivenessFabric()
		if createNewFabric {
			for j := range peers {
				id := peerAddrs[j]
				fabric.AddPeer(id)
			}
		}
	}

	for j, p := range peers {
		id := peerAddrs[j]
		switch v := p.(type) {
		case nil:
			nstorage[id] = newTestMemoryStorage(withPeers(peerAddrs...))
			cfg := newTestConfig(id, 10, 1, nstorage[id],
				withStoreLiveness(fabric.GetStoreLiveness(id)))
			if configFunc != nil {
				configFunc(cfg)
			}
			sm := newRaft(cfg)
			npeers[id] = sm
		case *raft:
			// TODO(tbg): this is all pretty confused. Clean this up.
			learners := make(map[pb.PeerID]bool, len(v.config.Learners))
			for i := range v.config.Learners {
				learners[i] = true
			}
			v.id = id
			v.trk = tracker.MakeProgressTracker(&v.config, tracker.MakeEmptyProgressMap())
			if len(learners) > 0 {
				v.config.Learners = map[pb.PeerID]struct{}{}
			}
			for i := 0; i < size; i++ {
				pr := &tracker.Progress{}
				if _, ok := learners[peerAddrs[i]]; ok {
					pr.IsLearner = true
					v.config.Learners[peerAddrs[i]] = struct{}{}
				} else {
					v.config.Voters[0][peerAddrs[i]] = struct{}{}
				}
				v.trk.TestingSetProgress(peerAddrs[i], pr)
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
		peers:          npeers,
		storage:        nstorage,
		dropm:          make(map[connem]float64),
		ignorem:        make(map[pb.MessageType]bool),
		livenessFabric: fabric,
	}
}

func preVoteConfig(c *Config) {
	c.PreVote = true
}

func fortificationDisabledConfig(c *Config) {
	c.StoreLiveness = raftstoreliveness.Disabled{}
}

func preVoteConfigWithFortificationDisabled(c *Config) {
	c.PreVote = true
	c.StoreLiveness = raftstoreliveness.Disabled{}
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

// tick takes a raft instance and calls tick(). It then uses the network.send
// function if that generates any messages.
func (nw *network) tick(p *raft) {
	p.tick()
	msgs := nw.filter(p.readMessages())
	if len(msgs) > 0 {
		nw.send(msgs...)
	}
}

func (nw *network) drop(from, to pb.PeerID, perc float64) {
	nw.dropm[connem{from, to}] = perc
}

func (nw *network) cut(one, other pb.PeerID) {
	nw.drop(one, other, 2.0) // always drop
	nw.drop(other, one, 2.0) // always drop
}

func (nw *network) isolate(id pb.PeerID) {
	for i := 0; i < len(nw.peers); i++ {
		nid := pb.PeerID(i + 1)
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
	from, to pb.PeerID
}

type blackHole struct{}

func (blackHole) Step(pb.Message) error       { return nil }
func (blackHole) readMessages() []pb.Message  { return nil }
func (blackHole) advanceMessagesAfterAppend() {}

var nopStepper = &blackHole{}

func idsBySize(size int) []pb.PeerID {
	ids := make([]pb.PeerID, size)
	for i := 0; i < size; i++ {
		ids[i] = pb.PeerID(1 + i)
	}
	return ids
}

// setRandomizedElectionTimeout set up the value by caller instead of choosing
// by system, in some test scenario we need to fill in some expected value to
// ensure the certainty
func setRandomizedElectionTimeout(r *raft, v int64) {
	r.randomizedElectionTimeout = v
}

// SetRandomizedElectionTimeout is like setRandomizedElectionTimeout, but
// exported for use by tests that are not in the raft package, using RawNode.
func SetRandomizedElectionTimeout(r *RawNode, v int64) {
	setRandomizedElectionTimeout(r.raft, v)
}

// testConfigModifiers allows callers to optionally modify newTestConfig.
type testConfigModifiers struct {
	testingStoreLiveness raftstoreliveness.StoreLiveness
	testingLogger        raftlogger.Logger
}

// testConfigModifierOpt is the type of an optional parameter to newTestConfig
// that may be used to modify the config.
type testConfigModifierOpt func(*testConfigModifiers)

// emptyTestConfigModifierOpt returns an empty testConfigModifierOpt.
func emptyTestConfigModifierOpt() testConfigModifierOpt {
	return func(modifier *testConfigModifiers) {}
}

// withStoreLiveness explicitly uses the supplied StoreLiveness implementation.
func withStoreLiveness(storeLiveness raftstoreliveness.StoreLiveness) testConfigModifierOpt {
	return func(modifier *testConfigModifiers) {
		modifier.testingStoreLiveness = storeLiveness
	}
}

// withLogger explicitly uses the supplied raft logger.
func withLogger(logger raftlogger.Logger) testConfigModifierOpt {
	return func(modifier *testConfigModifiers) {
		modifier.testingLogger = logger
	}
}

func newTestConfig(
	id pb.PeerID, election, heartbeat int64, storage Storage, opts ...testConfigModifierOpt,
) *Config {
	modifiers := testConfigModifiers{}
	for _, opt := range opts {
		opt(&modifiers)
	}
	var storeLiveness raftstoreliveness.StoreLiveness
	if modifiers.testingStoreLiveness != nil {
		storeLiveness = modifiers.testingStoreLiveness
	} else {
		storeLiveness = raftstoreliveness.AlwaysLive{}
	}

	var logger raftlogger.Logger
	if modifiers.testingLogger != nil {
		logger = modifiers.testingLogger
	} else {
		logger = raftlogger.DefaultRaftLogger
	}

	return &Config{
		ID:              id,
		ElectionTick:    election,
		HeartbeatTick:   heartbeat,
		Storage:         storage,
		MaxSizePerMsg:   noLimit,
		MaxInflightMsgs: 256,
		StoreLiveness:   storeLiveness,
		Logger:          logger,
		CRDBVersion:     cluster.MakeTestingClusterSettings().Version,
		Metrics:         NewMetrics(),
	}
}

type testMemoryStorageOptions func(*MemoryStorage)

func withPeers(peers ...pb.PeerID) testMemoryStorageOptions {
	return func(ms *MemoryStorage) {
		ms.snapshot.Metadata.ConfState.Voters = peers
	}
}

func withLearners(learners ...pb.PeerID) testMemoryStorageOptions {
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

func newTestRaft(
	id pb.PeerID, election, heartbeat int64, storage Storage, opts ...testConfigModifierOpt,
) *raft {
	return newRaft(newTestConfig(id, election, heartbeat, storage, opts...))
}

func newTestLearnerRaft(
	id pb.PeerID, election, heartbeat int64, storage Storage, opts ...testConfigModifierOpt,
) *raft {
	cfg := newTestConfig(id, election, heartbeat, storage, opts...)
	return newRaft(cfg)
}

// newTestRawNode sets up a RawNode with the given peers. The configuration will
// not be reflected in the Storage.
func newTestRawNode(id pb.PeerID, election, heartbeat int64, storage Storage) *RawNode {
	cfg := newTestConfig(id, election, heartbeat, storage)
	rn, err := NewRawNode(cfg)
	if err != nil {
		panic(err)
	}
	return rn
}
