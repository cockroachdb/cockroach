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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/raft/quorum"
	"github.com/cockroachdb/cockroach/pkg/raft/raftlogger"
	pb "github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/raft/raftstoreliveness"
	"github.com/cockroachdb/cockroach/pkg/raft/tracker"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRawNodeStep ensures that RawNode.Step ignore local message.
func TestRawNodeStep(t *testing.T) {
	for i, msgn := range pb.MessageType_name {
		t.Run(msgn, func(t *testing.T) {
			s := NewMemoryStorage()
			s.SetHardState(pb.HardState{Term: 1, Commit: 1})
			s.Append([]pb.Entry{{Term: 1, Index: 1}})
			require.NoError(t, s.ApplySnapshot(pb.Snapshot{Metadata: pb.SnapshotMetadata{
				ConfState: pb.ConfState{
					Voters: []pb.PeerID{1},
				},
				Index: 1,
				Term:  1,
			}}), "#%d", i)
			// Append an empty entry to make sure the non-local messages (like
			// vote requests) are ignored and don't trigger assertions.
			rawNode, err := NewRawNode(newTestConfig(1, 10, 1, s))
			require.NoError(t, err, "#%d", i)
			msgt := pb.MessageType(i)
			err = rawNode.Step(pb.Message{Type: msgt, From: 2})
			// LocalMsg should be ignored.
			if IsLocalMsg(msgt) {
				assert.Equal(t, ErrStepLocalMsg, err, "#%d", i)
			}
		})
	}
}

// TestNodeStepUnblock from node_test.go has no equivalent in rawNode because there is
// no goroutine in RawNode.

// TestRawNodeProposeAndConfChange tests the configuration change mechanism. Each
// test case sends a configuration change which is either simple or joint, verifies
// that it applies and that the resulting ConfState matches expectations, and for
// joint configurations makes sure that they are exited successfully.
func TestRawNodeProposeAndConfChange(t *testing.T) {
	testCases := []struct {
		cc   pb.ConfChangeI
		exp  pb.ConfState
		exp2 *pb.ConfState
	}{
		// V1 config change.
		{
			pb.ConfChange{Type: pb.ConfChangeAddNode, NodeID: 2},
			pb.ConfState{Voters: []pb.PeerID{1, 2}},
			nil,
		},
		// Proposing the same as a V2 change works just the same, without entering
		// a joint config.
		{
			pb.ConfChangeV2{Changes: []pb.ConfChangeSingle{
				{Type: pb.ConfChangeAddNode, NodeID: 2},
			},
			},
			pb.ConfState{Voters: []pb.PeerID{1, 2}},
			nil,
		},
		// Ditto if we add it as a learner instead.
		{
			pb.ConfChangeV2{Changes: []pb.ConfChangeSingle{
				{Type: pb.ConfChangeAddLearnerNode, NodeID: 2},
			},
			},
			pb.ConfState{Voters: []pb.PeerID{1}, Learners: []pb.PeerID{2}},
			nil,
		},
		// We can ask explicitly for joint consensus if we want it.
		{
			pb.ConfChangeV2{Changes: []pb.ConfChangeSingle{
				{Type: pb.ConfChangeAddLearnerNode, NodeID: 2},
			},
				Transition: pb.ConfChangeTransitionJointExplicit,
			},
			pb.ConfState{Voters: []pb.PeerID{1}, VotersOutgoing: []pb.PeerID{1}, Learners: []pb.PeerID{2}},
			&pb.ConfState{Voters: []pb.PeerID{1}, Learners: []pb.PeerID{2}},
		},
		// Ditto, but with implicit transition (the harness checks this).
		{
			pb.ConfChangeV2{Changes: []pb.ConfChangeSingle{
				{Type: pb.ConfChangeAddLearnerNode, NodeID: 2},
			},
				Transition: pb.ConfChangeTransitionJointImplicit,
			},
			pb.ConfState{
				Voters: []pb.PeerID{1}, VotersOutgoing: []pb.PeerID{1}, Learners: []pb.PeerID{2},
				AutoLeave: true,
			},
			&pb.ConfState{Voters: []pb.PeerID{1}, Learners: []pb.PeerID{2}},
		},
		// Add a new node and demote n1. This exercises the interesting case in
		// which we really need joint config changes and also need LearnersNext.
		{
			pb.ConfChangeV2{Changes: []pb.ConfChangeSingle{
				{NodeID: 2, Type: pb.ConfChangeAddNode},
				{NodeID: 1, Type: pb.ConfChangeAddLearnerNode},
				{NodeID: 3, Type: pb.ConfChangeAddLearnerNode},
			},
			},
			pb.ConfState{
				Voters:         []pb.PeerID{2},
				VotersOutgoing: []pb.PeerID{1},
				Learners:       []pb.PeerID{3},
				LearnersNext:   []pb.PeerID{1},
				AutoLeave:      true,
			},
			&pb.ConfState{Voters: []pb.PeerID{2}, Learners: []pb.PeerID{1, 3}},
		},
		// Ditto explicit.
		{
			pb.ConfChangeV2{Changes: []pb.ConfChangeSingle{
				{NodeID: 2, Type: pb.ConfChangeAddNode},
				{NodeID: 1, Type: pb.ConfChangeAddLearnerNode},
				{NodeID: 3, Type: pb.ConfChangeAddLearnerNode},
			},
				Transition: pb.ConfChangeTransitionJointExplicit,
			},
			pb.ConfState{
				Voters:         []pb.PeerID{2},
				VotersOutgoing: []pb.PeerID{1},
				Learners:       []pb.PeerID{3},
				LearnersNext:   []pb.PeerID{1},
			},
			&pb.ConfState{Voters: []pb.PeerID{2}, Learners: []pb.PeerID{1, 3}},
		},
		// Ditto implicit.
		{
			pb.ConfChangeV2{
				Changes: []pb.ConfChangeSingle{
					{NodeID: 2, Type: pb.ConfChangeAddNode},
					{NodeID: 1, Type: pb.ConfChangeAddLearnerNode},
					{NodeID: 3, Type: pb.ConfChangeAddLearnerNode},
				},
				Transition: pb.ConfChangeTransitionJointImplicit,
			},
			pb.ConfState{
				Voters:         []pb.PeerID{2},
				VotersOutgoing: []pb.PeerID{1},
				Learners:       []pb.PeerID{3},
				LearnersNext:   []pb.PeerID{1},
				AutoLeave:      true,
			},
			&pb.ConfState{Voters: []pb.PeerID{2}, Learners: []pb.PeerID{1, 3}},
		},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			s := newTestMemoryStorage(withPeers(1))
			rawNode, err := NewRawNode(newTestConfig(1, 10, 1, s))
			require.NoError(t, err)

			rawNode.Campaign()
			proposed := false
			var (
				lastIndex uint64
				ccdata    []byte
			)
			// Propose the ConfChange, wait until it applies, save the resulting
			// ConfState.
			var cs *pb.ConfState
			for cs == nil {
				rd := rawNode.Ready()
				s.Append(rd.Entries)
				for _, ent := range rd.CommittedEntries {
					var cc pb.ConfChangeI
					if ent.Type == pb.EntryConfChange {
						var ccc pb.ConfChange
						require.NoError(t, ccc.Unmarshal(ent.Data))
						cc = ccc
					} else if ent.Type == pb.EntryConfChangeV2 {
						var ccc pb.ConfChangeV2
						require.NoError(t, ccc.Unmarshal(ent.Data))
						cc = ccc
					}
					if cc != nil {
						cs = rawNode.ApplyConfChange(cc)
					}
				}
				rawNode.Advance(rd)
				// Once we are the leader, propose a command and a ConfChange.
				if !proposed && rd.HardState.Lead == rawNode.raft.id {
					require.NoError(t, rawNode.Propose([]byte("somedata")))
					if ccv1, ok := tc.cc.AsV1(); ok {
						ccdata, err = ccv1.Marshal()
						require.NoError(t, err)
						rawNode.ProposeConfChange(ccv1)
					} else {
						ccv2 := tc.cc.AsV2()
						ccdata, err = ccv2.Marshal()
						require.NoError(t, err)
						rawNode.ProposeConfChange(ccv2)
					}
					proposed = true
				}
			}

			// Check that the last index is exactly the conf change we put in,
			// down to the bits. Note that this comes from the Storage, which
			// will not reflect any unstable entries that we'll only be presented
			// with in the next Ready.
			lastIndex = s.LastIndex()
			entries, err := s.Entries(lastIndex-1, lastIndex+1, noLimit)
			require.NoError(t, err)
			require.Len(t, entries, 2)
			assert.Equal(t, []byte("somedata"), entries[0].Data)

			typ := pb.EntryConfChange
			if _, ok := tc.cc.AsV1(); !ok {
				typ = pb.EntryConfChangeV2
			}
			require.Equal(t, typ, entries[1].Type)
			assert.Equal(t, ccdata, entries[1].Data)

			require.Equal(t, &tc.exp, cs)

			var maybePlusOne uint64
			if autoLeave, ok := tc.cc.AsV2().EnterJoint(); ok && autoLeave {
				// If this is an auto-leaving joint conf change, it will have
				// appended the entry that auto-leaves, so add one to the last
				// index that forms the basis of our expectations on
				// pendingConfIndex. (Recall that lastIndex was taken from stable
				// storage, but this auto-leaving entry isn't on stable storage
				// yet).
				maybePlusOne = 1
			}
			require.Equal(t, lastIndex+maybePlusOne, rawNode.raft.pendingConfIndex)

			// Move the RawNode along. If the ConfChange was simple, nothing else
			// should happen. Otherwise, we're in a joint state, which is either
			// left automatically or not. If not, we add the proposal that leaves
			// it manually.
			rd := rawNode.Ready()
			var context []byte
			if !tc.exp.AutoLeave {
				require.Empty(t, rd.Entries)
				rawNode.Advance(rd)
				if tc.exp2 == nil {
					return
				}
				context = []byte("manual")
				t.Log("leaving joint state manually")
				require.NoError(t, rawNode.ProposeConfChange(pb.ConfChangeV2{Context: context}))
				rd = rawNode.Ready()
			}

			// Check that the right ConfChange comes out.
			require.Len(t, rd.Entries, 1)
			require.Equal(t, pb.EntryConfChangeV2, rd.Entries[0].Type)
			var cc pb.ConfChangeV2
			require.NoError(t, cc.Unmarshal(rd.Entries[0].Data))
			require.Equal(t, pb.ConfChangeV2{Context: context}, cc)

			// Lie and pretend the ConfChange applied. It won't do so because now
			// we require the joint quorum and we're only running one node.
			cs = rawNode.ApplyConfChange(cc)
			require.Equal(t, tc.exp2, cs)

			rawNode.Advance(rd)
		})
	}
}

// TestRawNodeJointAutoLeave tests the configuration change auto leave even leader
// lost leadership.
func TestRawNodeJointAutoLeave(t *testing.T) {
	testutils.RunTrueAndFalse(t, "store-liveness-enabled",
		func(t *testing.T, storeLivenessEnabled bool) {
			testRawNodeJointAutoLeave(t, storeLivenessEnabled)
		})
}

func testRawNodeJointAutoLeave(t *testing.T, storeLivenessEnabled bool) {
	testCc := pb.ConfChangeV2{Changes: []pb.ConfChangeSingle{
		{Type: pb.ConfChangeAddLearnerNode, NodeID: 2},
	},
		Transition: pb.ConfChangeTransitionJointImplicit,
	}
	expCs := pb.ConfState{
		Voters: []pb.PeerID{1}, VotersOutgoing: []pb.PeerID{1}, Learners: []pb.PeerID{2},
		AutoLeave: true,
	}
	exp2Cs := pb.ConfState{Voters: []pb.PeerID{1}, Learners: []pb.PeerID{2}}

	s := newTestMemoryStorage(withPeers(1))

	var fabric *raftstoreliveness.LivenessFabric
	var rawNode *RawNode
	var err error
	if storeLivenessEnabled {
		fabric = raftstoreliveness.NewLivenessFabricWithPeers(1, 2, 3)
		rawNode, err = NewRawNode(newTestConfig(1, 10, 1, s,
			withStoreLiveness(fabric.GetStoreLiveness(1))))
	} else {
		rawNode, err = NewRawNode(newTestConfig(1, 10, 1, s,
			withStoreLiveness(raftstoreliveness.Disabled{})))
	}

	require.NoError(t, err)

	rawNode.Campaign()
	proposed := false
	var (
		lastIndex uint64
		ccdata    []byte
	)
	// Propose the ConfChange, wait until it applies, save the resulting
	// ConfState.
	var cs *pb.ConfState
	for cs == nil {
		rd := rawNode.Ready()
		s.Append(rd.Entries)
		for _, ent := range rd.CommittedEntries {
			var cc pb.ConfChangeI
			if ent.Type == pb.EntryConfChangeV2 {
				var ccc pb.ConfChangeV2
				require.NoError(t, ccc.Unmarshal(ent.Data))
				cc = &ccc
			}
			if cc != nil {
				// Force it to step down.
				rawNode.Step(pb.Message{Type: pb.MsgHeartbeatResp, From: 1, Term: rawNode.raft.Term + 1})
				require.Equal(t, pb.StateFollower, rawNode.raft.state)
				if storeLivenessEnabled {
					// And also wait for defortification.
					for range rawNode.raft.heartbeatTimeout {
						rawNode.Tick()
					}
				}
				cs = rawNode.ApplyConfChange(cc)
			}
		}
		rawNode.Advance(rd)
		// Once we are the leader, propose a command and a ConfChange.
		if !proposed && rawNode.raft.state == pb.StateLeader {
			require.NoError(t, rawNode.Propose([]byte("somedata")))
			ccdata, err = testCc.Marshal()
			require.NoError(t, err)
			require.NoError(t, rawNode.ProposeConfChange(testCc))
			proposed = true
		}
	}

	// Check that the last index is exactly the conf change we put in,
	// down to the bits. Note that this comes from the Storage, which
	// will not reflect any unstable entries that we'll only be presented
	// with in the next Ready.
	lastIndex = s.LastIndex()
	entries, err := s.Entries(lastIndex-1, lastIndex+1, noLimit)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	assert.Equal(t, []byte("somedata"), entries[0].Data)
	require.Equal(t, pb.EntryConfChangeV2, entries[1].Type)
	assert.Equal(t, ccdata, entries[1].Data)

	require.Equal(t, &expCs, cs)

	require.Zero(t, rawNode.raft.pendingConfIndex)

	// Move the RawNode along. It should not leave joint because it's follower.
	rd := rawNode.readyWithoutAccept()
	// Check that the right ConfChange comes out.
	require.Empty(t, rd.Entries)

	// Make it leader again. It should leave joint automatically after moving apply index.
	rawNode.Campaign()
	rd = rawNode.Ready()
	t.Log(DescribeReady(rd, nil))
	s.Append(rd.Entries)
	rawNode.Advance(rd)
	rd = rawNode.Ready()
	t.Log(DescribeReady(rd, nil))
	s.Append(rd.Entries)
	rawNode.Advance(rd)
	rd = rawNode.Ready()
	t.Log(DescribeReady(rd, nil))
	s.Append(rd.Entries)
	rawNode.Advance(rd)
	rd = rawNode.Ready()
	t.Log(DescribeReady(rd, nil))
	s.Append(rd.Entries)
	// Check that the right ConfChange comes out.
	require.Len(t, rd.Entries, 1)
	require.Equal(t, pb.EntryConfChangeV2, rd.Entries[0].Type)
	var cc pb.ConfChangeV2
	require.NoError(t, cc.Unmarshal(rd.Entries[0].Data))
	require.Equal(t, pb.ConfChangeV2{Context: nil}, cc)
	// Lie and pretend the ConfChange applied. It won't do so because now
	// we require the joint quorum and we're only running one node.
	cs = rawNode.ApplyConfChange(cc)
	require.Equal(t, exp2Cs, *cs)
}

// TestRawNodeProposeAddDuplicateNode ensures that two proposes to add the same node should
// not affect the later propose to add new node.
func TestRawNodeProposeAddDuplicateNode(t *testing.T) {
	s := newTestMemoryStorage(withPeers(1))
	rawNode, err := NewRawNode(newTestConfig(1, 10, 1, s))
	require.NoError(t, err)

	rd := rawNode.Ready()
	s.Append(rd.Entries)
	rawNode.Advance(rd)

	rawNode.Campaign()
	for {
		rd = rawNode.Ready()
		s.Append(rd.Entries)
		if rd.HardState.Lead == rawNode.raft.id {
			rawNode.Advance(rd)
			break
		}
		rawNode.Advance(rd)
	}

	proposeConfChangeAndApply := func(cc pb.ConfChange) {
		rawNode.ProposeConfChange(cc)
		rd = rawNode.Ready()
		s.Append(rd.Entries)
		for _, entry := range rd.CommittedEntries {
			if entry.Type == pb.EntryConfChange {
				var cc pb.ConfChange
				cc.Unmarshal(entry.Data)
				rawNode.ApplyConfChange(cc)
			}
		}
		rawNode.Advance(rd)
	}

	cc1 := pb.ConfChange{Type: pb.ConfChangeAddNode, NodeID: 1}
	ccdata1, err := cc1.Marshal()
	require.NoError(t, err)
	proposeConfChangeAndApply(cc1)

	// try to add the same node again
	proposeConfChangeAndApply(cc1)

	// the new node join should be ok
	cc2 := pb.ConfChange{Type: pb.ConfChangeAddNode, NodeID: 2}
	ccdata2, err := cc2.Marshal()
	require.NoError(t, err)
	proposeConfChangeAndApply(cc2)

	lastIndex := s.LastIndex()
	// the last three entries should be: ConfChange cc1, cc1, cc2
	entries, err := s.Entries(lastIndex-2, lastIndex+1, noLimit)
	require.NoError(t, err)
	require.Len(t, entries, 3)
	assert.Equal(t, ccdata1, entries[0].Data)
	assert.Equal(t, ccdata2, entries[2].Data)
}

// TestBlockProposal from node_test.go has no equivalent in rawNode because there is
// no leader check in RawNode.

// TestNodeTick from node_test.go has no equivalent in rawNode because
// it reaches into the raft object which is not exposed.

// TestNodeStop from node_test.go has no equivalent in rawNode because there is
// no goroutine in RawNode.

// TestRawNodeStart ensures that a node can be started correctly. Note that RawNode
// requires the application to bootstrap the state, i.e. it does not accept peers
// and will not create faux configuration change entries.
func TestRawNodeStart(t *testing.T) {
	entries := []pb.Entry{
		{Term: 1, Index: 2, Data: nil},           // empty entry
		{Term: 1, Index: 3, Data: []byte("foo")}, // non-empty entry
	}
	want := Ready{
		SoftState:        &SoftState{RaftState: pb.StateLeader},
		HardState:        pb.HardState{Term: 1, Commit: 3, Vote: 1, Lead: 1, LeadEpoch: 1},
		Entries:          nil, // emitted & checked in intermediate Ready cycle
		CommittedEntries: entries,
		MustSync:         true, // because we are advancing the commit index
	}

	storage := NewMemoryStorage()
	storage.ls = LogSlice{term: 1, prev: entryID{index: 1, term: 1}}

	// TODO(tbg): this is a first prototype of what bootstrapping could look
	// like (without the annoying faux ConfChanges). We want to persist a
	// ConfState at some index and make sure that this index can't be reached
	// from log position 1, so that followers are forced to pick up the
	// ConfState in order to move away from log position 1 (unless they got
	// bootstrapped in the same way already). Failing to do so would mean that
	// followers diverge from the bootstrapped nodes and don't learn about the
	// initial config.
	//
	// NB: this is exactly what CockroachDB does. The Raft log really begins at
	// index 10, so empty followers (at index 1) always need a snapshot first.
	type appenderStorage interface {
		Storage
		ApplySnapshot(pb.Snapshot) error
	}
	bootstrap := func(storage appenderStorage, cs pb.ConfState) error {
		require.NotEmpty(t, cs.Voters, "no voters specified")
		fi, li := storage.FirstIndex(), storage.LastIndex()
		require.GreaterOrEqual(t, fi, uint64(2), "FirstIndex >= 2 is prerequisite for bootstrap")
		require.Equal(t, fi, li+1, "the log must be empty")

		entries, err := storage.Entries(fi, li+1, math.MaxUint64)
		require.NoError(t, err)
		require.Empty(t, entries, "should not have been able to load any entries")

		hs, ics, err := storage.InitialState()
		require.NoError(t, err)
		require.True(t, IsEmptyHardState(hs))
		require.Empty(t, ics.Voters)

		meta := pb.SnapshotMetadata{
			Index:     1,
			Term:      0,
			ConfState: cs,
		}
		snap := pb.Snapshot{Metadata: meta}
		return storage.ApplySnapshot(snap)
	}

	require.NoError(t, bootstrap(storage, pb.ConfState{Voters: []pb.PeerID{1}}))

	rawNode, err := NewRawNode(newTestConfig(1, 10, 1, storage))
	require.NoError(t, err)
	require.False(t, rawNode.HasReady())

	rawNode.Campaign()
	rd := rawNode.Ready()
	storage.Append(rd.Entries)
	rawNode.Advance(rd)
	rawNode.Propose([]byte("foo"))
	require.True(t, rawNode.HasReady())

	rd = rawNode.Ready()
	require.Equal(t, entries, rd.Entries)
	storage.Append(rd.Entries)
	rawNode.Advance(rd)

	require.True(t, rawNode.HasReady())
	rd = rawNode.Ready()
	require.Empty(t, rd.Entries)
	require.True(t, rd.MustSync)
	rawNode.Advance(rd)

	rd.SoftState, want.SoftState = nil, nil

	require.Equal(t, want, rd)
	assert.False(t, rawNode.HasReady())
}

func TestRawNodeRestart(t *testing.T) {
	entries := []pb.Entry{
		{Term: 1, Index: 1},
		{Term: 1, Index: 2, Data: []byte("foo")},
	}
	st := pb.HardState{Term: 1, Commit: 1, Lead: 1, LeadEpoch: 1}

	want := Ready{
		HardState: emptyState, // no HardState is emitted because there was no change
		// commit up to commit index in st
		CommittedEntries: entries[:st.Commit],
		MustSync:         false,
	}

	storage := newTestMemoryStorage(withPeers(1, 2))
	storage.SetHardState(st)
	storage.Append(entries)
	rawNode1, err := NewRawNode(newTestConfig(1, 10, 1, storage))
	require.NoError(t, err)
	rawNode2, err := NewRawNode(newTestConfig(2, 10, 1, storage))
	require.NoError(t, err)
	rd := rawNode1.Ready()
	assert.Equal(t, want, rd)
	rawNode1.Advance(rd)
	assert.False(t, rawNode1.HasReady())

	// Ensure that the HardState was correctly loaded post rawNode1 restart.
	assert.Equal(t, uint64(1), rawNode1.raft.Term)
	assert.Equal(t, uint64(1), rawNode1.raft.raftLog.committed)
	assert.True(t, rawNode1.raft.state == pb.StateFollower)
	// Since rawNode1 was the leader, it should become a follower while forgetting
	// that it was the leader/leadEpoch was for this term.
	assert.Equal(t, None, rawNode1.raft.lead)
	assert.Equal(t, pb.Epoch(0), rawNode1.raft.leadEpoch)

	// Ensure that the HardState was correctly loaded post rawNode2 restart.
	assert.Equal(t, uint64(1), rawNode2.raft.Term)
	assert.Equal(t, uint64(1), rawNode2.raft.raftLog.committed)
	assert.True(t, rawNode2.raft.state == pb.StateFollower)
	// Since rawNode2 was a follower, it should remember who the leader was, and
	// what the leadEpoch was.
	assert.Equal(t, pb.PeerID(1), rawNode2.raft.lead)
	assert.Equal(t, pb.Epoch(1), rawNode2.raft.leadEpoch)

	// Ensure we campaign after the election timeout has elapsed.
	for i := int64(0); i < rawNode1.raft.randomizedElectionTimeout; i++ {
		// TODO(arul): consider getting rid of this hack to reset the epoch so that
		// we can call an election without panicking.
		rawNode1.raft.leadEpoch = 0
		rawNode1.raft.tick()
	}
	assert.Equal(t, pb.StateCandidate, rawNode1.raft.state)
	assert.Equal(t, uint64(2), rawNode1.raft.Term) // this should in-turn bump the term
}

func TestRawNodeRestartFromSnapshot(t *testing.T) {
	snap := pb.Snapshot{
		Metadata: pb.SnapshotMetadata{
			ConfState: pb.ConfState{Voters: []pb.PeerID{1, 2}},
			Index:     2,
			Term:      1,
		},
	}
	entries := []pb.Entry{
		{Term: 1, Index: 3, Data: []byte("foo")},
	}
	st := pb.HardState{Term: 1, Commit: 3}

	want := Ready{
		HardState: emptyState,
		// commit up to commit index in st
		CommittedEntries: entries,
		MustSync:         false,
	}

	s := NewMemoryStorage()
	s.SetHardState(st)
	s.ApplySnapshot(snap)
	s.Append(entries)
	rawNode, err := NewRawNode(newTestConfig(1, 10, 1, s))
	require.NoError(t, err)
	rd := rawNode.Ready()
	if assert.Equal(t, want, rd) {
		rawNode.Advance(rd)
	}
	assert.False(t, rawNode.HasReady())
}

// TestNodeAdvance from node_test.go has no equivalent in rawNode because there is
// no dependency check between Ready() and Advance()
func TestRawNodeStatus(t *testing.T) {
	s := newTestMemoryStorage(withPeers(1))
	rn, err := NewRawNode(newTestConfig(1, 10, 1, s))
	require.NoError(t, err)
	require.Nil(t, rn.Status().Progress)
	require.NoError(t, rn.Campaign())

	rd := rn.Ready()
	s.Append(rd.Entries)
	rn.Advance(rd)
	status := rn.Status()
	require.Equal(t, pb.PeerID(1), status.Lead)
	require.Equal(t, pb.StateLeader, status.RaftState)
	require.Equal(t, *rn.raft.trk.Progress(1), status.Progress[1])

	expCfg := quorum.Config{Voters: quorum.JointConfig{
		quorum.MajorityConfig{1: {}},
		nil,
	}}
	require.Equal(t, expCfg, status.Config)
}

// TestRawNodeCommitPaginationAfterRestart is the RawNode version of
// TestNodeCommitPaginationAfterRestart. The anomaly here was even worse as the
// Raft group would forget to apply entries:
//
//   - node learns that index 11 is committed
//   - nextCommittedEnts returns index 1..10 in CommittedEntries (but index 10
//     already exceeds maxBytes), which isn't noticed internally by Raft
//   - Commit index gets bumped to 10
//   - the node persists the HardState, but crashes before applying the entries
//   - upon restart, the storage returns the same entries, but `slice` takes a
//     different code path and removes the last entry.
//   - Raft does not emit a HardState, but when the app calls Advance(), it bumps
//     its internal applied index cursor to 10 (when it should be 9)
//   - the next Ready asks the app to apply index 11 (omitting index 10), losing a
//     write.
func TestRawNodeCommitPaginationAfterRestart(t *testing.T) {
	s := &ignoreSizeHintMemStorage{
		MemoryStorage: newTestMemoryStorage(withPeers(1)),
	}
	s.hardState = pb.HardState{
		Term:   1,
		Vote:   1,
		Commit: 10,
	}
	entries := make([]pb.Entry, 10)
	var size uint64
	for i := range entries {
		ent := pb.Entry{
			Term:  1,
			Index: uint64(i + 1),
			Type:  pb.EntryNormal,
			Data:  []byte("a"),
		}
		entries[i] = ent
		size += uint64(ent.Size())
	}
	s.ls = LogSlice{term: 1, entries: entries}

	cfg := newTestConfig(1, 10, 1, s)
	// Set a MaxSizePerMsg that would suggest to Raft that the last committed entry should
	// not be included in the initial rd.CommittedEntries. However, our storage will ignore
	// this and *will* return it (which is how the Commit index ended up being 10 initially).
	cfg.MaxSizePerMsg = size - uint64(entries[len(entries)-1].Size()) - 1

	s.ls.entries = append(s.ls.entries, pb.Entry{
		Term:  1,
		Index: uint64(11),
		Type:  pb.EntryNormal,
		Data:  []byte("boom"),
	})

	rawNode, err := NewRawNode(cfg)
	require.NoError(t, err)

	for highestApplied := uint64(0); highestApplied != 11; {
		rd := rawNode.Ready()
		n := len(rd.CommittedEntries)
		require.NotZero(t, n, "stopped applying entries at index %d", highestApplied)
		next := rd.CommittedEntries[0].Index
		require.False(t, highestApplied != 0 && highestApplied+1 != next,
			"attempting to apply index %d after index %d, leaving a gap", next, highestApplied)

		highestApplied = rd.CommittedEntries[n-1].Index
		rawNode.Advance(rd)
		rawNode.Step(pb.Message{
			Type:    pb.MsgApp,
			To:      1,
			From:    2, // illegal, but we get away with it
			Term:    1,
			LogTerm: 1,
			Index:   11,
			Commit:  11,
		})
	}
}

// TestRawNodePersistenceRegression tests that a follower panics on receiving a
// message from a leader thinking that the follower's log is persisted at a
// previously promised match index that is out of bounds for this log now.
//
// This emulates the situation when the follower crashed and restarted, and its
// storage durability guarantees were broken.
func TestRawNodePersistenceRegression(t *testing.T) {
	const nodeID = 1
	newNode := func() *RawNode {
		s := newTestMemoryStorage(withPeers(1, 2))
		require.NoError(t, s.Append(index(1).terms(1, 2, 5)))
		require.NoError(t, s.SetHardState(pb.HardState{
			Term:   5,
			Vote:   1,
			Commit: 3,
		}))
		return newTestRawNode(nodeID, 10, 1, s)
	}

	t.Run("MsgApp", func(t *testing.T) {
		node := newNode()
		msg := pb.Message{
			From: 2, To: 1, Type: pb.MsgApp,
			Term: 5, Index: 3, LogTerm: 5, Commit: 3,
		}
		// Don't panic if we haven't reported a higher match index.
		for _, match := range []uint64{0, 1, 3} {
			msg.Match = match
			require.NoError(t, node.Step(msg))
		}
		// Panic if the leader believes the match index is beyond our log size.
		msg.Match = 4
		require.Panics(t, func() { _ = node.Step(msg) })
	})

	t.Run("MsgHeartbeat", func(t *testing.T) {
		node := newNode()
		msg := pb.Message{
			From: 2, To: 1, Type: pb.MsgHeartbeat,
			Term: 5, Commit: 3,
		}
		// Don't panic if we haven't reported a higher match index.
		for _, match := range []uint64{0, 1, 3} {
			msg.Match = match
			require.NoError(t, node.Step(msg))
		}
		// Panic if the leader believes the match index is beyond our log size.
		msg.Match = 4
		require.Panics(t, func() { _ = node.Step(msg) })
	})
}

// TestRawNodeBoundedLogGrowthWithPartition tests a scenario where a leader is
// partitioned from a quorum of nodes. It verifies that the leader's log is
// protected from unbounded growth even as new entries continue to be proposed.
// This protection is provided by the MaxUncommittedEntriesSize configuration.
func TestRawNodeBoundedLogGrowthWithPartition(t *testing.T) {
	const maxEntries = 16
	data := []byte("testdata")
	testEntry := pb.Entry{Data: data}
	maxEntrySize := maxEntries * payloadSize(testEntry)
	t.Log("maxEntrySize", maxEntrySize)

	s := newTestMemoryStorage(withPeers(1))
	cfg := newTestConfig(1, 10, 1, s)
	cfg.MaxUncommittedEntriesSize = uint64(maxEntrySize)
	rawNode, err := NewRawNode(cfg)
	require.NoError(t, err)

	// Become the leader and apply empty entry.
	rawNode.Campaign()
	for {
		rd := rawNode.Ready()
		s.Append(rd.Entries)
		rawNode.Advance(rd)
		if len(rd.CommittedEntries) > 0 {
			break
		}
	}

	// Simulate a network partition while we make our proposals by never
	// committing anything. These proposals should not cause the leader's
	// log to grow indefinitely.
	for i := 0; i < 1024; i++ {
		rawNode.Propose(data)
	}

	// Check the size of leader's uncommitted log tail. It should not exceed the
	// MaxUncommittedEntriesSize limit.
	checkUncommitted := func(exp entryPayloadSize) {
		t.Helper()
		require.Equal(t, exp, rawNode.raft.uncommittedSize)
	}
	checkUncommitted(maxEntrySize)

	// Recover from the partition. The uncommitted tail of the Raft log should
	// disappear as entries are committed.
	rd := rawNode.Ready()
	require.Len(t, rd.Entries, maxEntries)
	s.Append(rd.Entries)
	rawNode.Advance(rd)

	// Entries are appended, but not applied.
	checkUncommitted(maxEntrySize)

	rd = rawNode.Ready()
	require.Empty(t, rd.Entries)
	require.Len(t, rd.CommittedEntries, maxEntries)
	rawNode.Advance(rd)

	checkUncommitted(0)
}

func BenchmarkStatus(b *testing.B) {
	setup := func(members int) *RawNode {
		peers := make([]pb.PeerID, members)
		for i := range peers {
			peers[i] = pb.PeerID(i + 1)
		}

		raftPeers := make([]stateMachine, members)
		for i := range raftPeers {
			raftPeers[i] = newTestRaft(pb.PeerID(i+1), 10, 1, newTestMemoryStorage(withPeers(peers...)),
				withLogger(raftlogger.DiscardLogger))
		}

		tt := newNetworkWithConfig(nil, raftPeers...)
		tt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})
		return &RawNode{raft: raftPeers[0].(*raft)}
	}

	for _, members := range []int{1, 3, 5, 100} {
		b.Run(fmt.Sprintf("members=%d", members), func(b *testing.B) {
			rn := setup(members)

			b.Run("Status", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					_ = rn.Status()
				}
			})

			b.Run("Status-example", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					s := rn.Status()
					var n uint64
					for _, pr := range s.Progress {
						n += pr.Match
					}
					_ = n
				}
			})

			b.Run("BasicStatus", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					_ = rn.BasicStatus()
				}
			})

			b.Run("WithProgress", func(b *testing.B) {
				b.ReportAllocs()
				visit := func(pb.PeerID, ProgressType, tracker.Progress) {}

				for i := 0; i < b.N; i++ {
					rn.WithProgress(visit)
				}
			})
			b.Run("WithProgress-example", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					var n uint64
					visit := func(_ pb.PeerID, _ ProgressType, pr tracker.Progress) {
						n += pr.Match
					}
					rn.WithProgress(visit)
					_ = n
				}
			})
		})
	}
}

func TestRawNodeConsumeReady(t *testing.T) {
	// Check that readyWithoutAccept() does not call acceptReady (which resets
	// the messages) but Ready() does.
	s := newTestMemoryStorage(withPeers(1))
	rn := newTestRawNode(1, 3, 1, s)
	m1 := pb.Message{Context: []byte("foo")}
	m2 := pb.Message{Context: []byte("bar")}

	// Inject first message, make sure it's visible via readyWithoutAccept.
	rn.raft.msgs = append(rn.raft.msgs, m1)
	rd := rn.readyWithoutAccept()
	require.Len(t, rd.Messages, 1)
	require.Equal(t, m1, rd.Messages[0])
	require.Len(t, rn.raft.msgs, 1)
	require.Equal(t, m1, rn.raft.msgs[0])

	// Now call Ready() which should move the message into the Ready (as opposed
	// to leaving it in both places).
	rd = rn.Ready()
	require.Empty(t, rn.raft.msgs)
	require.Len(t, rd.Messages, 1)
	require.Equal(t, m1, rd.Messages[0])

	// Add a message to raft to make sure that Advance() doesn't drop it.
	rn.raft.msgs = append(rn.raft.msgs, m2)
	rn.Advance(rd)
	require.Len(t, rn.raft.msgs, 1)
	require.Equal(t, m2, rn.raft.msgs[0])
}

func TestStorageUsageForTermCheckBeforeCommit(t *testing.T) {
	// Check for eliminating storage access for getting the term number of an entry that is unavailable in unstable log
	// (because it has already been persisted on storage, and removed from unstable log)
	//
	// Before adding raft.idxPreLeading optimization, the leader node often needs to access storage
	// to know the term number of a specific entry
	//
	// High level overview of the scenario before the optimization:
	//		Within a cluster, Node "A" campaigns and wins the election.
	//		This new leader A now is in term X
	//		A has some uncommitted entries from previous term(s) < X
	//
	//    The following (can) happens asynchronously through "Ready messages" that are picked up by the application layer
	//				A tries to replicate all its uncommitted entries to followers (send MsgApp to followers)
	//    		A also tells the application layer of what is currently in the unstable log of A. (MsgStorageAppend)
	//
	//		The following happens asynchronously:
	//        A receives MsgAppResp from a follower, eventually A receives MsgAppResp from a quorum of followers
	//							one MsgAppResp from an individual follower indicates
	//							the specific follower node has replicated the entry to its stable storage
	//
	//							A keeps track of whether each follower has persisted the entry in its progress tracker
	//							If A sees that a quorum of followers has persisted the entry,
	//							A tries to commit the entry with raft.maybeCommit()
	//     		A receives MsgStorageAppendResp,
	//    					indicating application layer has persisted the entries currently in unstable
	//   		   			(which are also sent out to followers in MsgAppResp)
	//  		   			to storage
	//							A goes ahead and remove those entries from unstable log (since A knows its safely in storage)
	//
	//
	//		As you can see if "A receives MsgStorageAppendResp" happens first, A will remove the entries from unstable
	// 		After this has happened, if A receives MsgAppResp(for the entry) from a quorum of followers,
	//    A tries to commit the entry(s) because it has quorum, but A still needs to know the entry it is trying to commit
	//    is from the current term.
	//
	//  	So it checks for the term number of the entry, but the entry is no longer in stable, so A calls the storage
	//		to ask for the term number. This disk access is very slow compared to just knowing what the term is in memory.
	//		(or an invariant info that serves the same purpose)

	// Implementation of the unit test:
	// We will have two nodes, node 1 and node 2, node 1 will be the leader node
	// We will manually operate/trigger the actions/behaviours of node 1 (id: 1),
	// node 1 is aware of the existence of node 2 (id: 2)
	// but we won't actually create a raw node for node2 or do stuff on node 2
	// we will mimic node 2 sending messages to node 1
	// by manually stepping messages on node 1 that seems to be sent from node 2.
	// By calling rn.Step(message) rn is short for rawnode which represents node 1 in this test

	// Creating an in-memory storage/network scenario with two nodes. node1 (id:1) and node2 (id:2)
	// s is the persistent storage object of node1
	s := newTestMemoryStorage(withPeers(1, 2))

	// Node 1 starts with commit index 0
	hs := pb.HardState{
		Term:   2,
		Vote:   1,
		Commit: 0,
	}
	require.NoError(t, s.SetHardState(hs))
	// node1 starts with two entries in its storage at the beginning
	// which is why node 1 starts with commit index 0
	require.NoError(t, s.Append(entryID{}.append(1, 2).entries))

	// config for node 1 (id:1), election timeout and heartbeat doesn't matter
	// cfg has the storage object s passed in, which represents node 1 knowing that there is another node 2 in the cluster
	cfg := newTestConfig(1, 3, 1, s)
	cfg.AsyncStorageWrites = true

	// rn is node1
	rn, err := NewRawNode(cfg)
	require.NoError(t, err)
	require.Equal(t, uint64(0), rn.raft.raftLog.committed)

	// node1 starts campaign
	rn.Campaign() // prepares the ready with state candidate
	t.Log("node 1 becomes leader")
	// Generate a Ready object
	rd := rn.Ready()
	t.Log("describe node1 1st ready")
	t.Logf("%s",
		DescribeReady(rd, func(bytes []byte) string { return "" }))
	//Ready MustSync=true:
	//State:StateCandidate
	//HardState Term:3 Vote:1 Commit:0 Lead:0 LeadEpoch:0
	//Messages:
	//1->2 MsgVote Term:3 Log:2/2
	//1->AppendThread MsgStorageAppend Term:3 Log:0/0 Vote:1 Responses:[
	//  1->1 MsgVoteResp Term:3 Log:0/0
	//]
	s.SetHardState(rd.HardState) // actually becomes candidate, and send out vote request

	rn.Step(rd.Messages[1].Responses[0])                               // 1->1 MsgVoteResp, Mimic node1 voting itself
	rn.Step(pb.Message{To: 1, From: 2, Term: 3, Type: pb.MsgVoteResp}) //2->1 MsgVoteResp. Mimic node2 voting for node1
	//node 1 receives the quorum(itself and node2 vote), therefore becomes the leader
	require.Equal(t, pb.StateLeader, rn.raft.state)
	// a dummy entry is appended to the local unstable log and to the ready struct,
	// which will be picked up by application and sent out to followers via MsgApp

	t.Log(rn.raft.trk.Progress(1).String())
	t.Log(rn.raft.trk.Progress(2).String())

	// mimic that node 2 has replicated the two uncommited entries(from perspective of node1)
	// This will invoke maybeCommit.
	// but cannot commit because the term number of those two uncommited entries is less than current node1 leader term
	// there is also no storage read for term involved since node 1 still has those entries in unstable
	// 2->1 MsgAppResp. Mimic node2 has persisted the logs up to index 2
	rn.Step(pb.Message{To: 1, From: 2, Term: 3, Type: pb.MsgAppResp, Index: 2})

	t.Log(rn.raft.trk.Progress(1).String())
	t.Log(rn.raft.trk.Progress(2).String())

	t.Log("describe node1 2nd ready")
	//A new dummy entry should be in ready for replicating to followers and persisting because node1 just became leader
	t.Log("dummy")
	rd = rn.Ready()
	t.Logf("%s",
		DescribeReady(rd, func(bytes []byte) string { return "" }))
	//Ready MustSync=true:
	//State:StateLeader
	//HardState Term:3 Vote:1 Commit:0 Lead:1 LeadEpoch:1
	//Entries:
	//3/3 EntryNormal
	//Messages:
	//1->2 MsgFortifyLeader Term:3 Log:0/0
	//1->2 MsgApp Term:3 Log:2/2 Entries:[3/3 EntryNormal]
	//1->2 MsgApp Term:3 Log:2/2 Entries:[3/3 EntryNormal]
	//1->AppendThread MsgStorageAppend Term:3 Log:3/3 Vote:1 Lead:1 LeadEpoch:1 Entries:[3/3 EntryNormal] Responses:[
	//  AppendThread->1 MsgStorageAppendResp Term:0 Log:3/3
	//  1->1 MsgAppResp Term:3 Log:0/3
	//  1->1 MsgFortifyLeaderResp Term:3 Log:0/0 LeadEpoch:1
	//]

	// a dummy entry is generated after node1 becomes leader, which is added to ready for replicating to followers
	s.SetHardState(rd.HardState)
	s.Append(rd.Entries)

	// MsgStorageAppend
	require.Equal(t, pb.MsgStorageAppend, rd.Messages[3].Type)

	// Process MsgStorageAppendResp
	// We are telling the leader node1 that the new dummy entry(index3, term3) has been persisted on log
	// therefore the leader rawnode flushes its unstablelog,
	// and its prev.entry variable which is still in memory, now points to the dummy entry(which was just flushed)
	rn.Step(rd.Messages[3].Responses[0]) // AppendThread->1 MsgStorageAppendResp Term:0 Log:3/4

	// The leader node's application layer telling itself that it has received MsgApp(for dummy entry) from itself.
	// This will invoke maybeCommit again for the previous 2 entries because there is still a quorum for index 2
	// (maybeCommit() is invoked as long as there is an uncommited quorum index, maybe need optimization)
	// The action is the same as before,
	// only this time a storage access is involved since we lost memory access for the entry we are checking
	// "Returned term from storage. Slow access. index: 2 , term: 2"
	rn.Step(rd.Messages[3].Responses[1]) // 1->1 MsgAppResp Term:3 Log:0/3,

	// Leader can't commit anything, commit index should still stay at 0
	require.Equal(t, uint64(0), rn.raft.raftLog.committed)

	// Add new entry 4 to the Ready message. This is mimicking client's request for new entry
	rn.Propose([]byte("foo")) // entry 4
	rd = rn.Ready()

	t.Logf("%s",
		DescribeReady(rd, func(bytes []byte) string { return "" }))
	// Ready MustSync=true:
	// Entries:
	// 3/4 EntryNormal
	// Messages:
	// 1->2 MsgApp Term:3 Log:3/3 Entries:[3/4 EntryNormal]
	// 1->AppendThread MsgStorageAppend Term:0 Log:3/4 Entries:[3/4 EntryNormal] Responses:[
	//   AppendThread->1 MsgStorageAppendResp Term:0 Log:3/4
	//   1->1 MsgAppResp Term:3 Log:0/4
	// ]

	s.SetHardState(rd.HardState)
	s.Append(rd.Entries)
	t.Log("entry 4")
	// Local store has persisted entry 4
	rn.Step(rd.Messages[1].Responses[0]) //AppendThread->1 MsgStorageAppendResp Term:0 Log:3/4
	// t.Log("entry 4 second response")
	// right now entry4 with entry id{term:3, index:4} is removed from unstable log of node1(leader)
	// because storage has responded and said that log is persisted
	// unstable log of node1(leader) is empty right now
	// the node still has access to the term of entry id{term:3, index:4}, because prev.term is kept in memory
	// but doesn't have access to the term of entry id{term:3, index:3}

	// This will invoke maybeCommit and a term check
	// the leader calls maybeCommit() everytime the leader receive a MsgAppResp.
	// The want to commit quorum is still at index 2,
	// because we never heard back from node2 saying it has persisted any entry greater than 2
	// "Returned term from storage. Slow access. index: 2 , term: 2"
	// maybeCommit will return false because the term check failed,  leader is currently in term 3. The commitIndex will stay at zero
	rn.Step(rd.Messages[1].Responses[1]) // 1->1 MsgAppResp Term:3 Log:0/4
	// leader still can't commit anything
	require.Equal(t, uint64(0), rn.raft.raftLog.committed)

	t.Log("step MsgAppResp 3")
	// The follower replies to the MsgApp of entry id{term:3, index:3}.
	// leader attempts to commit entry id{term:3, index:3}, and has to verify the term in storage

	// Since its MsgAppResp, this will invoke maybeCommit and a term check,
	// The term check is a fetch from storage, since we lost access to the term of entry index 3
	// "Returned term from storage. Slow access. index: 3 , term: 3"
	// maybeCommit will return true because the leader is currently in term 3.
	//
	// And we are trying to advance commit index to 3, which is the dummy entry generated automatically by leader node1,
	// with entry id{term:3, index:3}
	//
	// Even though the previous 2 entries are from term2, we can still commit them along with the dummy entry(from term3).
	// Section 5.4.2 of raft paper has more details (https://raft.github.io/raft.pdf):
	//
	// 2->1 MsgAppResp. Mimic node2 has persisted the logs up to index 3(index3 is the dummy entry with term = 3).
	rn.Step(pb.Message{To: 1, From: 2, Type: pb.MsgAppResp, Index: 3})

	// This time we can commit, commit index is advanced to 3.
	// Not 4 because we don't have follower quorum for entry4(index:4, term:3),
	//
	// If node 2 sends another MsgAppResp with index:4 indicating that it has persisted entry 4,
	// then we can advance commit index to 4
	require.Equal(t, uint64(3), rn.raft.raftLog.committed)

	// should only trigger storage read for a term number once, after the optimization
	require.Equal(t, 1, s.callStats.term)
	// Before the idxPreLeading optimization, we would fetch a term number from storage exactly 4 times.
	// 3 times more than after the optimization.
	// You can kind of see that the storage fetch for term happens quite often, so this minor optimization can help a bit.
}

func BenchmarkRawNode(b *testing.B) {
	cases := []struct {
		name  string
		peers []pb.PeerID
	}{
		{
			name:  "single-voter",
			peers: []pb.PeerID{1},
		},
		{
			name:  "two-voters",
			peers: []pb.PeerID{1, 2},
		},
		// You can easily add more cases here.
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			benchmarkRawNodeImpl(b, tc.peers...)
		})
	}
}

func benchmarkRawNodeImpl(b *testing.B, peers ...pb.PeerID) {

	const debug = false

	s := newTestMemoryStorage(withPeers(peers...))
	cfg := newTestConfig(1, 10, 1, s)
	if !debug {
		cfg.Logger = raftlogger.DiscardLogger // avoid distorting benchmark output
	}
	rn, err := NewRawNode(cfg)
	if err != nil {
		b.Fatal(err)
	}

	run := make(chan struct{}, 1)
	defer close(run)

	var numReady uint64
	stabilize := func() (applied uint64) {
		for rn.HasReady() {
			numReady++
			rd := rn.Ready()
			if debug {
				b.Log(DescribeReady(rd, nil))
			}
			if n := len(rd.CommittedEntries); n > 0 {
				applied = rd.CommittedEntries[n-1].Index
			}
			s.Append(rd.Entries)
			for _, m := range rd.Messages {
				if m.Type == pb.MsgVote {
					resp := pb.Message{To: m.From, From: m.To, Term: m.Term, Type: pb.MsgVoteResp}
					if debug {
						b.Log(DescribeMessage(resp, nil))
					}
					rn.Step(resp)
				}
				if m.Type == pb.MsgApp {
					idx := m.Index
					if n := len(m.Entries); n > 0 {
						idx = m.Entries[n-1].Index
					}
					resp := pb.Message{To: m.From, From: m.To, Type: pb.MsgAppResp, Term: m.Term, Index: idx}
					if debug {
						b.Log(DescribeMessage(resp, nil))
					}
					rn.Step(resp)
				}
			}
			rn.Advance(rd)
		}
		return applied
	}

	rn.Campaign()
	stabilize()

	if debug {
		// nolint:staticcheck
		b.N = 1
	}

	var applied uint64
	for i := 0; i < b.N; i++ {
		if err := rn.Propose([]byte("foo")); err != nil {
			b.Fatal(err)
		}
		applied = stabilize()
	}
	if applied < uint64(b.N) {
		b.Fatalf("did not apply everything: %d < %d", applied, b.N)
	}
	b.ReportAllocs()
	b.ReportMetric(float64(s.callStats.firstIndex)/float64(b.N), "firstIndex/op")
	b.ReportMetric(float64(s.callStats.lastIndex)/float64(b.N), "lastIndex/op")
	b.ReportMetric(float64(s.callStats.term)/float64(b.N), "term/op")
	b.ReportMetric(float64(numReady)/float64(b.N), "ready/op")
	b.Logf("storage access stats: %+v", s.callStats)
}
