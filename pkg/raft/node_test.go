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
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/raft/raftstoreliveness"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNodePropose ensures that node.Propose sends the given proposal to the underlying raft.
func TestNodePropose(t *testing.T) {
	var msgs []raftpb.Message
	appendStep := func(r *raft, m raftpb.Message) error {
		t.Log(DescribeMessage(m, nil))
		if m.Type == raftpb.MsgAppResp {
			return nil // injected by (*raft).advance
		}
		msgs = append(msgs, m)
		return nil
	}

	s := newTestMemoryStorage(withPeers(1))
	rn := newTestRawNode(1, 10, 1, s)
	r := rn.raft
	require.NoError(t, rn.Campaign())
	for {
		rd := rn.Ready()
		require.NoError(t, s.Append(rd.Entries))
		// change the step function to appendStep until this raft becomes leader
		if rd.HardState.Lead == r.id {
			r.step = appendStep
			rn.Advance(rd)
			break
		}
		rn.Advance(rd)
	}
	require.NoError(t, rn.Propose([]byte("somedata")))

	require.Len(t, msgs, 2)
	assert.Equal(t, raftpb.MsgFortifyLeaderResp, msgs[0].Type)
	assert.Equal(t, raftpb.MsgProp, msgs[1].Type)
	assert.Equal(t, []byte("somedata"), msgs[1].Entries[0].Data)
}

// TestDisableProposalForwarding ensures that proposals are not forwarded to
// the leader when DisableProposalForwarding is true.
func TestDisableProposalForwarding(t *testing.T) {
	r1 := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	r2 := newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	cfg3 := newTestConfig(3, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	cfg3.DisableProposalForwarding = true
	r3 := newRaft(cfg3)
	nt := newNetwork(r1, r2, r3)

	// elect r1 as leader
	nt.send(raftpb.Message{From: 1, To: 1, Type: raftpb.MsgHup})

	var testEntries = []raftpb.Entry{{Data: []byte("testdata")}}

	// send proposal to r2(follower) where DisableProposalForwarding is false
	r2.Step(raftpb.Message{From: 2, To: 2, Type: raftpb.MsgProp, Entries: testEntries})

	// verify r2(follower) does forward the proposal when DisableProposalForwarding is false
	require.Len(t, r2.msgs, 1)

	// send proposal to r3(follower) where DisableProposalForwarding is true
	r3.Step(raftpb.Message{From: 3, To: 3, Type: raftpb.MsgProp, Entries: testEntries})

	// verify r3(follower) does not forward the proposal when DisableProposalForwarding is true
	require.Empty(t, r3.msgs)
}

// TestNodeProposeConfig ensures that node.ProposeConfChange sends the given configuration proposal
// to the underlying raft.
func TestNodeProposeConfig(t *testing.T) {
	var msgs []raftpb.Message
	appendStep := func(r *raft, m raftpb.Message) error {
		if m.Type == raftpb.MsgAppResp {
			return nil // injected by (*raft).advance
		}
		msgs = append(msgs, m)
		return nil
	}

	s := newTestMemoryStorage(withPeers(1))
	rn := newTestRawNode(1, 10, 1, s)
	r := rn.raft
	require.NoError(t, rn.Campaign())
	for {
		rd := rn.Ready()
		require.NoError(t, s.Append(rd.Entries))
		// change the step function to appendStep until this raft becomes leader
		if rd.HardState.Lead == r.id {
			r.step = appendStep
			rn.Advance(rd)
			break
		}
		rn.Advance(rd)
	}
	cc := raftpb.ConfChange{Type: raftpb.ConfChangeAddNode, NodeID: 1}
	ccdata, err := cc.Marshal()
	require.NoError(t, err)
	require.NoError(t, rn.ProposeConfChange(cc))

	require.Len(t, msgs, 2)
	assert.Equal(t, raftpb.MsgFortifyLeaderResp, msgs[0].Type)
	assert.Equal(t, raftpb.MsgProp, msgs[1].Type)
	assert.Equal(t, ccdata, msgs[1].Entries[0].Data)
}

// TestNodeProposeAddDuplicateNode ensures that two proposes to add the same node should
// not affect the later propose to add new node.
func TestNodeProposeAddDuplicateNode(t *testing.T) {
	s := newTestMemoryStorage(withPeers(1))
	cfg := newTestConfig(1, 10, 1, s)
	rn, err := NewRawNode(cfg)
	require.NoError(t, err)
	require.NoError(t, rn.Campaign())

	allCommittedEntries := make([]raftpb.Entry, 0)

	rd := rn.Ready()
	require.NoError(t, s.Append(rd.Entries))
	rn.Advance(rd)

	ready := func() (appliedConfChange bool) {
		rd := rn.Ready()
		t.Log(DescribeReady(rd, nil))
		require.NoError(t, s.Append(rd.Entries))
		applied := false
		for _, e := range rd.CommittedEntries {
			allCommittedEntries = append(allCommittedEntries, e)
			switch e.Type {
			case raftpb.EntryNormal:
			case raftpb.EntryConfChange:
				var cc raftpb.ConfChange
				require.NoError(t, cc.Unmarshal(e.Data))
				rn.ApplyConfChange(cc)
				applied = true
			}
		}
		rn.Advance(rd)
		return applied
	}
	waitAppliedConfChange := func() bool {
		for i := 0; i < 10; i++ {
			if ready() {
				return true
			}
		}
		return false
	}

	cc1 := raftpb.ConfChange{Type: raftpb.ConfChangeAddNode, NodeID: 1}
	ccdata1, _ := cc1.Marshal()
	require.NoError(t, rn.ProposeConfChange(cc1))
	require.True(t, waitAppliedConfChange())

	// try add the same node again
	require.NoError(t, rn.ProposeConfChange(cc1))
	require.True(t, waitAppliedConfChange())

	// the new node join should be ok
	cc2 := raftpb.ConfChange{Type: raftpb.ConfChangeAddNode, NodeID: 2}
	ccdata2, _ := cc2.Marshal()
	require.NoError(t, rn.ProposeConfChange(cc2))
	require.True(t, waitAppliedConfChange())

	assert.Len(t, allCommittedEntries, 4)
	assert.Equal(t, ccdata1, allCommittedEntries[1].Data)
	assert.Equal(t, ccdata2, allCommittedEntries[3].Data)
}

// TestBlockProposal ensures that node will block proposal when it does not
// know who is the current leader; node will accept proposal when it knows
// who is the current leader.
func TestBlockProposal(t *testing.T) {
	s := newTestMemoryStorage(withPeers(1))
	rn := newTestRawNode(1, 10, 1, s)

	require.ErrorIs(t, ErrProposalDropped, rn.Propose([]byte("somedata")))

	require.NoError(t, rn.Campaign())
	rd := rn.Ready()
	require.NoError(t, s.Append(rd.Entries))
	rn.Advance(rd)

	require.NoError(t, rn.Propose([]byte("somedata")))
}

func TestNodeProposeWaitDropped(t *testing.T) {
	var msgs []raftpb.Message
	droppingMsg := []byte("test_dropping")
	dropStep := func(r *raft, m raftpb.Message) error {
		if m.Type == raftpb.MsgProp && strings.Contains(m.String(), string(droppingMsg)) {
			t.Logf("dropping message: %v", m.String())
			return ErrProposalDropped
		}
		if m.Type == raftpb.MsgAppResp {
			// This is produced by raft internally, see (*raft).advance.
			return nil
		}
		msgs = append(msgs, m)
		return nil
	}

	s := newTestMemoryStorage(withPeers(1))
	rn := newTestRawNode(1, 10, 1, s)
	r := rn.raft
	require.NoError(t, rn.Campaign())
	for {
		rd := rn.Ready()
		require.NoError(t, s.Append(rd.Entries))
		// change the step function to dropStep until this raft becomes leader
		if rd.HardState.Lead == r.id {
			r.step = dropStep
			rn.Advance(rd)
			break
		}
		rn.Advance(rd)
	}
	assert.Equal(t, ErrProposalDropped, rn.Propose(droppingMsg))

	require.Len(t, msgs, 1)
	assert.Equal(t, raftpb.MsgFortifyLeaderResp, msgs[0].Type)
}

// TestNodeTick ensures that node.Tick() will increase the
// elapsed of the underlying raft state machine.
func TestNodeTick(t *testing.T) {
	s := newTestMemoryStorage(withPeers(1))
	rn := newTestRawNode(1, 10, 1, s)
	r := rn.raft
	elapsed := r.electionElapsed
	rn.Tick()
	assert.Equal(t, elapsed+1, r.electionElapsed)
}

// TestNodeStart ensures that a node can be started correctly. The node should
// start with correct configuration change entries, and can accept and commit
// proposals.
func TestNodeStart(t *testing.T) {
	cc := raftpb.ConfChange{Type: raftpb.ConfChangeAddNode, NodeID: 1}
	ccdata, err := cc.Marshal()
	require.NoError(t, err)
	wants := []Ready{
		{
			HardState: raftpb.HardState{Term: 1, Commit: 1, Vote: 0, Lead: 0},
			Entries: []raftpb.Entry{
				{Type: raftpb.EntryConfChange, Term: 1, Index: 1, Data: ccdata},
			},
			CommittedEntries: []raftpb.Entry{
				{Type: raftpb.EntryConfChange, Term: 1, Index: 1, Data: ccdata},
			},
			MustSync: true,
		},
		{
			HardState:        raftpb.HardState{Term: 2, Commit: 2, Vote: 1, Lead: 1, LeadEpoch: 1},
			Entries:          []raftpb.Entry{{Term: 2, Index: 3, Data: []byte("foo")}},
			CommittedEntries: []raftpb.Entry{{Term: 2, Index: 2, Data: nil}},
			MustSync:         true,
		},
		{
			HardState:        raftpb.HardState{Term: 2, Commit: 3, Vote: 1, Lead: 1, LeadEpoch: 1},
			Entries:          nil,
			CommittedEntries: []raftpb.Entry{{Term: 2, Index: 3, Data: []byte("foo")}},
			MustSync:         true,
		},
	}
	storage := NewMemoryStorage()
	c := &Config{
		ID:              1,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		MaxSizePerMsg:   noLimit,
		MaxInflightMsgs: 256,
		StoreLiveness:   raftstoreliveness.AlwaysLive{},
		CRDBVersion:     cluster.MakeTestingClusterSettings().Version,
		Metrics:         NewMetrics(),
	}

	rn, err := NewRawNode(c)
	require.NoError(t, err)
	require.NoError(t, rn.Bootstrap([]Peer{{ID: 1}}))

	rd := rn.Ready()
	require.Equal(t, wants[0], rd)
	require.NoError(t, storage.Append(rd.Entries))
	rn.Advance(rd)

	require.NoError(t, rn.Campaign())

	// Persist vote.
	rd = rn.Ready()
	require.NoError(t, storage.Append(rd.Entries))
	rn.Advance(rd)
	// Append empty entry.
	rd = rn.Ready()
	require.NoError(t, storage.Append(rd.Entries))
	rn.Advance(rd)

	require.NoError(t, rn.Propose([]byte("foo")))
	rd = rn.Ready()
	assert.Equal(t, wants[1], rd)
	require.NoError(t, storage.Append(rd.Entries))
	rn.Advance(rd)

	rd = rn.Ready()
	assert.Equal(t, wants[2], rd)
	require.NoError(t, storage.Append(rd.Entries))
	rn.Advance(rd)

	require.False(t, rn.HasReady())
}

func TestNodeRestart(t *testing.T) {
	entries := []raftpb.Entry{
		{Term: 1, Index: 1},
		{Term: 1, Index: 2, Data: []byte("foo")},
	}
	st := raftpb.HardState{Term: 1, Commit: 1}

	want := Ready{
		// No HardState is emitted because there was no change.
		HardState: raftpb.HardState{},
		// commit up to index commit index in st
		CommittedEntries: entries[:st.Commit],
		// MustSync is false because no HardState or new entries are provided.
		MustSync: false,
	}

	storage := NewMemoryStorage()
	require.NoError(t, storage.SetHardState(st))
	require.NoError(t, storage.Append(entries))
	c := &Config{
		ID:              1,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		MaxSizePerMsg:   noLimit,
		MaxInflightMsgs: 256,
		StoreLiveness:   raftstoreliveness.AlwaysLive{},
		CRDBVersion:     cluster.MakeTestingClusterSettings().Version,
		Metrics:         NewMetrics(),
	}
	rn, err := NewRawNode(c)
	require.NoError(t, err)

	rd := rn.Ready()
	if assert.Equal(t, want, rd) {
		rn.Advance(rd)
	}
	require.False(t, rn.HasReady())
}

func TestNodeRestartFromSnapshot(t *testing.T) {
	snap := raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			ConfState: raftpb.ConfState{Voters: []raftpb.PeerID{1, 2}},
			Index:     2,
			Term:      1,
		},
	}
	entries := []raftpb.Entry{
		{Term: 1, Index: 3, Data: []byte("foo")},
	}
	st := raftpb.HardState{Term: 1, Commit: 3}

	want := Ready{
		// No HardState is emitted because nothing changed relative to what is
		// already persisted.
		HardState: raftpb.HardState{},
		// commit up to index commit index in st
		CommittedEntries: entries,
		// MustSync is only true when there is a new HardState or new entries;
		// neither is the case here.
		MustSync: false,
	}

	s := NewMemoryStorage()
	require.NoError(t, s.SetHardState(st))
	require.NoError(t, s.ApplySnapshot(snap))
	require.NoError(t, s.Append(entries))
	c := &Config{
		ID:              1,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         s,
		MaxSizePerMsg:   noLimit,
		MaxInflightMsgs: 256,
		StoreLiveness:   raftstoreliveness.AlwaysLive{},
		CRDBVersion:     cluster.MakeTestingClusterSettings().Version,
		Metrics:         NewMetrics(),
	}
	rn, err := NewRawNode(c)
	require.NoError(t, err)

	rd := rn.Ready()
	if assert.Equal(t, want, rd) {
		rn.Advance(rd)
	}
	require.False(t, rn.HasReady())
}

func TestNodeAdvance(t *testing.T) {
	storage := newTestMemoryStorage(withPeers(1))
	c := &Config{
		ID:              1,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		MaxSizePerMsg:   noLimit,
		MaxInflightMsgs: 256,
		StoreLiveness:   raftstoreliveness.AlwaysLive{},
		CRDBVersion:     cluster.MakeTestingClusterSettings().Version,
		Metrics:         NewMetrics(),
	}
	rn, err := NewRawNode(c)
	require.NoError(t, err)

	require.NoError(t, rn.Campaign())
	// Persist vote.
	rd := rn.Ready()
	require.NoError(t, storage.Append(rd.Entries))
	rn.Advance(rd)
	// Append empty entry.
	rd = rn.Ready()
	require.NoError(t, storage.Append(rd.Entries))
	rn.Advance(rd)

	require.NoError(t, rn.Propose([]byte("foo")))
	rd = rn.Ready()
	require.NoError(t, storage.Append(rd.Entries))
	rn.Advance(rd)

	require.True(t, rn.HasReady())
}

func TestSoftStateEqual(t *testing.T) {
	tests := []struct {
		st *SoftState
		we bool
	}{
		{&SoftState{}, true},
		{&SoftState{RaftState: raftpb.StateLeader}, false},
	}
	for i, tt := range tests {
		assert.Equal(t, tt.we, tt.st.equal(&SoftState{}), "#%d", i)
	}
}

func TestIsHardStateEqual(t *testing.T) {
	tests := []struct {
		ht raftpb.HardState
		we bool
	}{
		{emptyState, true},
		{raftpb.HardState{Vote: 1}, false},
		{raftpb.HardState{Commit: 1}, false},
		{raftpb.HardState{Term: 1}, false},
		{raftpb.HardState{Lead: 1}, false},
	}

	for i, tt := range tests {
		assert.Equal(t, tt.we, isHardStateEqual(tt.ht, emptyState), "#%d", i)
	}
}

func TestNodeProposeAddLearnerNode(t *testing.T) {
	s := newTestMemoryStorage(withPeers(1))
	rn := newTestRawNode(1, 10, 1, s)
	require.NoError(t, rn.Campaign())
	rd := rn.Ready()
	require.NoError(t, s.Append(rd.Entries))
	rn.Advance(rd)

	ready := func() (appliedConfChange bool) {
		rd := rn.Ready()
		require.NoError(t, s.Append(rd.Entries))
		t.Logf("raft: %v", rd.Entries)
		for _, ent := range rd.CommittedEntries {
			if ent.Type != raftpb.EntryConfChange {
				continue
			}
			var cc raftpb.ConfChange
			require.NoError(t, cc.Unmarshal(ent.Data))
			state := rn.ApplyConfChange(cc)
			assert.True(t, len(state.Learners) > 0 && state.Learners[0] == cc.NodeID && cc.NodeID == 2,
				"apply conf change should return new added learner: %v", state.String())
			assert.Len(t, state.Voters, 1,
				"add learner should not change the nodes: %v", state.String())

			t.Logf("apply raft conf %v changed to: %v", cc, state.String())
			appliedConfChange = true
		}
		rn.Advance(rd)
		return appliedConfChange
	}
	waitAppliedConfChange := func() bool {
		for i := 0; i < 10; i++ {
			if ready() {
				return true
			}
		}
		return false
	}

	cc := raftpb.ConfChange{Type: raftpb.ConfChangeAddLearnerNode, NodeID: 2}
	require.NoError(t, rn.ProposeConfChange(cc))
	require.True(t, waitAppliedConfChange())
}

func TestAppendPagination(t *testing.T) {
	const maxSizePerMsg = 2048
	n := newNetworkWithConfig(func(c *Config) {
		c.MaxSizePerMsg = maxSizePerMsg
	}, nil, nil, nil)

	seenFullMessage := false
	// Inspect all messages to see that we never exceed the limit, but
	// we do see messages of larger than half the limit.
	n.msgHook = func(m raftpb.Message) bool {
		if m.Type == raftpb.MsgApp {
			size := 0
			for _, e := range m.Entries {
				size += len(e.Data)
			}
			assert.LessOrEqual(t, size, maxSizePerMsg, "sent MsgApp that is too large")
			if size > maxSizePerMsg/2 {
				seenFullMessage = true
			}
		}
		return true
	}

	n.send(raftpb.Message{From: 1, To: 1, Type: raftpb.MsgHup})

	// Partition the network while we make our proposals. This forces
	// the entries to be batched into larger messages.
	n.isolate(1)
	blob := []byte(strings.Repeat("a", 1000))
	for i := 0; i < 5; i++ {
		n.send(raftpb.Message{From: 1, To: 1, Type: raftpb.MsgProp, Entries: []raftpb.Entry{{Data: blob}}})
	}
	n.recover()

	// After the partition recovers, tick the clock to wake everything
	// back up and send the messages.
	p := n.peers[raftpb.PeerID(1)].(*raft)
	for ticks := p.heartbeatTimeout; ticks > 0; ticks-- {
		n.tick(p)
	}
	assert.True(t, seenFullMessage, "didn't see any messages more than half the max size; something is wrong with this test")
}

func TestCommitPagination(t *testing.T) {
	s := newTestMemoryStorage(withPeers(1))
	cfg := newTestConfig(1, 10, 1, s)
	cfg.MaxCommittedSizePerReady = 2048
	rn, err := NewRawNode(cfg)
	require.NoError(t, err)

	require.NoError(t, rn.Campaign())

	// Persist vote.
	rd := rn.Ready()
	require.NoError(t, s.Append(rd.Entries))
	rn.Advance(rd)

	// Append empty entry.
	rd = rn.Ready()
	require.NoError(t, s.Append(rd.Entries))
	rn.Advance(rd)
	// Apply empty entry.
	rd = rn.Ready()
	require.Len(t, rd.CommittedEntries, 1)
	require.NoError(t, s.Append(rd.Entries))
	rn.Advance(rd)

	blob := []byte(strings.Repeat("a", 1000))
	for i := 0; i < 3; i++ {
		require.NoError(t, rn.Propose(blob), "#%d", i)
	}

	// First the three proposals have to be appended.
	rd = rn.Ready()
	require.Len(t, rd.Entries, 3)
	require.NoError(t, s.Append(rd.Entries))
	rn.Advance(rd)

	// The 3 proposals will commit in two batches.
	rd = rn.Ready()
	require.Len(t, rd.CommittedEntries, 2)
	require.NoError(t, s.Append(rd.Entries))
	rn.Advance(rd)

	rd = rn.Ready()
	require.Len(t, rd.CommittedEntries, 1)
	require.NoError(t, s.Append(rd.Entries))
	rn.Advance(rd)
}

func TestCommitPaginationWithAsyncStorageWrites(t *testing.T) {
	s := newTestMemoryStorage(withPeers(1))
	cfg := newTestConfig(1, 10, 1, s)
	cfg.MaxCommittedSizePerReady = 2048
	cfg.AsyncStorageWrites = true

	rn, err := NewRawNode(cfg)
	require.NoError(t, err)
	require.NoError(t, rn.Campaign())

	// Persist vote.
	rd := rn.Ready()
	require.Len(t, rd.Messages, 1)
	m := rd.Messages[0]
	require.Equal(t, raftpb.MsgStorageAppend, m.Type)
	require.NoError(t, s.Append(m.Entries))
	for _, resp := range m.Responses {
		require.NoError(t, rn.Step(resp))
	}
	// Append empty entry.
	rd = rn.Ready()
	require.Len(t, rd.Messages, 1)
	m = rd.Messages[0]
	require.Equal(t, raftpb.MsgStorageAppend, m.Type)
	require.NoError(t, s.Append(m.Entries))
	for _, resp := range m.Responses {
		require.NoError(t, rn.Step(resp))
	}
	// Apply empty entry.
	rd = rn.Ready()
	require.Len(t, rd.Messages, 2)
	for _, m := range rd.Messages {
		switch m.Type {
		case raftpb.MsgStorageAppend:
			require.NoError(t, s.Append(m.Entries))
			for _, resp := range m.Responses {
				require.NoError(t, rn.Step(resp))
			}
		case raftpb.MsgStorageApply:
			require.Len(t, m.Entries, 1)
			require.Len(t, m.Responses, 1)
			require.NoError(t, rn.Step(m.Responses[0]))
		default:
			t.Fatalf("unexpected: %v", m)
		}
	}

	// Propose first entry.
	blob := []byte(strings.Repeat("a", 1024))
	require.NoError(t, rn.Propose(blob))

	// Append first entry.
	rd = rn.Ready()
	require.Len(t, rd.Messages, 1)
	m = rd.Messages[0]
	require.Equal(t, raftpb.MsgStorageAppend, m.Type)
	require.Len(t, m.Entries, 1)
	require.NoError(t, s.Append(m.Entries))
	for _, resp := range m.Responses {
		require.NoError(t, rn.Step(resp))
	}

	// Propose second entry.
	require.NoError(t, rn.Propose(blob))

	// Append second entry. Don't apply first entry yet.
	rd = rn.Ready()
	require.Len(t, rd.Messages, 2)
	var applyResps []raftpb.Message
	for _, m := range rd.Messages {
		switch m.Type {
		case raftpb.MsgStorageAppend:
			require.NoError(t, s.Append(m.Entries))
			for _, resp := range m.Responses {
				require.NoError(t, rn.Step(resp))
			}
		case raftpb.MsgStorageApply:
			require.Len(t, m.Entries, 1)
			require.Len(t, m.Responses, 1)
			applyResps = append(applyResps, m.Responses[0])
		default:
			t.Fatalf("unexpected: %v", m)
		}
	}

	// Propose third entry.
	require.NoError(t, rn.Propose(blob))

	// Append third entry. Don't apply second entry yet.
	rd = rn.Ready()
	require.Len(t, rd.Messages, 2)
	for _, m := range rd.Messages {
		switch m.Type {
		case raftpb.MsgStorageAppend:
			require.NoError(t, s.Append(m.Entries))
			for _, resp := range m.Responses {
				require.NoError(t, rn.Step(resp))
			}
		case raftpb.MsgStorageApply:
			require.Len(t, m.Entries, 1)
			require.Len(t, m.Responses, 1)
			applyResps = append(applyResps, m.Responses[0])
		default:
			t.Fatalf("unexpected: %v", m)
		}
	}

	// Third entry should not be returned to be applied until first entry's
	// application is acknowledged.
	for rn.HasReady() { // drain the Ready-s
		rd := rn.Ready()
		for _, m := range rd.Messages {
			require.NotEqual(t, raftpb.MsgStorageApply, m.Type, "unexpected message: %v", m)
		}
	}

	// Acknowledged first entry application.
	require.NoError(t, rn.Step(applyResps[0]))
	applyResps = applyResps[1:]

	// Third entry now returned for application.
	rd = rn.Ready()
	require.Len(t, rd.Messages, 1)
	m = rd.Messages[0]
	require.Equal(t, raftpb.MsgStorageApply, m.Type)
	require.Len(t, m.Entries, 1)
	applyResps = append(applyResps, m.Responses[0])

	// Acknowledged second and third entry application.
	for _, resp := range applyResps {
		require.NoError(t, rn.Step(resp))
	}
	applyResps = nil
}

type ignoreSizeHintMemStorage struct {
	*MemoryStorage
}

func (s *ignoreSizeHintMemStorage) Entries(lo, hi uint64, _ uint64) ([]raftpb.Entry, error) {
	return s.MemoryStorage.Entries(lo, hi, math.MaxUint64)
}

// TestNodeCommitPaginationAfterRestart regression tests a scenario in which the
// Storage's Entries size limitation is slightly more permissive than Raft's
// internal one. The original bug was the following:
//
//   - node learns that index 11 (or 100, doesn't matter) is committed
//   - nextCommittedEnts returns index 1..10 in CommittedEntries due to size limiting.
//     However, index 10 already exceeds maxBytes, due to a user-provided impl of Entries.
//   - Commit index gets bumped to 10
//   - the node persists the HardState, but crashes before applying the entries
//   - upon restart, the storage returns the same entries, but `slice` takes a different code path
//     (since it is now called with an upper bound of 10) and removes the last entry.
//   - Raft emits a HardState with a regressing commit index.
//
// A simpler version of this test would have the storage return a lot less entries than dictated
// by maxSize (for example, exactly one entry) after the restart, resulting in a larger regression.
// This wouldn't need to exploit anything about Raft-internal code paths to fail.
func TestNodeCommitPaginationAfterRestart(t *testing.T) {
	s := &ignoreSizeHintMemStorage{
		MemoryStorage: newTestMemoryStorage(withPeers(1)),
	}
	persistedHardState := raftpb.HardState{
		Term:   1,
		Vote:   1,
		Commit: 10,
	}

	s.hardState = persistedHardState
	entries := make([]raftpb.Entry, 10)
	var size uint64
	for i := range entries {
		ent := raftpb.Entry{
			Term:  1,
			Index: uint64(i + 1),
			Type:  raftpb.EntryNormal,
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

	rn, err := NewRawNode(cfg)
	require.NoError(t, err)

	rd := rn.Ready()
	assert.False(t, !IsEmptyHardState(rd.HardState) && rd.HardState.Commit < persistedHardState.Commit,
		"HardState regressed: Commit %d -> %d\nCommitting:\n%+v",
		persistedHardState.Commit, rd.HardState.Commit,
		DescribeEntries(rd.CommittedEntries, func(data []byte) string { return fmt.Sprintf("%q", data) }))
}
