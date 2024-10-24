// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package raftstoreliveness

import (
	pb "github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// livenessEntry is an entry in the MockStoreLiveness state.
type livenessEntry struct {
	// isSupporting controls whether supportFor returns true or false.
	isSupporting    bool
	supportForEpoch pb.Epoch

	// isSupported controls whether supportFrom returns true or false.
	isSupported      bool
	supportFromEpoch pb.Epoch
}

// initLivenessEntry is the initial state entry placed in MockStoreLiveness.
var initLivenessEntry = livenessEntry{
	// Initially, the node is giving support to all other nodes.
	isSupporting:    true,
	supportForEpoch: 1,

	// Initially, the node is receiving support from all other nodes.
	isSupported:      true,
	supportFromEpoch: 1,
}

// MockStoreLiveness is a mock implementation of StoreLiveness that
// is used in raft unit tests. It initially treats all store to store
// connections as live, but it can be configured to withdraw support, grant
// support, and bump the supported epoch to/from any two nodes.
//
// Each node's state can be altered independently. This makes it possible to
// not have a shared liveness fabric between all the nodes in the test. This is
// ideal for Raft unit tests as nodes are created independently. It also makes
// it possible to construct a uni-directional partition.
type MockStoreLiveness struct {
	state          map[pb.PeerID]livenessEntry
	supportExpired bool
}

var _ StoreLiveness = MockStoreLiveness{}

func NewMockStoreLiveness() *MockStoreLiveness {
	return &MockStoreLiveness{
		// Each node has its own liveness entries for other nodes.
		state: make(map[pb.PeerID]livenessEntry),

		// Controls whether this peer considers the leader support expired or not.
		supportExpired: false,
	}
}

// maybeCreateStoreLivenessEntry creates a new state entry for the given node if
// it doesn't exist already.
func (m MockStoreLiveness) maybeCreateStoreLivenessEntry(id pb.PeerID) {
	if _, ok := m.state[id]; !ok {
		m.state[id] = initLivenessEntry
	}
}

// SupportFor implements the StoreLiveness interface.
func (m MockStoreLiveness) SupportFor(id pb.PeerID) (pb.Epoch, bool) {
	m.maybeCreateStoreLivenessEntry(id)
	return m.state[id].supportForEpoch, m.state[id].isSupporting
}

// SupportFrom implements the StoreLiveness interface.
func (m MockStoreLiveness) SupportFrom(id pb.PeerID) (pb.Epoch, hlc.Timestamp) {
	m.maybeCreateStoreLivenessEntry(id)

	if m.state[id].isSupported {
		return m.state[id].supportFromEpoch, hlc.MaxTimestamp
	}

	return 0, hlc.Timestamp{}
}

// SupportFromEnabled implements the StoreLiveness interface.
func (MockStoreLiveness) SupportFromEnabled() bool {
	return true
}

// SupportExpired implements the StoreLiveness interface.
func (m MockStoreLiveness) SupportExpired(ts hlc.Timestamp) bool {
	if m.supportExpired {
		return true
	}

	// If not configured explicitly, infer from the supplied timestamp.
	switch ts {
	case hlc.Timestamp{}:
		return true
	case hlc.MaxTimestamp:
		return false
	default:
		panic("unexpected timestamp")
	}
}

// BumpSupportForEpoch bumps the supportFor epoch for the given node.
func (m *MockStoreLiveness) BumpSupportForEpoch(id pb.PeerID) {
	m.maybeCreateStoreLivenessEntry(id)
	entry := m.state[id]
	entry.supportForEpoch++
	m.state[id] = entry
}

// BumpSupportFromEpoch bumps the supportFrom epoch for the given node.
func (m *MockStoreLiveness) BumpSupportFromEpoch(id pb.PeerID) {
	m.maybeCreateStoreLivenessEntry(id)
	entry := m.state[id]
	entry.supportFromEpoch++
	m.state[id] = entry
}

// GrantSupportFor grants support for the given node.
func (m *MockStoreLiveness) GrantSupportFor(id pb.PeerID) {
	m.maybeCreateStoreLivenessEntry(id)
	entry := m.state[id]
	entry.isSupporting = true
	m.state[id] = entry
}

// WithdrawSupportFor withdraws support for the given node.
func (m *MockStoreLiveness) WithdrawSupportFor(id pb.PeerID) {
	m.maybeCreateStoreLivenessEntry(id)
	entry := m.state[id]
	entry.isSupporting = false
	m.state[id] = entry
}

// GrantSupportFrom grants support from the given node.
func (m *MockStoreLiveness) GrantSupportFrom(id pb.PeerID) {
	m.maybeCreateStoreLivenessEntry(id)
	entry := m.state[id]
	entry.isSupported = true
	m.state[id] = entry
}

// WithdrawSupportFrom withdraws support from the given node.
func (m *MockStoreLiveness) WithdrawSupportFrom(id pb.PeerID) {
	m.maybeCreateStoreLivenessEntry(id)
	entry := m.state[id]
	entry.isSupported = false
	m.state[id] = entry
}

// SetSupportExpired explicitly controls what SupportExpired returns regardless
// of the timestamp.
func (m *MockStoreLiveness) SetSupportExpired(expired bool) {
	m.supportExpired = expired
}
