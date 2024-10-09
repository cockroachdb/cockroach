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
	// Initially, the peer is giving support to all other peers.
	isSupporting:    true,
	supportForEpoch: 1,

	// Initially, the peer is receiving support from all other peers.
	isSupported:      true,
	supportFromEpoch: 1,
}

// MockStoreLiveness is a mock implementation of StoreLiveness. It initially
// treats all store to store connections as live, but it can be configured to
// withdraw support, grant support, and bump the supported epoch to/from any two
// peers.
//
// Each peer's state can be altered independently This makes it possible to
// construct a uni-directional partition.
type MockStoreLiveness struct {
	// state is a map, where state[i] represents the liveness entry for peer i.
	state map[pb.PeerID]livenessEntry

	// supportExpired controls whether this peer considers the leader support
	// expired or not.
	supportExpired bool
}

var _ StoreLiveness = MockStoreLiveness{}

func NewMockStoreLiveness() *MockStoreLiveness {
	return &MockStoreLiveness{
		state:          make(map[pb.PeerID]livenessEntry),
		supportExpired: false,
	}
}

// maybeCreateStoreLivenessEntry creates a new state entry for the given peer if
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

// bumpSupportForEpoch bumps the supportFor epoch for the given peer.
func (m *MockStoreLiveness) bumpSupportForEpoch(id pb.PeerID) {
	m.maybeCreateStoreLivenessEntry(id)
	entry := m.state[id]
	entry.supportForEpoch++
	m.state[id] = entry
}

// bumpSupportFromEpoch bumps the supportFrom epoch for the given peer.
func (m *MockStoreLiveness) bumpSupportFromEpoch(id pb.PeerID) {
	m.maybeCreateStoreLivenessEntry(id)
	entry := m.state[id]
	entry.supportFromEpoch++
	m.state[id] = entry
}

// grantSupportFor grants support for the given peer.
func (m *MockStoreLiveness) grantSupportFor(id pb.PeerID) {
	m.maybeCreateStoreLivenessEntry(id)
	entry := m.state[id]
	entry.isSupporting = true
	m.state[id] = entry
}

// grantSupportFrom grants support from the given peer.
func (m *MockStoreLiveness) grantSupportFrom(id pb.PeerID) {
	m.maybeCreateStoreLivenessEntry(id)
	entry := m.state[id]
	entry.isSupported = true
	m.state[id] = entry
}

// WithdrawSupportFor withdraws support for the given peer.
func (m *MockStoreLiveness) WithdrawSupportFor(id pb.PeerID) {
	m.maybeCreateStoreLivenessEntry(id)
	entry := m.state[id]
	entry.isSupporting = false
	m.state[id] = entry
}

// withdrawSupportFrom withdraws support from the given peer.
func (m *MockStoreLiveness) withdrawSupportFrom(id pb.PeerID) {
	m.maybeCreateStoreLivenessEntry(id)
	entry := m.state[id]
	entry.isSupported = false
	m.state[id] = entry
}

// setSupportExpired explicitly controls what SupportExpired returns regardless
// of the timestamp.
func (m *MockStoreLiveness) setSupportExpired(expired bool) {
	m.supportExpired = expired
}

// LivenessFabric is a global view of the store liveness state.
type LivenessFabric struct {
	// state is an array, where state[i] represents the MockStoreLiveness entry
	// for peer i.
	state map[pb.PeerID]*MockStoreLiveness
}

// NewLivenessFabric initializes and returns a LivenessFabric.
func NewLivenessFabric() *LivenessFabric {
	state := make(map[pb.PeerID]*MockStoreLiveness)
	return &LivenessFabric{
		state: state,
	}
}

// MaybeAddPeer adds a peer to the liveness fabric if it doesn't already exist.
// Also, it iterates over all existing peers and adds the new peer to their
// state.
func (l *LivenessFabric) MaybeAddPeer(id pb.PeerID) {
	if _, ok := l.state[id]; !ok {
		l.state[id] = NewMockStoreLiveness()

		// Iterate over all liveness stores in the fabric, and add the new peer to
		// their state.
		for _, storeLiveness := range l.state {
			storeLiveness.maybeCreateStoreLivenessEntry(id)
		}
	}
}

// GetStoreLiveness return the MockStoreLiveness for the given peer.
func (l *LivenessFabric) GetStoreLiveness(id pb.PeerID) *MockStoreLiveness {
	l.MaybeAddPeer(id)
	return l.state[id]
}

// BumpEpoch bidirectionally bumps the support epoch from `from` to `to`.
func (l *LivenessFabric) BumpEpoch(from pb.PeerID, to pb.PeerID) {
	l.MaybeAddPeer(from)
	l.state[from].bumpSupportForEpoch(to)

	l.MaybeAddPeer(to)
	l.state[to].bumpSupportFromEpoch(from)
}

// WithdrawSupport bidirectionally withdraws support from `from` to `to`.
func (l *LivenessFabric) WithdrawSupport(from pb.PeerID, to pb.PeerID) {
	l.MaybeAddPeer(from)
	l.state[from].WithdrawSupportFor(to)

	l.MaybeAddPeer(to)
	l.state[to].withdrawSupportFrom(from)
}

// WithdrawSupport bidirectionally grants support `from` from to `to`.
func (l *LivenessFabric) GrantSupport(from pb.PeerID, to pb.PeerID) {
	l.MaybeAddPeer(from)
	l.state[from].grantSupportFor(to)

	l.MaybeAddPeer(to)
	l.state[to].grantSupportFrom(from)
}

// SetSupportExpired explicitly controls what SupportExpired returns regardless
// of the timestamp.
func (l *LivenessFabric) SetSupportExpired(peer pb.PeerID, expired bool) {
	l.MaybeAddPeer(peer)
	l.state[peer].setSupportExpired(expired)
}

// WithdrawAllSupportForPeer causes all peers in the liveness fabric to withdraw
// their support to the target peer.
func (l *LivenessFabric) WithdrawAllSupportForPeer(target pb.PeerID) {
	for curID := range l.state {
		l.WithdrawSupport(curID, target)
	}
}

// GrantAllSupportForPeer causes all peers in the liveness fabric to grant
// their support to the target peer.
func (l *LivenessFabric) GrantAllSupportForPeer(targetID pb.PeerID) {
	for curID := range l.state {
		l.GrantSupport(curID, targetID)
	}
}

// BumpAllSupportEpochs bumps all the support epochs in the liveness fabric.
func (l *LivenessFabric) BumpAllSupportEpochs() {
	for s1ID, storeLiveness := range l.state {
		for s2ID := range storeLiveness.state {
			l.BumpEpoch(s1ID, s2ID)
		}
	}
}
