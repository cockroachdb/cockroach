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
	id pb.PeerID

	// state is a map, where state[i] represents the liveness entry for peer i.
	state map[pb.PeerID]livenessEntry

	// supportExpired controls whether this peer considers the leader support
	// expired or not.
	supportExpired bool
}

var _ StoreLiveness = &MockStoreLiveness{}

func NewMockStoreLiveness(id pb.PeerID) *MockStoreLiveness {
	return &MockStoreLiveness{
		id:             id,
		state:          make(map[pb.PeerID]livenessEntry),
		supportExpired: false,
	}
}

// createStoreLivenessEntry creates a new state entry for the given peer.
func (m *MockStoreLiveness) createStoreLivenessEntry(id pb.PeerID) {
	if _, exists := m.state[id]; exists {
		panic("attempting to create a store liveness entry that already exists")
	}

	m.state[id] = initLivenessEntry
}

// SupportFor implements the StoreLiveness interface.
func (m *MockStoreLiveness) SupportFor(id pb.PeerID) (pb.Epoch, bool) {
	entry, exists := m.state[id]
	if !exists {
		panic("attempting to call SupportFor() for a non-existing entry")
	}
	return entry.supportForEpoch, entry.isSupporting
}

// SupportFrom implements the StoreLiveness interface.
func (m *MockStoreLiveness) SupportFrom(id pb.PeerID) (pb.Epoch, hlc.Timestamp) {
	entry, exists := m.state[id]
	if !exists {
		panic("attempting to call SupportFrom() for a non-existing entry")
	}

	if entry.isSupported {
		return entry.supportFromEpoch, hlc.MaxTimestamp
	}
	return 0, hlc.Timestamp{}
}

// SupportFromEnabled implements the StoreLiveness interface.
func (*MockStoreLiveness) SupportFromEnabled() bool {
	return true
}

// SupportExpired implements the StoreLiveness interface.
func (m *MockStoreLiveness) SupportExpired(ts hlc.Timestamp) bool {
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
	entry, exists := m.state[id]
	if !exists {
		panic("attempting to call bumpSupportForEpoch() for a non-existing entry")
	}

	entry.supportForEpoch++
	m.state[id] = entry
}

// bumpSupportFromEpoch bumps the supportFrom epoch for the given peer.
func (m *MockStoreLiveness) bumpSupportFromEpoch(id pb.PeerID) {
	entry, exists := m.state[id]
	if !exists {
		panic("attempting to call bumpSupportFromEpoch() for a non-existing entry")
	}

	entry.supportFromEpoch++
	m.state[id] = entry
}

// grantSupportFor grants support for the given peer.
func (m *MockStoreLiveness) grantSupportFor(id pb.PeerID) {
	entry, exists := m.state[id]
	if !exists {
		panic("attempting to call grantSupportFor() for a non-existing entry")
	}

	entry.isSupporting = true
	m.state[id] = entry
}

// grantSupportFrom grants support from the given peer.
func (m *MockStoreLiveness) grantSupportFrom(id pb.PeerID) {
	entry, exists := m.state[id]
	if !exists {
		panic("attempting to call grantSupportFrom() for a non-existing entry")
	}

	entry.isSupported = true
	m.state[id] = entry
}

// withdrawSupportFor withdraws support for the given peer.
func (m *MockStoreLiveness) withdrawSupportFor(id pb.PeerID) {
	entry, exists := m.state[id]
	if !exists {
		panic("attempting to call withdrawSupportFor() for a non-existing entry")
	}

	entry.isSupporting = false
	m.state[id] = entry
}

// withdrawSupportFrom withdraws support from the given peer.
func (m *MockStoreLiveness) withdrawSupportFrom(id pb.PeerID) {
	entry, exists := m.state[id]
	if !exists {
		panic("attempting to call withdrawSupportFrom() for a non-existing entry")
	}

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
	return NewLivenessFabricWithPeers()
}

// NewLivenessFabricWithPeers initializes and returns a LivenessFabric with the
// given peer IDs provided.
func NewLivenessFabricWithPeers(peers ...pb.PeerID) *LivenessFabric {
	state := make(map[pb.PeerID]*MockStoreLiveness)
	fabric := &LivenessFabric{
		state: state,
	}

	for _, peer := range peers {
		fabric.AddPeer(peer)
	}

	return fabric
}

// AddPeer adds a peer to the liveness fabric.
func (l *LivenessFabric) AddPeer(id pb.PeerID) {
	if _, exists := l.state[id]; exists {
		panic("attempting to add a peer that already exists")
	}

	l.state[id] = NewMockStoreLiveness(id)
	l.state[id].createStoreLivenessEntry(id)

	// Iterate over all liveness stores in the fabric, and add the new peer to
	// their state.
	for _, storeLiveness := range l.state {
		// We added our self above.
		if storeLiveness.id == id {
			continue
		}

		storeLiveness.createStoreLivenessEntry(id)
		l.state[id].createStoreLivenessEntry(storeLiveness.id)
	}
}

// GetStoreLiveness return the MockStoreLiveness for the given peer.
func (l *LivenessFabric) GetStoreLiveness(id pb.PeerID) *MockStoreLiveness {
	entry, exists := l.state[id]
	if !exists {
		panic("attempting to call GetStoreLiveness() for a non-existing id")
	}
	return entry
}

// BumpEpoch bumps the epoch supported by fromID for forID and starts supporting
// the new epoch. We also update state on forID to reflect support at this new
// epoch.
func (l *LivenessFabric) BumpEpoch(fromID pb.PeerID, forID pb.PeerID) {
	fromEntry, exists := l.state[fromID]
	if !exists {
		panic("attempting to call BumpEpoch() for a non-existing fromID entry")
	}
	fromEntry.bumpSupportForEpoch(forID)
	fromEntry.grantSupportFor(forID)

	forEntry, exists := l.state[forID]
	if !exists {
		panic("attempting to call BumpEpoch() for a non-existing forID entry")
	}
	forEntry.bumpSupportFromEpoch(fromID)
	forEntry.grantSupportFrom(fromID)
}

// WithdrawSupportFor causes the store liveness SupportFor() to return
// not supported when fromID calls it for forID.
func (l *LivenessFabric) WithdrawSupportFor(fromID pb.PeerID, forID pb.PeerID) {
	fromEntry, exists := l.state[fromID]
	if !exists {
		panic("attempting to call WithdrawSupportFor() for a non-existing fromID entry")
	}
	fromEntry.withdrawSupportFor(forID)
}

// WithdrawSupportFrom causes the store liveness SupportFrom() to return not
// supported when fromID calls it for forID.
func (l *LivenessFabric) WithdrawSupportFrom(fromID pb.PeerID, forID pb.PeerID) {
	fromEntry, exists := l.state[fromID]
	if !exists {
		panic("attempting to call WithdrawSupportFrom() for a non-existing fromID entry")
	}
	fromEntry.withdrawSupportFrom(forID)
}

// WithdrawSupport withdraws the support by fromID for forID. We also update
// state on forID to reflect the withdrawal of support.
func (l *LivenessFabric) WithdrawSupport(fromID pb.PeerID, forID pb.PeerID) {
	l.WithdrawSupportFor(fromID, forID)
	l.WithdrawSupportFrom(forID, fromID)
}

// GrantSupportFor causes the store liveness SupportFor() to return supported
// when fromID calls it for forID.
func (l *LivenessFabric) GrantSupportFor(fromID pb.PeerID, forID pb.PeerID) {
	fromEntry, exists := l.state[fromID]
	if !exists {
		panic("attempting to call GrantSupportFor() for a non-existing fromID entry")
	}
	fromEntry.grantSupportFor(forID)
}

// GrantSupportFrom causes the store liveness SupportFrom() to return supported
// when fromID calls it for forID.
func (l *LivenessFabric) GrantSupportFrom(fromID pb.PeerID, forID pb.PeerID) {
	fromEntry, exists := l.state[fromID]
	if !exists {
		panic("attempting to call GrantSupportFrom() for a non-existing fromID entry")
	}
	fromEntry.grantSupportFrom(forID)
}

// GrantSupport grants the support by fromID for forID. We also update state on
// forID to reflect the support grant.
func (l *LivenessFabric) GrantSupport(fromID pb.PeerID, forID pb.PeerID) {
	l.GrantSupportFor(fromID, forID)
	l.GrantSupportFrom(forID, fromID)
}

// SetSupportExpired explicitly controls what SupportExpired returns regardless
// of the timestamp supplied to it.
func (l *LivenessFabric) SetSupportExpired(peer pb.PeerID, expired bool) {
	entry, exists := l.state[peer]
	if !exists {
		panic("attempting to call SetSupportExpired() for a non-existing peer entry")
	}
	entry.setSupportExpired(expired)
}

// WithdrawSupportForPeerFromAllPeers withdraws support for the target peer from
// all peers in the liveness fabric.
func (l *LivenessFabric) WithdrawSupportForPeerFromAllPeers(targetID pb.PeerID) {
	for curID := range l.state {
		l.WithdrawSupport(curID, targetID)
	}
}

// GrantSupportForPeerFromAllPeers grants support for the target peer from
// // all peers in the liveness fabric.
func (l *LivenessFabric) GrantSupportForPeerFromAllPeers(targetID pb.PeerID) {
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

// Isolate isolates the target peer from all other peers in the liveness fabric.
func (l *LivenessFabric) Isolate(targetID pb.PeerID) {
	for id := range l.state {
		if id == targetID {
			// We don't want to withdraw the support from our self.
			continue
		}

		l.WithdrawSupport(id, targetID)
		l.WithdrawSupport(targetID, id)
	}
}

// UnIsolate is the opposite of Isolate(). It unisolates the target peer from
// all other peers in the liveness fabric.
func (l *LivenessFabric) UnIsolate(targetID pb.PeerID) {
	for id := range l.state {
		if id == targetID {
			// We don't have to grant support to our self since Isolate() doesn't
			// withdraw it.
			continue
		}

		l.GrantSupport(id, targetID)
		l.GrantSupport(targetID, id)
	}
}
