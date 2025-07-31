// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tracker

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/raft/quorum"
	"github.com/cockroachdb/cockroach/pkg/raft/raftlogger"
	pb "github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/raft/raftstoreliveness"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestFortificationEnabled(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		storeLiveness raftstoreliveness.StoreLiveness
		expectEnabled bool
	}{
		{
			storeLiveness: raftstoreliveness.Disabled{},
			expectEnabled: false,
		},
		{
			storeLiveness: raftstoreliveness.AlwaysLive{},
			expectEnabled: true,
		},
	}

	for _, tc := range testCases {
		cfg := quorum.MakeEmptyConfig()
		fortificationTracker := NewFortificationTracker(&cfg, tc.storeLiveness, raftlogger.DiscardLogger)
		require.Equal(t, tc.expectEnabled, fortificationTracker.FortificationEnabledForTerm())
	}
}

func TestLeadSupportUntil(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ts := func(ts int64) hlc.Timestamp {
		return hlc.Timestamp{
			WallTime: ts,
		}
	}

	mockLiveness3Peers := makeMockStoreLiveness(
		map[pb.PeerID]mockLivenessEntry{
			1: makeMockLivenessEntry(10, ts(10)),
			2: makeMockLivenessEntry(20, ts(15)),
			3: makeMockLivenessEntry(30, ts(20)),
		},
	)

	testCases := []struct {
		ids           []pb.PeerID
		storeLiveness raftstoreliveness.StoreLiveness
		setup         func(tracker *FortificationTracker)
		expTS         hlc.Timestamp
	}{
		{
			ids:           []pb.PeerID{1, 2, 3},
			storeLiveness: mockLiveness3Peers,
			setup: func(supportTracker *FortificationTracker) {
				// No fortification recorded.
			},
			expTS: hlc.Timestamp{},
		},
		{
			ids:           []pb.PeerID{1, 2, 3},
			storeLiveness: mockLiveness3Peers,
			setup: func(supportTracker *FortificationTracker) {
				supportTracker.RecordFortification(1, 10)
			},
			expTS: hlc.Timestamp{},
		},
		{
			ids:           []pb.PeerID{1, 2, 3},
			storeLiveness: mockLiveness3Peers,
			setup: func(supportTracker *FortificationTracker) {
				supportTracker.RecordFortification(1, 10)
				supportTracker.RecordFortification(3, 30)
			},
			expTS: ts(10),
		},
		{
			ids:           []pb.PeerID{1, 2, 3},
			storeLiveness: mockLiveness3Peers,
			setup: func(supportTracker *FortificationTracker) {
				supportTracker.RecordFortification(1, 10)
				supportTracker.RecordFortification(3, 30)
				supportTracker.RecordFortification(2, 20)
			},
			expTS: ts(15),
		},
		{
			ids:           []pb.PeerID{1, 2, 3},
			storeLiveness: mockLiveness3Peers,
			setup: func(supportTracker *FortificationTracker) {
				// Record fortification at epochs at expired epochs.
				supportTracker.RecordFortification(1, 9)
				supportTracker.RecordFortification(3, 29)
				supportTracker.RecordFortification(2, 19)
			},
			expTS: hlc.Timestamp{},
		},
		{
			ids:           []pb.PeerID{1, 2, 3},
			storeLiveness: mockLiveness3Peers,
			setup: func(supportTracker *FortificationTracker) {
				// Record fortification at newer epochs than what are present in
				// StoreLiveness.
				//
				// NB: This is possible if there is a race between store liveness
				// heartbeats updates and fortification responses.
				supportTracker.RecordFortification(1, 11)
				supportTracker.RecordFortification(3, 31)
				supportTracker.RecordFortification(2, 21)
			},
			expTS: hlc.Timestamp{},
		},
		{
			ids:           []pb.PeerID{1, 2, 3},
			storeLiveness: mockLiveness3Peers,
			setup: func(supportTracker *FortificationTracker) {
				// One of the epochs being supported is expired.
				supportTracker.RecordFortification(1, 10)
				supportTracker.RecordFortification(3, 29) // expired
				supportTracker.RecordFortification(2, 20)
			},
			expTS: ts(10),
		},
		{
			ids:           []pb.PeerID{1, 2, 3},
			storeLiveness: mockLiveness3Peers,
			setup: func(supportTracker *FortificationTracker) {
				// Two of the epochs being supported is expired.
				supportTracker.RecordFortification(1, 10)
				supportTracker.RecordFortification(3, 29) // expired
				supportTracker.RecordFortification(2, 19) // expired
			},
			expTS: hlc.Timestamp{},
		},
	}

	for _, tc := range testCases {
		cfg := quorum.MakeEmptyConfig()
		for _, id := range tc.ids {
			cfg.Voters[0][id] = struct{}{}
		}
		fortificationTracker := NewFortificationTracker(&cfg, tc.storeLiveness, raftlogger.DiscardLogger)

		tc.setup(fortificationTracker)
		require.Equal(t, tc.expTS, fortificationTracker.LeadSupportUntil(pb.StateLeader))
	}
}

func TestIsFortifiedBy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ts := func(ts int64) hlc.Timestamp {
		return hlc.Timestamp{
			WallTime: ts,
		}
	}

	mockLivenessOnePeer := makeMockStoreLiveness(
		map[pb.PeerID]mockLivenessEntry{
			1: makeMockLivenessEntry(10, ts(20)),
		},
	)

	testCases := []struct {
		ids           []pb.PeerID
		storeLiveness raftstoreliveness.StoreLiveness
		setup         func(tracker *FortificationTracker)
		expSupported  bool
		expFortified  bool
	}{
		{
			ids: []pb.PeerID{1},
			// No support recorded at the store liveness fabric.
			storeLiveness: makeMockStoreLiveness(map[pb.PeerID]mockLivenessEntry{}),
			setup: func(fortificationTracker *FortificationTracker) {
				// No support recorded.
			},
			expSupported: false,
			expFortified: false,
		},
		{
			ids:           []pb.PeerID{1},
			storeLiveness: mockLivenessOnePeer,
			setup: func(fortificationTracker *FortificationTracker) {
				// No support recorded.
			},
			expSupported: true,
			expFortified: false,
		},
		{
			ids:           []pb.PeerID{2},
			storeLiveness: mockLivenessOnePeer,
			setup: func(fortificationTracker *FortificationTracker) {
				// Support recorded for a different follower than the one in
				// storeLiveness.
				fortificationTracker.RecordFortification(2, 10)
			},
			expSupported: true,
			expFortified: false,
		},
		{
			ids:           []pb.PeerID{1},
			storeLiveness: mockLivenessOnePeer,
			setup: func(fortificationTracker *FortificationTracker) {
				// Support recorded for an expired epoch.
				fortificationTracker.RecordFortification(1, 9)
			},
			expSupported: true,
			expFortified: false,
		},
		{
			ids:           []pb.PeerID{1},
			storeLiveness: mockLivenessOnePeer,
			setup: func(fortificationTracker *FortificationTracker) {
				// Record support at newer epochs than what are present in
				// StoreLiveness.
				//
				// NB: This is possible if there is a race between store liveness
				// heartbeats updates and fortification responses.
				fortificationTracker.RecordFortification(1, 11)
			},
			expSupported: true,
			expFortified: false,
		},
		{
			ids:           []pb.PeerID{1},
			storeLiveness: mockLivenessOnePeer,
			setup: func(fortificationTracker *FortificationTracker) {
				// Record support at the same epoch as the storeLiveness.
				fortificationTracker.RecordFortification(1, 10)
			},
			expSupported: true,
			expFortified: true,
		},
	}

	for _, tc := range testCases {
		cfg := quorum.MakeEmptyConfig()
		for _, id := range tc.ids {
			cfg.Voters[0][id] = struct{}{}
		}
		fortificationTracker := NewFortificationTracker(&cfg, tc.storeLiveness, raftlogger.DiscardLogger)

		tc.setup(fortificationTracker)
		isFortified, isSupported := fortificationTracker.IsFortifiedBy(1)
		require.Equal(t, tc.expSupported, isSupported)
		require.Equal(t, tc.expFortified, isFortified)
	}
}

// TestQuorumActive ensures that we correctly determine whether a leader's
// quorum is active or not.
func TestQuorumActive(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ts := func(ts int64) hlc.Timestamp {
		return hlc.Timestamp{
			WallTime: ts,
		}
	}
	mockLiveness := makeMockStoreLiveness(
		map[pb.PeerID]mockLivenessEntry{
			1: makeMockLivenessEntry(10, ts(10)),
			2: makeMockLivenessEntry(20, ts(15)),
			3: makeMockLivenessEntry(30, ts(20)),
		},
	)

	testCases := []struct {
		setup           func(tracker *FortificationTracker)
		curTS           hlc.Timestamp
		expQuorumActive bool
	}{
		{
			setup: func(supportTracker *FortificationTracker) {
				// No fortification recorded.
			},
			curTS:           ts(10),
			expQuorumActive: false,
		},
		{
			setup: func(supportTracker *FortificationTracker) {
				supportTracker.RecordFortification(1, 10)
			},
			curTS:           ts(10),
			expQuorumActive: false,
		},
		{
			setup: func(supportTracker *FortificationTracker) {
				supportTracker.RecordFortification(1, 10)
				supportTracker.RecordFortification(3, 30)
			},
			curTS:           ts(9),
			expQuorumActive: true,
		},
		{
			setup: func(supportTracker *FortificationTracker) {
				supportTracker.RecordFortification(1, 10)
				supportTracker.RecordFortification(3, 30)
			},
			curTS:           ts(14),
			expQuorumActive: false,
		},
		{
			setup: func(supportTracker *FortificationTracker) {
				supportTracker.RecordFortification(1, 10)
				supportTracker.RecordFortification(3, 30)
				supportTracker.RecordFortification(2, 20)
			},
			curTS:           ts(14),
			expQuorumActive: true,
		},
		{
			setup: func(supportTracker *FortificationTracker) {
				supportTracker.RecordFortification(1, 10)
				supportTracker.RecordFortification(3, 30)
				supportTracker.RecordFortification(2, 20)
			},
			curTS:           ts(16),
			expQuorumActive: false,
		},
		{
			setup: func(supportTracker *FortificationTracker) {
				// Record fortification at epochs at expired epochs.
				supportTracker.RecordFortification(1, 9)
				supportTracker.RecordFortification(3, 29)
				supportTracker.RecordFortification(2, 19)
			},
			curTS:           ts(10),
			expQuorumActive: false,
		},
		{
			setup: func(supportTracker *FortificationTracker) {
				// Record fortification at newer epochs than what are present in
				// StoreLiveness.
				//
				// NB: This is possible if there is a race between store liveness
				// heartbeats updates and fortification responses.
				supportTracker.RecordFortification(1, 11)
				supportTracker.RecordFortification(3, 31)
				supportTracker.RecordFortification(2, 21)
			},
			expQuorumActive: false,
		},
		{
			setup: func(supportTracker *FortificationTracker) {
				// One of the epochs being supported is expired.
				supportTracker.RecordFortification(1, 10)
				supportTracker.RecordFortification(3, 29) // expired
				supportTracker.RecordFortification(2, 20)
			},
			curTS:           ts(5),
			expQuorumActive: true,
		},
		{
			setup: func(supportTracker *FortificationTracker) {
				// Two of the epochs being supported is expired.
				supportTracker.RecordFortification(1, 10)
				supportTracker.RecordFortification(3, 29) // expired
				supportTracker.RecordFortification(2, 19) // expired
			},
			curTS:           ts(10),
			expQuorumActive: false,
		},
	}

	for i, tc := range testCases {
		mockLiveness.curTS = tc.curTS
		cfg := quorum.MakeEmptyConfig()

		for _, id := range []pb.PeerID{1, 2, 3} {
			cfg.Voters[0][id] = struct{}{}
		}
		fortificationTracker := NewFortificationTracker(&cfg, mockLiveness, raftlogger.DiscardLogger)

		tc.setup(fortificationTracker)
		require.Equal(t, tc.expQuorumActive, fortificationTracker.QuorumActive(), "#%d %s %s",
			i, fortificationTracker.LeadSupportUntil(pb.StateLeader), tc.curTS)
	}
}

// TestCanDefortify tests whether a leader can safely de-fortify or not based
// on some tracked state.
func TestCanDefortify(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ts := func(ts int64) hlc.Timestamp {
		return hlc.Timestamp{
			WallTime: ts,
		}
	}
	mockLiveness := makeMockStoreLiveness(
		map[pb.PeerID]mockLivenessEntry{
			1: makeMockLivenessEntry(10, ts(10)),
			2: makeMockLivenessEntry(20, ts(15)),
			3: makeMockLivenessEntry(30, ts(20)),
		},
	)

	testCases := []struct {
		setup               func(tracker *FortificationTracker)
		curTS               hlc.Timestamp
		expCanDefortify     bool
		expLeadSupportUntil hlc.Timestamp
	}{
		{
			setup: func(ft *FortificationTracker) {
				ft.RecordFortification(1, 10)
				ft.RecordFortification(2, 20)
			},
			curTS:               ts(10),
			expLeadSupportUntil: ts(10),
			expCanDefortify:     false,
		},
		{
			setup: func(ft *FortificationTracker) {
				ft.RecordFortification(1, 10)
				ft.RecordFortification(2, 20)
			},
			curTS:               ts(12),
			expLeadSupportUntil: ts(10),
			expCanDefortify:     true,
		},
		{
			setup: func(ft *FortificationTracker) {
				ft.RecordFortification(1, 10)
				ft.RecordFortification(2, 20)
				ft.RecordFortification(3, 30)
			},
			curTS:               ts(12),
			expLeadSupportUntil: ts(15),
			expCanDefortify:     false,
		},
		{
			setup: func(ft *FortificationTracker) {
				ft.RecordFortification(1, 10)
				ft.RecordFortification(2, 20)
				ft.RecordFortification(3, 30)
			},
			curTS:               ts(18),
			expLeadSupportUntil: ts(15),
			expCanDefortify:     true,
		},
		{
			setup: func(ft *FortificationTracker) {
				ft.RecordFortification(1, 10)
				ft.RecordFortification(2, 20)
				ft.RecordFortification(3, 30)
			},
			curTS: ts(10),
			// LeadSupportUntil = ts(15); however, because we don't call it explicitly,
			// we should be able to de-fortify.
			expCanDefortify: true,
		},
		{
			setup: func(ft *FortificationTracker) {
				ft.term = 0 // empty term; nothing is being tracked in the fortification tracker
			},
			expCanDefortify: false,
		},
	}

	for _, tc := range testCases {
		mockLiveness.curTS = tc.curTS
		cfg := quorum.MakeEmptyConfig()

		for _, id := range []pb.PeerID{1, 2, 3} {
			cfg.Voters[0][id] = struct{}{}
		}
		ft := NewFortificationTracker(&cfg, mockLiveness, raftlogger.DiscardLogger)

		ft.Reset(10) // set non-zero term
		tc.setup(ft)
		if !tc.expLeadSupportUntil.IsEmpty() {
			require.Equal(t, tc.expLeadSupportUntil, ft.LeadSupportUntil(pb.StateLeader))
		}
		require.Equal(t, tc.expCanDefortify, ft.CanDefortify())
	}
}

// TestConfigChangeSafe tests whether a leader can safely propose a config
// change.
func TestConfigChangeSafe(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ts := func(ts int64) hlc.Timestamp {
		return hlc.Timestamp{
			WallTime: ts,
		}
	}

	testCases := []struct {
		afterConfigChange   func(sl *mockStoreLiveness, tracker *FortificationTracker)
		expConfigChangeSafe bool
		expLeadSupportUntil hlc.Timestamp
	}{
		{
			afterConfigChange: func(sl *mockStoreLiveness, ft *FortificationTracker) {
				// Nothing. r4 not providing store liveness support or fortified.
			},
			expConfigChangeSafe: false,
			expLeadSupportUntil: ts(15), // clamped at 15
		},
		{
			afterConfigChange: func(sl *mockStoreLiveness, ft *FortificationTracker) {
				// r4 providing store liveness support but not fortified.
				sl.liveness[4] = makeMockLivenessEntry(40, ts(5))
			},
			expConfigChangeSafe: false,
			expLeadSupportUntil: ts(15), // clamped at 15
		},
		{
			afterConfigChange: func(sl *mockStoreLiveness, ft *FortificationTracker) {
				// r4 fortified at earlier epoch.
				sl.liveness[4] = makeMockLivenessEntry(40, ts(5))
				ft.RecordFortification(4, 39)
			},
			expConfigChangeSafe: false,
			expLeadSupportUntil: ts(15), // clamped at 15
		},
		{
			afterConfigChange: func(sl *mockStoreLiveness, ft *FortificationTracker) {
				// r4 fortified, but support still lagging.
				sl.liveness[4] = makeMockLivenessEntry(40, ts(5))
				ft.RecordFortification(4, 40)
			},
			expConfigChangeSafe: false,
			expLeadSupportUntil: ts(15), // clamped at 15
		},
		{
			afterConfigChange: func(sl *mockStoreLiveness, ft *FortificationTracker) {
				// r4 fortified, support caught up.
				sl.liveness[4] = makeMockLivenessEntry(40, ts(15))
				ft.RecordFortification(4, 40)
			},
			expConfigChangeSafe: true,
			expLeadSupportUntil: ts(15),
		},
		{
			afterConfigChange: func(sl *mockStoreLiveness, ft *FortificationTracker) {
				// r4 fortified, support caught up.
				sl.liveness[4] = makeMockLivenessEntry(40, ts(25))
				ft.RecordFortification(4, 40)
			},
			expConfigChangeSafe: true,
			expLeadSupportUntil: ts(15),
		},
		{
			afterConfigChange: func(sl *mockStoreLiveness, ft *FortificationTracker) {
				// r4 fortified, support beyond previous config.
				sl.liveness[2] = makeMockLivenessEntry(20, ts(25))
				sl.liveness[4] = makeMockLivenessEntry(40, ts(25))
				ft.RecordFortification(4, 40)
			},
			expConfigChangeSafe: true,
			expLeadSupportUntil: ts(20),
		},
		{
			afterConfigChange: func(sl *mockStoreLiveness, ft *FortificationTracker) {
				// r4 not providing store liveness support or fortified. However,
				// support from other peers caught up.
				sl.liveness[1] = makeMockLivenessEntry(10, ts(15))
			},
			expConfigChangeSafe: true,
			expLeadSupportUntil: ts(15),
		},
		{
			afterConfigChange: func(sl *mockStoreLiveness, ft *FortificationTracker) {
				// r4 not providing store liveness support or fortified. However,
				// support from other peers beyond previous config.
				sl.liveness[1] = makeMockLivenessEntry(10, ts(20))
				sl.liveness[2] = makeMockLivenessEntry(20, ts(20))
			},
			expConfigChangeSafe: true,
			expLeadSupportUntil: ts(20),
		},
	}

	for _, tc := range testCases {
		mockLiveness := makeMockStoreLiveness(
			map[pb.PeerID]mockLivenessEntry{
				1: makeMockLivenessEntry(10, ts(10)),
				2: makeMockLivenessEntry(20, ts(15)),
				3: makeMockLivenessEntry(30, ts(20)),
			},
		)

		cfg := quorum.MakeEmptyConfig()
		for _, id := range []pb.PeerID{1, 2, 3} {
			cfg.Voters[0][id] = struct{}{}
		}
		ft := NewFortificationTracker(&cfg, mockLiveness, raftlogger.DiscardLogger)

		// Fortify the leader before the configuration change.
		ft.RecordFortification(1, 10)
		ft.RecordFortification(2, 20)
		ft.RecordFortification(3, 30)
		require.Equal(t, ts(15), ft.LeadSupportUntil(pb.StateLeader))

		// Perform a configuration change that adds r4 to the voter set.
		cfg.Voters[0][4] = struct{}{}

		tc.afterConfigChange(&mockLiveness, ft)

		require.Equal(t, tc.expConfigChangeSafe, ft.ConfigChangeSafe())
		require.Equal(t, tc.expLeadSupportUntil, ft.LeadSupportUntil(pb.StateLeader))
	}
}

type mockLivenessEntry struct {
	epoch pb.Epoch
	ts    hlc.Timestamp
}

func makeMockLivenessEntry(epoch pb.Epoch, ts hlc.Timestamp) mockLivenessEntry {
	return mockLivenessEntry{
		epoch: epoch,
		ts:    ts,
	}
}

type mockStoreLiveness struct {
	liveness map[pb.PeerID]mockLivenessEntry
	curTS    hlc.Timestamp
}

func makeMockStoreLiveness(liveness map[pb.PeerID]mockLivenessEntry) mockStoreLiveness {
	return mockStoreLiveness{
		liveness: liveness,
	}
}

// SupportFor implements the raftstoreliveness.StoreLiveness interface.
func (mockStoreLiveness) SupportFor(pb.PeerID) (pb.Epoch, bool) {
	panic("unimplemented")
}

// SupportFrom implements the raftstoreliveness.StoreLiveness interface.
func (m mockStoreLiveness) SupportFrom(id pb.PeerID) (pb.Epoch, hlc.Timestamp) {
	entry := m.liveness[id]
	return entry.epoch, entry.ts
}

// SupportFromEnabled implements the raftstoreliveness.StoreLiveness interface.
func (mockStoreLiveness) SupportFromEnabled() bool {
	return true
}

// SupportExpired implements the raftstoreliveness.StoreLiveness interface.
func (m mockStoreLiveness) SupportExpired(ts hlc.Timestamp) bool {
	return ts.IsEmpty() || ts.Less(m.curTS)
}
