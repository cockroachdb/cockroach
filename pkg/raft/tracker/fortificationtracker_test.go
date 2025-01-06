// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tracker

import (
	"fmt"
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
		ids                    []pb.PeerID
		storeLiveness          raftstoreliveness.StoreLiveness
		setup                  func(tracker *FortificationTracker)
		state                  pb.StateType
		initLeadSupportedUntil hlc.Timestamp
		expTS                  hlc.Timestamp
	}{
		{
			ids:           []pb.PeerID{1, 2, 3},
			storeLiveness: mockLiveness3Peers,
			setup: func(fortificationTracker *FortificationTracker) {
				// No fortification recorded.
			},
			state:                  pb.StateLeader,
			initLeadSupportedUntil: hlc.Timestamp{},
			expTS:                  hlc.Timestamp{},
		},
		{
			ids:           []pb.PeerID{1, 2, 3},
			storeLiveness: mockLiveness3Peers,
			setup: func(fortificationTracker *FortificationTracker) {
				fortificationTracker.RecordFortification(1, 10)
			},
			state:                  pb.StateLeader,
			initLeadSupportedUntil: hlc.Timestamp{},
			expTS:                  hlc.Timestamp{},
		},
		{
			ids:           []pb.PeerID{1, 2, 3},
			storeLiveness: mockLiveness3Peers,
			setup: func(fortificationTracker *FortificationTracker) {
				fortificationTracker.RecordFortification(1, 10)
				fortificationTracker.RecordFortification(3, 30)
			},
			state:                  pb.StateLeader,
			initLeadSupportedUntil: hlc.Timestamp{},
			expTS:                  ts(10),
		},
		{
			ids:           []pb.PeerID{1, 2, 3},
			storeLiveness: mockLiveness3Peers,
			setup: func(fortificationTracker *FortificationTracker) {
				fortificationTracker.RecordFortification(1, 10)
				fortificationTracker.RecordFortification(3, 30)
				fortificationTracker.RecordFortification(2, 20)
			},
			state:                  pb.StateLeader,
			initLeadSupportedUntil: hlc.Timestamp{},
			expTS:                  ts(15),
		},
		{
			ids:           []pb.PeerID{1, 2, 3},
			storeLiveness: mockLiveness3Peers,
			setup: func(fortificationTracker *FortificationTracker) {
				// Record fortification at epochs at expired epochs.
				fortificationTracker.RecordFortification(1, 9)
				fortificationTracker.RecordFortification(3, 29)
				fortificationTracker.RecordFortification(2, 19)
			},
			state:                  pb.StateLeader,
			initLeadSupportedUntil: hlc.Timestamp{},
			expTS:                  hlc.Timestamp{},
		},
		{
			ids:           []pb.PeerID{1, 2, 3},
			storeLiveness: mockLiveness3Peers,
			setup: func(fortificationTracker *FortificationTracker) {
				// Record fortification at newer epochs than what are present in
				// StoreLiveness.
				//
				// NB: This is possible if there is a race between store liveness
				// heartbeats updates and fortification responses.
				fortificationTracker.RecordFortification(1, 11)
				fortificationTracker.RecordFortification(3, 31)
				fortificationTracker.RecordFortification(2, 21)
			},
			state:                  pb.StateLeader,
			initLeadSupportedUntil: hlc.Timestamp{},
			expTS:                  hlc.Timestamp{},
		},
		{
			ids:           []pb.PeerID{1, 2, 3},
			storeLiveness: mockLiveness3Peers,
			setup: func(fortificationTracker *FortificationTracker) {
				// One of the epochs being supported is expired.
				fortificationTracker.RecordFortification(1, 10)
				fortificationTracker.RecordFortification(3, 29) // expired
				fortificationTracker.RecordFortification(2, 20)
			},
			state:                  pb.StateLeader,
			initLeadSupportedUntil: hlc.Timestamp{},
			expTS:                  ts(10),
		},
		{
			ids:           []pb.PeerID{1, 2, 3},
			storeLiveness: mockLiveness3Peers,
			setup: func(fortificationTracker *FortificationTracker) {
				// Two of the epochs being supported is expired.
				fortificationTracker.RecordFortification(1, 10)
				fortificationTracker.RecordFortification(3, 29) // expired
				fortificationTracker.RecordFortification(2, 19) // expired
			},
			state:                  pb.StateLeader,
			initLeadSupportedUntil: hlc.Timestamp{},
			expTS:                  hlc.Timestamp{},
		},
		{
			ids:           []pb.PeerID{1, 2, 3},
			storeLiveness: mockLiveness3Peers,
			setup: func(fortificationTracker *FortificationTracker) {
				// If we are stepping down, expect that LeadSupportUntil won't be
				// computed, and the previous value will be returned.
				fortificationTracker.BeginSteppingDown(1)
				fortificationTracker.RecordFortification(1, 10)
				fortificationTracker.RecordFortification(3, 30)
				fortificationTracker.RecordFortification(2, 20)
			},
			state:                  pb.StateLeader,
			initLeadSupportedUntil: ts(1),
			expTS:                  ts(1),
		},
	}

	for _, tc := range testCases {
		cfg := quorum.MakeEmptyConfig()
		for _, id := range tc.ids {
			cfg.Voters[0][id] = struct{}{}
		}
		fortificationTracker := NewFortificationTracker(&cfg, tc.storeLiveness, raftlogger.DiscardLogger)

		tc.setup(fortificationTracker)
		fortificationTracker.leaderMaxSupported.Forward(tc.initLeadSupportedUntil)
		fortificationTracker.ComputeLeadSupportUntil(tc.state)
		require.Equal(t, tc.expTS, fortificationTracker.LeadSupportUntil())
	}
}

// TestBeginSteppingDownUsesMaxTerm ensures that the max term is used when
// stepping down. This helps to simulate the situation where the leader receives
// multiple messages with different terms that indicate that it should step
// down.
func TestBeginSteppingDownUsesMaxTerm(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		setup               func(tracker *FortificationTracker)
		expSteppingDown     bool
		expSteppingDownTerm uint64
	}{
		{
			setup: func(fortificationTracker *FortificationTracker) {
				// Not stepping down.
			},
			expSteppingDown:     false,
			expSteppingDownTerm: 0,
		},
		{
			setup: func(fortificationTracker *FortificationTracker) {
				fortificationTracker.BeginSteppingDown(1)
			},
			expSteppingDown:     true,
			expSteppingDownTerm: 1,
		},
		{
			setup: func(fortificationTracker *FortificationTracker) {
				fortificationTracker.BeginSteppingDown(5)
			},
			expSteppingDown:     true,
			expSteppingDownTerm: 5,
		},
		{
			setup: func(fortificationTracker *FortificationTracker) {
				fortificationTracker.BeginSteppingDown(5)
				// The max steppingDown term should be used.
				fortificationTracker.BeginSteppingDown(7)
				fortificationTracker.BeginSteppingDown(4)
			},
			expSteppingDown:     true,
			expSteppingDownTerm: 7,
		},
	}

	for _, tc := range testCases {
		cfg := quorum.MakeEmptyConfig()
		fortificationTracker := NewFortificationTracker(&cfg, raftstoreliveness.AlwaysLive{},
			raftlogger.DiscardLogger)
		tc.setup(fortificationTracker)
		require.Equal(t, tc.expSteppingDown, fortificationTracker.SteppingDown())
		require.Equal(t, tc.expSteppingDownTerm, fortificationTracker.SteppingDownTerm())
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
			setup: func(fortificationTracker *FortificationTracker) {
				// No fortification recorded.
			},
			curTS:           ts(10),
			expQuorumActive: false,
		},
		{
			setup: func(fortificationTracker *FortificationTracker) {
				fortificationTracker.RecordFortification(1, 10)
			},
			curTS:           ts(10),
			expQuorumActive: false,
		},
		{
			setup: func(fortificationTracker *FortificationTracker) {
				fortificationTracker.RecordFortification(1, 10)
				fortificationTracker.RecordFortification(3, 30)
			},
			curTS:           ts(9),
			expQuorumActive: true,
		},
		{
			setup: func(fortificationTracker *FortificationTracker) {
				fortificationTracker.RecordFortification(1, 10)
				fortificationTracker.RecordFortification(3, 30)
			},
			curTS:           ts(14),
			expQuorumActive: false,
		},
		{
			setup: func(fortificationTracker *FortificationTracker) {
				fortificationTracker.RecordFortification(1, 10)
				fortificationTracker.RecordFortification(3, 30)
				fortificationTracker.RecordFortification(2, 20)
			},
			curTS:           ts(14),
			expQuorumActive: true,
		},
		{
			setup: func(fortificationTracker *FortificationTracker) {
				fortificationTracker.RecordFortification(1, 10)
				fortificationTracker.RecordFortification(3, 30)
				fortificationTracker.RecordFortification(2, 20)
			},
			curTS:           ts(16),
			expQuorumActive: false,
		},
		{
			setup: func(fortificationTracker *FortificationTracker) {
				// Record fortification at epochs at expired epochs.
				fortificationTracker.RecordFortification(1, 9)
				fortificationTracker.RecordFortification(3, 29)
				fortificationTracker.RecordFortification(2, 19)
			},
			curTS:           ts(10),
			expQuorumActive: false,
		},
		{
			setup: func(fortificationTracker *FortificationTracker) {
				// Record fortification at newer epochs than what are present in
				// StoreLiveness.
				//
				// NB: This is possible if there is a race between store liveness
				// heartbeats updates and fortification responses.
				fortificationTracker.RecordFortification(1, 11)
				fortificationTracker.RecordFortification(3, 31)
				fortificationTracker.RecordFortification(2, 21)
			},
			expQuorumActive: false,
		},
		{
			setup: func(fortificationTracker *FortificationTracker) {
				// One of the epochs being supported is expired.
				fortificationTracker.RecordFortification(1, 10)
				fortificationTracker.RecordFortification(3, 29) // expired
				fortificationTracker.RecordFortification(2, 20)
			},
			curTS:           ts(5),
			expQuorumActive: true,
		},
		{
			setup: func(fortificationTracker *FortificationTracker) {
				// Two of the epochs being supported is expired.
				fortificationTracker.RecordFortification(1, 10)
				fortificationTracker.RecordFortification(3, 29) // expired
				fortificationTracker.RecordFortification(2, 19) // expired
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
			i, fortificationTracker.LeadSupportUntil(), tc.curTS)
	}
}

// TestQuorumSupported ensures that we correctly determine whether a leader's
// quorum is supported or not.
func TestQuorumSupported(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ts := func(ts int64) hlc.Timestamp {
		return hlc.Timestamp{
			WallTime: ts,
		}
	}

	createJointQuorum := func(c0 []pb.PeerID, c1 []pb.PeerID) quorum.JointConfig {
		jointConfig := quorum.JointConfig{}

		// Populate the first joint config entry.
		if len(c0) > 0 {
			jointConfig[0] = make(quorum.MajorityConfig, len(c0))
		}
		for _, id := range c0 {
			jointConfig[0][id] = struct{}{}
		}

		// Populate the second joint config entry.
		if len(c1) > 0 {
			jointConfig[1] = make(quorum.MajorityConfig, len(c1))
		}
		for _, id := range c1 {
			jointConfig[1][id] = struct{}{}
		}
		return jointConfig
	}

	testCases := []struct {
		curTS              hlc.Timestamp
		voters             quorum.JointConfig
		storeLiveness      mockStoreLiveness
		expQuorumSupported bool
	}{
		{
			curTS:  ts(10),
			voters: createJointQuorum([]pb.PeerID{1, 2, 3}, []pb.PeerID{}),
			storeLiveness: makeMockStoreLiveness(
				// No support recorded.
				map[pb.PeerID]mockLivenessEntry{},
			),
			expQuorumSupported: false,
		},
		{
			curTS:  ts(10),
			voters: createJointQuorum([]pb.PeerID{1, 2, 3}, []pb.PeerID{}),
			storeLiveness: makeMockStoreLiveness(
				map[pb.PeerID]mockLivenessEntry{
					1: makeMockLivenessEntry(10, ts(10)),
					// Missing peer 2.
					3: makeMockLivenessEntry(30, ts(20)),
				},
			),
			expQuorumSupported: true,
		},
		{
			curTS:  ts(10),
			voters: createJointQuorum([]pb.PeerID{1, 2, 3}, []pb.PeerID{}),
			storeLiveness: makeMockStoreLiveness(
				map[pb.PeerID]mockLivenessEntry{
					1: makeMockLivenessEntry(10, ts(10)),
					// Missing peers 2 and 3.
				},
			),
			expQuorumSupported: false,
		},
		{
			curTS:  ts(10),
			voters: createJointQuorum([]pb.PeerID{1, 2, 3}, []pb.PeerID{}),
			storeLiveness: makeMockStoreLiveness(
				map[pb.PeerID]mockLivenessEntry{
					// Expired support for peer 1.
					1: makeMockLivenessEntry(10, ts(5)),
					2: makeMockLivenessEntry(20, ts(15)),
					3: makeMockLivenessEntry(30, ts(20)),
				},
			),
			expQuorumSupported: true,
		},
		{
			curTS:  ts(10),
			voters: createJointQuorum([]pb.PeerID{1, 2, 3}, []pb.PeerID{}),
			storeLiveness: makeMockStoreLiveness(
				map[pb.PeerID]mockLivenessEntry{
					// Expired support for peers 1 and 2.
					1: makeMockLivenessEntry(10, ts(5)),
					2: makeMockLivenessEntry(20, ts(5)),
					3: makeMockLivenessEntry(30, ts(20)),
				},
			),
			expQuorumSupported: false,
		},
		{
			curTS:  ts(10),
			voters: createJointQuorum([]pb.PeerID{1, 2, 3}, []pb.PeerID{}),
			storeLiveness: makeMockStoreLiveness(
				map[pb.PeerID]mockLivenessEntry{
					1: makeMockLivenessEntry(10, ts(10)),
					2: makeMockLivenessEntry(20, ts(15)),
					3: makeMockLivenessEntry(30, ts(20)),
				},
			),
			expQuorumSupported: true,
		},
		{
			curTS: ts(10),
			// Simulate a joint quorum when adding two more nodes.
			voters: createJointQuorum([]pb.PeerID{1, 2, 3}, []pb.PeerID{1, 2, 3, 4, 5}),
			storeLiveness: makeMockStoreLiveness(
				map[pb.PeerID]mockLivenessEntry{
					// Expired supported from 1 and 2.
					1: makeMockLivenessEntry(10, ts(5)),
					2: makeMockLivenessEntry(20, ts(5)),
					3: makeMockLivenessEntry(20, ts(15)),
					4: makeMockLivenessEntry(20, ts(15)),
					5: makeMockLivenessEntry(10, ts(15)),
				},
			),
			// Expect the quorum to NOT be supported since the one of the majorities
			// doesn't provide support.
			expQuorumSupported: false,
		},
		{
			curTS: ts(10),
			// Simulate a joint quorum when adding two more nodes.
			voters: createJointQuorum([]pb.PeerID{1, 2, 3}, []pb.PeerID{1, 2, 3, 4, 5}),
			storeLiveness: makeMockStoreLiveness(
				map[pb.PeerID]mockLivenessEntry{
					// Expired supported from 1 and 5.
					1: makeMockLivenessEntry(10, ts(5)),
					2: makeMockLivenessEntry(20, ts(15)),
					3: makeMockLivenessEntry(10, ts(15)),
					4: makeMockLivenessEntry(20, ts(15)),
					5: makeMockLivenessEntry(10, ts(5)),
				},
			),
			// Expect the quorum to be supported since the two majorities provided
			// support.
			expQuorumSupported: true,
		},
	}

	for _, tc := range testCases {
		tc.storeLiveness.curTS = tc.curTS
		cfg := quorum.MakeEmptyConfig()
		cfg.Voters = tc.voters
		fortificationTracker := NewFortificationTracker(&cfg, tc.storeLiveness,
			raftlogger.DiscardLogger)
		require.Equal(t, tc.expQuorumSupported, fortificationTracker.QuorumSupported())
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
			// LeadSupportUntil = ts(15); even if we don't call it explicitly,
			// we should not be able to de-fortify.
			expCanDefortify: false,
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
			require.Equal(t, tc.expLeadSupportUntil, ft.LeadSupportUntil())
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
		require.Equal(t, ts(15), ft.LeadSupportUntil())

		// Perform a configuration change that adds r4 to the voter set.
		cfg.Voters[0][4] = struct{}{}

		tc.afterConfigChange(&mockLiveness, ft)

		ft.ComputeLeadSupportUntil(pb.StateLeader)
		require.Equal(t, tc.expConfigChangeSafe, ft.ConfigChangeSafe())
		require.Equal(t, tc.expLeadSupportUntil, ft.LeadSupportUntil())
	}
}

// BenchmarkComputeLeadSupportUntil keeps calling ComputeLeadSupportUntil() for
// different number of members.
func BenchmarkComputeLeadSupportUntil(b *testing.B) {
	ts := func(ts int64) hlc.Timestamp {
		return hlc.Timestamp{
			WallTime: ts,
		}
	}

	for _, members := range []int{1, 3, 5, 7, 100} {
		b.Run(fmt.Sprintf("members=%d", members), func(b *testing.B) {
			// Prepare the mock store liveness, and record fortifications.
			livenessMap := map[pb.PeerID]mockLivenessEntry{}
			for i := 1; i <= members; i++ {
				livenessMap[pb.PeerID(i)] = makeMockLivenessEntry(10, ts(100))
			}

			mockLiveness := makeMockStoreLiveness(livenessMap)
			cfg := quorum.MakeEmptyConfig()
			for i := 1; i <= members; i++ {
				cfg.Voters[0][pb.PeerID(i)] = struct{}{}
			}

			ft := NewFortificationTracker(&cfg, mockLiveness, raftlogger.DiscardLogger)
			for i := 1; i <= members; i++ {
				ft.RecordFortification(pb.PeerID(i), 10)
			}

			// The benchmark actually starts here.
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ft.ComputeLeadSupportUntil(pb.StateLeader)
			}
		})
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
