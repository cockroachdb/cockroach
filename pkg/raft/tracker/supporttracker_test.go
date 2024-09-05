// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tracker

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/raft/quorum"
	pb "github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/raft/raftstoreliveness"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

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
		setup         func(tracker *SupportTracker)
		expTS         hlc.Timestamp
	}{
		{
			ids:           []pb.PeerID{1, 2, 3},
			storeLiveness: mockLiveness3Peers,
			setup: func(supportTracker *SupportTracker) {
				// No support recorded.
			},
			expTS: hlc.Timestamp{},
		},
		{
			ids:           []pb.PeerID{1, 2, 3},
			storeLiveness: mockLiveness3Peers,
			setup: func(supportTracker *SupportTracker) {
				supportTracker.RecordSupport(1, 10)
			},
			expTS: hlc.Timestamp{},
		},
		{
			ids:           []pb.PeerID{1, 2, 3},
			storeLiveness: mockLiveness3Peers,
			setup: func(supportTracker *SupportTracker) {
				supportTracker.RecordSupport(1, 10)
				supportTracker.RecordSupport(3, 30)
			},
			expTS: ts(10),
		},
		{
			ids:           []pb.PeerID{1, 2, 3},
			storeLiveness: mockLiveness3Peers,
			setup: func(supportTracker *SupportTracker) {
				supportTracker.RecordSupport(1, 10)
				supportTracker.RecordSupport(3, 30)
				supportTracker.RecordSupport(2, 20)
			},
			expTS: ts(15),
		},
		{
			ids:           []pb.PeerID{1, 2, 3},
			storeLiveness: mockLiveness3Peers,
			setup: func(supportTracker *SupportTracker) {
				// Record support at epochs at expired epochs.
				supportTracker.RecordSupport(1, 9)
				supportTracker.RecordSupport(3, 29)
				supportTracker.RecordSupport(2, 19)
			},
			expTS: hlc.Timestamp{},
		},
		{
			ids:           []pb.PeerID{1, 2, 3},
			storeLiveness: mockLiveness3Peers,
			setup: func(supportTracker *SupportTracker) {
				// Record support at newer epochs than what are present in
				// StoreLiveness.
				//
				// NB: This is possible if there is a race between store liveness
				// heartbeats updates and fortification responses.
				supportTracker.RecordSupport(1, 11)
				supportTracker.RecordSupport(3, 31)
				supportTracker.RecordSupport(2, 21)
			},
			expTS: hlc.Timestamp{},
		},
		{
			ids:           []pb.PeerID{1, 2, 3},
			storeLiveness: mockLiveness3Peers,
			setup: func(supportTracker *SupportTracker) {
				// One of the epochs being supported is expired.
				supportTracker.RecordSupport(1, 10)
				supportTracker.RecordSupport(3, 29) // expired
				supportTracker.RecordSupport(2, 20)
			},
			expTS: ts(10),
		},
		{
			ids:           []pb.PeerID{1, 2, 3},
			storeLiveness: mockLiveness3Peers,
			setup: func(supportTracker *SupportTracker) {
				// Two of the epochs being supported is expired.
				supportTracker.RecordSupport(1, 10)
				supportTracker.RecordSupport(3, 29) // expired
				supportTracker.RecordSupport(2, 19) // expired
			},
			expTS: hlc.Timestamp{},
		},
	}

	for _, tc := range testCases {
		cfg := quorum.MakeEmptyConfig()
		for _, id := range tc.ids {
			cfg.Voters[0][id] = struct{}{}
		}
		supportTracker := MakeSupportTracker(&cfg, tc.storeLiveness)

		tc.setup(&supportTracker)
		require.Equal(t, tc.expTS, supportTracker.LeadSupportUntil())
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
func (m mockStoreLiveness) SupportFrom(id pb.PeerID) (pb.Epoch, hlc.Timestamp, bool) {
	entry := m.liveness[id]
	return entry.epoch, entry.ts, true
}

// SupportFromEnabled implements the raftstoreliveness.StoreLiveness interface.
func (mockStoreLiveness) SupportFromEnabled() bool {
	return true
}

// SupportExpired implements the raftstoreliveness.StoreLiveness interface.
func (mockStoreLiveness) SupportExpired(hlc.Timestamp) bool {
	panic("unimplemented")
}
