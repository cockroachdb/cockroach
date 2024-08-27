// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package quorum

import (
	"testing"

	pb "github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestLeadSupportExpiration ensures that the leader's support expiration is
// correctly calculated.
func TestLeadSupportExpiration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ts := func(ts int64) hlc.Timestamp {
		return hlc.Timestamp{
			WallTime: ts,
		}
	}

	testCases := []struct {
		ids     []pb.PeerID
		support map[pb.PeerID]hlc.Timestamp
		expQSE  hlc.Timestamp
	}{
		{
			ids:     []pb.PeerID{1, 2, 3},
			support: map[pb.PeerID]hlc.Timestamp{1: ts(10), 2: ts(20), 3: ts(15)},
			expQSE:  ts(15),
		},
		{
			ids:     []pb.PeerID{1, 2, 3, 4},
			support: map[pb.PeerID]hlc.Timestamp{1: ts(10), 2: ts(20), 3: ts(15), 4: ts(20)},
			expQSE:  ts(15),
		},
		{
			ids:     []pb.PeerID{1, 2, 3, 4, 5},
			support: map[pb.PeerID]hlc.Timestamp{1: ts(10), 2: ts(20), 3: ts(15), 4: ts(20), 5: ts(20)},
			expQSE:  ts(20),
		},
		{
			ids:     []pb.PeerID{1, 2, 3},
			support: map[pb.PeerID]hlc.Timestamp{1: ts(10), 2: ts(20)},
			expQSE:  ts(10),
		},
		{
			ids:     []pb.PeerID{1, 2, 3},
			support: map[pb.PeerID]hlc.Timestamp{1: ts(10)},
			expQSE:  hlc.Timestamp{},
		},
		{
			ids:     []pb.PeerID{},
			support: map[pb.PeerID]hlc.Timestamp{},
			expQSE:  hlc.MaxTimestamp,
		},
	}

	for _, tc := range testCases {
		m := MajorityConfig{}
		for _, id := range tc.ids {
			m[id] = struct{}{}
		}

		require.Equal(t, tc.expQSE, m.LeadSupportExpiration(tc.support))
	}
}

// TestComputeQSEJointConfig ensures that the QSE is calculated correctly for
// joint configurations. In particular, it's the minimum of the two majority
// configs.
func TestComputeQSEJointConfig(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ts := func(ts int64) hlc.Timestamp {
		return hlc.Timestamp{
			WallTime: ts,
		}
	}

	testCases := []struct {
		cfg1    []pb.PeerID
		cfg2    []pb.PeerID
		support map[pb.PeerID]hlc.Timestamp
		expQSE  hlc.Timestamp
	}{
		{
			cfg1:    []pb.PeerID{1, 2, 3},
			cfg2:    []pb.PeerID{},
			support: map[pb.PeerID]hlc.Timestamp{1: ts(10), 2: ts(20), 3: ts(15)},
			expQSE:  ts(15), // cfg2 is empty, should behave like the (cfg1) majority config case
		},
		{
			cfg1:    []pb.PeerID{},
			cfg2:    []pb.PeerID{1, 2, 3},
			support: map[pb.PeerID]hlc.Timestamp{1: ts(10), 2: ts(20), 3: ts(15)},
			expQSE:  ts(15), // cfg1 is empty, should behave like the (cfg2) majority config case
		},
		{
			cfg1:    []pb.PeerID{3, 4, 5},
			cfg2:    []pb.PeerID{1, 2, 3},
			support: map[pb.PeerID]hlc.Timestamp{1: ts(10), 2: ts(20), 3: ts(15), 4: ts(20), 5: ts(25)},
			expQSE:  ts(15), // lower of the two
		},
		{
			cfg1:    []pb.PeerID{3, 4, 5},
			cfg2:    []pb.PeerID{1, 2, 3},
			support: map[pb.PeerID]hlc.Timestamp{1: ts(10), 2: ts(20), 3: ts(15), 4: ts(10), 5: ts(10)},
			expQSE:  ts(10), // lower of the two; this time, cfg2 has the lower qse
		},
	}

	for _, tc := range testCases {
		j := JointConfig{
			MajorityConfig{},
			MajorityConfig{},
		}
		for _, id := range tc.cfg1 {
			j[0][id] = struct{}{}
		}
		for _, id := range tc.cfg2 {
			j[1][id] = struct{}{}
		}

		require.Equal(t, tc.expQSE, j.LeadSupportExpiration(tc.support))
	}
}
