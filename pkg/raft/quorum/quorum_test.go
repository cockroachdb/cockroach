// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package quorum

import (
	"slices"
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
		exp     hlc.Timestamp
	}{
		{
			ids:     []pb.PeerID{1, 2, 3},
			support: map[pb.PeerID]hlc.Timestamp{1: ts(10), 2: ts(20), 3: ts(15)},
			exp:     ts(15),
		},
		{
			ids:     []pb.PeerID{1, 2, 3, 4},
			support: map[pb.PeerID]hlc.Timestamp{1: ts(10), 2: ts(20), 3: ts(15), 4: ts(20)},
			exp:     ts(15),
		},
		{
			ids:     []pb.PeerID{1, 2, 3, 4, 5},
			support: map[pb.PeerID]hlc.Timestamp{1: ts(10), 2: ts(20), 3: ts(15), 4: ts(20), 5: ts(20)},
			exp:     ts(20),
		},
		{
			ids:     []pb.PeerID{1, 2, 3},
			support: map[pb.PeerID]hlc.Timestamp{1: ts(10), 2: ts(20)},
			exp:     ts(10),
		},
		{
			ids:     []pb.PeerID{1, 2, 3},
			support: map[pb.PeerID]hlc.Timestamp{1: ts(10)},
			exp:     hlc.Timestamp{},
		},
		{
			ids:     []pb.PeerID{},
			support: map[pb.PeerID]hlc.Timestamp{},
			exp:     hlc.MaxTimestamp,
		},
	}

	for _, tc := range testCases {
		m := MajorityConfig{}
		for _, id := range tc.ids {
			m[id] = struct{}{}
		}

		// Convert the map to a slice of support timestamps.
		supportSlice := make([]hlc.Timestamp, len(tc.ids))
		i := 0
		for _, ts := range tc.support {
			supportSlice[i] = ts
			i++
		}
		require.Equal(t, tc.exp, m.LeadSupportExpiration(supportSlice))
	}
}

// TestLeadSupportExpirationJointConfig ensures that the LeadSupportExpiration
// is calculated correctly for joint configurations. In particular, it's the
// minimum of the two majority configs.
func TestLeadSupportExpirationJointConfig(t *testing.T) {
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
		exp     hlc.Timestamp
	}{
		{
			cfg1:    []pb.PeerID{1, 2, 3},
			cfg2:    []pb.PeerID{},
			support: map[pb.PeerID]hlc.Timestamp{1: ts(10), 2: ts(20), 3: ts(15)},
			exp:     ts(15), // cfg2 is empty, should behave like the (cfg1) majority config case
		},
		{
			cfg1:    []pb.PeerID{},
			cfg2:    []pb.PeerID{1, 2, 3},
			support: map[pb.PeerID]hlc.Timestamp{1: ts(10), 2: ts(20), 3: ts(15)},
			exp:     ts(15), // cfg1 is empty, should behave like the (cfg2) majority config case
		},
		{
			cfg1:    []pb.PeerID{3, 4, 5},
			cfg2:    []pb.PeerID{1, 2, 3},
			support: map[pb.PeerID]hlc.Timestamp{1: ts(10), 2: ts(20), 3: ts(15), 4: ts(20), 5: ts(25)},
			exp:     ts(15), // lower of the two
		},
		{
			cfg1:    []pb.PeerID{3, 4, 5},
			cfg2:    []pb.PeerID{1, 2, 3},
			support: map[pb.PeerID]hlc.Timestamp{1: ts(10), 2: ts(20), 3: ts(15), 4: ts(10), 5: ts(10)},
			exp:     ts(10), // lower of the two; this time, cfg2 has the lower expiration
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

		// Convert the support map into two slices, one for each majority config.
		supportSlice1 := make([]hlc.Timestamp, len(tc.cfg1))
		i := 0
		for _, id := range tc.cfg1 {
			if ts, ok := tc.support[id]; ok {
				supportSlice1[i] = ts
				i++
			}
		}

		supportSlice2 := make([]hlc.Timestamp, len(tc.cfg2))
		i = 0
		for _, id := range tc.cfg2 {
			if ts, ok := tc.support[id]; ok {
				supportSlice2[i] = ts
				i++
			}
		}

		require.Equal(t, tc.exp, j.LeadSupportExpiration(supportSlice1, supportSlice2))
	}
}

func TestJointConfigVisit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	j := JointConfig{
		MajorityConfig{1: struct{}{}, 2: struct{}{}, 3: struct{}{}},
		MajorityConfig{2: struct{}{}, 3: struct{}{}, 4: struct{}{}},
	}

	var visited []pb.PeerID
	j.Visit(func(id pb.PeerID) {
		visited = append(visited, id)
	})
	slices.Sort(visited)

	require.Equal(t, []pb.PeerID{1, 2, 3, 4}, visited)
}
