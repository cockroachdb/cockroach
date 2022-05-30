// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package state

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/workload"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/stretchr/testify/require"
)

func TestStateUpdates(t *testing.T) {
	s := NewState()
	node := s.AddNode()
	s.AddStore(node.NodeID())
	require.Equal(t, 1, len(s.Nodes()))
	require.Equal(t, 1, len(s.Stores()))
}

func TestRangeMap(t *testing.T) {
	s := newState()
	k2 := Key(1)
	k3 := Key(2)
	k4 := Key(3)

	r1, r2, ok := s.SplitRange(k2)
	require.True(t, ok)
	_, r3, ok := s.SplitRange(k3)
	require.True(t, ok)
	_, r4, ok := s.SplitRange(k4)
	require.True(t, ok)

	// Assert that the range is segmented into [minKey, EndKey) intervals.
	require.Equal(t, k2.ToRKey(), r1.Descriptor().EndKey)
	require.Equal(t, k3.ToRKey(), r2.Descriptor().EndKey)
	require.Equal(t, k4.ToRKey(), r3.Descriptor().EndKey)

	require.Equal(t, r2.RangeID(), s.rangeFor(k2).rangeID)
	require.Equal(t, r3.RangeID(), s.rangeFor(k3).rangeID)
	require.Equal(t, r4.RangeID(), s.rangeFor(k4).rangeID)
}

func TestAddReplica(t *testing.T) {
	s := NewState()

	_, r1, _ := s.SplitRange(1)
	_, r2, _ := s.SplitRange(2)

	n1 := s.AddNode()
	s1, _ := s.AddStore(n1.NodeID())
	s2, _ := s.AddStore(n1.NodeID())

	// Add two replicas on s1, one on s2.
	r1repl1, _ := s.AddReplica(r1.RangeID(), s1.StoreID())
	r2repl1, _ := s.AddReplica(r2.RangeID(), s1.StoreID())
	r2repl2, _ := s.AddReplica(r2.RangeID(), s2.StoreID())

	require.Equal(t, ReplicaID(1), r1repl1.ReplicaID())
	require.Equal(t, ReplicaID(1), r2repl1.ReplicaID())
	require.Equal(t, ReplicaID(2), r2repl2.ReplicaID())

	require.Len(t, s1.Replicas(), 2)
	require.Len(t, s2.Replicas(), 1)
}

// TestWorkloadApply asserts that applying workload on a key, will be reflected
// on the leaseholder for the range that key is contained within.
func TestWorkloadApply(t *testing.T) {
	s := NewState()

	n1 := s.AddNode()
	s1, _ := s.AddStore(n1.NodeID())
	s2, _ := s.AddStore(n1.NodeID())
	s3, _ := s.AddStore(n1.NodeID())

	_, r1, _ := s.SplitRange(100)
	_, r2, _ := s.SplitRange(1000)
	_, r3, _ := s.SplitRange(10000)

	s.AddReplica(r1.RangeID(), s1.StoreID())
	s.AddReplica(r2.RangeID(), s2.StoreID())
	s.AddReplica(r3.RangeID(), s3.StoreID())

	applyLoadToStats := func(key int64, count int) {
		for i := 0; i < count; i++ {
			s.ApplyLoad(workload.LoadEvent{Key: key})
		}
	}

	applyLoadToStats(100, 100)
	applyLoadToStats(1000, 1000)
	applyLoadToStats(10000, 10000)

	// Assert that the leaseholder replica load correctly matches the number of
	// requests made.
	require.Equal(t, float64(100), s.UsageInfo(r1.RangeID()).QueriesPerSecond)
	require.Equal(t, float64(1000), s.UsageInfo(r2.RangeID()).QueriesPerSecond)
	require.Equal(t, float64(10000), s.UsageInfo(r3.RangeID()).QueriesPerSecond)

	expectedLoad := roachpb.StoreCapacity{QueriesPerSecond: 100, LeaseCount: 1, RangeCount: 1}
	_ = s.StoreDescriptors()
	sc1 := s1.Descriptor().Capacity
	sc2 := s2.Descriptor().Capacity
	sc3 := s3.Descriptor().Capacity

	// Assert that the store load is also updated upon request GetStoreLoad.
	require.Equal(t, expectedLoad, sc1)
	expectedLoad.QueriesPerSecond *= 10
	require.Equal(t, expectedLoad, sc2)
	expectedLoad.QueriesPerSecond *= 10
	require.Equal(t, expectedLoad, sc3)
}
