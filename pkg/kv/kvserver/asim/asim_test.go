// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package asim_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/stretchr/testify/require"
)

func TestStateUpdates(t *testing.T) {
	s := asim.NewState()
	s.AddNode()
	require.Equal(t, 1, len(s.Nodes))
	require.Equal(t, 1, len(s.Nodes[1].Stores))
}

func TestRunAllocatorSimulator(t *testing.T) {
	ctx := context.Background()
	rwg := make([]asim.WorkloadGenerator, 1)
	rwg[0] = &asim.RandomWorkloadGenerator{}
	start := time.Date(2022, 03, 21, 11, 0, 0, 0, time.UTC)
	end := start.Add(25 * time.Second)
	interval := 10 * time.Second
	s := asim.LoadConfig(asim.SingleRegionConfig)
	sim := asim.NewSimulator(start, end, interval, rwg, s)
	sim.RunSim(ctx)
}

func TestRangeMap(t *testing.T) {
	m := asim.NewRangeMap()

	r1 := m.AddRange("b")
	r2 := m.AddRange("f")
	r3 := m.AddRange("x")

	// Assert that the range is segmented into [minKey, EndKey) intervals.
	require.Equal(t, roachpb.RKey("f"), r1.Desc.EndKey)
	require.Equal(t, roachpb.RKey("x"), r2.Desc.EndKey)
	require.Equal(t, roachpb.RKeyMax, r3.Desc.EndKey)

	require.Equal(t, (*asim.Range)(nil), m.GetRange("a"))
	require.Equal(t, r1.MinKey, m.GetRange("b").MinKey)
	require.Equal(t, r1.MinKey, m.GetRange("c").MinKey)
	require.Equal(t, r2.MinKey, m.GetRange("g").MinKey)
	require.Equal(t, r3.MinKey, m.GetRange("z").MinKey)
}

func TestAddReplica(t *testing.T) {
	s := asim.NewState()
	s.Ranges = asim.NewRangeMap()
	rm := s.Ranges

	r1 := rm.AddRange("b")
	r2 := rm.AddRange("h")

	n1 := s.AddNode()
	n2 := s.AddNode()

	// Add two replicas on s1, one on s2.
	r1repl0 := s.AddReplica(r1, n1)
	r2repl0 := s.AddReplica(r2, n1)
	r1repl1 := s.AddReplica(r2, n2)

	require.Equal(t, 0, r1repl0)
	require.Equal(t, 0, r2repl0)
	require.Equal(t, 1, r1repl1)

	s1 := s.Nodes[n1].Stores[0]
	s2 := s.Nodes[n2].Stores[0]

	require.Len(t, s1.Replicas, 2)
	require.Len(t, s2.Replicas, 1)
}

// TestWorkloadApply asserts that applying workload on a key, will be reflected
// on the leaseholder for the range that key is contained within.
func TestWorkloadApply(t *testing.T) {
	ctx := context.Background()

	s := asim.NewState()
	s.Ranges = asim.NewRangeMap()
	n1 := s.AddNode()
	n2 := s.AddNode()
	n3 := s.AddNode()

	r1 := s.Ranges.AddRange("100")
	r2 := s.Ranges.AddRange("1000")
	r3 := s.Ranges.AddRange("10000")

	s.AddReplica(r1, n1)
	s.AddReplica(r2, n2)
	s.AddReplica(r3, n3)

	applyLoadToStats := func(key int64, count int) {
		for i := 0; i < count; i++ {
			s.ApplyLoad(ctx, asim.LoadEvent{Key: key})
		}
	}

	applyLoadToStats(100, 100)
	applyLoadToStats(1000, 1000)
	applyLoadToStats(10000, 10000)

	// Assert that the leaseholder replica load correctly matches the number of
	// requests made.
	require.Equal(t, float64(100), r1.Leaseholder.ReplicaLoad.Load().QueriesPerSecond)
	require.Equal(t, float64(1000), r2.Leaseholder.ReplicaLoad.Load().QueriesPerSecond)
	require.Equal(t, float64(10000), r3.Leaseholder.ReplicaLoad.Load().QueriesPerSecond)

	expectedLoad := roachpb.StoreCapacity{QueriesPerSecond: 100, LeaseCount: 1, RangeCount: 1}

	// Assert that the store load is also updated upon request GetStoreLoad.
	require.Equal(t, expectedLoad, s.Nodes[n1].Stores[0].Capacity())
	expectedLoad.QueriesPerSecond *= 10
	require.Equal(t, expectedLoad, s.Nodes[n2].Stores[0].Capacity())
	expectedLoad.QueriesPerSecond *= 10
	require.Equal(t, expectedLoad, s.Nodes[n3].Stores[0].Capacity())
}
