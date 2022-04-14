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

	r1 := asim.Range{MinKey: "b"}
	r2 := asim.Range{MinKey: "f"}
	r3 := asim.Range{MinKey: "x"}

	m.AddRange(&r1)
	m.AddRange(&r2)
	m.AddRange(&r3)

	require.Equal(t, (*asim.Range)(nil), m.GetRange("a"))
	require.Equal(t, &r1, m.GetRange("b"))
	require.Equal(t, &r1, m.GetRange("c"))
	require.Equal(t, &r3, m.GetRange("z"))
}
