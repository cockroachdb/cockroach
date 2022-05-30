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
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/workload"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestRunAllocatorSimulator(t *testing.T) {
	ctx := context.Background()
	rwg := make([]workload.Generator, 1)
	rwg[0] = &workload.RandomGenerator{}
	start := state.TestingStartTime()
	end := start.Add(1000 * time.Second)
	interval := 10 * time.Second

	exchange := state.NewFixedDelayExhange(start, interval, interval)
	changer := state.NewReplicaChanger()
	s := state.LoadConfig(state.ComplexConfig)

	sim := asim.NewSimulator(start, end, interval, rwg, s, exchange, changer)
	sim.RunSim(ctx)
}

// TestAllocatorSimulatorSpeed tests that the simulation runs at a rate of at
// least 5 simulated minutes per wall clock second (1:600) for a 32 node
// cluster, with 1000 replicas per node.
// NB: In practice, on a single thread N2 GCP VM, this completes with a minimum
// run of 100ms, approx 10x faster than what this test asserts. The limit is
// set much higher due to --stress and inconsistent processor speeds.
func TestAllocatorSimulatorSpeed(t *testing.T) {
	ctx := context.Background()
	rwg := make([]workload.Generator, 1)
	rwg[0] = &workload.RandomGenerator{}
	start := state.TestingStartTime()
	end := start.Add(5 * time.Minute)
	interval := 10 * time.Second

	sample := func() int64 {
		exchange := state.NewFixedDelayExhange(start, interval, interval)
		changer := state.NewReplicaChanger()
		replicaCounts := make(map[state.StoreID]int)
		for i := 0; i < 32; i++ {
			replicaCounts[state.StoreID(i+1)] = 1000
		}
		s := state.NewTestStateReplCounts(replicaCounts)
		sim := asim.NewSimulator(start, end, interval, rwg, s, exchange, changer)

		startTime := timeutil.Now()
		sim.RunSim(ctx)
		return timeutil.Since(startTime).Nanoseconds()
	}

	// We sample 5 runs and take the minimum. The minimum is the cleanest
	// estimate here of performance, as any additional time over the minimum is
	// noise in a run.
	minRunTime := int64(math.MaxInt64)
	samples := 5
	for i := 0; i < samples; i++ {
		if sampledRun := sample(); sampledRun < minRunTime {
			minRunTime = sampledRun
		}
	}

	fmt.Println(time.Duration(minRunTime).Seconds())
	require.Less(t, minRunTime, time.Second.Nanoseconds())
}
