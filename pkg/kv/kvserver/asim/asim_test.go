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
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/metrics"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/workload"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestRunAllocatorSimulator(t *testing.T) {
	ctx := context.Background()
	settings := config.DefaultSimulationSettings()
	duration := 1000 * time.Second
	settings.TickInterval = 10 * time.Second
	rwg := make([]workload.Generator, 1)
	rwg[0] = workload.TestCreateWorkloadGenerator(settings.Seed, settings.StartTime, 1, 10)
	m := metrics.NewTracker(settings.MetricsInterval, metrics.NewClusterMetricsTracker(os.Stdout))
	s := state.LoadConfig(state.ComplexConfig)

	sim := asim.NewSimulator(duration, rwg, s, settings, m)
	sim.RunSim(ctx)
}

// TestAllocatorSimulatorSpeed tests that the simulation runs at a rate of at
// least 1.67 simulated minutes per wall clock second (1:100) for a 32 node
// cluster, with 32000 replicas. The workload is generating 16000 keys per
// second with a uniform distribution.
func TestAllocatorSimulatorSpeed(t *testing.T) {
	ctx := context.Background()

	skipString := "Skipping test under (?stress|?race) as it asserts on speed of the run."
	skip.UnderStress(t, skipString)
	skip.UnderStressRace(t, skipString)
	skip.UnderRace(t, skipString)

	settings := config.DefaultSimulationSettings()

	// Run each simulation for 5 minutes.
	duration := 5 * time.Minute
	settings.TickInterval = 2 * time.Second

	stores := 32
	replsPerRange := 3
	replicasPerStore := 1000
	// NB: We want 1000 replicas per store, so the number of ranges required
	// will be 1/3 of the total replicas.
	ranges := (replicasPerStore * stores) / replsPerRange
	// NB: In this test we are using a uniform workload and expect to see at
	// most 3 splits occur due to range size, therefore the keyspace need not
	// be larger than 3 keys per range.
	keyspace := 3 * ranges

	sample := func() int64 {
		rwg := make([]workload.Generator, 1)
		rwg[0] = workload.TestCreateWorkloadGenerator(settings.Seed, settings.StartTime, stores, int64(keyspace))
		m := metrics.NewTracker(settings.TickInterval) // no output
		replicaDistribution := make([]float64, stores)

		// NB: Here create half of the stores with equal replica counts, the
		// other half have no replicas. This will lead to a flurry of activity
		// rebalancing towards these stores, based on the replica count
		// imbalance.
		for i := 0; i < stores/2; i++ {
			replicaDistribution[i] = 1.0 / float64(stores/2)
		}
		for i := stores / 2; i < stores; i++ {
			replicaDistribution[i] = 0
		}

		s := state.NewTestStateReplDistribution(replicaDistribution, ranges, replsPerRange, keyspace)
		sim := asim.NewSimulator(duration, rwg, s, settings, m)

		startTime := timeutil.Now()
		sim.RunSim(ctx)
		return timeutil.Since(startTime).Nanoseconds()
	}

	// We sample 5 runs and take the minimum. The minimum is the cleanest
	// estimate here of performance, as any additional time over the minimum is
	// noise in a run.
	minRunTime := int64(math.MaxInt64)
	// TODO(kvoli): Hit perf wall again when running at a lower tick rate on
	// selected operartions.
	requiredRunTime := 30 * time.Second.Nanoseconds()
	samples := 5
	for i := 0; i < samples; i++ {
		if sampledRun := sample(); sampledRun < minRunTime {
			minRunTime = sampledRun
		}
		// NB: When we satisfy the test required runtime, exit early to avoid
		// additional runs.
		if minRunTime < requiredRunTime {
			break
		}
	}

	fmt.Println(time.Duration(minRunTime).Seconds())
	require.Less(t, minRunTime, requiredRunTime)
}

func TestAllocatorSimulatorDeterministic(t *testing.T) {

	settings := config.DefaultSimulationSettings()

	runs := 3
	duration := 15 * time.Minute
	settings.TickInterval = 2 * time.Second

	stores := 7
	replsPerRange := 3
	replicasPerStore := 100
	// NB: We want 100 replicas per store, so the number of ranges required
	// will be 1/3 of the total replicas.
	ranges := (replicasPerStore * stores) / replsPerRange
	// NB: In this test we are using a uniform workload and expect to see at
	// most 3 splits occur due to range size, therefore the keyspace need not
	// be larger than 3 keys per range.
	keyspace := 3 * ranges
	// Track the run to compare against for determinism.
	var refRun []roachpb.StoreDescriptor

	for run := 0; run < runs; run++ {
		rwg := make([]workload.Generator, 1)
		rwg[0] = workload.TestCreateWorkloadGenerator(settings.Seed, settings.StartTime, stores, int64(keyspace))
		m := metrics.NewTracker(settings.TickInterval) // no output
		replicaDistribution := make([]float64, stores)

		// NB: Here create half of the stores with equal replica counts, the
		// other half have no replicas. This will lead to a flurry of activity
		// rebalancing towards these stores, based on the replica count
		// imbalance.
		for i := 0; i < stores/2; i++ {
			replicaDistribution[i] = 1.0 / float64(stores/2)
		}
		for i := stores / 2; i < stores; i++ {
			replicaDistribution[i] = 0
		}

		s := state.NewTestStateReplDistribution(replicaDistribution, ranges, replsPerRange, keyspace)
		sim := asim.NewSimulator(duration, rwg, s, settings, m)

		ctx := context.Background()
		sim.RunSim(ctx)

		storeRefs := s.Stores()
		storeIDs := make([]state.StoreID, len(storeRefs))
		for i, store := range storeRefs {
			storeIDs[i] = store.StoreID()
		}
		descs := s.StoreDescriptors(false /* cached */, storeIDs...)

		if run == 0 {
			refRun = descs
			continue
		}
		require.Equal(t, refRun, descs)
	}
}
