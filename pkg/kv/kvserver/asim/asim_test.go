// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package asim_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/history"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/metrics"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/scheduled"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/workload"
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
	s := state.LoadConfig(state.ComplexConfig, state.SingleRangeConfig, settings)

	sim := asim.NewSimulator(duration, rwg, s, settings, m, scheduled.NewExecutorWithNoEvents())
	sim.RunSim(ctx)
}

func TestAsimDeterministic(t *testing.T) {
	settings := config.DefaultSimulationSettings()

	runs := 3
	duration := 15 * time.Minute
	settings.TickInterval = 2 * time.Second

	stores := 21
	replsPerRange := 3
	replicasPerStore := 600

	// NB: We want 100 replicas per store, so the number of ranges required
	// will be 1/3 of the total replicas.
	ranges := (replicasPerStore * stores) / replsPerRange
	// NB: In this test we are using a uniform workload and expect to see at
	// most 3 splits occur due to range size, therefore the keyspace need not
	// be larger than 3 keys per range.
	keyspace := 3 * ranges
	// Track the run to compare against for determinism.
	var refRun history.History

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

		s := state.NewStateWithDistribution(replicaDistribution, ranges, replsPerRange, keyspace, settings)
		sim := asim.NewSimulator(duration, rwg, s, settings, m, scheduled.NewExecutorWithNoEvents())

		ctx := context.Background()
		sim.RunSim(ctx)
		history := sim.History()

		if run == 0 {
			refRun = history
			continue
		}
		require.Equal(t, refRun.Recorded, history.Recorded)
	}
}
