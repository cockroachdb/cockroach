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
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/event"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/gen"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/metrics"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/workload"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigtestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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

	sim := asim.NewSimulator(duration, rwg, s, settings, m)
	sim.RunSim(ctx)
}

func TestAsimDeterministic(t *testing.T) {
	skip.UnderRace(t, 105904, "asim is non-deterministic under race")
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
	var refRun asim.History

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
		sim := asim.NewSimulator(duration, rwg, s, settings, m)

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

func TestAsimDeterministicZoneConf(t *testing.T) {
	const defaultSeed = 42
	const defaultKeyspace = 10000
	settingsGen := gen.StaticSettings{Settings: config.DefaultSimulationSettings()}
	duration := 30 * time.Minute
	clusterGen := gen.LoadedCluster{
		Info: state.MultiRegionConfig,
	}
	rangeGen := gen.BasicRanges{
		BaseRanges: gen.BaseRanges{
			Ranges:            200,
			ReplicationFactor: 3,
			KeySpace:          defaultKeyspace,
		},
		PlacementType: gen.Uniform,
	}

	loadGen := gen.BasicLoad{}
	eventGen := gen.StaticEvents{DelayedEvents: event.DelayedEventList{}}
	span := spanconfigtestutils.ParseSpan(t, "[0,9999999999)")
	conf := spanconfigtestutils.ParseZoneConfig(t, "num_replicas=3 constraints={'+region=US_East'}").AsSpanConfig()
	eventGen.DelayedEvents = append(eventGen.DelayedEvents, event.DelayedEvent{
		EventFn: func(ctx context.Context, tick time.Time, s state.State) {
			s.SetSpanConfig(span, conf)
		},
		At: settingsGen.Settings.StartTime,
	})

	num := 3
	var refRun asim.History
	for run := 0; run < num; run++ {
		simulator := gen.GenerateSimulation(duration, clusterGen, rangeGen, loadGen, settingsGen, eventGen, defaultSeed)
		simulator.RunSim(context.Background())
		history := simulator.History()
		if run == 0 {
			refRun = history
			continue
		}
		require.Equal(t, refRun.Recorded, history.Recorded)
	}
}

func TestAsimDeterministicDiskFull(t *testing.T) {
	const defaultSeed = 42
	settingsGen := gen.StaticSettings{Settings: config.DefaultSimulationSettings()}
	duration := 30 * time.Minute
	defaultKeyspace := 10000
	clusterGen := gen.BasicCluster{
		Nodes:         5,
		StoresPerNode: 1,
	}
	rangeGen := gen.BasicRanges{
		BaseRanges: gen.BaseRanges{
			Ranges:            500,
			ReplicationFactor: 3,
			KeySpace:          defaultKeyspace,
			Bytes:             300000000,
		},
		PlacementType: gen.Uniform,
	}

	var rwRatio = 0.0
	var minKey, maxKey = int64(1), int64(defaultKeyspace)
	var accessSkew bool
	loadGen := gen.BasicLoad{
		RWRatio:      rwRatio,
		Rate:         500,
		SkewedAccess: accessSkew,
		MinBlockSize: 128000,
		MaxBlockSize: 128000,
		MinKey:       minKey,
		MaxKey:       maxKey,
	}

	capacityOverride := state.NewCapacityOverride()
	capacityOverride.Capacity = 45000000000
	capacityOverride.Available = -1
	store := 5

	var delay time.Duration
	eventGen := gen.StaticEvents{DelayedEvents: event.DelayedEventList{}}
	eventGen.DelayedEvents = append(eventGen.DelayedEvents, event.DelayedEvent{
		EventFn: func(ctx context.Context, tick time.Time, s state.State) {
			log.Infof(ctx, "setting capacity override %+v", capacityOverride)
			s.SetCapacityOverride(state.StoreID(store), capacityOverride)
		},
		At: settingsGen.Settings.StartTime.Add(delay),
	})

	num := 10
	var refRun asim.History
	for run := 0; run < num; run++ {
		simulator := gen.GenerateSimulation(duration, clusterGen, rangeGen, loadGen, settingsGen, eventGen, defaultSeed)
		simulator.RunSim(context.Background())
		history := simulator.History()
		if run == 0 {
			refRun = history
			continue
		}
		require.Equal(t, refRun.Recorded, history.Recorded)
	}
}
