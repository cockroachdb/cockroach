// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package gen

import (
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/metrics"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/workload"
)

// SettingsGen provides a method to generate simulations settings given a seed.
type SettingsGen interface {
	// Generate returns a new simulation settings that is parameterized
	// randomly by the seed provided.
	Generate(seed int64) config.SimulationSettings
}

// LoadGen provides a method to generate a workload generator given a seed and
// simulation settings.
type LoadGen interface {
	// Generate returns a workload generator that is parameterized randomly by
	// the seed and simulation settings provided.
	Generate(seed int64, settings *config.SimulationSettings) []workload.Generator
}

// StateGen provides a method to generate a state given a seed and simulation
// settings.
type StateGen interface {
	// Generate returns a state that is parameterized randomly by the seed and
	// simulation settings provided.
	Generate(seed int64, settings *config.SimulationSettings) state.State
}

// GenerateSimulation is a utility function that creates a new allocation
// simulation using the provided state, workload, settings generators and seed.
func GenerateSimulation(
	duration time.Duration, stateGen StateGen, loadGen LoadGen, settingsGen SettingsGen, seed int64,
) *asim.Simulator {
	settings := settingsGen.Generate(seed)
	return asim.NewSimulator(
		duration,
		loadGen.Generate(seed, &settings),
		stateGen.Generate(seed, &settings),
		&settings,
		metrics.NewTracker(settings.MetricsInterval),
	)
}

// StaticSettings implements the SettingsGen interface.
type StaticSettings struct {
	Settings *config.SimulationSettings
}

// Generate returns a new simulation settings where the settings are identical
// to the original settings but have the seed updated to be equal to the
// provided seed.
func (ss StaticSettings) Generate(seed int64) config.SimulationSettings {
	ret := *ss.Settings
	ret.Seed = seed
	return ret
}

// BasicLoad implements the LoadGen interface.
type BasicLoad struct {
	RWRatio        float64
	Rate           float64
	SkewedAccess   bool
	MinBlockSize   int
	MaxBlockSize   int
	MinKey, MaxKey int64
}

// Generate returns a new list of workload generators where the generator
// parameters are populated with the parameters from the generator and either a
// uniform or zipfian key generator is created depending on whether
// SkewedAccess is true. The returned workload generators are seeded with the
// provided seed.
func (bl BasicLoad) Generate(seed int64, settings *config.SimulationSettings) []workload.Generator {
	var keyGen workload.KeyGenerator
	rand := rand.New(rand.NewSource(seed))
	if bl.SkewedAccess {
		keyGen = workload.NewZipfianKeyGen(bl.MinKey, bl.MaxKey, 1.1, 1, rand)
	} else {
		keyGen = workload.NewUniformKeyGen(bl.MinKey, bl.MaxKey, rand)
	}

	return []workload.Generator{
		workload.NewRandomGenerator(
			settings.StartTime,
			seed,
			keyGen,
			bl.Rate,
			bl.RWRatio,
			bl.MaxBlockSize,
			bl.MinBlockSize,
		),
	}
}

// BasicState implements the StateGen interface.
type BasicState struct {
	Stores            int
	Ranges            int
	SkewedPlacement   bool
	KeySpace          int
	ReplicationFactor int
}

// Generate returns a new state that is created with the number of stores,
// ranges, keyspace and replication factor from the basic state fields. The
// initial assignment of replicas and leases for ranges follows either a
// uniform or powerlaw distribution depending on if SkewedPlacement is true.
func (bs BasicState) Generate(seed int64, settings *config.SimulationSettings) state.State {
	var s state.State
	if bs.SkewedPlacement {
		s = state.NewStateSkewedDistribution(bs.Stores, bs.Ranges, bs.ReplicationFactor, bs.KeySpace, settings)
	} else {
		s = state.NewStateEvenDistribution(bs.Stores, bs.Ranges, bs.ReplicationFactor, bs.KeySpace, settings)
	}
	return s
}
