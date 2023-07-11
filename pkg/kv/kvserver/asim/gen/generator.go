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
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/event"
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

// ClusterGen provides a method to generate the initial cluster state,  given a
// seed and simulation settings. The initial cluster state includes: nodes
// (including locality) and stores.
type ClusterGen interface {
	// Generate returns a new State that is parameterized randomly by the seed
	// and simulation settings provided.
	Generate(seed int64, settings *config.SimulationSettings) state.State
}

// RangeGen provides a method to generate the initial range splits, range
// replica and lease placement within a cluster.
type RangeGen interface {
	// Generate returns an updated state, given the initial state, seed and
	// simulation settings provided. In the updated state, ranges will have been
	// created, replicas and leases assigned to stores in the cluster.
	Generate(seed int64, settings *config.SimulationSettings, s state.State) state.State
}

// EventGen provides a  method to generate a list of events that will apply to
// the simulated cluster. Currently, only delayed (fixed time) events are
// supported.
type EventGen interface {
	// Generate returns a list of events, which should be exectued at the delay specified.
	Generate(seed int64) event.DelayedEventList
}

// GenerateSimulation is a utility function that creates a new allocation
// simulation using the provided state, workload, settings generators and seed.
func GenerateSimulation(
	duration time.Duration,
	clusterGen ClusterGen,
	rangeGen RangeGen,
	loadGen LoadGen,
	settingsGen SettingsGen,
	eventGen EventGen,
	seed int64,
) *asim.Simulator {
	settings := settingsGen.Generate(seed)
	s := clusterGen.Generate(seed, &settings)
	s = rangeGen.Generate(seed, &settings, s)
	return asim.NewSimulator(
		duration,
		loadGen.Generate(seed, &settings),
		s,
		&settings,
		metrics.NewTracker(settings.MetricsInterval),
		eventGen.Generate(seed)...,
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
	if bl.Rate == 0 {
		return []workload.Generator{}
	}

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

// LoadedCluster implements the ClusterGen interface.
type LoadedCluster struct {
	Info state.ClusterInfo
}

// Generate returns a new simulator state, where the cluster is loaded based on
// the cluster info the loaded cluster generator is created with. There is no
// randomness in this cluster generation.
func (lc LoadedCluster) Generate(seed int64, settings *config.SimulationSettings) state.State {
	return state.LoadClusterInfo(lc.Info, settings)
}

// BasicCluster implements the ClusterGen interace.
type BasicCluster struct {
	Nodes         int
	StoresPerNode int
}

// Generate returns a new simulator state, where the cluster is created with all
// nodes having the same locality and with the specified number of stores/nodes
// created. The cluster is created based on the stores and stores-per-node
// values the basic cluster generator is created with.
func (lc BasicCluster) Generate(seed int64, settings *config.SimulationSettings) state.State {
	info := state.ClusterInfoWithStoreCount(lc.Nodes, lc.StoresPerNode)
	return state.LoadClusterInfo(info, settings)
}

// LoadedRanges implements the RangeGen interface.
type LoadedRanges struct {
	Info state.RangesInfo
}

// Generate returns an updated simulator state, where the cluster is loaded
// with the range info that the generator was created with. There is no
// randomness in this cluster generation.
func (lr LoadedRanges) Generate(
	seed int64, settings *config.SimulationSettings, s state.State,
) state.State {
	state.LoadRangeInfo(s, lr.Info...)
	return s
}

// PlacementType represents a type of placement distribution.
type PlacementType int

const (
	Uniform PlacementType = iota
	Skewed
)

// BasicRanges implements the RangeGen interface.
type BasicRanges struct {
	Ranges            int
	PlacementType     PlacementType
	KeySpace          int
	ReplicationFactor int
	Bytes             int64
}

func NewBasicRanges(
	ranges int,
	placementType PlacementType,
	keySpace int,
	replicationFactor int,
	bytes int64,
) BasicRanges {
	return BasicRanges{
		Ranges:            ranges,
		PlacementType:     placementType,
		KeySpace:          keySpace,
		ReplicationFactor: replicationFactor,
		Bytes:             bytes,
	}
}

func (br BasicRanges) GetRangesInfo(stores int) (rangesInfo state.RangesInfo) {
	switch br.PlacementType {
	case Uniform:
		rangesInfo = state.RangesInfoEvenDistribution(stores, br.Ranges, br.KeySpace, br.ReplicationFactor, br.Bytes)
	case Skewed:
		rangesInfo = state.RangesInfoSkewedDistribution(stores, br.Ranges, br.KeySpace, br.ReplicationFactor, br.Bytes)
	}
	return rangesInfo
}

func (br BasicRanges) LoadRangeInfo(s state.State, rangesInfo state.RangesInfo) state.State {
	for _, rangeInfo := range rangesInfo {
		rangeInfo.Size = br.Bytes
	}
	state.LoadRangeInfo(s, rangesInfo...)
	return s
}

// Generate returns an updated simulator state, where the cluster is loaded
// with ranges based on the parameters of basic ranges.
func (br BasicRanges) Generate(
	seed int64, settings *config.SimulationSettings, s state.State,
) state.State {
	stores := len(s.Stores())
	rangesInfo := br.GetRangesInfo(stores)
	return br.LoadRangeInfo(s, rangesInfo)
}

// StaticEvents implements the EventGen interface.
// TODO(kvoli): introduce conditional events.
type StaticEvents struct {
	DelayedEvents event.DelayedEventList
}

// Generate returns a list of events, exactly the same as the events
// StaticEvents was created with.
func (se StaticEvents) Generate(seed int64) event.DelayedEventList {
	sort.Sort(se.DelayedEvents)
	return se.DelayedEvents
}
