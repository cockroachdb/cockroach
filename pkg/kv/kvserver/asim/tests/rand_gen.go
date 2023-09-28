// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/assertion"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/event"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/gen"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/multiregion"
)

// randomClusterInfoGen returns a randomly picked predefined configuration.
func (f randTestingFramework) randomClusterInfoGen(randSource *rand.Rand) gen.LoadedCluster {
	switch t := f.s.clusterGen.clusterGenType; t {
	case singleRegion:
		chosenIndex := randSource.Intn(len(state.SingleRegionClusterOptions))
		chosenType := state.SingleRegionClusterOptions[chosenIndex]
		return loadClusterInfo(chosenType)
	case multiRegion:
		chosenIndex := randSource.Intn(len(state.MultiRegionClusterOptions))
		chosenType := state.MultiRegionClusterOptions[chosenIndex]
		return loadClusterInfo(chosenType)
	case anyRegion:
		chosenIndex := randSource.Intn(len(state.AllClusterOptions))
		chosenType := state.AllClusterOptions[chosenIndex]
		return loadClusterInfo(chosenType)
	default:
		panic("unknown cluster gen type")
	}
}

// RandomizedBasicRanges implements the RangeGen interface, supporting random
// range info distribution.
type RandomizedBasicRanges struct {
	gen.BaseRanges
	placementType gen.PlacementType
	randSource    *rand.Rand
}

var _ gen.RangeGen = &RandomizedBasicRanges{}

func (r RandomizedBasicRanges) String() string {
	return fmt.Sprintf("randomized ranges with placement_type=%v, %v", r.placementType, r.BaseRanges)
}

func (r RandomizedBasicRanges) Generate(
	seed int64, settings *config.SimulationSettings, s state.State,
) state.State {
	if r.placementType != gen.Random {
		panic("RandomizedBasicRanges generate only randomized distributions")
	}
	rangesInfo := r.GetRangesInfo(r.placementType, len(s.Stores()), r.randSource, []float64{})
	r.LoadRangeInfo(s, rangesInfo)
	return s
}

// WeightedRandomizedBasicRanges implements the RangeGen interface, supporting
// weighted random range info distribution.
type WeightedRandomizedBasicRanges struct {
	gen.BaseRanges
	placementType gen.PlacementType
	randSource    *rand.Rand
	weightedRand  []float64
}

var _ gen.RangeGen = &WeightedRandomizedBasicRanges{}

func (wr WeightedRandomizedBasicRanges) String() string {
	return fmt.Sprintf("weighted randomized ranges with placement_type=%v, weighted_rand=%v, %v", wr.placementType, wr.weightedRand, wr.BaseRanges)
}

func (wr WeightedRandomizedBasicRanges) Generate(
	seed int64, settings *config.SimulationSettings, s state.State,
) state.State {
	if wr.placementType != gen.WeightedRandom || len(wr.weightedRand) == 0 {
		panic("RandomizedBasicRanges generate only weighted randomized distributions with non-empty weightedRand")
	}
	if len(s.Stores()) != len(wr.weightedRand) {
		panic("mismatch: len(weighted_rand) != stores count")
	}
	rangesInfo := wr.GetRangesInfo(wr.placementType, len(s.Stores()), wr.randSource, wr.weightedRand)
	wr.LoadRangeInfo(s, rangesInfo)
	return s
}

// TODO(wenyihu6): Instead of duplicating the key generator logic in simulators,
// we should directly reuse the code from the repo pkg/workload/(kv|ycsb) to
// ensure consistent testing.

// generator generates both ranges and keyspace parameters for ranges
// generations.
type generator interface {
	key() int64
}

type uniformKeyGenerator struct {
	min, max int64
	random   *rand.Rand
}

// newUniformKeyGen returns a generator that generates number∈[min, max] with a
// uniform distribution.
func newUniformKeyGen(min, max int64, rand *rand.Rand) generator {
	if max <= min {
		panic(fmt.Sprintf("max (%d) must be greater than min (%d)", max, min))
	}
	return &uniformKeyGenerator{
		min:    min,
		max:    max,
		random: rand,
	}
}

func (g *uniformKeyGenerator) key() int64 {
	return g.random.Int63n(g.max-g.min) + g.min
}

type zipfianKeyGenerator struct {
	min, max int64
	random   *rand.Rand
	zipf     *rand.Zipf
}

// newZipfianKeyGen returns a generator that generates number ∈[min, max] with a
// zipfian distribution.
func newZipfianKeyGen(min, max int64, s float64, v float64, random *rand.Rand) generator {
	if max <= min {
		panic(fmt.Sprintf("max (%d) must be greater than min (%d)", max, min))
	}
	return &zipfianKeyGenerator{
		min:    min,
		max:    max,
		random: random,
		zipf:   rand.NewZipf(random, s, v, uint64(max-min)),
	}
}

func (g *zipfianKeyGenerator) key() int64 {
	return int64(g.zipf.Uint64()) + g.min
}

type generatorType int

const (
	uniformGenerator generatorType = iota
	zipfGenerator
)

func (g generatorType) String() string {
	switch g {
	case uniformGenerator:
		return "uniform"
	case zipfGenerator:
		return "zipf"
	default:
		panic("unknown cluster type")
	}
}

func getGeneratorType(s string) generatorType {
	switch s {
	case "uniform":
		return uniformGenerator
	case "zipf":
		return zipfGenerator
	default:
		panic(fmt.Sprintf("unknown generator type: %s", s))
	}
}

// newGenerator returns a generator that generates a number ∈[min, max]
// following a distribution based on gType.
func newGenerator(randSource *rand.Rand, iMin int64, iMax int64, gType generatorType) generator {
	switch gType {
	case uniformGenerator:
		return newUniformKeyGen(iMin, iMax, randSource)
	case zipfGenerator:
		return newZipfianKeyGen(iMin, iMax, 1.1, 1, randSource)
	default:
		panic(fmt.Sprintf("unexpected generator type %v", gType))
	}
}

type clusterConfigType int

const (
	singleRegion clusterConfigType = iota
	multiRegion
	anyRegion
)

func (c clusterConfigType) String() string {
	switch c {
	case singleRegion:
		return "single_region"
	case multiRegion:
		return "multi_region"
	case anyRegion:
		return "any_region"
	default:
		panic("unknown cluster type")
	}
}

func getClusterConfigType(s string) clusterConfigType {
	switch s {
	case "single_region":
		return singleRegion
	case "multi_region":
		return multiRegion
	case "any_region":
		return anyRegion
	default:
		panic(fmt.Sprintf("unknown cluster type: %s", s))
	}
}

// These settings apply only to randomized generations and are NOT used if the
// there are no randomization configured for that particular aspect of
// generation. For instance, rangeGenSettings is only used if randOption.range
// is true.
type rangeGenSettings struct {
	placementType     gen.PlacementType
	replicationFactor int
	rangeGenType      generatorType
	keySpaceGenType   generatorType
	weightedRand      []float64
}

func (t rangeGenSettings) String() string {
	return fmt.Sprintf("placement_type=%v, range_gen_type=%v, key_space=%v, replication_factor=%v, weightedRand=%v",
		t.placementType, t.rangeGenType, t.keySpaceGenType, t.replicationFactor, t.weightedRand)
}

const (
	defaultRangeGenType    = uniformGenerator
	defaultKeySpaceGenType = uniformGenerator
)

var defaultWeightedRand []float64

type clusterGenSettings struct {
	clusterGenType clusterConfigType
}

func (c clusterGenSettings) String() string {
	return fmt.Sprintf("cluster_gen_type=%v", c.clusterGenType)
}

const (
	defaultClusterGenType = multiRegion
)

type eventSeriesType int

const (
	// Cycle through predefined region and zone survival configurations. Note
	// that only cluster_gen_type=multi_region can execute this event.
	cycleViaHardcodedSurvivalGoals eventSeriesType = iota
	// Cycle through randomly generated region and zone survival configurations.
	// Note that only cluster_gen_type=multi_region can execute this event.
	cycleViaRandomSurvivalGoals
)

type eventGenSettings struct {
	durationToAssertOnEvent time.Duration
	eventsType              eventSeriesType
}

const (
	defaultEventsType              = cycleViaHardcodedSurvivalGoals
	defaultDurationToAssertOnEvent = 10 * time.Minute
)

func (e eventSeriesType) String() string {
	switch e {
	case cycleViaHardcodedSurvivalGoals:
		return "cycle_via_hardcoded_survival_goals"
	case cycleViaRandomSurvivalGoals:
		return "cycle_via_random_survival_goals"
	default:
		panic("unknown event series type")
	}
}

func getEventSeriesType(s string) eventSeriesType {
	switch s {
	case "cycle_via_hardcoded_survival_goals":
		return cycleViaHardcodedSurvivalGoals
	case "cycle_via_random_survival_goals":
		return cycleViaRandomSurvivalGoals
	default:
		panic(fmt.Sprintf("unknown event series type: %s", s))
	}
}

func (e eventGenSettings) String() string {
	return fmt.Sprintf("duration_to_assert_on_event=%s, type=%v",
		e.durationToAssertOnEvent, e.eventsType)
}

func constructSetZoneConfigEventWithConformanceAssertion(
	span roachpb.Span, config zonepb.ZoneConfig, durationToAssert time.Duration,
) event.MutationWithAssertionEvent {
	return event.MutationWithAssertionEvent{
		MutationEvent: event.SetSpanConfigEvent{
			Span:   span,
			Config: config.AsSpanConfig(),
		},
		AssertionEvent: event.NewAssertionEvent([]assertion.SimulationAssertion{
			assertion.ConformanceAssertion{
				Underreplicated:      0,
				Overreplicated:       0,
				ViolatingConstraints: 0,
				Unavailable:          0,
			},
		}),
		DurationToAssert: durationToAssert,
	}
}

// generateHardcodedSurvivalGoalsEvents sets up two MutationWithAssertionEvents.
// The first mutation event starts at the beginning, followed by an assertion
// event after some time (durationToAssert). Right after that, the second
// MutationWithAssertionEvent happens. Both of these mutation events are
// onSpanConfigChange events which use two hardcoded zone configurations.
func generateHardcodedSurvivalGoalsEvents(
	regions []state.Region, startTime time.Time, durationToAssert time.Duration,
) gen.StaticEvents {
	if len(regions) < 3 {
		panic("only multi-region cluster with more than three regions can use cycle_via_hardcoded_survival_goals")
	}

	eventGen := gen.NewStaticEventsWithNoEvents()
	regionsOne, regionTwo, regionThree := regions[0].Name, regions[1].Name, regions[2].Name
	const numOfConfigs = 2
	configs := [numOfConfigs]zonepb.ZoneConfig{state.GetZoneSurvivalConfig(regionsOne, regionTwo, regionThree),
		state.GetRegionSurvivalConfig(regionsOne, regionTwo, regionThree)}

	span := roachpb.Span{
		Key:    state.MinKey.ToRKey().AsRawKey(),
		EndKey: state.MaxKey.ToRKey().AsRawKey(),
	}

	delay := time.Duration(0)
	// TODO(wenyihu6): enable adding event name tag when registering events for better format span config in output
	for _, eachConfig := range configs {
		eventGen.ScheduleMutationWithAssertionEvent(startTime, delay,
			constructSetZoneConfigEventWithConformanceAssertion(span, eachConfig, durationToAssert))
		delay += durationToAssert
	}
	return eventGen
}

// getRegionNames takes a list of regions and returns the extracted region names
// in the catpb.RegionNames format.
func getRegionNames(regions []state.Region) (regionNames catpb.RegionNames) {
	for _, r := range regions {
		regionNames = append(regionNames, catpb.RegionName(r.Name))
	}
	return regionNames
}

// randomlySelectSurvivalGoal randomly selects between SurvivalGoal_ZONE_FAILURE
// and SurvivalGoal_REGION_FAILURE.
func randomlySelectSurvivalGoal(randSource *rand.Rand) descpb.SurvivalGoal {
	if randBool(randSource) {
		return descpb.SurvivalGoal_ZONE_FAILURE
	} else {
		return descpb.SurvivalGoal_REGION_FAILURE
	}
}

// randomlySelectDataPlacement randomly selects between DataPlacement_DEFAULT
// and DataPlacement_RESTRICTED.
func randomlySelectDataPlacement(randSource *rand.Rand) descpb.DataPlacement {
	if randBool(randSource) {
		return descpb.DataPlacement_DEFAULT
	} else {
		return descpb.DataPlacement_RESTRICTED
	}
}

// generateRandomSurvivalGoalsEvents generates onSpanConfigChange events spaced at
// intervals defined by durationToAssert from the start time. These events apply
// a randomly generated zone configuration followed by an assertion event. Note
// that these random configurations might be unsatisfiable under the cluster
// setup.
func generateRandomSurvivalGoalsEvents(
	regions []state.Region,
	startTime time.Time,
	durationToAssert time.Duration,
	duration time.Duration,
	randSource *rand.Rand,
) gen.StaticEvents {
	if len(regions) < 3 {
		panic("iterate all zone configs is only possible for clusters with > 3 regions")
	}
	eventGen := gen.NewStaticEventsWithNoEvents()
	delay := time.Duration(0)

	span := roachpb.Span{
		Key:    state.MinKey.ToRKey().AsRawKey(),
		EndKey: state.MaxKey.ToRKey().AsRawKey(),
	}

	regionNames := getRegionNames(regions)
	for delay < duration {
		randomRegionIndex := randIndex(randSource, len(regions))
		randomPrimaryRegion := regions[randomRegionIndex].Name
		randomSurvivalGoal := randomlySelectSurvivalGoal(randSource)
		randomDataPlacement := randomlySelectDataPlacement(randSource)
		rc := multiregion.MakeRegionConfig(
			regionNames,
			catpb.RegionName(randomPrimaryRegion),
			randomSurvivalGoal,
			descpb.InvalidID,
			randomDataPlacement,
			nil,
			descpb.ZoneConfigExtensions{},
		)

		zoneConfig, convertErr := sql.TestingConvertRegionToZoneConfig(rc)
		if convertErr != nil {
			panic(fmt.Sprintf("failed to convert region to zone config %s", convertErr.Error()))
		}
		if validateErr := zoneConfig.Validate(); validateErr != nil {
			panic(fmt.Sprintf("zone config generated is invalid %s", validateErr.Error()))
		}
		if validateErr := zoneConfig.EnsureFullyHydrated(); validateErr != nil {
			panic(fmt.Sprintf("zone config generated is not fully hydrated %s", validateErr.Error()))
		}
		eventGen.ScheduleMutationWithAssertionEvent(startTime, delay,
			constructSetZoneConfigEventWithConformanceAssertion(span, zoneConfig, durationToAssert))
		delay += durationToAssert
	}
	return eventGen
}
