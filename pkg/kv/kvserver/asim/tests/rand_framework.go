// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/assertion"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/gen"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/history"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
)

type testRandOptions struct {
	cluster        bool
	ranges         bool
	load           bool
	staticSettings bool
	staticEvents   bool
}

type testSettings struct {
	numIterations int
	duration      time.Duration
	verbose       OutputFlags
	randSource    *rand.Rand
	assertions    []assertion.SimulationAssertion
	randOptions   testRandOptions
	clusterGen    clusterGenSettings
	rangeGen      rangeGenSettings
	eventGen      eventGenSettings
}

type randTestingFramework struct {
	s                     testSettings
	defaultStaticSettings staticOptionSettings
	rangeGenerator        generator
	keySpaceGenerator     generator
}

// newRandTestingFramework constructs a new testing framework with the given
// testSettings. It also initializes generators for randomized range generation.
// Since generators persist across iterations, this leads to the formation of a
// distribution as ranges are generated. Additionally, it initializes a buffer
// that persists across all iterations, recording outputs and states of each
// iteration.
func newRandTestingFramework(
	s testSettings, staticOptionSettings staticOptionSettings,
) randTestingFramework {
	if int64(defaultMaxRange) > defaultMinKeySpace {
		panic(fmt.Sprintf(
			"Max number of ranges specified (%d) is greater than number of keys in key space (%d) ",
			defaultMaxRange, defaultMinKeySpace))
	}
	rangeGenerator := newGenerator(s.randSource, defaultMinRange, defaultMaxRange, s.rangeGen.rangeGenType)
	keySpaceGenerator := newGenerator(s.randSource, defaultMinKeySpace, defaultMaxKeySpace, s.rangeGen.keySpaceGenType)

	return randTestingFramework{
		defaultStaticSettings: staticOptionSettings,
		s:                     s,
		rangeGenerator:        rangeGenerator,
		keySpaceGenerator:     keySpaceGenerator,
	}
}

func (f randTestingFramework) getCluster() gen.ClusterGen {
	if !f.s.randOptions.cluster {
		return f.defaultBasicClusterGen()
	}
	return f.randomClusterInfoGen(f.s.randSource)
}

func (f randTestingFramework) getRanges() gen.RangeGen {
	if !f.s.randOptions.ranges {
		return f.defaultBasicRangesGen()
	}
	return f.randomBasicRangesGen()
}

func (f randTestingFramework) getLoad() gen.LoadGen {
	if !f.s.randOptions.load {
		return f.defaultLoadGen()
	}
	return gen.BasicLoad{}
}

func (f randTestingFramework) getStaticSettings() gen.StaticSettings {
	if !f.s.randOptions.staticSettings {
		return f.defaultStaticSettingsGen()
	}
	return gen.StaticSettings{}
}

func (f randTestingFramework) getStaticEvents(
	cluster gen.ClusterGen, settings gen.StaticSettings,
) gen.StaticEvents {
	if !f.s.randOptions.staticEvents {
		return f.defaultStaticEventsGen()
	}
	return f.randomEventSeriesGen(cluster, settings)
}

// runRandTest creates randomized configurations based on the specified test
// settings and runs one test using those configurations.
func (f randTestingFramework) runRandTest() testResult {
	ctx := context.Background()
	cluster := f.getCluster()
	ranges := f.getRanges()
	load := f.getLoad()
	staticSettings := f.getStaticSettings()
	staticEvents := f.getStaticEvents(cluster, staticSettings)
	seed := f.s.randSource.Int63()
	simulator := gen.GenerateSimulation(f.s.duration, cluster, ranges, load, staticSettings, staticEvents, seed)
	initialStateStr, initialTime := simulator.State().PrettyPrint(), simulator.Curr()
	simulator.RunSim(ctx)
	history := simulator.History()
	failed, reason := checkAssertions(ctx, history, f.s.assertions)
	return testResult{
		seed:            seed,
		failed:          failed,
		reason:          reason,
		clusterGen:      cluster,
		rangeGen:        ranges,
		loadGen:         load,
		eventGen:        staticEvents,
		initialStateStr: initialStateStr,
		initialTime:     initialTime,
		history:         history,
		eventExecutor:   simulator.EventExecutor(),
	}
}

// runRandTestRepeated runs the test multiple times, each time with a new
// randomly generated configuration. The result of each iteration is recorded in
// f.recordBuf.
func (f randTestingFramework) runRandTestRepeated() testResultsReport {
	numIterations := f.s.numIterations
	outputs := make([]testResult, numIterations)
	for i := 0; i < numIterations; i++ {
		outputs[i] = f.runRandTest()
	}
	return testResultsReport{
		flags:          f.s.verbose,
		settings:       f.s,
		staticSettings: f.defaultStaticSettings,
		outputSlice:    outputs,
	}
}

// loadClusterInfo creates a LoadedCluster from a matching ClusterInfo based on
// the given configNam, or panics if no match is found in existing
// configurations.
func loadClusterInfo(configName string) gen.LoadedCluster {
	clusterInfo := state.GetClusterInfo(configName)
	return gen.LoadedCluster{
		Info: clusterInfo,
	}
}

// checkAssertions checks the given history and assertions, returning (bool,
// reason) indicating any failures and reasons if any assertions fail.
func checkAssertions(
	ctx context.Context, history history.History, assertions []assertion.SimulationAssertion,
) (bool, string) {
	assertionFailures := []string{}
	failureExists := false
	for _, assertion := range assertions {
		if holds, reason := assertion.Assert(ctx, history); !holds {
			failureExists = true
			assertionFailures = append(assertionFailures, reason)
		}
	}
	if failureExists {
		return true, strings.Join(assertionFailures, "")
	}
	return false, ""
}

const (
	defaultMinRange    = 1
	defaultMaxRange    = 1000
	defaultMinKeySpace = 1000
	defaultMaxKeySpace = 200000
)

func convertInt64ToInt(num int64) int {
	if num < math.MinInt32 || num > math.MaxUint32 {
		// Theoretically, this should be impossible given that we have defined
		// min and max boundaries for ranges and key space.
		panic(fmt.Sprintf("num overflows the max value or min value of int32 %d", num))
	}
	return int(num)
}

// randomBasicRangesGen returns range_gen, capable of creating an updated state
// with updated range information. Range_gen is created using the range,
// keyspace generator (initialized in rand testing framework) with the specified
// replication factor and placement type (set in rangeGenSettings).
func (f randTestingFramework) randomBasicRangesGen() gen.RangeGen {
	switch placementType := f.s.rangeGen.placementType; placementType {
	case gen.Even, gen.Skewed:
		if len(f.s.rangeGen.weightedRand) != 0 {
			panic("set placement_type to weighted_rand to use weighted random placement for stores")
		}
		return gen.BasicRanges{
			BaseRanges: gen.BaseRanges{
				Ranges:            convertInt64ToInt(f.rangeGenerator.key()),
				KeySpace:          convertInt64ToInt(f.keySpaceGenerator.key()),
				ReplicationFactor: f.s.rangeGen.replicationFactor,
				Bytes:             defaultBytes,
			},
			PlacementType: placementType,
		}
	case gen.Random:
		if len(f.s.rangeGen.weightedRand) != 0 {
			panic("set placement_type to weighted_rand to use weighted random placement for stores")
		}
		return RandomizedBasicRanges{
			BaseRanges: gen.BaseRanges{
				Ranges:            convertInt64ToInt(f.rangeGenerator.key()),
				KeySpace:          convertInt64ToInt(f.keySpaceGenerator.key()),
				ReplicationFactor: f.s.rangeGen.replicationFactor,
				Bytes:             defaultBytes,
			},
			placementType: gen.Random,
			randSource:    f.s.randSource,
		}

	case gen.WeightedRandom:
		if len(f.s.rangeGen.weightedRand) == 0 {
			panic("set weightedRand array for stores properly to use weighted random placement for stores")
		}
		if f.s.randOptions.cluster {
			panic("randomized cluster with weighted rand stores is not supported")
		}
		if stores, length := f.defaultStaticSettings.storesPerNode*f.defaultStaticSettings.nodes, len(f.s.rangeGen.weightedRand); stores != length {
			panic(fmt.Sprintf("number of stores %d does not match length of weighted_rand %d: ", stores, length))
		}
		return WeightedRandomizedBasicRanges{
			BaseRanges: gen.BaseRanges{
				Ranges:            convertInt64ToInt(f.rangeGenerator.key()),
				KeySpace:          convertInt64ToInt(f.keySpaceGenerator.key()),
				ReplicationFactor: f.s.rangeGen.replicationFactor,
				Bytes:             defaultBytes,
			},
			placementType: gen.WeightedRandom,
			randSource:    f.s.randSource,
			weightedRand:  f.s.rangeGen.weightedRand,
		}
	default:
		panic("unknown ranges placement type")
	}
}

func (f randTestingFramework) randomEventSeriesGen(
	cluster gen.ClusterGen, settings gen.StaticSettings,
) gen.StaticEvents {
	switch eventsType := f.s.eventGen.eventsType; eventsType {
	case cycleViaHardcodedSurvivalGoals:
		return generateHardcodedSurvivalGoalsEvents(cluster.Regions(), settings.Settings.StartTime, f.s.eventGen.durationToAssertOnEvent)
	case cycleViaRandomSurvivalGoals:
		return generateRandomSurvivalGoalsEvents(cluster.Regions(), settings.Settings.StartTime, f.s.eventGen.durationToAssertOnEvent, f.s.duration, f.s.randSource)
	default:
		panic("unknown event series type")
	}
}
