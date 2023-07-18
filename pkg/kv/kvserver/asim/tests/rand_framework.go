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
	"context"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/gen"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/metrics"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/workload"
	"github.com/guptarohit/asciigraph"
)

type testSettings struct {
	numIterations int
	duration      time.Duration
	verbose       bool
	randSource    *rand.Rand
	randOptions   map[string]bool
	rangeGen      rangeGenSettings
}

func newTestSettings(
	numIterations int,
	duration time.Duration,
	verbose bool,
	seed int64,
	randOptions map[string]bool,
	rangeGenSettings rangeGenSettings,
) testSettings {
	return testSettings{
		numIterations: numIterations,
		duration:      duration,
		verbose:       verbose,
		randSource:    rand.New(rand.NewSource(seed)),
		randOptions:   randOptions,
		rangeGen:      rangeGenSettings,
	}
}

type randTestingFramework struct {
	s                 testSettings
	rangeGenerator    generator
	keySpaceGenerator generator
}

func newRandTestingFramework(settings testSettings) randTestingFramework {
	rangeGenerator := newRandomizedGenerator(settings.randSource, defaultMinRange, defaultMaxRange, settings.rangeGen.rangeKeyGenType)
	keySpaceGenerator := newRandomizedGenerator(settings.randSource, defaultMinKeySpace, defaultMaxKeySpace, settings.rangeGen.keySpaceGenType)
	return randTestingFramework{
		s:                 settings,
		rangeGenerator:    rangeGenerator,
		keySpaceGenerator: keySpaceGenerator,
	}
}

func (f randTestingFramework) getCluster() gen.ClusterGen {
	if !f.s.randOptions["cluster"] {
		return defaultBasicClusterGen()
	}
	return f.randomClusterInfoGen(f.s.randSource)
}

func (f randTestingFramework) getRanges() gen.RangeGen {
	if !f.s.randOptions["ranges"] {
		return defaultBasicRangesGen()
	}
	return f.randomBasicRangesGen()
}

func (f randTestingFramework) getLoad() gen.LoadGen {
	if !f.s.randOptions["load"] {
		return defaultLoadGen()
	}
	return gen.BasicLoad{}
}

func (f randTestingFramework) getStaticSettings() gen.StaticSettings {
	if !f.s.randOptions["static_settings"] {
		return defaultStaticSettingsGen()
	}
	return gen.StaticSettings{}
}

func (f randTestingFramework) getStaticEvents() gen.StaticEvents {
	if !f.s.randOptions["static_events"] {
		return defaultStaticEventsGen()
	}
	return gen.StaticEvents{}
}

func (f randTestingFramework) getAssertions() []SimulationAssertion {
	if !f.s.randOptions["assertions"] {
		return defaultAssertions()
	}
	return []SimulationAssertion{}
}

func (f randTestingFramework) runRandTest() (asim.History, bool, string) {
	ctx := context.Background()
	cluster := f.getCluster()
	ranges := f.getRanges()
	load := f.getLoad()
	staticSettings := f.getStaticSettings()
	staticEvents := f.getStaticEvents()
	assertions := f.getAssertions()
	simulator := gen.GenerateSimulation(f.s.duration, cluster, ranges, load, staticSettings, staticEvents, f.s.randSource.Int63())
	simulator.RunSim(ctx)
	history := simulator.History()
	failed, reason := checkAssertions(ctx, history, assertions)
	return history, failed, reason
}

func (f randTestingFramework) runRandTestRepeated(t *testing.T) {
	numIterations := f.s.numIterations
	runs := make([]asim.History, numIterations)
	failureExists := false
	var buf strings.Builder
	for i := 0; i < numIterations; i++ {
		history, failed, reason := f.runRandTest()
		runs[i] = history
		if failed {
			failureExists = true
			fmt.Fprintf(&buf, "failed assertion sample %d\n%s", i+1, reason)
		}
	}

	if f.s.verbose {
		plotAllHistory(runs, &buf)
	}

	if failureExists {
		t.Fatal(buf.String())
	}
}

// Helper Function
// loadClusterInfo creates a LoadedCluster from a matching ClusterInfo based on
// the given configNam, or panics if no match is found in existing
// configurations.
func loadClusterInfo(configName string) gen.LoadedCluster {
	clusterInfo := gen.GetClusterInfo(configName)
	return gen.LoadedCluster{
		Info: clusterInfo,
	}
}

// PlotAllHistory outputs stat plots for the provided asim history array into
// the given strings.Builder buf.
func plotAllHistory(runs []asim.History, buf *strings.Builder) {
	settings := defaultPlotSettings()
	stat, height, width := settings.stat, settings.height, settings.width
	for i := 0; i < len(runs); i++ {
		history := runs[i]
		ts := metrics.MakeTS(history.Recorded)
		statTS := ts[stat]
		buf.WriteString("\n")
		buf.WriteString(asciigraph.PlotMany(
			statTS,
			asciigraph.Caption(stat),
			asciigraph.Height(height),
			asciigraph.Width(width),
		))
		buf.WriteString("\n")
	}
}

// checkAssertions checks the given history and assertions, returning (bool,
// reason) indicating any failures and reasons if any assertions fail.
func checkAssertions(
	ctx context.Context, history asim.History, assertions []SimulationAssertion,
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

type generator interface {
	Num() int64
}

type generatorType int

const (
	uniformGenerator generatorType = iota
	zipfGenerator
)

func newGenerator(randSource *rand.Rand, iMin int64, iMax int64, gType generatorType) generator {
	switch gType {
	case uniformGenerator:
		return workload.NewUniformKeyGen(iMin, iMax, randSource)
	case zipfGenerator:
		return workload.NewZipfianKeyGen(iMin, iMax, 1.1, 1, randSource)
	default:
		panic(fmt.Sprintf("unexpected generator type %v", gType))
	}
}

func newRandomizedGenerator(
	randSource *rand.Rand, iMin int64, iMax int64, gType generatorType,
) generator {
	return newGenerator(randSource, iMin, iMax, gType)
}

const (
	defaultMinRange    = 1
	defaultMaxRange    = 1000
	defaultMinKeySpace = 1000
	defaultMaxKeySpace = 200000
)

func convertInt64ToInt(num int64) int {
	// Should be impossible since we have set imax, imin to something smaller to imax32
	if num < math.MinInt32 {
		return math.MinInt32
	}
	if num > math.MaxUint32 {
		return math.MaxUint32
	}
	return int(num)
}

func (f randTestingFramework) randomBasicRangesGen() gen.RangeGen {
	options := gen.GetAvailablePlacementTypes()
	randIndex := f.s.randSource.Intn(len(options))
	chosenType := options[randIndex]
	if len(f.s.rangeGen.weightedRand) == 0 {
		return NewRandomizedBasicRanges(
			f.s.randSource,
			convertInt64ToInt(f.rangeGenerator.Num()),
			convertInt64ToInt(f.keySpaceGenerator.Num()),
			chosenType,
			defaultReplicationFactor,
			defaultBytes,
		)
	} else {
		return NewWeightedRandomizedBasicRanges(
			f.s.randSource,
			f.s.rangeGen.weightedRand,
			convertInt64ToInt(f.rangeGenerator.Num()),
			convertInt64ToInt(f.keySpaceGenerator.Num()),
			defaultReplicationFactor,
			defaultBytes,
		)
	}
}
