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
	"text/tabwriter"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/gen"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/metrics"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/guptarohit/asciigraph"
)

type testRandOptions struct {
	cluster        bool
	ranges         bool
	load           bool
	staticSettings bool
	staticEvents   bool
}

func (t testRandOptions) printRandOptions(w *tabwriter.Writer) {
	_, _ = fmt.Fprintf(w,
		"rand_options\tcluster=%t\tranges=%t\tload=%t\tstaticSettings=%t\tstaticEvents=%t\n", t.cluster, t.ranges, t.load, t.staticSettings, t.staticEvents)
}

type testSettings struct {
	numIterations int
	duration      time.Duration
	verbose       bool
	randSource    *rand.Rand
	assertions    []SimulationAssertion
	randOptions   testRandOptions
	clusterGen    clusterGenSettings
	rangeGen      rangeGenSettings
}

func (t testSettings) printTestSettings(w *tabwriter.Writer) {
	_, _ = fmt.Fprintf(w, "settings\tnum_iterations=%v\tduration=%s\n", t.numIterations, t.duration.Round(time.Second))

	t.randOptions.printRandOptions(w)
	if t.randOptions.cluster {
		_, _ = fmt.Fprint(w, "rand_cluster=true\t")
		t.clusterGen.printClusterGenSettings(w)
	} else {
		_, _ = fmt.Fprintln(w, "rand_cluster=false")
	}

	if t.randOptions.ranges {
		_, _ = fmt.Fprint(w, "rand_ranges=true\t")
		t.rangeGen.printRangeGenSettings(w)
	} else {
		_, _ = fmt.Fprintln(w, "rand_cluster=false")
	}

	_, _ = fmt.Fprintf(w, "rand_load=%t\n", t.randOptions.load)
	_, _ = fmt.Fprintf(w, "rand_events=%t\n", t.randOptions.staticEvents)
	_, _ = fmt.Fprintf(w, "rand_settings=%t\n", t.randOptions.staticSettings)
}

type randTestingFramework struct {
	recordBuf         *strings.Builder
	s                 testSettings
	rangeGenerator    generator
	keySpaceGenerator generator
}

// newRandTestingFramework constructs a new testing framework with the given
// testSettings. It also initializes generators for randomized range generation.
// Since generators persist across iterations, this leads to the formation of a
// distribution as ranges are generated. Additionally, it initializes a buffer
// that persists across all iterations, recording outputs and states of each
// iteration.
func newRandTestingFramework(settings testSettings) randTestingFramework {
	if int64(defaultMaxRange) > defaultMinKeySpace {
		panic(fmt.Sprintf(
			"Max number of ranges specified (%d) is greater than number of keys in key space (%d) ",
			defaultMaxRange, defaultMinKeySpace))
	}
	rangeGenerator := newGenerator(settings.randSource, defaultMinRange, defaultMaxRange, settings.rangeGen.rangeGenType)
	keySpaceGenerator := newGenerator(settings.randSource, defaultMinKeySpace, defaultMaxKeySpace, settings.rangeGen.keySpaceGenType)
	var buf strings.Builder

	return randTestingFramework{
		recordBuf:         &buf,
		s:                 settings,
		rangeGenerator:    rangeGenerator,
		keySpaceGenerator: keySpaceGenerator,
	}
}

func (f randTestingFramework) getCluster() gen.ClusterGen {
	if !f.s.randOptions.cluster {
		return defaultBasicClusterGen()
	}
	return f.randomClusterInfoGen(f.s.randSource)
}

func (f randTestingFramework) getRanges() gen.RangeGen {
	if !f.s.randOptions.ranges {
		return defaultBasicRangesGen()
	}
	return f.randomBasicRangesGen()
}

func (f randTestingFramework) getLoad() gen.LoadGen {
	if !f.s.randOptions.load {
		return defaultLoadGen()
	}
	return gen.BasicLoad{}
}

func (f randTestingFramework) getStaticSettings() gen.StaticSettings {
	if !f.s.randOptions.staticSettings {
		return defaultStaticSettingsGen()
	}
	return gen.StaticSettings{}
}

func (f randTestingFramework) getStaticEvents() gen.StaticEvents {
	if !f.s.randOptions.staticEvents {
		return defaultStaticEventsGen()
	}
	return gen.StaticEvents{}
}

// runRandTest creates randomized configurations based on the specified test
// settings and runs one test using those configurations.
func (f randTestingFramework) runRandTest() (asim.History, bool, string) {
	ctx := context.Background()
	cluster := f.getCluster()
	ranges := f.getRanges()
	load := f.getLoad()
	staticSettings := f.getStaticSettings()
	staticEvents := f.getStaticEvents()
	simulator := gen.GenerateSimulation(f.s.duration, cluster, ranges, load, staticSettings, staticEvents, f.s.randSource.Int63())
	simulator.RunSim(ctx)
	history := simulator.History()
	failed, reason := checkAssertions(ctx, history, f.s.assertions)
	return history, failed, reason
}

// runRandTestRepeated runs the test multiple times, each time with a new
// randomly generated configuration. The result of each iteration is recorded in
// f.recordBuf.
func (f randTestingFramework) runRandTestRepeated() {
	numIterations := f.s.numIterations
	runs := make([]asim.History, numIterations)
	w := tabwriter.NewWriter(f.recordBuf, 4, 0, 2, ' ', 0)
	f.s.printTestSettings(w)
	for i := 0; i < numIterations; i++ {
		if i == 0 {
			f.recordBuf.WriteString(fmt.Sprintln("----------------------------------"))
		}
		f.recordBuf.WriteString(fmt.Sprintf("sample%d: start running\n", i+1))
		history, failed, reason := f.runRandTest()
		runs[i] = history
		if failed {
			f.recordBuf.WriteString(fmt.Sprintf("sample%d: failed assertion\n%s", i+1, reason))
		} else {
			f.recordBuf.WriteString(fmt.Sprintf("sample%d: pass\n", i+1))
		}
		f.recordBuf.WriteString(fmt.Sprintln("----------------------------------"))
	}

	if f.s.verbose {
		plotAllHistory(runs, f.recordBuf)
	}
}

// printResults outputs the following information: 1. test settings used for
// generating the tests 2. results of each randomized test
func (f randTestingFramework) printResults() string {
	return f.recordBuf.String()
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

// PlotAllHistory outputs stat plots for the provided asim history array into
// the given strings.Builder buf.
func plotAllHistory(runs []asim.History, buf *strings.Builder) {
	settings := defaultPlotSettings()
	stat, height, width := settings.stat, settings.height, settings.width
	for i := 0; i < len(runs); i++ {
		history := runs[i]
		ts := metrics.MakeTS(history.Recorded)
		statTS := ts[stat]
		buf.WriteString(fmt.Sprintf("sample%d\n", i+1))
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
