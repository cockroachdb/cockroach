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

// OutputFlags sets flags for what to output in tests. If you want to add a flag
// here, please add after OutputNothing and before OutputAll.
type OutputFlags int

const (
	OutputNothing      OutputFlags       = 0
	OutputTestSettings                   = 1 << (iota - 1) // 1 << 0: 0000 0001
	OutputInitialState                                     // 1 << 1: 0000 0010
	OutputConfigGen                                        // 1 << 2: 0000 0100
	OutputPlotHistory                                      // 1 << 3: 0000 1000
	OutputAll          = (1 << iota) - 1                   // (1 << 4) - 1: 0000 1111
)

func (o OutputFlags) ScanFlags(inputs []string) OutputFlags {
	dict := map[string]OutputFlags{"nothing": OutputNothing, "test_settings": OutputTestSettings,
		"initial_state": OutputInitialState, "config_gen": OutputConfigGen, "plot_history": OutputPlotHistory, "all": OutputAll}
	flag := OutputNothing
	for _, input := range inputs {
		flag = flag.set(dict[input])
	}
	return flag
}

func (o OutputFlags) set(f OutputFlags) OutputFlags {
	return o | f
}

func (o OutputFlags) Has(f OutputFlags) bool {
	return o&f != 0
}

type testSettings struct {
	numIterations int
	duration      time.Duration
	verbose       OutputFlags
	randSource    *rand.Rand
	assertions    []SimulationAssertion
	randOptions   testRandOptions
	clusterGen    clusterGenSettings
	rangeGen      rangeGenSettings
}

// printTestSettings outputs the string representation of the test settings to
// the given buffer w.
// For example,
// SETTINGS      num_iterations=3  duration=10m0s
// rand_cluster=false
// rand_cluster=false
// rand_load=false
// rand_events=false
// rand_settings=false
func (t testSettings) printTestSettings(w *tabwriter.Writer) {
	_, _ = fmt.Fprintf(w, "SETTINGS\tnum_iterations=%v\tduration=%s\n", t.numIterations, t.duration.Round(time.Second))
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
	recordBuf            *strings.Builder
	staticOptionSettings staticOptionSettings
	s                    testSettings
	rangeGenerator       generator
	keySpaceGenerator    generator
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
	var buf strings.Builder

	return randTestingFramework{
		recordBuf:            &buf,
		staticOptionSettings: staticOptionSettings,
		s:                    s,
		rangeGenerator:       rangeGenerator,
		keySpaceGenerator:    keySpaceGenerator,
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

func (f randTestingFramework) getStaticEvents() gen.StaticEvents {
	if !f.s.randOptions.staticEvents {
		return f.defaultStaticEventsGen()
	}
	return gen.StaticEvents{}
}

// printConfigGen outputs the string representation of the configurations
// generated in f.recordBuf.
// For example,
// CONFIG_GEN: duration=[5m0s] clusterGen=[basic_cluster: nodes=3, stores=1]
// rangeGen=[placement_type=even, base_ranges=ranges=1, key_space=200000,
// replication_factor=3, bytes=0] loadGen=[rw_ratio=0.00, rate=0.00,
// skewed_access=false, min_block_size=1, max_block_size=1, min_key=1,
// max_key=200000] eventGen=[len(delayed_event_list)=0]
// seed=[3440579354231278675]
func (f randTestingFramework) printConfigGen(
	duration time.Duration,
	clusterGen gen.ClusterGen,
	rangeGen gen.RangeGen,
	loadGen gen.LoadGen,
	eventGen gen.EventGen,
	seed int64,
) {
	f.recordBuf.WriteString(fmt.Sprintf("CONFIG_GEN:\nduration=[%s]\nclusterGen=[%v]\nrangeGen=[%v]\nloadGen=[%v]\neventGen=[%v]\nseed=[%d]\n",
		duration, clusterGen, rangeGen, loadGen, eventGen, seed))
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
	seed := f.s.randSource.Int63()
	if f.s.verbose.Has(OutputConfigGen) {
		f.printConfigGen(f.s.duration, cluster, ranges, load, staticEvents, seed)
	}
	simulator := gen.GenerateSimulation(f.s.duration, cluster, ranges, load, staticSettings, staticEvents, seed)
	if f.s.verbose.Has(OutputInitialState) {
		simulator.PrintState(f.recordBuf)
	}
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
	if f.s.verbose.Has(OutputTestSettings) {
		f.s.printTestSettings(w)
	}
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

	if f.s.verbose.Has(OutputPlotHistory) {
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
func (f randTestingFramework) plotAllHistory(runs []asim.History, buf *strings.Builder) {
	settings := f.defaultPlotSettings()
	stat, height, width := settings.stat, settings.height, settings.width
	buf.WriteString(fmt.Sprintln("PLOT"))
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
		if f.s.randOptions.cluster {
			panic("randomized cluster with weighted rand stores is not supported")
		}
		if stores, length := f.staticOptionSettings.storesPerNode*f.staticOptionSettings.nodes, len(f.s.rangeGen.weightedRand); stores != length {
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
