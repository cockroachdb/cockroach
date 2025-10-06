// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/gen"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/history"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/scheduled"
)

// OutputFlags sets flags for what to output in tests. If you want to add a flag
// here, please add after OutputResultOnly and before OutputAll.
type OutputFlags int

const (
	// OutputResultOnly only shows whether the test passed or failed, along with
	// any failure messages.
	OutputResultOnly OutputFlags = 0
	// OutputTestSettings displays settings used for the repeated tests.
	OutputTestSettings = 1 << (iota - 1) // 1 << 0: 0000 0001
	// OutputInitialState displays the initial state of each test iteration.
	OutputInitialState // 1 << 1: 0000 0010
	// OutputConfigGen displays the input configurations generated for each test
	// iteration.
	OutputConfigGen // 1 << 2: 0000 0100
	// OutputTopology displays the topology of cluster configurations.
	OutputTopology // 1 << 3: 0000 1000
	// OutputEvents displays delayed events executed.
	OutputEvents // 1 << 4: 0001 0000
	// OutputAll shows everything above.
	OutputAll = (1 << (iota - 1)) - 1 // (1 << 5) - 1: 0001 1111
)

// ScanFlags converts an array of input strings into a single flag.
func (o OutputFlags) ScanFlags(inputs []string) OutputFlags {
	dict := map[string]OutputFlags{"result_only": OutputResultOnly, "test_settings": OutputTestSettings,
		"initial_state": OutputInitialState, "config_gen": OutputConfigGen, "topology": OutputTopology,
		"events": OutputEvents, "all": OutputAll}
	flag := OutputResultOnly
	for _, input := range inputs {
		flag = flag.set(dict[input])
	}
	return flag
}

// set returns a flag with f set on.
func (o OutputFlags) set(f OutputFlags) OutputFlags {
	return o | f
}

// Has returns true if this flag has all of the given f OutputFlags on.
func (o OutputFlags) Has(f OutputFlags) bool {
	return o&f == f
}

type testResult struct {
	seed            int64
	failed          bool
	reason          string
	clusterGen      gen.ClusterGen
	rangeGen        gen.RangeGen
	loadGen         gen.LoadGen
	eventGen        gen.EventGen
	initialTime     time.Time
	initialStateStr string
	eventExecutor   scheduled.EventExecutor
	history         history.History
}

type testResultsReport struct {
	flags          OutputFlags
	settings       testSettings
	staticSettings staticOptionSettings
	outputSlice    []testResult
}

func printTestSettings(t testSettings, staticSettings staticOptionSettings, buf *strings.Builder) {
	buf.WriteString(fmt.Sprintln("test settings"))
	buf.WriteString(fmt.Sprintf("\tnum_iterations=%v duration=%s\n", t.numIterations, t.duration.Round(time.Second)))
	divider := fmt.Sprintln("----------------------------------")
	buf.WriteString(divider)

	configStr := func(configType string, optionType string) string {
		return fmt.Sprintf("generating %s configurations using %s option\n", configType, optionType)
	}

	if t.randOptions.cluster {
		buf.WriteString(configStr("cluster", "randomized"))
		buf.WriteString(fmt.Sprintf("\t%v\n", t.clusterGen))
	} else {
		buf.WriteString(configStr("cluster", "static"))
		buf.WriteString(fmt.Sprintf("\tnodes=%d, stores_per_node=%d\n", staticSettings.nodes, staticSettings.storesPerNode))
	}

	if t.randOptions.ranges {
		buf.WriteString(configStr("ranges", "randomized"))
		buf.WriteString(fmt.Sprintf("\t%v\n", t.rangeGen))
	} else {
		buf.WriteString(configStr("ranges", "static"))
		buf.WriteString(fmt.Sprintf("\tplacement_type=%v, ranges=%d, key_space=%d, replication_factor=%d, bytes=%d\n",
			staticSettings.placementType, staticSettings.ranges, staticSettings.keySpace, staticSettings.replicationFactor, staticSettings.bytes))
	}

	if t.randOptions.load {
		buf.WriteString(configStr("load", "randomized"))
	} else {
		buf.WriteString(configStr("load", "static"))
		buf.WriteString(fmt.Sprintf("\trw_ratio=%0.2f, rate=%0.2f, min_block=%d, max_block=%d, min_key=%d, max_key=%d, skewed_access=%t\n",
			staticSettings.rwRatio, staticSettings.rate, staticSettings.minBlock, staticSettings.maxBlock, staticSettings.minKey, staticSettings.maxKey, staticSettings.skewedAccess))
	}

	if t.randOptions.staticEvents {
		buf.WriteString(configStr("events", "randomized"))
		buf.WriteString(fmt.Sprintf("\t%v\n", t.eventGen))
	} else {
		buf.WriteString(configStr("events", "static"))
	}

	if t.randOptions.staticSettings {
		buf.WriteString(configStr("settings", "randomized"))
	} else {
		buf.WriteString(configStr("settings", "static"))
	}
}

// String() formats testResultsReport into a string for output including: 1.
// test settings 2. configurations generated 3. initial state 4. topology 5.
// assertion result
func (tr testResultsReport) String() string {
	buf := strings.Builder{}
	if tr.flags.Has(OutputTestSettings) {
		printTestSettings(tr.settings, tr.staticSettings, &buf)
	}
	divider := fmt.Sprintln("----------------------------------")
	for i, output := range tr.outputSlice {
		nthSample := i + 1
		failed := output.failed
		if i == 0 {
			buf.WriteString(divider)
		}
		buf.WriteString(fmt.Sprintf("sample%d: start running\n", nthSample))
		if failed || tr.flags.Has(OutputConfigGen) {
			buf.WriteString(fmt.Sprintf("configurations generated using seed %d\n", output.seed))
			buf.WriteString(fmt.Sprintf("\t%v\n", output.clusterGen))
			buf.WriteString(fmt.Sprintf("\t%v\n", output.rangeGen))
			buf.WriteString(fmt.Sprintf("\t%v\n", output.loadGen))
			buf.WriteString(fmt.Sprintf("\t%v\n", output.eventGen))
		}
		if failed || tr.flags.Has(OutputInitialState) {
			buf.WriteString(fmt.Sprintf("initial state at %s:\n", output.initialTime.Format("2006-01-02 15:04:05")))
			buf.WriteString(fmt.Sprintf("\t%v\n", output.initialStateStr))
		}
		if failed || tr.flags.Has(OutputTopology) {
			topology := output.history.S.Topology()
			buf.WriteString(fmt.Sprintf("topology:\n%s", topology.String()))
		}
		if failed || tr.flags.Has(OutputEvents) {
			buf.WriteString(output.eventExecutor.PrintEventsExecuted())
		}
		if failed {
			buf.WriteString(fmt.Sprintf("sample%d: failed assertion\n%s\n", nthSample, output.reason))
		} else {
			buf.WriteString(fmt.Sprintf("sample%d: pass\n", nthSample))
		}
		buf.WriteString(divider)
	}
	return buf.String()
}
