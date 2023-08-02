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
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/gen"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
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
	// OutputAll shows everything above.
	OutputAll = (1 << (iota - 1)) - 1 // (1 << 4) - 1: 0000 1111
)

// ScanFlags converts an array of input strings into a single flag.
func (o OutputFlags) ScanFlags(inputs []string) OutputFlags {
	dict := map[string]OutputFlags{"result_only": OutputResultOnly, "test_settings": OutputTestSettings,
		"initial_state": OutputInitialState, "config_gen": OutputConfigGen, "topology": OutputTopology, "all": OutputAll}
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

// Has returns true if this flag has the given f OutputFlags on.
func (o OutputFlags) Has(f OutputFlags) bool {
	return o&f != 0
}

type testResult struct {
	seed         int64
	failed       bool
	reason       string
	clusterGen   gen.ClusterGen
	rangeGen     gen.RangeGen
	loadGen      gen.LoadGen
	eventGen     gen.EventGen
	initialTime  time.Time
	initialState state.State
}

type testResultsReport struct {
	flags       OutputFlags
	settings    testSettings
	outputSlice []testResult
}

func printTestSettings(t testSettings, buf *strings.Builder) {
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
	}

	if t.randOptions.ranges {
		buf.WriteString(configStr("ranges", "randomized"))
		buf.WriteString(fmt.Sprintf("\t%v\n", t.rangeGen))
	} else {
		buf.WriteString(configStr("ranges", "static"))
	}

	if t.randOptions.load {
		buf.WriteString(configStr("load", "randomized"))
	} else {
		buf.WriteString(configStr("load", "static"))
	}

	if t.randOptions.staticEvents {
		buf.WriteString(configStr("events", "randomized"))
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
		printTestSettings(tr.settings, &buf)
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
			buf.WriteString(fmt.Sprintf("initial state at %s:\n", output.initialTime.String()))
			buf.WriteString(fmt.Sprintf("\t%v\n", output.initialState.PrettyPrint()))
		}
		if failed || tr.flags.Has(OutputTopology) {
			topology := output.initialState.Topology()
			buf.WriteString(fmt.Sprintf("topology:\n%s", topology.String()))
		}
		if failed {
			buf.WriteString(fmt.Sprintf("sample%d: failed assertion\n%s", nthSample, output.reason))
		} else {
			buf.WriteString(fmt.Sprintf("sample%d: pass\n", nthSample))
		}
		buf.WriteString(divider)
	}
	return buf.String()
}
