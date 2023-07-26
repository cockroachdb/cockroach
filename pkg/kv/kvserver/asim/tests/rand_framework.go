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
	"math/rand"
	"strings"
	"testing"
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

type testSettings struct {
	numIterations int
	duration      time.Duration
	verbose       bool
	randSource    *rand.Rand
	assertions    []SimulationAssertion
	randOptions   testRandOptions
}

type randTestingFramework struct {
	s testSettings
}

func (f randTestingFramework) getCluster() gen.ClusterGen {
	if !f.s.randOptions.cluster {
		return defaultBasicClusterGen()
	}
	return gen.BasicCluster{}
}

func (f randTestingFramework) getRanges() gen.RangeGen {
	if !f.s.randOptions.ranges {
		return defaultBasicRangesGen()
	}
	return gen.BasicRanges{}
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
