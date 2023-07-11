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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/event"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/gen"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/metrics"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/guptarohit/asciigraph"
	"math/rand"
)

const (
	defaultNumIterations = 5
	defaultSeed          = 42
	defaultDuration      = 30 * time.Minute
)

type settings struct {
	numIterations int
	duration      time.Duration
	randSource    *rand.Rand
	verbose       bool
}

func defaultSettings(t *testing.T, randSource *rand.Rand) *settings {
	return &settings{
		numIterations: defaultNumIterations,
		duration:      defaultDuration,
		randSource:    randSource,
		verbose:       false,
	}
}

func defaultWeightedRand(stores int) []float64 {
	// TODO: check how skewDistribution works and if it can be re-used.
	return state.NormalizedSkewedDistribution(stores)
}

func checkAssertions(
	ctx context.Context, history asim.History, assertions []SimulationAssertion,
) (_ asim.History, failed bool, reason string) {
	assertionFailures := []string{}
	failureExists := false
	for _, assertion := range assertions {
		if holds, reason := assertion.Assert(ctx, history); !holds {
			failureExists = true
			assertionFailures = append(assertionFailures, reason)
		}
	}
	if failureExists {
		return history, true, strings.Join(assertionFailures, "")
	}
	return history, false, ""
}

// Inputs:
// workload,
// ranges,
// cluster,
// nodes (with delay),
// set span config (with delay),
// liveness (with delay),
// default capacity
// assertion (conformance)
func runRandomizedTest(
	randSource *rand.Rand,
	duration time.Duration,
) (history asim.History, failed bool, reason string) {
	ctx := context.Background()
	clusterGen := loadClusterInfoGen("MultiRegionSecondaryReproConfig")
	spanCfgInitial := testingSpanConfigInitial()
	spanCfgWithDelay := testingSpanConfigWithDelay()
	rg := NewRandomizedGenerators(randSource)
	rangeGen := rg.randomBasicRangesGen(randSource, defaultWeightedRand(clusterGen.Info.GetNumOfStores()))
	loadGen := defaultLoadGen()
	settingsGen := defaultSettingsGen()
	eventGen := defaultEventGen()
	var delay time.Duration
	eventGen.DelayedEvents = append(eventGen.DelayedEvents, event.DelayedEvent{
		EventFn: func(ctx context.Context, tick time.Time, s state.State) {
			s.SetSpanConfig(allSpanConfig(), spanCfgInitial.AsSpanConfig())
		},
		At: settingsGen.Settings.StartTime.Add(delay),
	})
	delay = 10 * time.Minute
	eventGen.DelayedEvents = append(eventGen.DelayedEvents, event.DelayedEvent{
		EventFn: func(ctx context.Context, tick time.Time, s state.State) {
			s.SetSpanConfig(allSpanConfig(), spanCfgWithDelay.AsSpanConfig())
		},
		At: settingsGen.Settings.StartTime.Add(delay),
	})
	assertions := defaultAssertions()
	simulator := gen.GenerateSimulation(duration, clusterGen, rangeGen, loadGen, settingsGen, eventGen, randSource.Int63())
	simulator.RunSim(ctx)
	history = simulator.History()
	return checkAssertions(ctx, history, assertions)
}

func plotAllHistory(runs []asim.History) string {
	settings := defaultPlotSettings()
	stat, height, width := settings.stat, settings.height, settings.width
	var buf strings.Builder
	for i := 0; i < defaultNumIterations; i++ {
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
	return buf.String()
}

func runRandomizedTestsRepeated(t *testing.T, settings *settings) {
	runs := []asim.History{}
	numIterations := settings.numIterations
	failureExists := false
	var buf strings.Builder
	for iter := 0; iter < numIterations; iter++ {
		history, failed, reason := runRandomizedTest(settings.randSource, settings.duration)
		runs = append(runs, history)
		if failed {
			failureExists = true
			fmt.Fprintf(&buf, "failed assertion sample %d\n%s", iter+1, reason)
		}
	}

	if settings.verbose {
		plotStr := plotAllHistory(runs)
		log.Infof(context.Background(), "%v", plotStr)
	}

	if failureExists {
		t.Fatal(buf.String())
	}
}

func TestRandomized(t *testing.T) {
	// BasicRanges: RangesInfo
	// Configuration: randomized cluster set up
	// Workload: randomized workload generation
	// TODO: check if seed should be changed to randInt63
	randSource := rand.New(rand.NewSource(defaultSeed))
	runRandomizedTestsRepeated(t, defaultSettings(t, randSource))
}

// missing add_node, set_span_config, set_liveness, set_capacity, setting
