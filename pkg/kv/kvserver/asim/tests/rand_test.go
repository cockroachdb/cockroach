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

func defaultSettings(randSource *rand.Rand) *settings {
	return &settings{
		numIterations: defaultNumIterations,
		duration:      defaultDuration,
		randSource:    randSource,
		verbose:       false,
	}
}

func runRandTest(
	randSource *rand.Rand, duration time.Duration, randOptions map[string]bool,
) (asim.History, bool, string) {
	ctx := context.Background()
	cluster := getCluster(randOptions["cluster"])
	ranges := getRanges(randOptions["ranges"])
	load := getLoad(randOptions["load"])
	staticSettings := getStaticSettings(randOptions["static_settings"])
	staticEvents := getStaticEvents(randOptions["static_events"])
	assertions := getAssertions(randOptions["assertions"])
	simulator := gen.GenerateSimulation(duration, cluster, ranges, load, staticSettings, staticEvents, randSource.Int63())
	simulator.RunSim(ctx)
	history := simulator.History()
	failed, reason := checkAssertions(ctx, history, assertions)
	return history, failed, reason
}

func runRandTestRepeated(t *testing.T, settings *settings, randOptions map[string]bool) {
	numIterations := settings.numIterations
	runs := make([]asim.History, numIterations)
	failureExists := false
	var buf strings.Builder
	for i := 0; i < numIterations; i++ {
		history, failed, reason := runRandTest(settings.randSource, settings.duration, randOptions)
		runs[i] = history
		if failed {
			failureExists = true
			fmt.Fprintf(&buf, "failed assertion sample %d\n%s", i+1, reason)
		}
	}

	if settings.verbose {
		plotAllHistory(runs, &buf)
	}

	if failureExists {
		t.Fatal(buf.String())
	}
}

// TestRandomized is a randomized testing framework which validates an allocator
// simulator by creating randomized configurations, exposing potential edge
// cases and bugs.
//
// Inputs:
//
// numIterations - Number of test iterations.
// duration - Duration of each test iteration.
// randSeed - Random number generation seed.
// verbose - Boolean to enable detailed failure output.
// randOptions - A map with (key: option, value: boolean indicating if the key
// option should be randomized).
func TestRandomized(t *testing.T) {
	randOptions := map[string]bool{
		"cluster":         false,
		"ranges":          false,
		"load":            false,
		"static_settings": false,
		"static_events":   false,
		"assertions":      false,
	}
	randSource := rand.New(rand.NewSource(defaultSeed))
	runRandTestRepeated(t, defaultSettings(randSource), randOptions)
}
