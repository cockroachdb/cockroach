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
	"math/rand"
	"testing"
	"time"
)

const (
	defaultNumIterations = 5
	defaultSeed          = 42
	defaultDuration      = 30 * time.Minute
	defaultVerbosity     = false
)

func defaultSettings(randOptions testRandOptions) testSettings {
	return testSettings{
		numIterations: defaultNumIterations,
		duration:      defaultDuration,
		verbose:       defaultVerbosity,
		randSource:    rand.New(rand.NewSource(defaultSeed)),
		assertions:    defaultAssertions(),
		randOptions:   randOptions,
	}
}

// TestRandomized is a randomized testing framework designed to validate
// allocator by creating randomized configurations, generating corresponding
// allocator simulations, and validating assertions on the final state.
//
// Input of the framework (fields in the testSetting struct):
// 1. numIterations (int, default: 3): specifies number of test iterations to be
// run, each with different random configurations generated
// 2. duration (time.Duration, default: 30min): defined simulated duration of
// each iteration verbose (bool, default: false): enables detailed simulation
// information failing output
// 3. randSeed (int64, default: 42): sets seed value for random number
// generation
// 4. assertions ([]SimulationAssertion, default: conformanceAssertion with 0
// under-replication, 0 over-replication, 0 violating, and 0 unavailable):
// defines criteria for validation assertions
// 5. randOptions: guides the aspect of the test configuration that should be
// randomized. This includes:
// - cluster (bool): indicates if the cluster configuration should be randomized
// - ranges (bool): indicates if the range configuration should be randomized
// - load (bool): indicates if the workload configuration should be randomized
// - staticSettings (bool): indicates if the simulation static settings should
// be randomized
// - staticEvents (bool): indicates if static events, including any delayed
// events to be applied during the simulation, should be randomized
//
// RandTestingFramework is initialized with a specified testSetting and
// maintained its state across all iterations. Each iteration in
// RandTestingFramework executes the following steps:
// 1. Generates a random configuration based on whether the aspect of the test
// configuration is set to be randomized in randOptions
// 2. Executes a simulation and store any assertion failures in a buffer
// TODO(wenyihu6): change input structure to datadriven + print more useful info
// for test output + add more tests to cover cases that are not tested by
// default
func TestRandomized(t *testing.T) {
	randOptions := testRandOptions{
		cluster:        false,
		ranges:         false,
		load:           false,
		staticSettings: false,
		staticEvents:   false,
	}
	settings := defaultSettings(randOptions)
	f := randTestingFramework{
		s: settings,
	}
	f.runRandTestRepeated(t)
}
