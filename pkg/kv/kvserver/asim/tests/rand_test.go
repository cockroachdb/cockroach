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
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/datadriven"
)

// TestRandomized is a randomized testing framework designed to validate
// allocator by creating randomized configurations, generating corresponding
// allocator simulations, and validating assertions on the final state.
//
// Input of the framework (fields in the testSetting struct):
//
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
//
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
// 6. rangeGen (default: uniform rangeGenType, uniform keySpaceGenType, empty
// weightedRand).
// - rangeGenType: determines range generator type across iterations
// (default: uniformGenerator, min = 1, max = 1000)
// - keySpaceGenType: determines key space generator type across iterations
// (default: uniformGenerator, min = 1000, max = 200000)
// - weightedRand: if non-empty, enables weighted randomization for range
// distribution
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
	dir := datapathutils.TestDataPath(t, "rand")
	datadriven.Walk(t, dir, func(t *testing.T, path string) {
		randOptions := testRandOptions{}
		rGenSettings := defaultRangeGenSettings()
		cGenSettings := defaultClusterGenSettings()
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "clear":
				randOptions = testRandOptions{}
				rGenSettings = defaultRangeGenSettings()
				cGenSettings = defaultClusterGenSettings()
				return ""
			case "rand_cluster":
				randOptions.cluster = true
				clusterGenType := defaultClusterGenType
				scanIfExists(t, d, "cluster_gen_type", &clusterGenType)
				cGenSettings = clusterGenSettings{
					clusterGenType: clusterGenType,
				}
				return ""
			case "rand_ranges":
				randOptions.ranges = true
				placementType, replicationFactor, rangeGenType, keySpaceGenType := defaultPlacementType, defaultReplicationFactor, defaultRangeGenType, defaultKeySpaceGenType
				weightedRand := defaultWeightedRand
				scanIfExists(t, d, "placement_type", &placementType)
				scanIfExists(t, d, "range_gen_type", &rangeGenType)
				scanIfExists(t, d, "keyspace_gen_type", &keySpaceGenType)
				scanIfExists(t, d, "weighted_rand", &weightedRand)
				rGenSettings = rangeGenSettings{
					placementType:     placementType,
					replicationFactor: replicationFactor,
					rangeGenType:      rangeGenType,
					keySpaceGenType:   keySpaceGenType,
					weightedRand:      weightedRand,
				}
				return ""
			case "rand_load":
				return "unimplemented: randomized load"
			case "rand_events":
				return "unimplemented: randomized events"
			case "rand_settings":
				return "unimplemented: randomized settings"
			case "eval":
				seed := defaultSeed
				numIterations := defaultNumIterations
				duration := defaultDuration
				verbose := defaultVerbosity
				scanIfExists(t, d, "seed", &seed)
				scanIfExists(t, d, "num_iterations", &numIterations)
				scanIfExists(t, d, "duration", &duration)
				scanIfExists(t, d, "verbose", &verbose)
				settings := testSettings{
					numIterations: numIterations,
					duration:      duration,
					randSource:    rand.New(rand.NewSource(seed)),
					assertions:    defaultAssertions(),
					verbose:       verbose,
					randOptions:   randOptions,
					rangeGen:      rGenSettings,
					clusterGen:    cGenSettings,
				}
				f := newRandTestingFramework(settings)
				f.runRandTestRepeated()
				return f.printResults()
			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
	})
}
