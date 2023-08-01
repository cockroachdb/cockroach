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
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/datadriven"
)

const (
	defaultNumIterations = 3
	defaultSeed          = int64(42)
	defaultDuration      = 10 * time.Minute
	defaultVerbosity     = false
)

// TestRandomized is a randomized data-driven testing framework that validates
// allocators by creating randomized configurations. It is designed for
// regression and exploratory testing.

// There are three modes for every aspect of randomized generation.
// - Static Mode:
//   1. If randomization options are disabled (e.g. no rand_ranges command is
//   used), the system uses the default configurations (defined in
//   default_settings.go) with no randomization.
// - Randomized: two scenarios occur:
//   2. Use default settings for randomized generation (e.g.rand_ranges)
//   3. Use settings specified with commands (e.g.rand_ranges
//   range_gen_type=zipf)

// The following commands are provided:
//	1. "rand_cluster" [cluster_gen_type=(single_region|multi_region|any_region)]
//	e.g. rand_cluster cluster_gen_type=(multi_region)
//	- rand_cluster: randomly picks a predefined cluster configuration
//    according to the specified type.
//	- cluster_gen_type (default value is multi_region): cluster configuration
//    type. On the next eval, the cluster is generated as the initial state of
//    the simulation.

//	2. "rand_ranges" [placement_type=(even|skewed|random|weighted_rand)]
// 	[replication_factor=<int>] [range_gen_type=(uniform|zipf)]
//	[keyspace_gen_type=(uniform|zipf)] [weighted_rand=(<[]float64>)]
//	e.g. rand_ranges placement_type=weighted_rand weighted_rand=(0.1,0.2,0.7)
//	e.g. rand_ranges placement_type=skewed replication_factor=1
//		 range_gen_type=zipf keyspace_gen_type=uniform
//	- rand_ranges: randomly generate a distribution of ranges across stores
//    based on the specified parameters. On the next call to eval, ranges and
//    their replica placement are generated and loaded to initial state.
//	- placement_type(default value is even): defines the type of range placement
// 	  distribution across stores. Once set, it remains constant across
// 	  iterations with no randomization involved.
//	- replication_factor(default value is 3): represents the replication factor
//	  of each range. Once set, it remains constant across iterations with no
//	  randomization involved.
//	- range_gen_type(default value is uniform): represents the type of
//	  distribution used to yield the range parameter as ranges are generated
//    across iterations (range ∈[1, 1000])
//	- keyspace_gen_type: represents the type of distribution used to yield the
//    keyspace parameter as ranges are generated across iterations
//    (keyspace ∈[1000,200000])
//	- weighted_rand: specifies the weighted random distribution among stores.
//	  Requirements (will panic otherwise): 1. weighted_rand should only be
//    used with placement_type=weighted_rand and vice versa. 2. Must specify a
//    weight between [0.0, 1.0] for each element in the array, with each element
//    corresponding to a store 3. len(weighted_rand) cannot be greater than
//    number of stores 4. sum of weights in the array should be equal to 1

// 3. "eval" [seed=<int64>] [num_iterations=<int>] [duration=<time.Duration>]
// [verbose=<bool>]
// e.g. eval seed=20 duration=30m2s verbose=true
//  - eval: generates a simulation based on the configuration set with the given
//    commands
//  - seed(default value is int64(42)): used to create a new random number
//    generator which will then be used to create a new seed for each iteration
//	- num_iterations(default value is 3): specifies the number of simulations to
//	  run
//	- duration(default value is 10m): defines duration of each iteration
//	- verbose(default value is false): if set to true, plots all stat(as
//	  specified by defaultStat) history

// RandTestingFramework is initialized with specified testSetting and maintains
// its state across all iterations. It repeats the test with different random
// configurations. Each iteration in RandTestingFramework executes the following
// steps:
// 1. Generates a random configuration: based on whether randOption is on and
// the specific settings for randomized generation
// 2. Executes the simulation and checks the assertions on the final state.
// 3. Stores any outputs and assertion failures in a buffer
func TestRandomized(t *testing.T) {
	dir := datapathutils.TestDataPath(t, "rand")
	datadriven.Walk(t, dir, func(t *testing.T, path string) {
		randOptions := testRandOptions{}
		var rGenSettings rangeGenSettings
		var cGenSettings clusterGenSettings
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "clear":
				randOptions = testRandOptions{}
				rGenSettings = rangeGenSettings{}
				cGenSettings = clusterGenSettings{}
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
				scanIfExists(t, d, "replication_factor", &replicationFactor)
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
