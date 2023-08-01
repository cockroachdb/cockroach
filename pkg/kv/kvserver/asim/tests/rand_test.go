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

// TestRandomized is a randomized data-driven testing framework that validates
// allocators by creating randomized configurations. It is designed for
// regression and exploratory testing. The following commands are provided:

// rand_cluster: randomly picks a predefined cluster configuration according to
// the specified type
// - “rand_cluster”
// [cluster_gen_type=(single_region|multi_region|any_region)]:
// represents a type of cluster configuration
// e.g. rand_cluster cluster_gen_type=(multi_region)

// rand_ranges: randomly generate a distribution of ranges across stores
// - “rand_ranges”
// [placement_type=(uniform|skewed|random|weighted_rand)]
// [replication_factor=<int>]
// [range_gen_type=(uniform|zipf)]: default value is uniform
// [keyspace_gen_type=(uniform|zipf)]: default value is uniform
// [weighted_rand=(<[]float64>)]: default value is []float64{}
// e.g. rand_ranges placement_type=weighted_rand weighted_rand=(0.1,0.2,0.7)
// e.g. rand_ranges placement_type=skewed replication_factor=1
// range_gen_type=zipf keyspace_gen_type=uniform

// placement_type: represents the type of range placement distribution across
// stores
// range_gen_type, keyspace_gen_type: represent the range or keyspace generator
// type which forms a distribution every time ranges are generated across
// iterations
// replication_factor: represents the replication factor of each range
// weighted_rand: specifies the weighted random distribution among stores. Note
// that use weighted_rand only with placement_type=weighted_rand and vice
// versa. It is expected to specify a weight [0.0, 1.0] for each store in the
// configuration.

// eval: generates simulation with the configuration set with the commands
// - “eval”
// [seed=<int64>]: default value is int64(42)
// [num_iterations=<int>]: default value is 3
// [duration=<time.Duration>]: default value is 10m
// [verbose=<bool>]: default value is false
// e.g. eval seed=20 duration=30m2s verbose=true

// clear: clears the configurations set
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
			case "load_cluster":

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
