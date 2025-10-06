// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	defaultVerbosity     = OutputResultOnly
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
//    across iterations (range ∈[1, 1000]).
//	- keyspace_gen_type: represents the type of distribution used to yield the
//    keyspace parameter as ranges are generated across iterations
//    (keyspace ∈[1000,200000]).
//	- weighted_rand: specifies the weighted random distribution among stores.
//	  Requirements (will panic otherwise): 1. use static option for cluster
//	  generation, specify nodes(default:3) and stores_per_node(default:1)
//	  through change_static_option, and ensure len(weighted_rand) == number of
//	  stores == nodes * stores_per_node
//	  2. weighted_rand should only be used with placement_type=weighted_rand and
//	  vice versa.
//	  3. must specify a weight between [0.0, 1.0] for each element in the array,
//	  with each element corresponding to a store
//	  4. sum of weights in the array should be equal to 1

// 3. "eval" [seed=<int64>] [num_iterations=<int>] [duration=<time.Duration>]
// [verbose=(<[]("result_only","test_settings","initial_state","config_gen","event","topology","all")>)]
// e.g. eval seed=20 duration=30m2s verbose=(test_settings,initial_state)
//  - eval: generates a simulation based on the configuration set with the given
//    commands.
//  - seed(default value is int64(42)): used to create a new random number
//    generator which will then be used to create a new seed for each iteration.
//	- num_iterations(default value is 3): specifies the number of simulations to
//	  run.
//	- duration(default value is 10m): defines duration of each iteration.
//	- verbose(default value is OutputResultOnly): used to set flags on what to
//    show in the test output messages. By default, all details are displayed
//    upon assertion failure.
//    - result_only: only shows whether the test passed or failed, along with
//    any failure messages
//    - test_settings: displays settings used for the repeated tests
//    - initial_state: displays the initial state of each test iteration
//    - config_gen: displays the input configurations generated for each test
//    iteration
//    - topology: displays the topology of cluster configurations
//    - event: displays events executed for the simulation
//    - all: display everything above

// 4. “change_static_option”[nodes=<int>][stores_per_node=<int>]
// [rw_ratio=<float64>] [rate=<float64>] [min_block=<int>] [max_block=<int>]
// [min_key=<int64>] [max_key=<int64>] [skewed_access=<bool>] [ranges=<int>]
// [placement_type=<gen.PlacementType>] [key_space=<int>]
// [replication_factor=<int>] [bytes=<int64>] [stat=<string>] [height=<int>]
// [width=<int>]
// e.g. change_static_option nodes=2 stores_per_node=3 placement_type=skewed
//	- Change_static_option: modifies the settings for the static mode where no
//	  randomization is involved. Note that this does not change the default
//	  settings for any randomized generation.
//	- nodes (default value is 3): number of nodes in the generated cluster
//	- storesPerNode (default value is 1): number of store per nodes in the
// 	  generated cluster
//	- rwRatio (default value is 0.0): read-write ratio of the generated load
//	- rate (default value is 0.0): rate at which the load is generated
//	- minBlock (default value is 1): min size of each load event
//	- maxBlock (default value is 1): max size of each load event
//	- minKey (default value is int64(1)): min key of the generated load
//	- maxKey (default value is int64(200000)): max key of the generated load
//	- skewedAccess (default value is false): is true, workload key generator is
// 	  skewed (zipf)
//	- ranges (default value is 1): number of generated ranges
//	- keySpace (default value is 200000): keyspace for the generated range
//	- placementType (default value is gen.Even): type of distribution for how
// 	  ranges are distributed across stores
//	- replicationFactor (default value is 3): number of replica for each range
//	- bytes (default value is int64(0)): size of each range in bytes
//	- stat (default value is “replicas”): specifies the output to be plotted
//	  for the verbose option
//	- height (default value is 15): height of the plot
//	- width (default value is 80): width of the plot

//  5. "rand_events" [type=<string>{cycle_via_hardcoded_survival_goals,
//  cycle_via_random_survival_goals}]
//  [duration_to_assert_on_event=<time.Duration>]
//  e.g. rand_events type=cycle_via_hardcoded_survival_goals duration=5m
//  - rand_events: generates interesting event series to be scheduled in the
//  simulation.
//  - type: type of event series to be scheduled.
//  - duration_to_assert_on_event: delay to add the assertion events post
//    mutation events for mutation-assertion event series.

// RandTestingFramework is initialized with specified testSetting and maintains
// its state across all iterations. It repeats the test with different random
// configurations. Each iteration in RandTestingFramework executes the following
// steps:
// 1. Generates a random configuration: based on whether randOption is on and
// the specific settings for randomized generation.
// 2. Executes the simulation and checks the assertions on the final state.
// 3. Stores any outputs and assertion failures in a slice.
func TestRandomized(t *testing.T) {
	dir := datapathutils.TestDataPath(t, "rand")
	datadriven.Walk(t, dir, func(t *testing.T, path string) {
		randOptions := testRandOptions{}
		var rGenSettings rangeGenSettings
		var cGenSettings clusterGenSettings
		var eGenSettings eventGenSettings
		staticOptionSettings := getDefaultStaticOptionSettings()
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "clear":
				randOptions = testRandOptions{}
				rGenSettings = rangeGenSettings{}
				cGenSettings = clusterGenSettings{}
				eGenSettings = eventGenSettings{}
				staticOptionSettings = getDefaultStaticOptionSettings()
				return ""
			case "rand_cluster":
				randOptions.cluster = true
				clusterGenType := defaultClusterGenType
				scanIfExists(t, d, "cluster_gen_type", &clusterGenType)
				cGenSettings = clusterGenSettings{
					clusterGenType: clusterGenType,
				}
				return ""
			case "change_static_option":
				scanIfExists(t, d, "nodes", &staticOptionSettings.nodes)
				scanIfExists(t, d, "stores_per_node", &staticOptionSettings.storesPerNode)
				scanIfExists(t, d, "rw_ratio", &staticOptionSettings.rwRatio)
				scanIfExists(t, d, "rate", &staticOptionSettings.rate)
				scanIfExists(t, d, "min_block", &staticOptionSettings.minBlock)
				scanIfExists(t, d, "max_block", &staticOptionSettings.maxBlock)
				scanIfExists(t, d, "min_key", &staticOptionSettings.minKey)
				scanIfExists(t, d, "max_key", &staticOptionSettings.maxKey)
				scanIfExists(t, d, "skewed_access", &staticOptionSettings.skewedAccess)
				scanIfExists(t, d, "ranges", &staticOptionSettings.ranges)
				scanIfExists(t, d, "key_space", &staticOptionSettings.keySpace)
				scanIfExists(t, d, "placement_type", &staticOptionSettings.placementType)
				scanIfExists(t, d, "replication_factor", &staticOptionSettings.replicationFactor)
				scanIfExists(t, d, "bytes", &staticOptionSettings.bytes)
				scanIfExists(t, d, "stat", &staticOptionSettings.stat)
				scanIfExists(t, d, "height", &staticOptionSettings.height)
				scanIfExists(t, d, "width", &staticOptionSettings.width)
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
				randOptions.staticEvents = true
				seriesType, durationToAssertOnEvent := defaultEventsType, defaultDurationToAssertOnEvent
				scanIfExists(t, d, "type", &seriesType)
				scanIfExists(t, d, "duration_to_assert_on_event", &durationToAssertOnEvent)
				eGenSettings = eventGenSettings{
					durationToAssertOnEvent: durationToAssertOnEvent,
					eventsType:              seriesType,
				}
				return ""
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
				s := testSettings{
					numIterations: numIterations,
					duration:      duration,
					randSource:    rand.New(rand.NewSource(seed)),
					assertions:    defaultAssertions(),
					verbose:       verbose,
					randOptions:   randOptions,
					rangeGen:      rGenSettings,
					clusterGen:    cGenSettings,
					eventGen:      eGenSettings,
				}
				f := newRandTestingFramework(s, staticOptionSettings)
				outputs := f.runRandTestRepeated()
				return outputs.String()
			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
	})
}
