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

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/allocatorimpl"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/event"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/gen"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/metrics"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigtestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/guptarohit/asciigraph"
	"github.com/stretchr/testify/require"
)

// TestDataDriven is a data-driven test for the allocation system using the
// simulator. It gives contributors a way to understand how a cluster reacts to
// different settings, load and configuration. In addition, it may be used for
// regression and exploratory testing with parameterized random generation and
// assertions. The following syntax is provided.
//
//   - "gen_load" [rw_ratio=<float>] [rate=<float>] [access_skew=<bool>]
//     [min_block=<int>] [max_block=<int>] [min_key=<int>] [max_key=<int>]
//     Initialize the load generator with parameters. On the next call to eval,
//     the load generator is called to create the workload used in the
//     simulation. The default values are: rw_ratio=0 rate=0 min_block=1
//     max_block=1 min_key=1 max_key=10_000 access_skew=false.
//
//   - "ken_cluster" [nodes=<int>] [stores_per_node=<int>]
//     Initialize the cluster generator parameters. On the next call to eval,
//     the cluster generator is called to create the initial state used in the
//     simulation. The default values are: nodes=3 stores_per_node=1.
//
//   - "load_cluster": config=<name>
//     Load a defined cluster configuration to be the generated cluster in the
//     simulation. The available confiurations are: single_region: 15 nodes in
//     region=US, 5 in each zone US_1/US_2/US_3. single_region_multi_store: 3
//     nodes, 5 stores per node with the same zone/region configuration as
//     above. multi_region: 36 nodes, 12 in each region and 4 in each zone,
//     regions having 3 zones. complex: 28 nodes, 3 regions with a skewed
//     number of nodes per region.
//
//   - "gen_ranges" [ranges=<int>] [placement_skew=<bool>] [repl_factor=<int>]
//     [keyspace=<int>] [range_bytes=<int>]
//     Initialize the range generator parameters. On the next call to eval, the
//     range generator is called to assign an ranges and their replica
//     placement. The default values are ranges=1 repl_factor=3
//     placement_skew=false keyspace=10000.
//
//   - set_liveness node=<int> [delay=<duration>]
//     status=(dead|decommisssioning|draining|unavailable)
//     Set the liveness status of the node with ID NodeID. This applies at the
//     start of the simulation or with some delay after the simulation starts,
//     if specified.
//
//   - add_node: [stores=<int>] [locality=<string>] [delay=<duration>]
//     Add a node to the cluster after initial generation with some delay,
//     locality and number of stores on the node. The default values are
//     stores=0 locality=none delay=0.
//
//   - set_span_config [delay=<duration>]
//     [startKey, endKey): <span_config> Provide a new line separated list
//     of spans and span configurations e.g.
//     [0,100): num_replicas=5 num_voters=3 constraints={'+region=US_East'}
//     [100, 500): num_replicas=3
//     ...
//     This will update the span config for the span [0,100) to specify 3
//     voting replicas and 2 non-voting replicas, with a constraint that all
//     replicas are in the region US_East.
//
//   - "assertion" type=<string> [stat=<string>] [ticks=<int>]
//     [(exact_bound|upper_bound|lower_bound)=<float>] [store=<int>]
//     [(under|over|unavailable|violating)=<int>]
//     Add an assertion to the list of assertions that run against each sample
//     on subsequent calls to eval. When every assertion holds during eval, OK
//     is printed, otherwise the reason the assertion(s) failed is printed.
//     type=balance,steady,stat assertions look at the last 'ticks' duration of
//     the simulation run. type=conformance assertions look at the end of the
//     evaluation.
//
//     For type=balance assertions, the max stat (e.g. stat=qps) value of each
//     store is taken and divided by the mean store stat value. If the max/mean
//     violates the threshold constraint provided (e.g. upper_bound=1.15) during
//     any of the last ticks (e.g. ticks=5), then the assertion fails. This
//     assertion applies over all stores, at each tick, for the last 'ticks'
//     duration.
//
//     For type=steady assertions, if the max or min stat (e.g. stat=replicas)
//     value over the last ticks (e.g. ticks=6) duration violates the threshold
//     constraint provided (e.g. upper_bound=0.05) % of the mean, the assertion
//     fails. This assertion applies per-store, over 'ticks' duration.
//
//     For type=stat assertions, if the stat (e.g. stat=replicas) value over the
//     last ticks (e.g. ticks=5) duration violates the threshold constraint
//     provided (e.g. exact=0), the assertion fails. This applies for specified
//     stores which must be provided with stores=(storeID,...).
//
//     For type=conformance assertions, you may assert on the number of
//     replicas that you expect to be under-replicated (under),
//     over-replicated(over), unavailable(unavailable) and violating
//     constraints(violating) at the end of the evaluation.
//
//   - "setting" [rebalance_mode=<int>] [rebalance_interval=<duration>]
//     [rebalance_qps_threshold=<float>] [split_qps_threshold=<float>]
//     [rebalance_range_threshold=<float>] [gossip_delay=<duration>]
//     Configure the simulation's various settings. The default values are:
//     rebalance_mode=2 (leases and replicas) rebalance_interval=1m (1 minute)
//     rebalance_qps_threshold=0.1 split_qps_threshold=2500
//     rebalance_range_threshold=0.05 gossip_delay=500ms.
//
//   - "eval" [duration=<string>] [samples=<int>] [seed=<int>]
//     Run samples (e.g. samples=5) number of simulations for duration (e.g.
//     duration=10m) time and evaluate every prior declared assertion against
//     each sample. The seed (e.g. seed=42) given will be used to create a new
//     random number generator that creates the seed used to generate each
//     simulation sample. The default values are: duration=30m (30 minutes)
//     samples=1 seed=random.
//
//   - "plot" stat=<string> [sample=<int>] [height=<int>] [width=<int>]
//     Visually renders the stat (e.g. stat=qps) as a series where the x axis
//     is the simulated time and the y axis is the stat value. A series is
//     rendered per-store, so if there are 10 stores, 10 series will be
//     rendered.
//
//   - "topology" [sample=<int>]
//     Print the cluster locality topology of the sample given (default=last).
//     e.g. for the load_cluster config=single_region
//     US
//     ..US_1
//     ....└── [1 2 3 4 5]
//     ..US_2
//     ....└── [6 7 8 9 10]
//     ..US_3
//     ....└── [11 12 13 14 15]
func TestDataDriven(t *testing.T) {
	ctx := context.Background()
	dir := datapathutils.TestDataPath(t, ".")
	datadriven.Walk(t, dir, func(t *testing.T, path string) {
		const defaultKeyspace = 10000
		loadGen := gen.BasicLoad{}
		var clusterGen gen.ClusterGen
		var rangeGen gen.RangeGen = gen.BasicRanges{
			Ranges:            1,
			ReplicationFactor: 1,
			KeySpace:          defaultKeyspace,
		}
		settingsGen := gen.StaticSettings{Settings: config.DefaultSimulationSettings()}
		eventGen := gen.StaticEvents{DelayedEvents: event.DelayedEventList{}}
		assertions := []SimulationAssertion{}
		runs := []asim.History{}
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "gen_load":
				var rwRatio, rate = 0.0, 0.0
				var minBlock, maxBlock = 1, 1
				var minKey, maxKey = int64(1), int64(defaultKeyspace)
				var accessSkew bool

				scanIfExists(t, d, "rw_ratio", &rwRatio)
				scanIfExists(t, d, "rate", &rate)
				scanIfExists(t, d, "access_skew", &accessSkew)
				scanIfExists(t, d, "min_block", &minBlock)
				scanIfExists(t, d, "max_block", &maxBlock)
				scanIfExists(t, d, "min_key", &minKey)
				scanIfExists(t, d, "max_key", &maxKey)

				loadGen.SkewedAccess = accessSkew
				loadGen.MinKey = minKey
				loadGen.MaxKey = maxKey
				loadGen.RWRatio = rwRatio
				loadGen.Rate = rate
				loadGen.MaxBlockSize = maxBlock
				loadGen.MinBlockSize = minBlock
				return ""
			case "gen_ranges":
				var ranges, replFactor, keyspace = 1, 3, defaultKeyspace
				var bytes int64 = 0
				var placementSkew bool

				scanIfExists(t, d, "ranges", &ranges)
				scanIfExists(t, d, "repl_factor", &replFactor)
				scanIfExists(t, d, "placement_skew", &placementSkew)
				scanIfExists(t, d, "keyspace", &keyspace)
				scanIfExists(t, d, "bytes", &bytes)

				var placementType gen.PlacementType
				if placementSkew {
					placementType = gen.Skewed
				} else {
					placementType = gen.Uniform
				}
				rangeGen = gen.BasicRanges{
					Ranges:            ranges,
					PlacementType:     placementType,
					KeySpace:          keyspace,
					ReplicationFactor: replFactor,
					Bytes:             bytes,
				}
				return ""
			case "topology":
				var sample = len(runs)
				scanIfExists(t, d, "sample", &sample)
				top := runs[sample-1].S.Topology()
				return (&top).String()
			case "gen_cluster":
				var nodes = 3
				var storesPerNode = 1
				scanIfExists(t, d, "nodes", &nodes)
				scanIfExists(t, d, "stores_per_node", &storesPerNode)
				clusterGen = gen.BasicCluster{
					Nodes:         nodes,
					StoresPerNode: storesPerNode,
				}
				return ""
			case "load_cluster":
				var config string
				var clusterInfo state.ClusterInfo
				scanArg(t, d, "config", &config)

				switch config {
				case "single_region":
					clusterInfo = state.SingleRegionConfig
				case "single_region_multi_store":
					clusterInfo = state.SingleRegionMultiStoreConfig
				case "multi_region":
					clusterInfo = state.MultiRegionConfig
				case "complex":
					clusterInfo = state.ComplexConfig
				default:
					panic(fmt.Sprintf("unknown cluster config %s", config))
				}

				clusterGen = gen.LoadedCluster{
					Info: clusterInfo,
				}
				return ""
			case "add_node":
				var delay time.Duration
				var numStores = 1
				var localityString string
				scanIfExists(t, d, "delay", &delay)
				scanIfExists(t, d, "stores", &numStores)
				scanIfExists(t, d, "locality", &localityString)

				addEvent := event.DelayedEvent{
					EventFn: func(ctx context.Context, tick time.Time, s state.State) {
						node := s.AddNode()
						if localityString != "" {
							var locality roachpb.Locality
							if err := locality.Set(localityString); err != nil {
								panic(fmt.Sprintf("unable to set node locality %s", err.Error()))
							}
							s.SetNodeLocality(node.NodeID(), locality)
						}
						for i := 0; i < numStores; i++ {
							if _, ok := s.AddStore(node.NodeID()); !ok {
								panic(fmt.Sprintf("adding store to node=%d failed", node))
							}
						}
					},
					At: settingsGen.Settings.StartTime.Add(delay),
				}
				eventGen.DelayedEvents = append(eventGen.DelayedEvents, addEvent)
				return ""
			case "set_span_config":
				var delay time.Duration
				scanIfExists(t, d, "delay", &delay)
				for _, line := range strings.Split(d.Input, "\n") {
					line = strings.TrimSpace(line)
					if line == "" {
						continue
					}
					tag, data, found := strings.Cut(line, ":")
					require.True(t, found)
					tag, data = strings.TrimSpace(tag), strings.TrimSpace(data)
					span := spanconfigtestutils.ParseSpan(t, tag)
					conf := spanconfigtestutils.ParseZoneConfig(t, data).AsSpanConfig()
					eventGen.DelayedEvents = append(eventGen.DelayedEvents, event.DelayedEvent{
						EventFn: func(ctx context.Context, tick time.Time, s state.State) {
							s.SetSpanConfig(span, conf)
						},
						At: settingsGen.Settings.StartTime.Add(delay),
					})
				}
				return ""
			case "set_liveness":
				var nodeID int
				var liveness string
				var delay time.Duration
				livenessStatus := 3
				scanArg(t, d, "node", &nodeID)
				scanArg(t, d, "liveness", &liveness)
				scanIfExists(t, d, "delay", &delay)
				switch liveness {
				case "unknown":
					livenessStatus = 0
				case "dead":
					livenessStatus = 1
				case "unavailable":
					livenessStatus = 2
				case "live":
					livenessStatus = 3
				case "decommissioning":
					livenessStatus = 4
				case "draining":
					livenessStatus = 5
					panic(fmt.Sprintf("unkown liveness status: %s", liveness))
				}
				eventGen.DelayedEvents = append(eventGen.DelayedEvents, event.DelayedEvent{
					EventFn: func(ctx context.Context, tick time.Time, s state.State) {
						s.SetNodeLiveness(
							state.NodeID(nodeID),
							livenesspb.NodeLivenessStatus(livenessStatus),
						)
					},
					At: settingsGen.Settings.StartTime.Add(delay),
				})
				return ""
			case "set_capacity":
				var store int
				var ioThreshold float64 = -1
				var capacity, available int64 = -1, -1
				var delay time.Duration

				scanArg(t, d, "store", &store)
				scanIfExists(t, d, "io_threshold", &ioThreshold)
				scanIfExists(t, d, "capacity", &capacity)
				scanIfExists(t, d, "available", &available)
				scanIfExists(t, d, "delay", &delay)

				capacityOverride := state.NewCapacityOverride()
				capacityOverride.Capacity = capacity
				capacityOverride.Available = available
				if ioThreshold != -1 {
					capacityOverride.IOThreshold = allocatorimpl.TestingIOThresholdWithScore(ioThreshold)
				}

				eventGen.DelayedEvents = append(eventGen.DelayedEvents, event.DelayedEvent{
					EventFn: func(ctx context.Context, tick time.Time, s state.State) {
						log.Infof(ctx, "setting capacity override %+v", capacityOverride)
						s.SetCapacityOverride(state.StoreID(store), capacityOverride)
					},
					At: settingsGen.Settings.StartTime.Add(delay),
				})

				return ""
			case "eval":
				samples := 1
				seed := rand.Int63()
				duration := 30 * time.Minute
				failureExists := false

				scanIfExists(t, d, "duration", &duration)
				scanIfExists(t, d, "samples", &samples)
				scanIfExists(t, d, "seed", &seed)

				seedGen := rand.New(rand.NewSource(seed))
				sampleAssertFailures := make([]string, samples)
				// TODO(kvoli): Samples are evaluated sequentially (no
				// concurrency). Add a evaluator component which concurrently
				// evaluates samples with the option to stop evaluation early
				// if an assertion fails.
				for sample := 0; sample < samples; sample++ {
					assertionFailures := []string{}
					simulator := gen.GenerateSimulation(
						duration, clusterGen, rangeGen, loadGen,
						settingsGen, eventGen, seedGen.Int63(),
					)
					simulator.RunSim(ctx)
					history := simulator.History()
					runs = append(runs, history)
					for _, assertion := range assertions {
						if holds, reason := assertion.Assert(ctx, history); !holds {
							failureExists = true
							assertionFailures = append(assertionFailures, reason)
						}
					}
					sampleAssertFailures[sample] = strings.Join(assertionFailures, "")
				}

				// Every sample passed every assertion.
				if !failureExists {
					return "OK"
				}

				// There exists a sample where some assertion didn't hold. For
				// each sample that had at least one failing assertion, report
				// the sample and every failing assertion.
				buf := strings.Builder{}
				for sample, failString := range sampleAssertFailures {
					if failString != "" {
						fmt.Fprintf(&buf, "failed assertion sample %d\n%s",
							sample+1, failString)
					}
				}
				return buf.String()
			case "assertion":
				var stat string
				var typ string
				var ticks int
				scanArg(t, d, "type", &typ)

				switch typ {
				case "balance":
					scanArg(t, d, "stat", &stat)
					scanArg(t, d, "ticks", &ticks)
					assertions = append(assertions, balanceAssertion{
						ticks:     ticks,
						stat:      stat,
						threshold: scanThreshold(t, d),
					})
				case "steady":
					scanArg(t, d, "stat", &stat)
					scanArg(t, d, "ticks", &ticks)
					assertions = append(assertions, steadyStateAssertion{
						ticks:     ticks,
						stat:      stat,
						threshold: scanThreshold(t, d),
					})
				case "stat":
					var stores []int
					scanArg(t, d, "stat", &stat)
					scanArg(t, d, "ticks", &ticks)
					scanArg(t, d, "stores", &stores)
					assertions = append(assertions, storeStatAssertion{
						ticks:     ticks,
						stat:      stat,
						threshold: scanThreshold(t, d),
						stores:    stores,
					})
				case "conformance":
					var under, over, unavailable, violating int
					under = conformanceAssertionSentinel
					over = conformanceAssertionSentinel
					unavailable = conformanceAssertionSentinel
					violating = conformanceAssertionSentinel
					scanIfExists(t, d, "under", &under)
					scanIfExists(t, d, "over", &over)
					scanIfExists(t, d, "unavailable", &unavailable)
					scanIfExists(t, d, "violating", &violating)
					assertions = append(assertions, conformanceAssertion{
						underreplicated: under,
						overreplicated:  over,
						violating:       violating,
						unavailable:     unavailable,
					})
				}
				return ""
			case "setting":
				scanIfExists(t, d, "rebalance_mode", &settingsGen.Settings.LBRebalancingMode)
				scanIfExists(t, d, "rebalance_interval", &settingsGen.Settings.LBRebalancingInterval)
				scanIfExists(t, d, "rebalance_qps_threshold", &settingsGen.Settings.LBRebalanceQPSThreshold)
				scanIfExists(t, d, "split_qps_threshold", &settingsGen.Settings.SplitQPSThreshold)
				scanIfExists(t, d, "rebalance_range_threshold", &settingsGen.Settings.RangeRebalanceThreshold)
				scanIfExists(t, d, "gossip_delay", &settingsGen.Settings.StateExchangeDelay)
				scanIfExists(t, d, "range_size_split_threshold", &settingsGen.Settings.RangeSizeSplitThreshold)
				return ""
			case "plot":
				var stat string
				var height, width, sample = 15, 80, 1
				var buf strings.Builder

				scanArg(t, d, "stat", &stat)
				scanIfExists(t, d, "sample", &sample)
				scanIfExists(t, d, "height", &height)
				scanIfExists(t, d, "width", &width)

				require.GreaterOrEqual(t, len(runs), sample)

				history := runs[sample-1]
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
				return buf.String()
			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
	})
}
