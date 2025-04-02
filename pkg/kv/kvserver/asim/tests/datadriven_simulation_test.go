// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/allocatorimpl"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/assertion"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/event"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/gen"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/history"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/metrics"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigtestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
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
//     [add_to_existing=<bool>] [cpu_per_access=<int>] [raft_cpu_per_write=<int>]
//     Initialize the load generator with parameters. On the next call to eval,
//     the load generator is called to create the workload used in the
//     simulation. When add_to_existing is true, this workload doesn't replace
//     any existing workload specified by the simulation, it instead adds it
//     ontop. The default values are: rw_ratio=0 rate=0 min_block=1
//     max_block=1 min_key=1 max_key=10_000 access_skew=false
//     add_to_existing=false cpu_per_access=0 raft_cpu_per_write=0.
//
//   - "gen_cluster" [nodes=<int>] [stores_per_node=<int>]
//     [store_byte_capacity=<int>] [node_cpu_rate_capacity=<int>]
//     Initialize the cluster generator parameters. On the next call to eval,
//     the cluster generator is called to create the initial state used in the
//     simulation. The default values are: nodes=3 stores_per_node=1
//     store_byte_capacity=256<<32, node_cpu_rate_capacity=0.
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
//   - "gen_ranges" [ranges=<int>] [placement_type=(even|skewed|weighted)]
//     [repl_factor=<int>] [min_key=<int>] [max_key=<int>] [range_bytes=<int>]
//     [add_to_existing<bool>] [lease_weights=<float>] [replica_weights=<float>]
//     Initialize the range generator parameters. On the next call to eval, the
//     range generator is called to assign an ranges and their replica
//     placement. When add_to_existing is true, the range generator doesn't
//     replace any existing range generators, it is instead added on-top. The
//     The default values are ranges=1 repl_factor=3 placement_type=even
//     min_key=0 max_key=10000 add_to_existing=false.
//
//   - set_liveness node=<int> [delay=<duration>]
//     status=(dead|decommisssioning|draining|unavailable)
//     Set the liveness status of the node with ID NodeID. This applies at the
//     start of the simulation or with some delay after the simulation starts,
//     if specified.
//
//   - set_locality node=<int> [delay=<duration] locality=string
//     Sets the locality of the node with ID NodeID. This applies at the start
//     of the simulation or with some delay after the simulation stats, if
//     specified.
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
//     [show_last_value=<bool>]
//     Visually renders the stat (e.g. stat=qps) as a series where the x axis
//     is the simulated time and the y axis is the stat value. A series is
//     rendered per-store, so if there are 10 stores, 10 series will be
//     rendered. When show_last_value is true, the last value of the series is
//     shown for each store.
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
	dir := datapathutils.TestDataPath(t, "non_rand")
	datadriven.Walk(t, dir, func(t *testing.T, path string) {
		const defaultKeyspace = 10000
		loadGen := gen.MultiLoad{}
		var clusterGen gen.ClusterGen
		rangeGen := gen.MultiRanges{}
		settingsGen := gen.StaticSettings{Settings: config.DefaultSimulationSettings()}
		eventGen := gen.NewStaticEventsWithNoEvents()
		assertions := []assertion.SimulationAssertion{}
		runs := []history.History{}
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "gen_load":
				var rwRatio, rate = 0.0, 0.0
				var minBlock, maxBlock = 1, 1
				var minKey, maxKey = int64(1), int64(defaultKeyspace)
				var accessSkew, addToExisting bool
				var requestCPUPerAccess, raftCPUPerAccess int64

				scanIfExists(t, d, "rw_ratio", &rwRatio)
				scanIfExists(t, d, "rate", &rate)
				scanIfExists(t, d, "access_skew", &accessSkew)
				scanIfExists(t, d, "min_block", &minBlock)
				scanIfExists(t, d, "max_block", &maxBlock)
				scanIfExists(t, d, "min_key", &minKey)
				scanIfExists(t, d, "max_key", &maxKey)
				scanIfExists(t, d, "add_to_existing", &addToExisting)
				scanIfExists(t, d, "request_cpu_per_access", &requestCPUPerAccess)
				scanIfExists(t, d, "raft_cpu_per_write", &raftCPUPerAccess)

				nextLoadGen := gen.BasicLoad{}
				nextLoadGen.SkewedAccess = accessSkew
				nextLoadGen.MinKey = minKey
				nextLoadGen.MaxKey = maxKey
				nextLoadGen.RWRatio = rwRatio
				nextLoadGen.Rate = rate
				nextLoadGen.MaxBlockSize = maxBlock
				nextLoadGen.MinBlockSize = minBlock
				nextLoadGen.RequestCPUPerAccess = requestCPUPerAccess
				nextLoadGen.RaftCPUPerWrite = raftCPUPerAccess
				if addToExisting {
					loadGen = append(loadGen, nextLoadGen)
				} else {
					loadGen = gen.MultiLoad{nextLoadGen}
				}
				return ""
			case "gen_ranges":
				var ranges, replFactor = 1, 3
				var minKey, maxKey = int64(0), int64(defaultKeyspace)
				var bytes int64 = 0
				var addToExisting bool
				var placementTypeStr string = "even"
				var leaseWeights, replicaWeights []float64

				scanIfExists(t, d, "ranges", &ranges)
				scanIfExists(t, d, "repl_factor", &replFactor)
				scanIfExists(t, d, "placement_type", &placementTypeStr)
				scanIfExists(t, d, "min_key", &minKey)
				scanIfExists(t, d, "max_key", &maxKey)
				scanIfExists(t, d, "bytes", &bytes)
				scanIfExists(t, d, "add_to_existing", &addToExisting)

				placementType := gen.GetRangePlacementType(placementTypeStr)
				if placementType == gen.Weighted {
					// lease_weights and replica_weights are required for weighted
					// placement.
					scanArg(t, d, "lease_weights", &leaseWeights)
					scanArg(t, d, "replica_weights", &replicaWeights)
				}
				nextRangeGen := gen.BasicRanges{
					BaseRanges: gen.BaseRanges{
						Ranges:            ranges,
						MinKey:            minKey,
						MaxKey:            maxKey,
						ReplicationFactor: replFactor,
						Bytes:             bytes,
					},
					PlacementType:  placementType,
					LeaseWeights:   leaseWeights,
					ReplicaWeights: replicaWeights,
				}
				if addToExisting {
					rangeGen = append(rangeGen, nextRangeGen)
				} else {
					rangeGen = gen.MultiRanges{nextRangeGen}
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
				var storeByteCapacity int64 = 256 << 30 /* 256 GiB  */
				var nodeCPURateCapacity int64
				scanIfExists(t, d, "nodes", &nodes)
				scanIfExists(t, d, "stores_per_node", &storesPerNode)
				scanIfExists(t, d, "store_byte_capacity", &storeByteCapacity)
				scanIfExists(t, d, "node_cpu_rate_capacity", &nodeCPURateCapacity)
				clusterGen = gen.BasicCluster{
					Nodes:               nodes,
					StoresPerNode:       storesPerNode,
					StoreByteCapacity:   storeByteCapacity,
					NodeCPURateCapacity: nodeCPURateCapacity,
				}
				return ""
			case "load_cluster":
				var config string
				scanArg(t, d, "config", &config)
				clusterGen = loadClusterInfo(config)
				return ""
			case "add_node":
				var delay time.Duration
				var numStores = 1
				var localityString string
				scanIfExists(t, d, "delay", &delay)
				scanIfExists(t, d, "stores", &numStores)
				scanIfExists(t, d, "locality", &localityString)
				eventGen.ScheduleEvent(settingsGen.Settings.StartTime, delay, event.AddNodeEvent{
					NumStores:      numStores,
					LocalityString: localityString,
				})
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
					eventGen.ScheduleEvent(settingsGen.Settings.StartTime, delay, event.SetSpanConfigEvent{
						Span:   span,
						Config: conf,
					})
				}
				return ""
			case "set_liveness":
				var nodeID int
				var delay time.Duration
				livenessStatus := livenesspb.NodeLivenessStatus_LIVE
				scanArg(t, d, "node", &nodeID)
				scanArg(t, d, "liveness", &livenessStatus)
				scanIfExists(t, d, "delay", &delay)
				eventGen.ScheduleEvent(settingsGen.Settings.StartTime, delay, event.SetNodeLivenessEvent{
					NodeId:         state.NodeID(nodeID),
					LivenessStatus: livenessStatus,
				})
				return ""
			case "set_locality":
				var nodeID int
				var localityString string
				var delay time.Duration
				scanArg(t, d, "node", &nodeID)
				scanArg(t, d, "locality", &localityString)
				scanIfExists(t, d, "delay", &delay)

				eventGen.ScheduleEvent(settingsGen.Settings.StartTime, delay, event.SetNodeLocalityEvent{
					NodeID:         state.NodeID(nodeID),
					LocalityString: localityString,
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
					capacityOverride.IOThresholdMax = allocatorimpl.TestingIOThresholdWithScore(ioThreshold)
				}
				eventGen.ScheduleEvent(settingsGen.Settings.StartTime, delay, event.SetCapacityOverrideEvent{
					StoreID:          state.StoreID(store),
					CapacityOverride: capacityOverride,
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
					assertions = append(assertions, assertion.BalanceAssertion{
						Ticks:     ticks,
						Stat:      stat,
						Threshold: scanThreshold(t, d),
					})
				case "steady":
					scanArg(t, d, "stat", &stat)
					scanArg(t, d, "ticks", &ticks)
					assertions = append(assertions, assertion.SteadyStateAssertion{
						Ticks:     ticks,
						Stat:      stat,
						Threshold: scanThreshold(t, d),
					})
				case "stat":
					var stores []int
					scanArg(t, d, "stat", &stat)
					scanArg(t, d, "ticks", &ticks)
					scanArg(t, d, "stores", &stores)
					assertions = append(assertions, assertion.StoreStatAssertion{
						Ticks:     ticks,
						Stat:      stat,
						Threshold: scanThreshold(t, d),
						Stores:    stores,
					})
				case "conformance":
					var under, over, unavailable, violating, leaseViolating, leaseLessPref int
					under = assertion.ConformanceAssertionSentinel
					over = assertion.ConformanceAssertionSentinel
					unavailable = assertion.ConformanceAssertionSentinel
					violating = assertion.ConformanceAssertionSentinel
					leaseLessPref = assertion.ConformanceAssertionSentinel
					leaseViolating = assertion.ConformanceAssertionSentinel
					scanIfExists(t, d, "under", &under)
					scanIfExists(t, d, "over", &over)
					scanIfExists(t, d, "unavailable", &unavailable)
					scanIfExists(t, d, "violating", &violating)
					scanIfExists(t, d, "lease-violating", &leaseViolating)
					scanIfExists(t, d, "lease-less-preferred", &leaseLessPref)
					assertions = append(assertions, assertion.ConformanceAssertion{
						Underreplicated:           under,
						Overreplicated:            over,
						ViolatingConstraints:      violating,
						Unavailable:               unavailable,
						ViolatingLeasePreferences: leaseViolating,
						LessPreferredLeases:       leaseLessPref,
					})
				}
				return ""
			case "setting":
				scanIfExists(t, d, "replicate_queue_enabled", &settingsGen.Settings.ReplicateQueueEnabled)
				scanIfExists(t, d, "lease_queue_enabled", &settingsGen.Settings.LeaseQueueEnabled)
				scanIfExists(t, d, "split_queue_enabled", &settingsGen.Settings.SplitQueueEnabled)
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
				var showLastValue bool
				var buf strings.Builder

				scanArg(t, d, "stat", &stat)
				scanIfExists(t, d, "sample", &sample)
				scanIfExists(t, d, "height", &height)
				scanIfExists(t, d, "width", &width)
				scanIfExists(t, d, "show_last_value", &showLastValue)

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

				if showLastValue {
					type storeIDWithStat struct {
						StoreID int64
						Value   float64
					}
					lastTick := history.Recorded[len(history.Recorded)-1]
					orderedStoreIDs := make([]storeIDWithStat, 0, len(lastTick))
					for i, sm := range lastTick {
						orderedStoreIDs = append(orderedStoreIDs, storeIDWithStat{
							StoreID: sm.StoreID,
							Value:   statTS[i][len(statTS[i])-1],
						})
					}
					sort.Slice(orderedStoreIDs, func(i, j int) bool {
						return orderedStoreIDs[i].Value < orderedStoreIDs[j].Value
					})
					fmt.Fprintf(&buf, "\nlast store values: [")
					for i, store := range orderedStoreIDs {
						if i > 0 {
							fmt.Fprintf(&buf, " ")
						}
						if stat == "disk_fraction_used" {
							fmt.Fprintf(&buf, "s%v=%.2f", store.StoreID, store.Value)
						} else {
							fmt.Fprintf(&buf, "s%v=%.0f", store.StoreID, store.Value)
						}
					}
					fmt.Fprintf(&buf, "]")
				}
				return buf.String()
			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
	})
}
