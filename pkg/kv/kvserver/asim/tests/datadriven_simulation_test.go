// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"hash/fnv"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/allocatorimpl"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/assertion"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/event"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/gen"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/history"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/scheduled"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigtestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sniffarg"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/logtags"
	"github.com/stretchr/testify/require"
)

var runAsimTests = envutil.EnvOrDefaultBool("COCKROACH_RUN_ASIM_TESTS", false)

// TestDataDriven is a data-driven test for the allocation system using the
// simulator. It gives contributors a way to understand how a cluster reacts to
// different settings, load and configuration. In addition, it may be used for
// regression and exploratory testing with parameterized random generation and
// assertions. The following syntax is provided.
//
//   - "gen_load" [rw_ratio=<float>] [rate=<float>] [access_skew=<bool>]
//     [min_block=<int>] [max_block=<int>] [min_key=<int>] [max_key=<int>]
//     [replace=<bool>] [cpu_per_access=<int>] [raft_cpu_per_write=<int>]
//     Initialize the load generator with parameters. On the next call to eval,
//     the load generator is called to create the workload used in the
//     simulation. When `replace` is false, this workload doesn't replace
//     any existing workload specified by the simulation, it instead adds it
//     on top.The default values are: rw_ratio=0 rate=0 min_block=1
//     max_block=1 min_key=1 max_key=10_000 access_skew=false replace=false
//     cpu_per_access=0 raft_cpu_per_write=0
//
//   - "gen_cluster" [nodes=<int>] [stores_per_node=<int>]
//     [store_byte_capacity_gib=<int>] [node_cpu_cores=<float>]
//     Initialize the cluster generator parameters. On the next call to eval,
//     the cluster generator is called to create the initial state used in the
//     simulation. The default values are: nodes=3 stores_per_node=1
//     store_byte_capacity_gib=256, node_cpu_cores=8.0.
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
//   - "gen_ranges" [ranges=<int>]
//     [placement_type=(even|skewed|weighted|replica_placement)]
//     [repl_factor=<int>] [min_key=<int>] [max_key=<int>] [bytes_mib=<int>]
//     [reset=<bool>]
//     Initialize the range generator parameters. On the next call to eval, the
//     range generator is called to assign an ranges and their replica
//     placement. Unless `reset` is true, the range generator doesn't
//     replace any existing range generators, it is instead added on-top.
//     The default values are ranges=1 repl_factor=3 placement_type=even
//     min_key=0 max_key=10000 reset=false. If replica_placement is used,
//     an extra line should follow with the replica placement. A example of
//     the replica placement is: {s1:*,s2,s3:NON_VOTER}:1 {s4:*,s5,s6}:1.
//
//   - set_status store=<int> [delay=<duration>] liveness=(live|unavailable|dead)
//   - set_status node=<int> [delay=<duration>] [liveness=(live|unavailable|dead)]
//       [membership=(active|decommissioning|decommissioned)] [draining=<bool>]
//     Set status signals for stores or nodes. The store= form sets liveness for
//     a single store. The node= form can set liveness for all stores on a node
//     and/or set membership/draining (which are per-node). Defaults for node=:
//     membership=active, draining=false.
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
//     [(under|over|unavailable|violating)=<int>] [issue=<url>]
//     Add an assertion to the list of assertions that run against each sample
//     on subsequent calls to eval. When every assertion holds during eval, OK
//     is printed, otherwise the reason the assertion(s) failed is printed.
//     type=balance,steady,stat assertions look at the last 'ticks' duration of
//     the simulation run. type=conformance assertions look at the end of the
//     evaluation.
//
//     When issue=<url> is provided, the assertion is expected to fail (tracked
//     by the referenced issue). If the assertion stops failing, the test errors
//     out so the annotation can be cleaned up. Conversely, a failing assertion
//     without an issue= annotation causes a test error.
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
//   - "setting" [replicate_queue_enabled=bool] [lease_queue_enabled=bool]
//     [split_queue_enabled=bool] [rebalance_interval=<duration>]
//     [rebalance_mode=<int>] [split_qps_threshold=<float>]
//     [rebalance_range_threshold=<float>] [gossip_delay=<duration>]
//     [rebalance_objective=<int>]
//     Configure the simulation's various settings. The default values are:
//     replicate_queue_enabled=true lease_queue_enabled=true
//     split_queue_enabled=true rebalance_interval=1m (1 minute)
//     rebalance_mode=2 (leases and replicas) split_qps_threshold=2500
//     rebalance_range_threshold=0.05 gossip_delay=500ms rebalance_objective=0
//     (QPS) (1=CPU)
//
//   - "eval" [duration=<string>] [samples=<int>] [seed=<int>]
//     Run samples (e.g. samples=5) number of simulations for duration (e.g.
//     duration=10m) time and evaluate every prior declared assertion against
//     each sample. The seed (e.g. seed=42) given will be used to create a new
//     random number generator that creates the seed used to generate each
//     simulation sample. The default values are: duration=30m (30 minutes)
//     samples=1 seed=random.
//
// To run all tests and rewrite the testdata files as well as generate the
// artifacts in `testdata/generated`, you can use:
/*
./dev test pkg/kv/kvserver/asim/tests --ignore-cache --rewrite -v -f TestDataDriven -- --test_env COCKROACH_RUN_ASIM_TESTS=true --test_env COCKROACH_ALWAYS_KEEP_TEST_LOGS=true
*/
func TestDataDriven(t *testing.T) {
	skip.UnderDuressWithIssue(t, 149875)
	leakTestAfter := leaktest.AfterTest(t)

	// NB: we must use t.Cleanup instead of deferring this
	// cleanup in the main test due to the use of t.Parallel
	// below.
	// See https://github.com/golang/go/issues/31651.
	scope := log.Scope(t)
	t.Cleanup(func() {
		scope.Close(t)
		leakTestAfter()
	})
	testutils.RunValues(t, "mode", []string{"sma", "mma"}, func(t *testing.T, mode string) {
		dir := datapathutils.TestDataPath(t, "non_rand", mode)
		datadriven.Walk(t, dir, func(t *testing.T, path string) {
			if mode == "mma" && !runAsimTests {
				t.Logf("skipping %s: mode=mma and COCKROACH_RUN_ASIM_TESTS is not set", path)
				t.SkipNow()
			}
			// The inline comment below is required for TestLint/TestTParallel.
			// We use t.Cleanup to work around the issue this lint is trying to prevent.
			t.Parallel() // SAFE FOR TESTING
			const defaultKeyspace = 10000
			loadGen := gen.MultiLoad{}
			var clusterGen gen.ClusterGen
			var rangeGen gen.MultiRanges
			settingsGen := gen.StaticSettings{Settings: config.DefaultSimulationSettings()}
			var events []scheduled.ScheduledEvent
			type trackedAssertion struct {
				assertion.SimulationAssertion
				// When non-empty, the assertion is expected to fail and the URL
				// points at a tracking issue (GitHub, Jira, etc). If the assertion
				// stops failing, the test errors out so the annotation can be
				// cleaned up. If it is empty and the assertion fails, the test
				// also errors out, prompting the author to either fix the problem
				// or add a tracking issue.
				issue string
			}
			assertions := []trackedAssertion{}
			// TODO(tbg): make it unnecessary to hold on to a per-file
			// history of runs by removing commands that reference a specific
			// runs. Instead, all run-specific data should become a generated
			// artifact, and testdata output should be independent of the run
			// (i.e. act like a spec instead of a test output).
			var runs []modeHistory
			datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
				defer func() {
					if !t.Failed() {
						require.Empty(t, d.CmdArgs, "leftover arguments for %s", d.Cmd)
					}
				}()
				switch d.Cmd {
				case "skip_under_ci":
					if !runAsimTests {
						t.Logf("skipping %s: skip_under_ci and COCKROACH_RUN_ASIM_TESTS is not set", path)
						t.SkipNow()
					}
					return ""
				case "gen_load":
					var rwRatio, rate = 0.0, 0.0
					var minBlock, maxBlock = 1, 1
					var minKey, maxKey = int64(0), int64(defaultKeyspace)
					var accessSkew, replace bool
					var requestCPUPerAccess, raftCPUPerAccess int64

					scanIfExists(t, d, "rw_ratio", &rwRatio)
					scanIfExists(t, d, "rate", &rate)
					scanIfExists(t, d, "access_skew", &accessSkew)
					scanIfExists(t, d, "min_block", &minBlock)
					scanIfExists(t, d, "max_block", &maxBlock)
					scanIfExists(t, d, "min_key", &minKey)
					scanIfExists(t, d, "max_key", &maxKey)
					scanIfExists(t, d, "replace", &replace)
					scanIfExists(t, d, "request_cpu_per_access", &requestCPUPerAccess)
					scanIfExists(t, d, "raft_cpu_per_write", &raftCPUPerAccess)

					var buf strings.Builder

					// Catch tests that set unrealistically small or unrealistically large
					// CPU consumptions. This isn't exact because it doesn't account for
					// replication, but it's close enough.
					// NB: writes also consume requestCPUPerAccess.
					accessVCPUs := rate * float64(requestCPUPerAccess) / 1e9
					beforeReplicationRaftVCPUs := rate * (1 - rwRatio) * float64(raftCPUPerAccess) / 1e9
					approxVCPUs := accessVCPUs + beforeReplicationRaftVCPUs
					// Ditto for writes. Here too we don't account for replication. Note
					// that at least under uniform writes, real clusters can have a write
					// amp that easily surpasses 20, so writing at 40mb/s to a small set
					// of stores would often constitute an issue in production.
					beforeReplicationWriteBytes := float64(maxBlock+minBlock) * rate * (1.0 - rwRatio) / 2

					const tenkb = 10 * 1024
					neitherWriteNorCPUHeavy := beforeReplicationWriteBytes < tenkb && approxVCPUs < .5

					// We tolerate abnormally low CPU if there's a sensible amount of
					// write load. Otherwise, it's likely a mistake.
					if neitherWriteNorCPUHeavy && approxVCPUs > 0 {
						_, _ = fmt.Fprintf(&buf, "WARNING: CPU load of ≈%.2f cores is likely accidental\n", approxVCPUs)
					}
					// Similarly, tolerate abnormally low write load when there's
					// significant CPU. Independently, call out high write load.
					if (neitherWriteNorCPUHeavy && beforeReplicationWriteBytes > 0) || beforeReplicationWriteBytes > 40*(1<<20) {
						_, _ = fmt.Fprintf(&buf, "WARNING: write load of %s is likely accidental\n",
							humanizeutil.IBytes(int64(beforeReplicationWriteBytes)))
					}
					{
						var parts []string
						if accessVCPUs > 0 {
							parts = append(parts, fmt.Sprintf("%.2f access-vcpus", accessVCPUs))
						}
						if beforeReplicationRaftVCPUs > 0 {
							parts = append(parts, fmt.Sprintf("%.2f raft-vcpus", beforeReplicationRaftVCPUs))
						}
						if beforeReplicationWriteBytes > 0 {
							parts = append(parts, fmt.Sprintf("%s/s goodput", humanizeutil.IBytes(int64(beforeReplicationWriteBytes))))
						}
						if len(parts) > 0 {
							_, _ = fmt.Fprintln(&buf, strings.Join(parts, ", "))
						}
					}

					var nextLoadGen gen.BasicLoad
					nextLoadGen.SkewedAccess = accessSkew
					nextLoadGen.MinKey = minKey
					nextLoadGen.MaxKey = maxKey
					nextLoadGen.RWRatio = rwRatio
					nextLoadGen.Rate = rate
					nextLoadGen.MaxBlockSize = maxBlock
					nextLoadGen.MinBlockSize = minBlock
					nextLoadGen.RequestCPUPerAccess = requestCPUPerAccess
					nextLoadGen.RaftCPUPerWrite = raftCPUPerAccess
					if replace {
						loadGen = gen.MultiLoad{nextLoadGen}
					} else {
						loadGen = append(loadGen, nextLoadGen)
					}
					return buf.String()
				case "gen_ranges":
					var ranges, replFactor = 1, 3
					var minKey, maxKey = int64(0), int64(defaultKeyspace)
					var bytesMiB int64 = 0
					var replace bool
					var placementTypeStr = "even"
					buf := strings.Builder{}
					scanIfExists(t, d, "ranges", &ranges)
					scanIfExists(t, d, "repl_factor", &replFactor)
					scanIfExists(t, d, "placement_type", &placementTypeStr)
					scanIfExists(t, d, "min_key", &minKey)
					scanIfExists(t, d, "max_key", &maxKey)
					scanIfExists(t, d, "bytes_mib", &bytesMiB)
					scanIfExists(t, d, "replace", &replace)

					placementType := gen.GetRangePlacementType(placementTypeStr)
					var replicaPlacement state.ReplicaPlacement
					if placementType == gen.ReplicaPlacement {
						parsed := state.ParseReplicaPlacement(d.Input)
						buf.WriteString(fmt.Sprintf("%v", parsed))
						replicaPlacement = parsed
					}
					nextRangeGen := gen.BasicRanges{
						BaseRanges: gen.BaseRanges{
							Ranges:            ranges,
							MinKey:            minKey,
							MaxKey:            maxKey,
							ReplicationFactor: replFactor,
							Bytes:             bytesMiB << 20,
							ReplicaPlacement:  replicaPlacement,
						},
						PlacementType: placementType,
					}
					if replace {
						rangeGen = gen.MultiRanges{nextRangeGen}
					} else {
						rangeGen = append(rangeGen, nextRangeGen)
					}
					return buf.String()
				case "gen_cluster":
					var nodes = 3
					var storesPerNode = 1
					var nodeCPUCores = []float64{config.DefaultNodeCPUCores}
					var region []string
					var nodesPerRegion []int
					var storeByteCapacityGiB int64 = 256
					scanIfExists(t, d, "nodes", &nodes)
					scanIfExists(t, d, "stores_per_node", &storesPerNode)
					scanIfExists(t, d, "store_byte_capacity_gib", &storeByteCapacityGiB)
					scanIfExists(t, d, "region", &region)
					scanIfExists(t, d, "nodes_per_region", &nodesPerRegion)
					scanIfExists(t, d, "node_cpu_cores", &nodeCPUCores)

					var buf strings.Builder
					require.NotEmpty(t, nodeCPUCores)
					{
						n := len(nodeCPUCores)
						require.True(t, n == 1 || n == nodes, "need to specify node_cpu_cores for each node")

						for _, cores := range nodeCPUCores {
							if cores < 1.0 {
								// TODO(mma): fix up the tests that trigger this warning.
								// TODO(mma): print a warning whenever the measured CPU utilization
								// on a node exceeds this capacity, as that's likely not what the test
								// intended.
								_, _ = fmt.Fprintf(&buf, "WARNING: node CPU capacity of ≈%.2f cores is likely accidental\n", cores)
								break
							}
						}
					}

					clusterGen = gen.BasicCluster{
						Nodes:               nodes,
						StoresPerNode:       storesPerNode,
						StoreByteCapacity:   storeByteCapacityGiB << 30,
						Region:              region,
						NodesPerRegion:      nodesPerRegion,
						NodeCPURateCapacity: state.NodeCPUCores(nodeCPUCores).ToRateCapacityNanos(),
					}
					return buf.String()
				case "load_cluster":
					var cfg string
					scanMustExist(t, d, "config", &cfg)
					clusterGen = loadClusterInfo(cfg)
					return ""
				case "add_node":
					var delay time.Duration
					var numStores = 1
					var localityString string
					scanIfExists(t, d, "delay", &delay)
					scanIfExists(t, d, "stores", &numStores)
					scanIfExists(t, d, "locality", &localityString)
					events = append(events,
						scheduled.ScheduledEvent{
							At: settingsGen.Settings.StartTime.Add(delay),
							TargetEvent: event.AddNodeEvent{
								NumStores:      numStores,
								LocalityString: localityString,
							}},
					)
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
						events = append(events, scheduled.ScheduledEvent{
							At: settingsGen.Settings.StartTime.Add(delay),
							TargetEvent: event.SetSpanConfigEvent{
								Span:   span,
								Config: conf,
							},
						})
					}
					return ""
				case "set_status":
					var storeID, nodeID int
					var delay time.Duration
					var livenessStr string

					scanIfExists(t, d, "delay", &delay)

					// Check if this is a store-level or node-level command.
					if scanIfExists(t, d, "store", &storeID) {
						// Store-level: only liveness is valid.
						liveness := state.LivenessLive
						if scanIfExists(t, d, "liveness", &livenessStr) {
							switch livenessStr {
							case "live":
								liveness = state.LivenessLive
							case "unavailable":
								liveness = state.LivenessUnavailable
							case "dead":
								liveness = state.LivenessDead
							default:
								t.Fatalf("unknown liveness value: %s", livenessStr)
							}
						}
						events = append(events, scheduled.ScheduledEvent{
							At: settingsGen.Settings.StartTime.Add(delay),
							TargetEvent: event.SetStoreStatusEvent{
								StoreID: state.StoreID(storeID),
								Status:  state.StoreStatus{Liveness: liveness},
							},
						})
					} else if scanIfExists(t, d, "node", &nodeID) {
						// Node-level: can set liveness (for all stores) and/or membership/draining.
						var hasLiveness bool
						liveness := state.LivenessLive
						if scanIfExists(t, d, "liveness", &livenessStr) {
							hasLiveness = true
							switch livenessStr {
							case "live":
								liveness = state.LivenessLive
							case "unavailable":
								liveness = state.LivenessUnavailable
							case "dead":
								liveness = state.LivenessDead
							default:
								t.Fatalf("unknown liveness value: %s", livenessStr)
							}
						}

						// If liveness was specified, add a SetNodeLivenessEvent.
						// See SetNodeLivenessEvent for why we can't expand to individual
						// store events here.
						if hasLiveness {
							events = append(events, scheduled.ScheduledEvent{
								At: settingsGen.Settings.StartTime.Add(delay),
								TargetEvent: event.SetNodeLivenessEvent{
									NodeID:   state.NodeID(nodeID),
									Liveness: liveness,
								},
							})
						}

						// If membership or draining was specified, add a SetNodeStatusEvent.
						var membershipStr string
						var membership livenesspb.MembershipStatus
						var draining bool
						hasMembership := scanIfExists(t, d, "membership", &membershipStr)
						if hasMembership {
							switch membershipStr {
							case "active":
								membership = livenesspb.MembershipStatus_ACTIVE
							case "decommissioning":
								membership = livenesspb.MembershipStatus_DECOMMISSIONING
							case "decommissioned":
								membership = livenesspb.MembershipStatus_DECOMMISSIONED
							default:
								t.Fatalf("unknown membership value: %s", membershipStr)
							}
						}
						hasDraining := scanIfExists(t, d, "draining", &draining)
						if hasMembership || hasDraining {
							events = append(events, scheduled.ScheduledEvent{
								At: settingsGen.Settings.StartTime.Add(delay),
								TargetEvent: event.SetNodeStatusEvent{
									NodeID: state.NodeID(nodeID),
									Status: state.NodeStatus{
										Membership: membership,
										Draining:   draining,
									},
								},
							})
						}
					} else {
						t.Fatalf("set_status requires either store=<int> or node=<int>")
					}
					return ""
				case "set_locality":
					var nodeID int
					var localityString string
					var delay time.Duration
					scanMustExist(t, d, "node", &nodeID)
					scanMustExist(t, d, "locality", &localityString)
					scanIfExists(t, d, "delay", &delay)

					events = append(events, scheduled.ScheduledEvent{
						At: settingsGen.Settings.StartTime.Add(delay),
						TargetEvent: event.SetNodeLocalityEvent{
							NodeID:         state.NodeID(nodeID),
							LocalityString: localityString,
						},
					})
					return ""
				case "set_capacity":
					var store int
					var ioThreshold float64 = -1
					var capacity, available int64 = -1, -1
					var delay time.Duration

					scanMustExist(t, d, "store", &store)
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
					events = append(events, scheduled.ScheduledEvent{
						At: settingsGen.Settings.StartTime.Add(delay),
						TargetEvent: event.SetCapacityOverrideEvent{
							StoreID:          state.StoreID(store),
							CapacityOverride: capacityOverride,
						},
					})

					return ""
				case "eval":
					samples := 1
					full := false
					// We use a fixed seed to ensure determinism in the simulated data.
					// Multiple samples can be used for more coverage.
					seed := int64(42)
					duration := 30 * time.Minute
					name := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
					plotDir := datapathutils.TestDataPath(t, "generated", mode, name)
					var rewrite bool
					require.NoError(t, sniffarg.DoEnv("rewrite", &rewrite))
					var cfgs []string    // configurations to run the simulation with
					var metrics []string // metrics to summarize

					scanIfExists(t, d, "duration", &duration)
					scanIfExists(t, d, "samples", &samples)
					scanIfExists(t, d, "seed", &seed)
					scanIfExists(t, d, "cfgs", &cfgs)
					scanIfExists(t, d, "metrics", &metrics)
					scanIfExists(t, d, "full", &full)

					t.Logf("running eval for %s", name)

					if len(cfgs) == 0 {
						// TODO(tbg): force each test to specify the configs it wants to run
						// under.
						cfgs = []string{"sma-count"}
					}

					metricsMap := map[string]struct{}{}
					for _, s := range metrics {
						metricsMap[s] = struct{}{}
					}

					require.NotZero(t, rangeGen)

					knownConfigurations := map[string]func(eg *gen.StaticEvents){
						"sma-count": func(eg *gen.StaticEvents) {
							eg.ScheduleEvent(settingsGen.Settings.StartTime, 0,
								event.SetSimulationSettingsEvent{
									IsClusterSetting: true,
									Key:              "LBRebalancingMode",
									Value:            int64(kvserverbase.LBRebalancingLeasesAndReplicas),
								})
						},
						// 'mma-only' runs with the multi-metric allocator and turns off the
						// replicate and lease queues.
						"mma-only": func(eg *gen.StaticEvents) {
							eg.ScheduleEvent(settingsGen.Settings.StartTime, 0,
								event.SetSimulationSettingsEvent{
									IsClusterSetting: true,
									Key:              "LBRebalancingMode",
									Value:            int64(kvserverbase.LBRebalancingMultiMetricOnly),
								})
						},
						// Both the replicate/lease queues and the MMA are enabled.
						"mma-count": func(eg *gen.StaticEvents) {
							eg.ScheduleEvent(settingsGen.Settings.StartTime, 0,
								event.SetSimulationSettingsEvent{
									IsClusterSetting: true,
									Key:              "LBRebalancingMode",
									Value:            int64(kvserverbase.LBRebalancingMultiMetricAndCount),
								})
						},
					}
					var buf strings.Builder
					// stateStrForOnce stores the string representation of the cluster and
					// workload setup used in this test setup. The event will only include
					// the first LBRebalancingMode configuration since this string is only
					// generated for once at the start.
					var stateStrForOnce string
					// TODO(tbg): there's tension between having subtests per
					// config (for filtering and individual failure reporting),
					// running them concurrently (for speed), and combining
					// their output into a single testdata file.  Parallel
					// subtests don't start until the parent yields, which
					// deadlocks if we block waiting for results to combine. A
					// clean-ish fix is to split each config into its own
					// testdata file, sharing common setup via an `include`
					// directive (see TestClusterState for an example).
					assertionEverFailed := make([]bool, len(assertions))
					for _, mv := range cfgs {
						t.Run(mv, func(t *testing.T) {
							ctx := logtags.AddTag(context.Background(), "name", name+"/"+mv)
							sampleAssertFailures := make([]string, samples)
							run := modeHistory{
								mode: mv,
							}

							eventGen := gen.NewStaticEventsWithNoEvents()
							for _, ev := range events {
								eventGen.ScheduleEvent(ev.At, 0, ev.TargetEvent)
							}

							set := knownConfigurations[mv]
							require.NotNil(t, set, "unknown mode value: %s", mv)
							set(&eventGen)

							// TODO(tbg): need to decide whether multiple evals in a single file
							// is a feature or an anti-pattern. If it's a feature, we should let
							// the `name` part below be adjustable (but not the plotDir) via a
							// parameter to the `eval` command.
							testName := name + "_" + mv

							seedGen := rand.New(rand.NewSource(seed))
							for sample := 0; sample < samples; sample++ {
								tr := makeTraceHelper(rewrite, plotDir, testName, sample+1, duration)
								if tr.enabled {
									// Only populate OnRecording if we're going to save the results.
									// That way, we avoid creating trace spans during normal test runs.
									settingsGen.Settings.OnRecording = func(storeID int64, atDuration time.Duration, rec tracingpb.Recording) {
										tr.OnRecording(t, storeID, atDuration, rec)
									}
								}

								assertionFailures := []string{}
								var tmpStrB *strings.Builder = nil
								if stateStrForOnce == "" {
									tmpStrB = &strings.Builder{}
								}
								simulator := gen.GenerateSimulation(
									duration, clusterGen, rangeGen, loadGen,
									settingsGen, eventGen, seedGen.Int63(), tmpStrB, "\t",
								)
								if stateStrForOnce == "" {
									stateStrForOnce = tmpStrB.String()
								}
								simulator.RunSim(ctx)
								h := simulator.History()
								run.hs = append(run.hs, h)

								for i, stmt := range assertions {
									if holds, reason := stmt.Assert(ctx, h); !holds {
										assertionEverFailed[i] = true
										assertionFailures = append(assertionFailures, reason)
									}
								}
								sampleAssertFailures[sample] = strings.Join(assertionFailures, "")
							}

							runs = append(runs, run)

							_, _ = fmt.Fprintf(&buf, "%s:\n", mv)
							// Generate artifacts. Hash artifact input data to ensure they are
							// up to date.
							hasher := fnv.New64a()

							for sample, h := range run.hs {
								printStatsAndGenerateJSON(t, &buf, h, testName, sample+1, plotDir, hasher, rewrite,
									settingsGen.Settings.TickInterval, metricsMap)
							}
							artifactsHash := hasher.Sum64()

							// For each sample that had at least one failing assertion,
							// report the sample and every failing assertion.
							_, _ = fmt.Fprintf(&buf, "hash: %x\n", artifactsHash)
							for sample, failString := range sampleAssertFailures {
								if failString != "" {
									_, _ = fmt.Fprintf(&buf, "failed assertion sample %d\n%s\n",
										sample+1, failString)
								}
							}
							_, _ = fmt.Fprint(&buf, "==========================\n")
						})
					}
					for i, a := range assertions {
						if a.issue != "" && !assertionEverFailed[i] {
							t.Errorf("assertion %q has issue=%s but did not fail in any "+
								"config; the issue may be resolved, consider removing "+
								"the annotation", a.SimulationAssertion, a.issue)
						}
						if a.issue == "" && assertionEverFailed[i] {
							t.Errorf("assertion %q failed without a tracking issue; "+
								"add issue=<url> or fix the underlying problem",
								a.SimulationAssertion)
						}
					}
					writeStateStrToFile(t, filepath.Join(plotDir, fmt.Sprintf("%s_setup.txt", name)), stateStrForOnce, rewrite)
					if full {
						_, _ = fmt.Fprintf(&buf, "%v\n", stateStrForOnce)
						_, _ = fmt.Fprint(&buf, "==========================\n")
					}
					return buf.String()
				case "assertion":
					var stat string
					var typ string
					var ticks int
					var issue string
					scanMustExist(t, d, "type", &typ)
					scanIfExists(t, d, "issue", &issue)

					var buf strings.Builder
					var a assertion.SimulationAssertion
					switch typ {
					case "balance":
						scanMustExist(t, d, "stat", &stat)
						scanMustExist(t, d, "ticks", &ticks)
						threshold := scanThreshold(t, d)
						a = assertion.BalanceAssertion{
							Ticks:     ticks,
							Stat:      stat,
							Threshold: threshold,
						}
						_, _ = fmt.Fprintf(&buf, "asserting: max_{stores}(%s)/mean_{stores}(%s) %s %.2f at each of last %d ticks",
							stat, stat, threshold.ThresholdType, threshold.Value, ticks)
						// ^-- the max and mean are taken over the stores (with the tick fixed).
					case "steady":
						scanMustExist(t, d, "stat", &stat)
						scanMustExist(t, d, "ticks", &ticks)
						threshold := scanThreshold(t, d)
						a = assertion.SteadyStateAssertion{
							Ticks:     ticks,
							Stat:      stat,
							Threshold: threshold,
						}
						_, _ = fmt.Fprintf(&buf, "asserting: |%s(t)/mean_{T}(%s) - 1| %s %.2f ∀ t∈T and each store ("+
							"T=last %d ticks)",
							stat, stat, threshold.ThresholdType, threshold.Value, ticks)
						// ^-- the mean is taken over the ticks (and the check runs for each store).
						// These assertions are for "checking that change stops" (vs. balance
						// assertions, which verify that stores are close together on some metric).
					case "stat":
						var stores []int
						scanMustExist(t, d, "stat", &stat)
						scanMustExist(t, d, "ticks", &ticks)
						scanMustExist(t, d, "stores", &stores)
						a = assertion.StoreStatAssertion{
							Ticks:     ticks,
							Stat:      stat,
							Threshold: scanThreshold(t, d),
							Stores:    stores,
						}
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
						a = assertion.ConformanceAssertion{
							Underreplicated:           under,
							Overreplicated:            over,
							ViolatingConstraints:      violating,
							Unavailable:               unavailable,
							ViolatingLeasePreferences: leaseViolating,
							LessPreferredLeases:       leaseLessPref,
						}
					default:
						panic("unknown assertion: " + typ)
					}
					assertions = append(assertions, trackedAssertion{
						SimulationAssertion: a,
						issue:               issue,
					})
					return buf.String()
				case "setting":
					// NB: delay could be supported for the below settings,
					// but it hasn't been needed yet.
					var dns bool // "delay not supported"
					dns = scanIfExists(t, d, "replicate_queue_enabled", &settingsGen.Settings.ReplicateQueueEnabled) || dns
					dns = scanIfExists(t, d, "lease_queue_enabled", &settingsGen.Settings.LeaseQueueEnabled) || dns
					dns = scanIfExists(t, d, "split_queue_enabled", &settingsGen.Settings.SplitQueueEnabled) || dns
					dns = scanIfExists(t, d, "rebalance_interval", &settingsGen.Settings.LBRebalancingInterval) || dns
					dns = scanIfExists(t, d, "split_qps_threshold", &settingsGen.Settings.SplitQPSThreshold) || dns
					dns = scanIfExists(t, d, "rebalance_range_threshold", &settingsGen.Settings.RangeRebalanceThreshold) || dns
					dns = scanIfExists(t, d, "gossip_delay", &settingsGen.Settings.StateExchangeDelay) || dns
					dns = scanIfExists(t, d, "range_size_split_threshold", &settingsGen.Settings.RangeSizeSplitThreshold) || dns
					var snapshotRateMiB int
					dns = scanIfExists(t, d, "rebalancing_snapshot_rate_mib", &snapshotRateMiB) || dns
					if snapshotRateMiB != 0 {
						settingsGen.Settings.RebalancingSnapshotRate = int64(snapshotRateMiB) << 20
					}

					var delay time.Duration
					if scanIfExists(t, d, "delay", &delay) {
						require.False(t, dns, "delay not supported for at least one setting")
					}

					var rebalanceMode int64
					if scanIfExists(t, d, "rebalance_mode", &rebalanceMode) {
						events = append(events, scheduled.ScheduledEvent{
							At: settingsGen.Settings.StartTime.Add(delay),
							TargetEvent: event.SetSimulationSettingsEvent{
								IsClusterSetting: true,
								Key:              "LBRebalancingMode",
								Value:            rebalanceMode,
							}})
					}

					var rebalanceObjective int64
					if scanIfExists(t, d, "rebalance_objective", &rebalanceObjective) {
						events = append(events, scheduled.ScheduledEvent{
							At: settingsGen.Settings.StartTime.Add(delay),
							TargetEvent: event.SetSimulationSettingsEvent{
								IsClusterSetting: true,
								Key:              "LBRebalancingObjective",
								Value:            rebalanceObjective,
							}})
					}
					return ""
				default:
					return fmt.Sprintf("unknown command: %s", d.Cmd)
				}
			})
		})
	})
}

type modeHistory struct {
	mode string
	hs   []history.History
}

// writeStateStrToFile writes the state string to the given file.
func writeStateStrToFile(t *testing.T, topFile string, stateStr string, rewrite bool) {
	if rewrite {
		require.NoError(t, os.WriteFile(topFile, []byte(stateStr), 0644))
	}
}
