// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaprototype

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigtestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/dd"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

var testingBaseTime = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

func parseBool(t *testing.T, in string) bool {
	b, err := strconv.ParseBool(strings.TrimSpace(in))
	require.NoError(t, err)
	return b
}

func parseInt(t *testing.T, in string) int {
	i, err := strconv.Atoi(strings.TrimSpace(in))
	require.NoError(t, err)
	return i
}

func stripBrackets(t *testing.T, in string) string {
	rTrim := strings.TrimSuffix(in, "]")
	lrTrim := strings.TrimPrefix(rTrim, "[")
	return lrTrim
}

func stripParentheses(in string) string {
	rTrim := strings.TrimSuffix(in, ")")
	lrTrim := strings.TrimPrefix(rTrim, "(")
	return lrTrim
}

func parseLoadVector(t *testing.T, in string) LoadVector {
	var vec LoadVector
	parts := strings.Split(stripBrackets(t, in), ",")
	require.Len(t, parts, int(NumLoadDimensions))
	for dim := range vec {
		vec[dim] = LoadValue(parseInt(t, parts[dim]))
	}
	return vec
}

func parseSecondaryLoadVector(t *testing.T, in string) SecondaryLoadVector {
	var vec SecondaryLoadVector
	parts := strings.Split(stripBrackets(t, in), ",")
	require.LessOrEqual(t, len(parts), int(NumSecondaryLoadDimensions))
	for dim := range parts {
		vec[dim] = LoadValue(parseInt(t, parts[dim]))
	}
	return vec
}

func parseStatusFromArgs(t *testing.T, d *datadriven.TestData, status *Status) {
	if d.HasArg("health") {
		healthStr := dd.ScanArg[string](t, d, "health")
		found := false
		for i := Health(0); i < healthCount; i++ {
			if i.String() == healthStr {
				status.Health = i
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("unknown health: %s", healthStr)
		}
	}
	if d.HasArg("leases") {
		leaseStr := dd.ScanArg[string](t, d, "leases")
		found := false
		for i := LeaseDisposition(0); i < leaseDispositionCount; i++ {
			if i.String() == leaseStr {
				status.Disposition.Lease = i
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("unknown lease disposition: %s", leaseStr)
		}
	}
	if d.HasArg("replicas") {
		replicaStr := dd.ScanArg[string](t, d, "replicas")
		found := false
		for i := ReplicaDisposition(0); i < replicaDispositionCount; i++ {
			if i.String() == replicaStr {
				status.Disposition.Replica = i
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("unknown replica disposition: %s", replicaStr)
		}
	}
}

func parseStoreLoadMsg(t *testing.T, in string) StoreLoadMsg {
	var msg StoreLoadMsg
	for _, v := range strings.Fields(in) {
		parts := strings.Split(v, "=")
		require.Len(t, parts, 2)
		switch parts[0] {
		case "store-id":
			msg.StoreID = roachpb.StoreID(parseInt(t, parts[1]))
		case "node-id":
			msg.NodeID = roachpb.NodeID(parseInt(t, parts[1]))
		case "load":
			msg.Load = parseLoadVector(t, parts[1])
		case "capacity":
			msg.Capacity = parseLoadVector(t, parts[1])
			for i := range msg.Capacity {
				if msg.Capacity[i] < 0 {
					msg.Capacity[i] = UnknownCapacity
				}
			}
		case "load-time":
			duration, err := time.ParseDuration(parts[1])
			require.NoError(t, err)
			msg.LoadTime = testingBaseTime.Add(duration)
		case "secondary-load":
			msg.SecondaryLoad = parseSecondaryLoadVector(t, parts[1])
		default:
			t.Fatalf("Unknown argument: %s", parts[0])
		}
	}
	return msg
}

func parseStoreLeaseholderMsg(t *testing.T, in string) StoreLeaseholderMsg {
	var msg StoreLeaseholderMsg

	lines := strings.Split(in, "\n")
	require.True(t, strings.HasPrefix(lines[0], "store-id="))
	msg.StoreID = roachpb.StoreID(parseInt(t, strings.TrimPrefix(lines[0], "store-id=")))

	var rMsg RangeMsg
	var notPopulatedOverride bool
	tryAppendRangeMsg := func() {
		if rMsg.RangeID != 0 {
			if notPopulatedOverride {
				rMsg.MaybeSpanConfIsPopulated = false
			}
			msg.Ranges = append(msg.Ranges, rMsg)
			rMsg = RangeMsg{RangeID: 0}
		}
		notPopulatedOverride = false
	}
	for _, line := range lines[1:] {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "range-id") {
			tryAppendRangeMsg()
			for _, field := range strings.Fields(line) {
				parts := strings.SplitN(field, "=", 2)
				switch parts[0] {
				case "range-id":
					rMsg.RangeID = roachpb.RangeID(parseInt(t, parts[1]))
				case "load":
					rMsg.RangeLoad.Load = parseLoadVector(t, parts[1])
					rMsg.MaybeSpanConfIsPopulated = true
				case "raft-cpu":
					rMsg.RangeLoad.RaftCPU = LoadValue(parseInt(t, parts[1]))
					rMsg.MaybeSpanConfIsPopulated = true
				case "span-config-not-populated":
					notPopulatedOverride = true
				default:
					t.Fatalf("unknown argument: %s", parts[0])
				}
			}
		} else if strings.HasPrefix(line, "config=") {
			trimmedConfig := stripParentheses(strings.TrimPrefix(line, "config="))
			rMsg.MaybeSpanConf = spanconfigtestutils.ParseZoneConfig(t, trimmedConfig).AsSpanConfig()
			rMsg.MaybeSpanConfIsPopulated = true
		} else {
			var repl StoreIDAndReplicaState
			fields := strings.Fields(line)
			require.Greater(t, len(fields), 2)
			for _, field := range fields {
				parts := strings.Split(field, "=")
				require.GreaterOrEqual(t, len(parts), 2)
				switch parts[0] {
				case "store-id":
					repl.StoreID = roachpb.StoreID(parseInt(t, parts[1]))
				case "replica-id":
					repl.ReplicaID = roachpb.ReplicaID(parseInt(t, parts[1]))
				case "leaseholder":
					repl.IsLeaseholder = parseBool(t, parts[1])
				case "type":
					replType, err := parseReplicaType(parts[1])
					require.NoError(t, err)
					repl.ReplicaType.ReplicaType = replType
				case "lease-disposition":
					found := false
					for i := LeaseDisposition(0); i < leaseDispositionCount; i++ {
						if i.String() == parts[1] {
							repl.LeaseDisposition = i
							found = true
							break
						}
					}
					if !found {
						t.Fatalf("unknown lease disposition: %s", parts[1])
					}
				default:
					t.Fatalf("unknown argument: %s", parts[0])
				}
			}
			rMsg.Replicas = append(rMsg.Replicas, repl)
			rMsg.MaybeSpanConfIsPopulated = true
		}
	}
	tryAppendRangeMsg()
	return msg
}

// TODO(kvoli): Parse a NodeID here as well, for roachpb.ReplicationTarget.
func parseChangeAddRemove(
	t *testing.T, in string,
) (add, remove roachpb.StoreID, replType roachpb.ReplicaType) {
	// Note that remove or add will be unset if not found in this string.
	for _, v := range strings.Fields(in) {
		parts := strings.Split(v, "=")
		require.Len(t, parts, 2)
		switch parts[0] {
		case "add-store-id":
			add = roachpb.StoreID(parseInt(t, parts[1]))
		case "remove-store-id":
			remove = roachpb.StoreID(parseInt(t, parts[1]))
		case "type":
			var err error
			replType, err = parseReplicaType(parts[1])
			require.NoError(t, err)
		default:
			t.Fatalf("unknown argument: %s", parts[0])
		}
	}
	return add, remove, replType
}

func printPendingChangesTest(changes []*pendingReplicaChange) string {
	var buf strings.Builder
	fmt.Fprintf(&buf, "pending(%d)", len(changes))
	for _, change := range changes {
		fmt.Fprintf(&buf, "\nchange-id=%d store-id=%v node-id=%v range-id=%v load-delta=%v start=%v gc=%v",
			change.changeID, change.target.StoreID, change.target.NodeID, change.rangeID,
			change.loadDelta, change.startTime.Sub(testingBaseTime), change.gcTime.Sub(testingBaseTime),
		)
		if !(change.enactedAtTime == time.Time{}) {
			fmt.Fprintf(&buf, " enacted=%v",
				change.enactedAtTime.Sub(testingBaseTime))
		}
		fmt.Fprintf(&buf, "\n  prev=(%v)\n  next=(%v)", change.prev, change.next)
	}
	return buf.String()
}

func testingGetStoreList(t *testing.T, cs *clusterState) storeSet {
	var clusterStoreList, nodeStoreList storeSet
	// Ensure that the storeIDs in the cluster store map and the stores listed
	// under each node are the same.
	for storeID := range cs.stores {
		clusterStoreList.insert(storeID)
	}
	for _, node := range cs.nodes {
		for _, storeID := range node.stores {
			nodeStoreList.insert(storeID)
		}
	}
	require.True(t, clusterStoreList.isEqual(nodeStoreList),
		"expected store lists to be equal %v != %v", clusterStoreList, nodeStoreList)

	return clusterStoreList
}

func testingGetPendingChanges(t *testing.T, cs *clusterState) []*pendingReplicaChange {
	var clusterPendingChangeList []*pendingReplicaChange
	var storeLoadPendingChangeList []*pendingReplicaChange
	var rangePendingChangeList []*pendingReplicaChange
	for _, change := range cs.pendingChanges {
		clusterPendingChangeList = append(clusterPendingChangeList, change)
	}
	for _, store := range cs.stores {
		for _, change := range store.adjusted.loadPendingChanges {
			storeLoadPendingChangeList = append(storeLoadPendingChangeList, change)
		}
	}
	for _, rng := range cs.ranges {
		rangePendingChangeList = append(rangePendingChangeList, rng.pendingChanges...)
	}
	// NB: Although redundant, we compare all of the de-normalized pending change
	// to ensure that they are in sync.
	sort.Slice(clusterPendingChangeList, func(i, j int) bool {
		return clusterPendingChangeList[i].changeID < clusterPendingChangeList[j].changeID
	})
	sort.Slice(storeLoadPendingChangeList, func(i, j int) bool {
		return storeLoadPendingChangeList[i].changeID < storeLoadPendingChangeList[j].changeID
	})
	sort.Slice(rangePendingChangeList, func(i, j int) bool {
		return rangePendingChangeList[i].changeID < rangePendingChangeList[j].changeID
	})
	require.EqualValues(t, clusterPendingChangeList, rangePendingChangeList)
	require.LessOrEqual(t, len(clusterPendingChangeList), len(storeLoadPendingChangeList))
	i, j := 0, 0
	for i < len(clusterPendingChangeList) && j < len(storeLoadPendingChangeList) {
		require.GreaterOrEqual(
			t, clusterPendingChangeList[i].changeID, storeLoadPendingChangeList[j].changeID)
		if clusterPendingChangeList[i].changeID > storeLoadPendingChangeList[j].changeID {
			// Enacted.
			require.NotEqual(t, time.Time{}, storeLoadPendingChangeList[j].enactedAtTime)
			j++
			continue
		}
		require.Equal(t, time.Time{}, storeLoadPendingChangeList[j].enactedAtTime, "%v = %v",
			storeLoadPendingChangeList[j], clusterPendingChangeList[i])
		i++
		j++
	}
	require.Equal(t, i, len(clusterPendingChangeList))
	for j < len(storeLoadPendingChangeList) {
		// Remaining changes are enacted.
		require.NotEqual(t, time.Time{}, storeLoadPendingChangeList[j].enactedAtTime)
		j++
	}
	return storeLoadPendingChangeList
}

func TestClusterState(t *testing.T) {
	tdPath := datapathutils.TestDataPath(t, "cluster_state")
	datadriven.Walk(t,
		tdPath,
		func(t *testing.T, path string) {
			ts := timeutil.NewManualTime(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
			cs := newClusterState(ts, newStringInterner())
			tr := tracing.NewTracer()
			tr.SetRedactable(true)
			defer tr.Close()

			printNodeListMeta := func(t *testing.T) string {
				nodeList := []int{}
				for nodeID := range cs.nodes {
					nodeList = append(nodeList, int(nodeID))
				}
				sort.Ints(nodeList)
				var buf strings.Builder
				for _, nodeID := range nodeList {
					ns := cs.nodes[roachpb.NodeID(nodeID)]
					var nodeLine string
					for _, storeID := range ns.stores {
						ss := cs.stores[storeID]
						sal := ss.storeAttributesAndLocalityWithNodeTier
						loc := sal.NodeLocality

						// Compute the "node" line for each store and print it again
						// each time it changes. Usually it's printed only once per node,
						// but if there are multiple stores with a different view of node
						// attributes, it would print multiple times.
						var nodeLineBuf strings.Builder
						fmt.Fprintf(&nodeLineBuf, "node-id=%s locality-tiers=%s",
							ns.NodeID, loc)
						if len(sal.NodeAttrs.Attrs) > 0 {
							fmt.Fprintf(&nodeLineBuf, " node-attrs=%s", strings.Join(sal.NodeAttrs.Attrs, ","))
						}
						fmt.Fprintln(&nodeLineBuf)

						if nodeLine != nodeLineBuf.String() {
							nodeLine = nodeLineBuf.String()
							fmt.Fprint(&buf, nodeLine)
						}

						fmt.Fprintf(&buf, "  store-id=%v attrs=%s", ss.StoreID, ss.StoreAttrs)

						storeTierVals := cs.localityTierInterner.unintern(ss.localityTiers)
						{
							// The interned locality tier values must match the uninterned original.
							var expVals []string
							for _, tier := range loc.Tiers {
								expVals = append(expVals, tier.Value)
							}
							require.EqualValues(t, expVals, storeTierVals)
						}
						// Make sure that the constraintMatcher reflects the same attrs and
						// locality as the store.
						require.Equal(t, sal, cs.constraintMatcher.stores[ss.StoreID].sal)
						fmt.Fprintln(&buf)
					}
				}
				return buf.String()
			}

			// Recursively invoked in `include` directive.
			var invokeFn func(t *testing.T, d *datadriven.TestData) string
			invokeFn = func(t *testing.T, d *datadriven.TestData) string {
				// Start a recording span for each command. Commands that want to
				// include the trace in their output can call finishAndGet().
				ctx, finishAndGet := tracing.ContextWithRecordingSpan(
					context.Background(), tr, d.Cmd,
				)
				if d.HasArg("breakpoint") {
					// You can set a debugger breakpoint here and use `breakpoint=true`
					// in a datadriven command to hit it.
					t.Log("hit breakpoint")
				}
				switch d.Cmd {
				case "include":
					loc := dd.ScanArg[string](t, d, "path")
					datadriven.RunTest(t, filepath.Join(tdPath, loc), invokeFn)
					return "ok"

				case "ranges":
					var rangeIDs []int
					for rangeID := range cs.ranges {
						rangeIDs = append(rangeIDs, int(rangeID))
					}
					// Sort the range IDs before printing, iterating over a map would
					// lead to non-determinism.
					sort.Ints(rangeIDs)
					var buf strings.Builder
					for _, rangeID := range rangeIDs {
						rs := cs.ranges[roachpb.RangeID(rangeID)]
						fmt.Fprintf(&buf, "range-id=%v local-store=%v load=%v raft-cpu=%v\n", rangeID,
							rs.localRangeOwner, rs.load.Load, rs.load.RaftCPU)
						for _, repl := range rs.replicas {
							fmt.Fprintf(&buf, "  store-id=%v %v\n",
								repl.StoreID, repl.ReplicaIDAndType,
							)
						}
					}
					return buf.String()

				case "get-load-info":
					var buf strings.Builder
					memberStores := testingGetStoreList(t, cs)
					for _, storeID := range memberStores {
						ss := cs.stores[storeID]
						ns := cs.nodes[ss.NodeID]
						fmt.Fprintf(&buf,
							"store-id=%v node-id=%v status=%s reported=%v adjusted=%v node-reported-cpu=%v node-adjusted-cpu=%v seq"+
								"=%d\n",
							ss.StoreID, ss.NodeID, ss.status, ss.reportedLoad, ss.adjusted.load, ns.ReportedCPU, ns.adjustedCPU,
							ss.loadSeqNum,
						)
						var localStores []roachpb.StoreID
						for ls := range ss.adjusted.topKRanges {
							localStores = append(localStores, ls)
						}
						slices.Sort(localStores)
						for _, ls := range localStores {
							topk := ss.adjusted.topKRanges[ls]
							n := topk.len()
							if n == 0 {
								continue
							}
							fmt.Fprintf(&buf, "  top-k-ranges (local-store-id=%v) dim=%v:", ls, topk.dim)
							for i := 0; i < n; i++ {
								fmt.Fprintf(&buf, " r%v", topk.index(i))
							}
							fmt.Fprintf(&buf, "\n")
						}
					}
					return buf.String()

				case "set-store":
					for _, next := range strings.Split(d.Input, "\n") {
						sal := parseStoreAttributedAndLocality(t, next)
						t.Logf("set-store: %v from %s", sal, next)
						cs.setStore(sal.withNodeTier()) // see allocatorState.SetStore
						// For convenience, in these tests, stores start out
						// healthy.
						cs.stores[sal.StoreID].status = Status{Health: HealthOK}
					}
					return printNodeListMeta(t)

				case "set-store-status":
					storeID := dd.ScanArg[roachpb.StoreID](t, d, "store-id")
					ss, ok := cs.stores[storeID]
					if !ok {
						t.Fatalf("store %d not found", storeID)
					}
					// NB: we intentionall bypass the assertion in MakeStatus
					// here so that we can test all combinations of health,
					// lease, and replica dispositions, even those that we never
					// want to see in production.
					parseStatusFromArgs(t, d, &ss.status)
					return ss.status.String()

				case "store-load-msg":
					// TODO(sumeer): the load-time is passed as an argument, and is
					// independent of ts. This is by necessity, since the load-time can
					// be in the past, indicating gossip delay. However, having it be
					// some arbitrary value can be confusing for the test reader.
					// Consider making it relative to ts.
					for line := range strings.Lines(d.Input) {
						msg := parseStoreLoadMsg(t, line)
						cs.processStoreLoadMsg(context.Background(), &msg)
					}
					return ""

				case "store-leaseholder-msg":
					msg := parseStoreLeaseholderMsg(t, d.Input)
					n := numTopKReplicas
					if o, ok := dd.ScanArgOpt[int](t, d, "num-top-k-replicas"); ok {
						n = o
					}
					cs.processStoreLeaseholderMsgInternal(ctx, &msg, n, nil)
					if d.HasArg("trace") {
						rec := finishAndGet()
						var sb redact.StringBuilder
						rec.SafeFormatMinimal(&sb)
						return sb.String()
					}
					return ""

				case "make-pending-changes":
					rangeID := dd.ScanArg[roachpb.RangeID](t, d, "range-id")
					var changes []ReplicaChange
					rState := cs.ranges[rangeID]

					lines := strings.Split(d.Input, "\n")
					for _, line := range lines {
						parts := strings.Split(strings.TrimSpace(line), ":")
						switch parts[0] {
						case "transfer-lease":
							add, remove, _ := parseChangeAddRemove(t, parts[1])
							addTarget := roachpb.ReplicationTarget{NodeID: cs.stores[add].NodeID, StoreID: add}
							removeTarget := roachpb.ReplicationTarget{NodeID: cs.stores[remove].NodeID, StoreID: remove}
							transferChanges := MakeLeaseTransferChanges(rangeID, rState.replicas, rState.load, addTarget, removeTarget)
							changes = append(changes, transferChanges[:]...)
						case "add-replica":
							add, _, replType := parseChangeAddRemove(t, parts[1])
							replIDAndType := ReplicaIDAndType{
								ReplicaType: ReplicaType{
									ReplicaType: replType,
								},
							}
							addTarget := roachpb.ReplicationTarget{NodeID: cs.stores[add].NodeID, StoreID: add}
							changes = append(changes, MakeAddReplicaChange(rangeID, rState.load, replIDAndType, addTarget))
						case "remove-replica":
							_, remove, _ := parseChangeAddRemove(t, parts[1])
							var removeRepl StoreIDAndReplicaState
							for _, replica := range rState.replicas {
								if replica.StoreID == remove {
									removeRepl = replica
								}
							}
							removeTarget := roachpb.ReplicationTarget{NodeID: cs.stores[remove].NodeID, StoreID: remove}
							changes = append(changes, MakeRemoveReplicaChange(rangeID, rState.load, removeRepl.ReplicaState, removeTarget))
						case "rebalance-replica":
							add, remove, _ := parseChangeAddRemove(t, parts[1])
							addTarget := roachpb.ReplicationTarget{NodeID: cs.stores[add].NodeID, StoreID: add}
							removeTarget := roachpb.ReplicationTarget{NodeID: cs.stores[remove].NodeID, StoreID: remove}
							rebalanceChanges := makeRebalanceReplicaChanges(rangeID, rState.replicas, rState.load, addTarget, removeTarget)
							changes = append(changes, rebalanceChanges[:]...)
						}
					}
					rangeChange := MakePendingRangeChange(rangeID, changes)
					cs.addPendingRangeChange(rangeChange)
					return printPendingChangesTest(testingGetPendingChanges(t, cs))

				case "gc-pending-changes":
					cs.gcPendingChanges(cs.ts.Now())
					return printPendingChangesTest(testingGetPendingChanges(t, cs))

				case "reject-pending-changes":
					changeIDsInt := dd.ScanArg[[]changeID](t, d, "change-ids")
					expectPanic := false
					if d.HasArg("expect-panic") {
						expectPanic = true
					}
					for _, id := range changeIDsInt {
						if expectPanic {
							require.Panics(t, func() {
								cs.undoPendingChange(id)
							})
						} else {
							cs.undoPendingChange(id)
						}
					}
					return printPendingChangesTest(testingGetPendingChanges(t, cs))

				case "get-pending-changes":
					return printPendingChangesTest(testingGetPendingChanges(t, cs))

				case "rebalance-stores":
					storeID := dd.ScanArg[roachpb.StoreID](t, d, "store-id")
					rng := rand.New(rand.NewSource(0))
					dsm := newDiversityScoringMemo()
					passObs := makeRebalancingPassMetricsAndLogger(storeID)
					re := newRebalanceEnv(cs, rng, dsm, cs.ts.Now(), passObs)

					if n, ok := dd.ScanArgOpt[int](t, d, "max-lease-transfer-count"); ok {
						re.maxLeaseTransferCount = n
					}
					if n, ok := dd.ScanArgOpt[int](t, d, "max-range-move-count"); ok {
						re.maxRangeMoveCount = n
					}
					if f, ok := dd.ScanArgOpt[float64](t, d, "fraction-pending-decrease-threshold"); ok {
						re.fractionPendingIncreaseOrDecreaseThreshold = f
					}

					re.rebalanceStores(ctx, storeID)
					rec := finishAndGet()
					var sb redact.StringBuilder
					rec.SafeFormatMinimal(&sb)
					return sb.String() + printPendingChangesTest(testingGetPendingChanges(t, cs))

				case "tick":
					seconds := dd.ScanArg[int](t, d, "seconds")
					ts.Advance(time.Second * time.Duration(seconds))
					return fmt.Sprintf("t=%v", ts.Now().Sub(testingBaseTime))

				case "retain-ready-lease-target-stores-only":
					in := dd.ScanArg[[]roachpb.StoreID](t, d, "in")
					rangeID := dd.ScanArg[roachpb.RangeID](t, d, "range-id")
					lh, _ := dd.ScanArgOpt[roachpb.StoreID](t, d, "leaseholder")
					out := retainReadyLeaseTargetStoresOnly(ctx, storeSet(in), cs.stores, rangeID, lh)
					rec := finishAndGet()
					var sb redact.StringBuilder
					rec.SafeFormatMinimal(&sb)
					return fmt.Sprintf("%s%v\n", sb.String(), out)

				case "retain-ready-replica-target-stores-only":
					in := dd.ScanArg[[]roachpb.StoreID](t, d, "in")
					replicas, _ := dd.ScanArgOpt[[]roachpb.StoreID](t, d, "replicas")
					var replicasSet storeSet
					for _, replica := range replicas {
						replicasSet.insert(replica)
					}
					out := retainReadyReplicaTargetStoresOnly(ctx, storeSet(in), cs.stores, replicasSet)
					rec := finishAndGet()
					var sb redact.StringBuilder
					rec.SafeFormatMinimal(&sb)
					return fmt.Sprintf("%s%v\n", sb.String(), out)

				default:
					panic(fmt.Sprintf("unknown command: %v", d.Cmd))
				}
			}

			datadriven.RunTest(t, path, invokeFn)
		})
}
