// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaprototype

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigtestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/datadriven"
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
				rMsg.Populated = false
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
					rMsg.Populated = true
				case "raft-cpu":
					rMsg.RangeLoad.RaftCPU = LoadValue(parseInt(t, parts[1]))
					rMsg.Populated = true
				case "not-populated":
					notPopulatedOverride = true
				}
			}
		} else if strings.HasPrefix(line, "config=") {
			rMsg.Conf = spanconfigtestutils.ParseZoneConfig(t, strings.TrimPrefix(line, "config=")).AsSpanConfig()
			rMsg.Populated = true
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
				default:
					panic(fmt.Sprintf("unknown argument: %s", parts[0]))
				}
			}
			rMsg.Replicas = append(rMsg.Replicas, repl)
			rMsg.Populated = true
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
			panic(fmt.Sprintf("unknown argument: %s", parts[1]))
		}
	}
	return add, remove, replType
}

func printPendingChangesTest(changes []*pendingReplicaChange) string {
	var buf strings.Builder
	fmt.Fprintf(&buf, "pending(%d)", len(changes))
	for _, change := range changes {
		fmt.Fprintf(&buf, "\nchange-id=%d store-id=%v node-id=%v range-id=%v load-delta=%v start=%v",
			change.ChangeID, change.target.StoreID, change.target.NodeID, change.rangeID,
			change.loadDelta, change.startTime.Sub(testingBaseTime),
		)
		if !(change.enactedAtTime == time.Time{}) {
			fmt.Fprintf(&buf, " enacted=%v",
				change.enactedAtTime.Sub(testingBaseTime))
		}
		fmt.Fprintf(&buf, "\n  prev=(%v)\n  next=(%v)", change.prev, change.next)
	}
	return buf.String()
}

func testingGetStoreList(t *testing.T, cs *clusterState) (member, removed storeIDPostingList) {
	var clusterStoreList, nodeStoreList storeIDPostingList
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

	for storeID, ss := range cs.stores {
		switch ss.storeMembership {
		case storeMembershipMember, storeMembershipRemoving:
			member.insert(storeID)
		case storeMembershipRemoved:
			removed.insert(storeID)
		}
	}
	return member, removed
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
		return clusterPendingChangeList[i].ChangeID < clusterPendingChangeList[j].ChangeID
	})
	sort.Slice(storeLoadPendingChangeList, func(i, j int) bool {
		return storeLoadPendingChangeList[i].ChangeID < storeLoadPendingChangeList[j].ChangeID
	})
	sort.Slice(rangePendingChangeList, func(i, j int) bool {
		return rangePendingChangeList[i].ChangeID < rangePendingChangeList[j].ChangeID
	})
	require.EqualValues(t, clusterPendingChangeList, rangePendingChangeList)
	require.LessOrEqual(t, len(clusterPendingChangeList), len(storeLoadPendingChangeList))
	i, j := 0, 0
	for i < len(clusterPendingChangeList) && j < len(storeLoadPendingChangeList) {
		require.GreaterOrEqual(
			t, clusterPendingChangeList[i].ChangeID, storeLoadPendingChangeList[j].ChangeID)
		if clusterPendingChangeList[i].ChangeID > storeLoadPendingChangeList[j].ChangeID {
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
	datadriven.Walk(t,
		datapathutils.TestDataPath(t, "cluster_state"),
		func(t *testing.T, path string) {
			ts := timeutil.NewManualTime(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
			cs := newClusterState(ts, newStringInterner())

			printNodeListMeta := func() string {
				nodeList := []int{}
				for nodeID := range cs.nodes {
					nodeList = append(nodeList, int(nodeID))
				}
				sort.Ints(nodeList)
				var buf strings.Builder
				for _, nodeID := range nodeList {
					ns := cs.nodes[roachpb.NodeID(nodeID)]
					fmt.Fprintf(&buf, "node-id=%s failure-summary=%s locality-tiers=%s\n",
						ns.NodeID, ns.fdSummary, cs.stores[ns.stores[0]].StoreAttributesAndLocality.locality())
					for _, storeID := range ns.stores {
						ss := cs.stores[storeID]
						fmt.Fprintf(&buf, "  store-id=%v membership=%v attrs=%s locality-code=%s\n",
							ss.StoreID, ss.storeMembership, ss.StoreAttrs, ss.localityTiers.str)
					}
				}
				return buf.String()
			}

			datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
				switch d.Cmd {
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
						fmt.Fprintf(&buf, "range-id=%v load=%v raft-cpu=%v\n", rangeID, rs.load.Load, rs.load.RaftCPU)
						for _, repl := range rs.replicas {
							fmt.Fprintf(&buf, "  store-id=%v %v\n",
								repl.StoreID, repl.ReplicaIDAndType,
							)
						}
					}
					return buf.String()

				case "get-load-info":
					var buf strings.Builder
					memberStores, _ := testingGetStoreList(t, cs)
					for _, storeID := range memberStores {
						ss := cs.stores[storeID]
						ns := cs.nodes[ss.NodeID]
						fmt.Fprintf(&buf,
							"store-id=%v node-id=%v reported=%v adjusted=%v node-reported-cpu=%v node-adjusted-cpu=%v seq=%d\n",
							ss.StoreID, ss.NodeID, ss.reportedLoad, ss.adjusted.load, ns.ReportedCPU, ns.adjustedCPU, ss.loadSeqNum,
						)
						for ls, topk := range ss.adjusted.topKRanges {
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
						cs.setStore(sal)
					}
					return printNodeListMeta()

				case "set-store-membership":
					var storeID int
					d.ScanArgs(t, "store-id", &storeID)
					var storeMembershipString string
					d.ScanArgs(t, "membership", &storeMembershipString)
					var storeMembershipVal storeMembership
					switch storeMembershipString {
					case "member":
						storeMembershipVal = storeMembershipMember
					case "removing":
						storeMembershipVal = storeMembershipRemoving
					case "removed":
						storeMembershipVal = storeMembershipRemoved
					}
					cs.setStoreMembership(roachpb.StoreID(storeID), storeMembershipVal)

					var buf strings.Builder
					nonRemovedStores, removedStores := testingGetStoreList(t, cs)
					buf.WriteString("member store-ids: ")
					printPostingList(&buf, nonRemovedStores)
					buf.WriteString("\nremoved store-ids: ")
					printPostingList(&buf, removedStores)
					return buf.String()

				case "update-failure-detection":
					var nodeID int
					var failureDetectionString string
					d.ScanArgs(t, "node-id", &nodeID)
					d.ScanArgs(t, "summary", &failureDetectionString)
					var fd failureDetectionSummary
					for i := fdOK; i < fdDead+1; i++ {
						if i.String() == failureDetectionString {
							fd = i
							break
						}
					}
					cs.updateFailureDetectionSummary(roachpb.NodeID(nodeID), fd)
					return printNodeListMeta()

				case "store-load-msg":
					msg := parseStoreLoadMsg(t, d.Input)
					cs.processStoreLoadMsg(context.Background(), &msg)
					return ""

				case "store-leaseholder-msg":
					msg := parseStoreLeaseholderMsg(t, d.Input)
					cs.processStoreLeaseholderMsgInternal(context.Background(), &msg, 2, nil)
					return ""

				case "make-pending-changes":
					var rid int
					var changes []ReplicaChange
					d.ScanArgs(t, "range-id", &rid)
					rangeID := roachpb.RangeID(rid)
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
							replState := ReplicaState{
								ReplicaIDAndType: ReplicaIDAndType{
									ReplicaType: ReplicaType{
										ReplicaType: replType,
									},
								},
							}
							addTarget := roachpb.ReplicationTarget{NodeID: cs.stores[add].NodeID, StoreID: add}
							changes = append(changes, MakeAddReplicaChange(rangeID, rState.load, replState, addTarget))
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
					cs.createPendingChanges(changes...)
					return printPendingChangesTest(testingGetPendingChanges(t, cs))

				case "gc-pending-changes":
					cs.gcPendingChanges(cs.ts.Now())
					return printPendingChangesTest(testingGetPendingChanges(t, cs))

				case "reject-pending-changes":
					var changeIDsInt []int
					d.ScanArgs(t, "change-ids", &changeIDsInt)
					for _, id := range changeIDsInt {
						cs.undoPendingChange(ChangeID(id), true)
					}
					return printPendingChangesTest(testingGetPendingChanges(t, cs))

				case "get-pending-changes":
					return printPendingChangesTest(testingGetPendingChanges(t, cs))

				case "tick":
					var seconds int
					d.ScanArgs(t, "seconds", &seconds)
					ts.Advance(time.Second * time.Duration(seconds))
					return fmt.Sprintf("t=%v", ts.Now().Sub(testingBaseTime))

				default:
					panic(fmt.Sprintf("unknown command: %v", d.Cmd))
				}
			},
			)
		})
}
