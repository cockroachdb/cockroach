// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mma

import (
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

func parseLoadVector(t *testing.T, in string) loadVector {
	var vec loadVector
	parts := strings.Split(stripBrackets(t, in), ",")
	require.Len(t, parts, int(numLoadDimensions))
	for dim := range vec {
		vec[dim] = loadValue(parseInt(t, parts[dim]))
	}
	return vec
}

func parseSecondaryLoadVector(t *testing.T, in string) secondaryLoadVector {
	var vec secondaryLoadVector
	parts := strings.Split(stripBrackets(t, in), ",")
	require.Len(t, parts, int(numSecondaryLoadDimensions))
	for dim := range vec {
		vec[dim] = loadValue(parseInt(t, parts[dim]))
	}
	return vec
}

func parseStoreLoadMsg(t *testing.T, in string) storeLoadMsg {
	var msg storeLoadMsg
	for _, v := range strings.Fields(in) {
		parts := strings.Split(v, "=")
		require.Len(t, parts, 2)
		switch parts[0] {
		case "store-id":
			msg.StoreID = roachpb.StoreID(parseInt(t, parts[1]))
		case "load":
			msg.load = parseLoadVector(t, parts[1])
		case "capacity":
			msg.capacity = parseLoadVector(t, parts[1])
			for i := range msg.capacity {
				if msg.capacity[i] < 0 {
					msg.capacity[i] = parentCapacity
				}
			}
		case "secondary-load":
			msg.secondaryLoad = parseSecondaryLoadVector(t, parts[1])
		default:
			t.Fatalf("Unknown argument: %s", parts[0])
		}
	}
	return msg
}

func parseNodeLoadMsg(t *testing.T, in string) nodeLoadMsg {
	var msg nodeLoadMsg
	lines := strings.Split(in, "\n")
	for _, part := range strings.Fields(lines[0]) {
		parts := strings.Split(part, "=")
		require.Len(t, parts, 2)
		switch parts[0] {
		case "node-id":
			msg.nodeID = roachpb.NodeID(parseInt(t, parts[1]))
		case "cpu-load":
			msg.reportedCPU = loadValue(parseInt(t, parts[1]))
		case "cpu-capacity":
			msg.capacityCPU = loadValue(parseInt(t, parts[1]))
		case "load-time":
			duration, err := time.ParseDuration(parts[1])
			require.NoError(t, err)
			msg.loadTime = testingBaseTime.Add(duration)
		default:
			t.Fatalf("Unknown argument: %s", parts[0])
		}
	}
	msg.stores = make([]storeLoadMsg, 0, len(lines)-1)
	for _, line := range lines[1:] {
		msg.stores = append(msg.stores, parseStoreLoadMsg(t, line))
	}
	return msg
}

func parseStoreLeaseholderMsg(t *testing.T, in string) storeLeaseholderMsg {
	var msg storeLeaseholderMsg

	lines := strings.Split(in, "\n")
	require.True(t, strings.HasPrefix(lines[0], "store-id="))
	msg.StoreID = roachpb.StoreID(parseInt(t, strings.TrimPrefix(lines[0], "store-id=")))

	var rMsg rangeMsg
	for _, line := range lines[1:] {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "range-id") {
			if rMsg.RangeID != 0 {
				msg.ranges = append(msg.ranges, rMsg)
			}
			rMsg = rangeMsg{RangeID: 0}
			for _, field := range strings.Fields(line) {
				parts := strings.SplitN(field, "=", 2)
				switch parts[0] {
				case "range-id":
					rMsg.RangeID = roachpb.RangeID(parseInt(t, parts[1]))
				case "load":
					rMsg.rangeLoad.load = parseLoadVector(t, parts[1])
				case "raft-cpu":
					rMsg.rangeLoad.raftCPU = loadValue(parseInt(t, parts[1]))
				}
			}
		} else if strings.HasPrefix(line, "config=") {
			rMsg.conf = spanconfigtestutils.ParseZoneConfig(t, strings.TrimPrefix(line, "config=")).AsSpanConfig()
		} else {
			var repl storeIDAndReplicaState
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
					repl.isLeaseholder = parseBool(t, parts[1])
				case "type":
					replType, err := parseReplicaType(parts[1])
					require.NoError(t, err)
					repl.replicaType.replicaType = replType
				default:
					panic(fmt.Sprintf("unknown argument: %s", parts[0]))
				}
			}
			rMsg.replicas = append(rMsg.replicas, repl)
		}
	}
	if rMsg.RangeID != 0 {
		msg.ranges = append(msg.ranges, rMsg)
	}

	return msg
}

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

func printPendingChanges(changes []*pendingReplicaChange) string {
	var buf strings.Builder
	fmt.Fprintf(&buf, "pending(%d)", len(changes))
	for _, change := range changes {
		fmt.Fprintf(&buf, "\nchange-id=%d store-id=%v range-id=%v load-delta=%v start=%v",
			change.changeID, change.storeID, change.rangeID,
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

func testingGetStoreList(cs *clusterState) (member, removed storeIDPostingList) {
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

func testingGetPendingChanges(cs *clusterState) []*pendingReplicaChange {
	var pendingChangeList []*pendingReplicaChange
	for _, change := range cs.pendingChanges {
		pendingChangeList = append(pendingChangeList, change)
	}
	sort.Slice(pendingChangeList, func(i, j int) bool {
		return pendingChangeList[i].changeID < pendingChangeList[j].changeID
	})
	return pendingChangeList
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
						ns.nodeID, ns.fdSummary, cs.stores[ns.stores[0]].StoreDescriptor.Locality())
					for _, storeID := range ns.stores {
						ss := cs.stores[storeID]
						fmt.Fprintf(&buf, "  store-id=%v membership=%v attrs=%s locality-code=%s\n",
							ss.StoreID, ss.storeMembership, ss.Attrs, ss.localityTiers.str)
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
						fmt.Fprintf(&buf, "range-id=%v load=%v raft-cpu=%v\n", rangeID, rs.load.load, rs.load.raftCPU)
						for _, repl := range rs.replicas {
							fmt.Fprintf(&buf, "  store-id=%v %v\n",
								repl.StoreID, repl.replicaIDAndType,
							)
						}
					}
					return buf.String()

				case "get-load-info":
					var buf strings.Builder
					storeList, _ := testingGetStoreList(cs)
					for _, storeID := range storeList {
						ss := cs.stores[storeID]
						if ss.storeMembership == storeMembershipRemoved {
							continue
						}
						ns := cs.nodes[ss.NodeID]
						fmt.Fprintf(&buf,
							"store-id=%v reported=%v adjusted=%v node-reported-cpu=%v node-adjusted-cpu=%v seq=%d\n",
							ss.StoreID, ss.reportedLoad, ss.adjusted.load, ns.reportedCPU, ns.adjustedCPU, ss.loadSeqNum,
						)
					}
					return buf.String()

				case "set-store":
					for _, next := range strings.Split(d.Input, "\n") {
						desc := parseStoreDescriptor(t, next)
						cs.setStore(desc)
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
					nonRemovedStores, removedStores := testingGetStoreList(cs)
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

				case "node-load-msg":
					msg := parseNodeLoadMsg(t, d.Input)
					cs.processNodeLoadMsg(&msg)
					return ""

				case "store-leaseholder-msg":
					msg := parseStoreLeaseholderMsg(t, d.Input)
					cs.processStoreLeaseholderMsg(&msg)
					return ""

				case "make-pending-changes":
					var rid int
					var changes []replicaChange
					d.ScanArgs(t, "range-id", &rid)
					rangeID := roachpb.RangeID(rid)
					rState := cs.ranges[rangeID]

					lines := strings.Split(d.Input, "\n")
					for _, line := range lines {
						parts := strings.Split(strings.TrimSpace(line), ":")
						switch parts[0] {
						case "transfer-lease":
							add, remove, _ := parseChangeAddRemove(t, parts[1])
							transferChanges := makeLeaseTransferChanges(rangeID, rState.replicas, rState.load, add, remove)
							changes = append(changes, transferChanges[:]...)
						case "add-replica":
							add, _, replType := parseChangeAddRemove(t, parts[1])
							changes = append(changes, makeAddReplicaChange(rangeID, rState.load, add, replType))
						case "remove-replica":
							_, remove, _ := parseChangeAddRemove(t, parts[1])
							var removeRepl storeIDAndReplicaState
							for _, replica := range rState.replicas {
								if replica.StoreID == remove {
									removeRepl = replica
								}
							}
							changes = append(changes, makeRemoveReplicaChange(rangeID, rState.load, removeRepl))
						case "rebalance-replica":
							add, remove, _ := parseChangeAddRemove(t, parts[1])
							rebalanceChanges := makeRebalanceReplicaChanges(rangeID, rState.replicas, rState.load, add, remove)
							changes = append(changes, rebalanceChanges[:]...)
						}
					}
					cs.createPendingChanges(rangeID, changes...)
					return printPendingChanges(testingGetPendingChanges(cs))

				case "gc-pending-changes":
					cs.gcPendingChanges(cs.ts.Now())
					return printPendingChanges(testingGetPendingChanges(cs))

				case "reject-pending-changes":
					var changeIDsInt []int
					d.ScanArgs(t, "change-ids", &changeIDsInt)
					changeIDs := make([]changeID, 0, len(changeIDsInt))
					for _, id := range changeIDsInt {
						changeIDs = append(changeIDs, changeID(id))
					}
					cs.pendingChangesRejected(changeIDs)
					return printPendingChanges(testingGetPendingChanges(cs))

				case "get-pending-changes":
					return printPendingChanges(testingGetPendingChanges(cs))

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
