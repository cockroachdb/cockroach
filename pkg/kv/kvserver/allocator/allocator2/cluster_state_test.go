// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package allocator2

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func parseBool(t *testing.T, in string) bool {
	b, err := strconv.ParseBool(in)
	require.NoError(t, err)
	return b
}

func parseInt(t *testing.T, in string) int {
	i, err := strconv.Atoi(in)
	require.NoError(t, err)
	return i
}

func parseInts(t *testing.T, in string) []int {
	parts := strings.Split(in, ",")
	ints := make([]int, 0, len(parts))
	for _, i := range strings.Split(in, ",") {
		ints = append(ints, parseInt(t, i))
	}
	return ints
}

func stripParens(t *testing.T, in string) string {
	rTrim := strings.TrimSuffix(in, ")")
	lrTrim := strings.TrimPrefix(rTrim, "(")
	return lrTrim
}

func parseLoadVector(t *testing.T, in string) loadVector {
	var vec loadVector
	parts := strings.Split(stripParens(t, in), ",")
	require.Len(t, parts, int(numLoadDimensions))
	for dim := range vec {
		vec[dim] = loadValue(parseInt(t, parts[dim]))
	}
	return vec
}

func parseSecondaryLoadVector(t *testing.T, in string) secondaryLoadVector {
	var vec secondaryLoadVector
	parts := strings.Split(stripParens(t, in), ",")
	require.Len(t, parts, int(numSecondaryLoadDimensions))
	for dim := range vec {
		vec[dim] = loadValue(parseInt(t, parts[dim]))
	}
	return vec
}

func parseStoreLoad(t *testing.T, in string) storeLoad {
	var sl storeLoad
	for _, v := range strings.Fields(in) {
		parts := strings.Split(v, "=")
		require.Len(t, parts, 2)
		switch parts[0] {
		case "store-id":
			sl.StoreID = roachpb.StoreID(parseInt(t, parts[1]))
		case "node-id":
			sl.NodeID = roachpb.NodeID(parseInt(t, parts[1]))
		case "load":
			sl.reportedLoad = parseLoadVector(t, parts[1])
		case "capacity":
			sl.capacity = parseLoadVector(t, parts[1])
			for i := range sl.capacity {
				if sl.capacity[i] < 0 {
					sl.capacity[i] = parentCapacity
				}
			}
		case "secondary-load":
			sl.reportedSecondaryLoad = parseSecondaryLoadVector(t, parts[1])
		default:
			t.Fatalf("Unknown argument: %s", parts[0])
		}
	}
	return sl
}

func parseNodeLoad(t *testing.T, in string) nodeLoad {
	var nl nodeLoad
	for _, part := range strings.Fields(in) {
		parts := strings.Split(part, "=")
		require.Len(t, parts, 2)
		switch parts[0] {
		case "node-id":
			nl.nodeID = roachpb.NodeID(parseInt(t, parts[1]))
		case "cpu-load":
			nl.reportedCPU = loadValue(parseInt(t, parts[1]))
		case "cpu-capacity":
			nl.capacityCPU = loadValue(parseInt(t, parts[1]))
		default:
			t.Fatalf("Unknown argument: %s", parts[0])
		}
	}
	return nl
}

func parseStoreRange(t *testing.T, in string) storeRange {
	var sr storeRange
	for _, v := range strings.Fields(in) {
		parts := strings.Split(v, "=")
		require.Len(t, parts, 2)
		switch parts[0] {
		case "range-id":
			sr.RangeID = roachpb.RangeID(parseInt(t, parts[1]))
		case "lease":
			sr.isLeaseholder = parseBool(t, parts[1])
		case "type":
			rType, err := parseReplicaType(parts[1])
			require.NoError(t, err)
			sr.replicaType.replicaType = rType
		}
	}
	return sr
}

func parseChangeAddRemove(
	t *testing.T, in string,
) (add, remove roachpb.StoreID, replType roachpb.ReplicaType, rangeLoad rangeLoad) {
	// Note that remove or add will 0 if not found in this string.
	for _, v := range strings.Fields(in) {
		parts := strings.Split(v, "=")
		require.Len(t, parts, 2)
		switch parts[0] {
		case "add-store":
			add = roachpb.StoreID(parseInt(t, parts[1]))
		case "remove-store":
			remove = roachpb.StoreID(parseInt(t, parts[1]))
		case "type":
			var err error
			replType, err = parseReplicaType(parts[1])
			require.NoError(t, err)
		case "load":
			rangeLoad.load = parseLoadVector(t, parts[1])
		case "cpu-raft":
			rangeLoad.raftCPU = loadValue(parseInt(t, parts[1]))
		}
	}
	return add, remove, replType, rangeLoad
}

func parseRangeMsgReplica(t *testing.T, in string) storeIDAndReplicaState {
	var repl storeIDAndReplicaState
	for _, v := range strings.Fields(in) {
		parts := strings.Split(v, "=")
		require.Len(t, parts, 2)
		switch parts[0] {
		case "store-id":
			repl.StoreID = roachpb.StoreID(parseInt(t, parts[1]))
		case "replica-id":
			repl.ReplicaID = roachpb.ReplicaID(parseInt(t, parts[1]))
		case "lease":
			repl.isLeaseholder = parseBool(t, parts[1])
		case "type":
			replType, err := parseReplicaType(parts[1])
			require.NoError(t, err)
			repl.replicaType.replicaType = replType
		}
	}
	return repl
}

// TODO(kvoli,sumeerbhola): Convert string functions into SafeFormat functions
// on relevant structs to use in logging.
func printNodeListMeta(cs *clusterState) string {
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
			fmt.Fprintf(&buf, "  store-id=%v init=%s attrs=%s locality-code=%s\n",
				ss.StoreID, ss.storeInitState, ss.Attrs, ss.localityTiers.str)
		}
	}
	return buf.String()
}

func printPendingChanges(changes []*pendingReplicaChange) string {
	oldestFirst := pendingChangesOldestFirst(changes)
	sort.Sort(&oldestFirst)
	var buf strings.Builder
	fmt.Fprintf(&buf, "pending(%d)", len(oldestFirst))
	for _, change := range oldestFirst {
		fmt.Fprintf(&buf, "\nchange-id=%d start=%ds store-id=%v range-id=%v delta=%v",
			change.changeID, change.startTime.Unix(), change.storeID,
			change.rangeID, change.loadDelta,
		)
		if !(change.enactedAtTime == time.Time{}) {
			fmt.Fprintf(&buf, " enacted=%ds", change.enactedAtTime.Unix())
		}
		fmt.Fprintf(&buf, "\n  prev=(%v)\n  next=(%v)", change.prev, change.next)
	}
	return buf.String()
}

// testingGetStoreList returns the IDs of stores in the cluster state. Two
// lists of storeIDs are returned, containing the non-removed and removed
// stores respectively. Both lists are sorted. This testing helper method is
// intended to be used in order to ensure determinism.
func testingGetStoreList(cs *clusterState) (nonRemovedList, removedList storeIDPostingList) {
	for storeID, ss := range cs.stores {
		if ss.storeInitState == removed {
			removedList.insert(storeID)
		} else {
			nonRemovedList.insert(storeID)
		}
	}
	return nonRemovedList, removedList
}

func testingGetPendingChanges(cs *clusterState) []*pendingReplicaChange {
	pendingChangeSet := map[changeID]struct{}{}
	var pendingChangeList []*pendingReplicaChange
	maybeAddChange := func(change *pendingReplicaChange) {
		if _, ok := pendingChangeSet[change.changeID]; ok {
			return
		}
		pendingChangeList = append(pendingChangeList, change)
		pendingChangeSet[change.changeID] = struct{}{}
	}

	for _, change := range cs.pendingChangeList {
		maybeAddChange(change)
	}

	// Use the sorted store list to ensure deterministic iteration order.
	storeList, _ := testingGetStoreList(cs)
	for _, storeID := range storeList {
		for _, change := range cs.stores[storeID].adjusted.loadPendingChanges {
			maybeAddChange(change)
		}
	}
	return pendingChangeList
}

// manualTestClock implements the hlc.WallClock interface.
type manualTestClock struct {
	nanos int64
}

var _ hlc.WallClock = &manualTestClock{}

// Now returns the current time.
func (m *manualTestClock) Now() time.Time {
	return timeutil.Unix(0, m.nanos)
}

// Set sets the wall time to the supplied timestamp.
func (m *manualTestClock) Set(tsNanos int64) {
	m.nanos = tsNanos
}

func TestClusterState(t *testing.T) {
	dir := datapathutils.TestDataPath(t, "clusterstate")
	datadriven.Walk(t, dir, func(t *testing.T, path string) {
		manualClock := &manualTestClock{}
		cs := newClusterState(manualClock, newStringInterner())
		datadriven.RunTest(t, path,
			func(t *testing.T, d *datadriven.TestData) string {
				switch d.Cmd {
				case "ranges":
					var rangeIDs []int
					for rangeID := range cs.ranges {
						rangeIDs = append(rangeIDs, int(rangeID))
					}
					// Sort the range IDs before printing, iterating over a map would lead
					// to non-determinism.
					sort.Ints(rangeIDs)
					var buf strings.Builder
					for _, rangeID := range rangeIDs {
						rs := cs.ranges[roachpb.RangeID(rangeID)]
						fmt.Fprintf(&buf, "range-id=%v\n", rangeID)
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
						if ss.storeInitState == removed {
							continue
						}
						ns := cs.nodes[ss.NodeID]
						fmt.Fprintf(&buf,
							"store-id=%v reported=%v adjusted=%v node-reported-cpu=%v node-adjusted-cpu=%v seq=%d\n",
							ss.StoreID, ss.reportedLoad, ss.adjusted.load, ns.reportedCPU, ns.adjustedCPU, ss.loadSeqNum,
						)
					}
					return buf.String()

				case "get-pending-changes":
					return printPendingChanges(testingGetPendingChanges(cs))

				case "set-store":
					for _, next := range strings.Split(d.Input, "\n") {
						desc := parseStoreDescriptor(t, strings.TrimSpace(next))
						require.NoError(t, cs.setStore(desc))
					}
					return printNodeListMeta(cs)
				case "remove-node":
					var nodeID int

					d.ScanArgs(t, "node-id", &nodeID)
					require.NoError(t, cs.removeNodeAndStores(roachpb.NodeID(nodeID)))

					var buf strings.Builder
					nonRemovedStores, removedStores := testingGetStoreList(cs)
					buf.WriteString("non-removed store-ids: ")
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
					require.NoError(t, cs.updateFailureDetectionSummary(roachpb.NodeID(nodeID), fd))
					return printNodeListMeta(cs)

				case "msg":
					var lastSeqNum, curSeqNum int64
					var storeIdx, rangeIdx int
					var storeLeaseMsgs []storeLeaseholderMsg
					var storeLoadMsgs []storeLoadMsg

					d.ScanArgs(t, "last-seq", &lastSeqNum)
					d.ScanArgs(t, "cur-seq", &curSeqNum)
					lines := strings.Split(d.Input, "\n")
					// The node load should be specified first.
					nl := parseNodeLoad(t, lines[0])
					storeIdx, rangeIdx = -1, -1
					for _, next := range lines[1:] {
						line := strings.TrimSpace(next)
						if len(line) < 2 {
							continue
						}
						parts := strings.Split(line, ":")
						k, v := parts[0], parts[1]
						switch k {
						case "store":
							sl := parseStoreLoad(t, v)
							storeLoadMsgs = append(storeLoadMsgs, storeLoadMsg{
								StoreID:       sl.StoreID,
								load:          sl.reportedLoad,
								capacity:      sl.capacity,
								secondaryLoad: sl.reportedSecondaryLoad,
							})
							storeLeaseMsgs = append(
								storeLeaseMsgs,
								storeLeaseholderMsg{StoreID: sl.StoreID},
							)
							rangeIdx = -1
							storeIdx++
						case "range":
							storeRange := parseStoreRange(t, v)
							storeLoadMsgs[storeIdx].storeRanges = append(
								storeLoadMsgs[storeIdx].storeRanges,
								storeRange,
							)
							// If the store has a lease for the range, also create a store
							// lease message which will be populated with any replicas lower
							// down.
							if storeRange.isLeaseholder {
								storeLeaseMsgs[storeIdx].ranges = append(
									storeLeaseMsgs[storeIdx].ranges,
									rangeMsg{RangeID: storeRange.RangeID},
								)
								rangeIdx++
							}
						case "replica":
							replMsg := parseRangeMsgReplica(t, v)
							if replMsg.StoreID == storeLoadMsgs[storeIdx].StoreID {
								replMsg.isLeaseholder = true
							}
							rng := &storeLeaseMsgs[storeIdx].ranges[rangeIdx]
							rng.replicas = append(rng.replicas, replMsg)
						}
					}
					if err := cs.processNodeLoadResponse(&nodeLoadResponse{
						lastLoadSeqNum:    lastSeqNum,
						curLoadSeqNum:     curSeqNum,
						nodeLoad:          nl,
						stores:            storeLoadMsgs,
						leaseholderStores: storeLeaseMsgs,
					}); err != nil {
						return fmt.Sprintf("err=%v", err.Error())
					}
					return "OK"

				case "make-pending-changes":
					var rid int
					var changes []replicaChange

					d.ScanArgs(t, "range-id", &rid)
					rangeID := roachpb.RangeID(rid)
					rState := cs.ranges[rangeID]
					lines := strings.Split(d.Input, "\n")
					for _, next := range lines {
						line := strings.TrimSpace(next)
						if len(line) < 2 {
							continue
						}
						parts := strings.Split(line, ":")
						k, v := parts[0], parts[1]
						switch k {
						case "transfer-lease":
							add, remove, _, load := parseChangeAddRemove(t, v)
							load.RangeID = rangeID
							transferChanges := makeLeaseTransferChanges(load, rState, add, remove)
							changes = append(changes, transferChanges[:]...)
						case "add-replica":
							add, _, replType, load := parseChangeAddRemove(t, v)
							load.RangeID = rangeID
							addChanges := makeAddReplicaChange(load, add, replType)
							changes = append(changes, addChanges)
						case "remove-replica":
							_, remove, _, load := parseChangeAddRemove(t, v)
							load.RangeID = rangeID
							var removeRepl storeIDAndReplicaState
							for _, replica := range rState.replicas {
								if replica.StoreID == remove {
									removeRepl = replica
								}
							}
							changes = append(changes, makeRemoveReplicaChange(load, removeRepl))
						case "rebalance-replica":
							add, remove, _, load := parseChangeAddRemove(t, v)
							load.RangeID = rangeID
							rebalanceChanges := makeRebalanceReplicaChanges(load, rState, add, remove)
							changes = append(changes, rebalanceChanges[:]...)
						}
					}
					cs.makePendingChanges(rangeID, changes)
					return printPendingChanges(testingGetPendingChanges(cs))

				case "gc-pending-changes":
					cs.gcPendingChanges(cs.clock.Now())
					return printPendingChanges(testingGetPendingChanges(cs))

				case "reject-pending-changes":
					lines := strings.Split(d.Input, "\n")
					// The node load should be specified first.
					require.Len(t, lines, 1)
					line := lines[0]
					parts := strings.Split(line, "=")
					require.Len(t, parts, 2)
					changeIDInts := parseInts(t, stripParens(t, parts[1]))
					changeIDs := make([]changeID, len(changeIDInts))
					for i := range changeIDInts {
						changeIDs = append(changeIDs, changeID(changeIDInts[i]))
					}
					cs.pendingChangesRejected(changeIDs)
					return printPendingChanges(testingGetPendingChanges(cs))

				case "tick":
					var seconds int
					d.ScanArgs(t, "seconds", &seconds)
					manualClock.Set(manualClock.Now().Add(time.Second * time.Duration(seconds)).UnixNano())
					curSeconds := cs.clock.Now().UnixNano() / int64(time.Second)
					return fmt.Sprintf("clock=%ds", curSeconds)

				default:
					return fmt.Sprintf("unknown command: %s", d.Cmd)
				}
			},
		)
	})
}
