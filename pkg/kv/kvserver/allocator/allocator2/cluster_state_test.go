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

func newParseStoreDescriptor(t *testing.T, in string) roachpb.StoreDescriptor {
	var desc roachpb.StoreDescriptor
	for _, v := range strings.Split(in, " ") {
		parts := strings.SplitN(v, "=", 2)
		switch parts[0] {
		case "store-id":
			desc.StoreID = roachpb.StoreID(parseInt(t, parts[1]))
		case "node-id":
			desc.Node.NodeID = roachpb.NodeID(parseInt(t, parts[1]))
		case "attrs":
			desc.Attrs.Attrs = append(
				desc.Attrs.Attrs,
				strings.Split(parts[1], ",")...,
			)
		case "locality-tiers":
			for _, lt := range strings.Split(parts[1], ",") {
				kv := strings.Split(lt, "=")
				require.Equal(t, 2, len(kv))
				desc.Node.Locality.Tiers = append(
					desc.Node.Locality.Tiers, roachpb.Tier{Key: kv[0], Value: kv[1]})
			}
		}
	}
	return desc
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

func parseRangeLoad(t *testing.T, in string) rangeLoad {
	var rl rangeLoad
	for _, v := range strings.Fields(in) {
		parts := strings.Split(v, "=")
		require.Len(t, parts, 2)
		switch parts[0] {
		case "range-id":
			rl.RangeID = roachpb.RangeID(parseInt(t, parts[1]))
		case "load":
			rl.load = parseLoadVector(t, parts[1])
		case "cpu-raft":
			rl.raftCPU = loadValue(parseInt(t, parts[1]))
		default:
			t.Fatalf("Unknown argument: %s", parts[0])
		}
	}
	return rl
}

func parseRange(t *testing.T, in string) (roachpb.RangeID, []roachpb.StoreID) {
	var rangeID roachpb.RangeID
	var voters []roachpb.StoreID
	for _, v := range strings.Split(in, " ") {
		parts := strings.Split(v, "=")
		require.Len(t, parts, 2)
		switch parts[0] {
		case "range-id":
			rangeID = roachpb.RangeID(parseInt(t, parts[1]))
		case "voters":
			// We assume the first voter store is the leaseholder for the range.
			ints := parseInts(t, stripParens(t, parts[1]))
			voters = make([]roachpb.StoreID, len(ints))
			for i := range voters {
				voters[i] = roachpb.StoreID(ints[i])
			}
		default:
			t.Fatalf("Unknown argument: %s", parts[0])
		}
	}
	return rangeID, voters
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
			fmt.Fprintf(&buf, "  store-id=%v attrs=%s locality-code=%s\n",
				ss.StoreID, ss.Attrs, ss.localityTiers.str)
		}
	}
	return buf.String()
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
	manualClock := &manualTestClock{}
	cs := newClusterState(manualClock, newStringInterner())
	storeMap := map[roachpb.StoreID]roachpb.StoreDescriptor{}
	var bufferedResp *nodeLoadResponse
	datadriven.RunTest(t, "testdata/cluster_state",
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "set-store":
				for _, next := range strings.Split(d.Input, "\n") {
					desc := newParseStoreDescriptor(t, strings.TrimSpace(next))
					storeMap[desc.StoreID] = desc
					require.NoError(t, cs.setStore(desc))
				}
				return printNodeListMeta(cs)
			case "remove-node":
				var nodeID int
				d.ScanArgs(t, "node-id", &nodeID)
				require.NoError(t, cs.removeNodeAndStores(roachpb.NodeID(nodeID)))
				return printNodeListMeta(cs)
			case "update-failure-detection":
				var nodeID int
				var failureDetectionString string
				d.ScanArgs(t, "node-id", &nodeID)
				d.ScanArgs(t, "summary", &failureDetectionString)
				var fd failureDetectionSummary
				for summary, summaryStr := range failureDetectionSummaryMap {
					if summaryStr == failureDetectionString {
						fd = summary
						break
					}
				}
				require.NoError(t, cs.updateFailureDetectionSummary(roachpb.NodeID(nodeID), fd))
				return printNodeListMeta(cs)
			case "buffer-load-msg":
				var storeLoadMsgs []storeLoadMsg
				lines := strings.Split(d.Input, "\n")
				// The node load should be specified first.
				nl := parseNodeLoad(t, lines[0])
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
					case "topk":
						rl := parseRangeLoad(t, v)
						topK := &storeLoadMsgs[len(storeLoadMsgs)-1].topKRanges
						*topK = append(*topK, rl)
					}
				}
				if bufferedResp != nil {
					require.Equal(t, bufferedResp.nodeID, nl.nodeID)
				} else {
					bufferedResp = &nodeLoadResponse{}
				}
				bufferedResp.nodeLoad = nl
				bufferedResp.stores = storeLoadMsgs
				return ""
			case "buffer-lease-msg":
				var storeIdx, rangeIdx, nodeID int
				var storeLeaseMsgs []storeLeaseholderMsg

				d.ScanArgs(t, "node-id", &nodeID)
				lines := strings.Split(d.Input, "\n")
				storeIdx, rangeIdx = -1, -1
				for _, next := range lines {
					line := strings.TrimSpace(next)
					if len(line) < 2 {
						continue
					}
					parts := strings.Split(line, ":")
					k, v := parts[0], parts[1]
					switch k {
					case "store":
						rangeIdx = -1
						storeID := parseInt(t, strings.Split(v, "=")[1])
						storeLeaseMsgs = append(
							storeLeaseMsgs,
							storeLeaseholderMsg{StoreID: roachpb.StoreID(storeID)},
						)
						storeIdx++
					case "range":
						rangeID := parseInt(t, strings.Split(v, "=")[1])
						storeLeaseMsgs[storeIdx].ranges = append(
							storeLeaseMsgs[storeIdx].ranges,
							rangeMsg{RangeID: roachpb.RangeID(rangeID)},
						)
						rangeIdx++
					case "replica":
						replMsg := parseRangeMsgReplica(t, v)
						rng := &storeLeaseMsgs[storeIdx].ranges[rangeIdx]
						rng.replicas = append(rng.replicas, replMsg)
					}
				}
				if bufferedResp != nil {
					require.Equal(t, bufferedResp.nodeID, roachpb.NodeID(nodeID))
				} else {
					bufferedResp = &nodeLoadResponse{}
					bufferedResp.nodeID = roachpb.NodeID(nodeID)
				}
				bufferedResp.leaseholderStores = storeLeaseMsgs
				return ""
			case "ranges":
				var buf strings.Builder
				for rangeID, rs := range cs.ranges {
					fmt.Fprintf(&buf, "range-id=%v\n", rangeID)
					for _, repl := range rs.replicas {
						fmt.Fprintf(&buf, "  store-id=%v replica-id=%v lease=%v type=%v\n",
							repl.StoreID, repl.ReplicaID, repl.isLeaseholder, repl.replicaType.replicaType,
						)
					}
				}
				return buf.String()
			case "send":
				d.ScanArgs(t, "last-seq", &bufferedResp.lastLoadSeqNum)
				d.ScanArgs(t, "cur-seq", &bufferedResp.curLoadSeqNum)
				err := cs.processNodeLoadResponse(bufferedResp)
				// Clear the node load response message after each send. Any buffering
				// commands should be done back-to-back.
				bufferedResp = nil
				require.NoError(t, err)
				return "OK"
			case "load-summary":
				var msl meanStoreLoad
				var mnl meanNodeLoad

				lines := strings.Split(d.Input, "\n")
				for _, next := range lines {
					line := strings.TrimSpace(next)
					if len(line) < 2 {
						continue
					}
					parts := strings.Split(line, ":")
					k, v := parts[0], parts[1]
					switch k {
					case "store-means":
						sl := parseStoreLoad(t, v)
						msl = meanStoreLoad{
							load:          sl.reportedLoad,
							capacity:      sl.capacity,
							util:          loadUtilization(sl.reportedLoad, sl.capacity),
							secondaryLoad: sl.reportedSecondaryLoad,
						}
					case "node-means":
						nl := parseNodeLoad(t, v)
						mnl = meanNodeLoad{
							loadCPU:     nl.reportedCPU,
							capacityCPU: nl.capacityCPU,
							utilCPU:     float64(nl.reportedCPU) / float64(nl.capacityCPU),
						}
					}
				}
				var buf strings.Builder
				for _, storeID := range cs.storeList {
					fmt.Fprintf(&buf, "store-id=%d %s\n", storeID, cs.computeLoadSummary(storeID, &msl, &mnl))
				}
				return buf.String()
			case "make-pending-changes":
				var tick, rid int
				var changes []replicaChange

				d.ScanArgs(t, "tick", &tick)
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
				_ = cs.makePendingChanges(rangeID, changes)
				var buf strings.Builder
				buf.WriteString("pending\n")
				for _, pending := range cs.pendingChangeList {
					fmt.Fprintf(&buf, "  %s\n", pending)
				}
				return buf.String()
			case "gc-pending-changes":
				cs.gcPendingChanges(cs.clock.Now())
				var buf strings.Builder
				buf.WriteString("pending\n")
				for _, pending := range cs.pendingChangeList {
					fmt.Fprintf(&buf, "  %s\n", pending)
				}
				return buf.String()
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
}
