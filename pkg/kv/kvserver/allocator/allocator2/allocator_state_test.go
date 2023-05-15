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
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

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

func testingClock()

func TestRebalanceTarget(t *testing.T) {
	dir := datapathutils.TestDataPath(t, "target")
	datadriven.Walk(t, dir, func(t *testing.T, path string) {
		ctx := context.Background()
		storeMap := map[roachpb.StoreID]roachpb.StoreDescriptor{}
		storeLoadMap := map[roachpb.StoreID]storeLoad{}
		nodeLoadMap := map[roachpb.NodeID]nodeLoad{}
		rangeLoadMap := map[roachpb.RangeID]rangeLoad{}
		rangeMap := map[roachpb.RangeID][]roachpb.StoreID{}
		rangeConfigMap := map[roachpb.RangeID]roachpb.SpanConfig{}
		a := newAllocatorState(hlc.NewClockForTesting(nil))
		datadriven.RunTest(t, path,
			func(t *testing.T, d *datadriven.TestData) string {
				switch d.Cmd {
				case "store":
					descs := []roachpb.StoreDescriptor{}
					for _, next := range strings.Split(d.Input, "\n") {
						desc := newParseStoreDescriptor(t, strings.TrimSpace(next))
						storeMap[desc.StoreID] = desc
						descs = append(descs, desc)
					}

					var buf strings.Builder
					for _, desc := range descs {
						require.NoError(t, a.cs.addNodeID(desc.Node.NodeID))
						require.NoError(t, a.cs.addStore(desc))
						fmt.Fprintf(&buf, " %v", desc.StoreID)
					}
					return buf.String()
				case "load":
					var stores []roachpb.StoreID
					var nodes []roachpb.NodeID
					var ranges []roachpb.RangeID
					for _, next := range strings.Split(d.Input, "\n") {
						line := strings.TrimSpace(next)
						if len(line) < 2 {
							continue
						}
						switch strings.Split(line, "=")[0] {
						case "store-id":
							sl := parseStoreLoad(t, line)
							sl.StoreDescriptor = storeMap[sl.StoreID]
							sl.NodeID = sl.StoreDescriptor.Node.NodeID
							storeLoadMap[sl.StoreID] = sl
							stores = append(stores, sl.StoreID)
						case "node-id":
							nl := parseNodeLoad(t, line)
							nodeLoadMap[nl.nodeID] = nl
							nodes = append(nodes, nl.nodeID)
						case "range-id":
							rl := parseRangeLoad(t, line)
							rangeLoadMap[rl.RangeID] = rl
							ranges = append(ranges, rl.RangeID)
						}
					}

					for _, nodeID := range nodes {
						require.NoError(t, a.cs.processNodeLoadMsg(nodeLoadMap[nodeID]))
					}

					for _, storeID := range stores {
						sl := storeLoadMap[storeID]
						storeMsg := storeLoadMsg{
							StoreID: storeID,
							load:    sl.reportedLoad,
							// TODO(kvoli): Fill in the store ranges or leave blank.
							storeRanges:          []storeRange{},
							capacity:             sl.capacity,
							secondaryLoad:        sl.reportedSecondaryLoad,
							topKRanges:           []rangeLoad{},
							meanNonTopKRangeLoad: rangeLoad{},
						}
						for _, rangeID := range ranges {
							rLoad := rangeLoadMap[rangeID]
							leaseStore := rangeMap[rangeID][0]
							if leaseStore == storeID {
								storeMsg.topKRanges = append(storeMsg.topKRanges, rLoad)
							}
						}
						require.NoError(t, a.cs.processStoreLoadMsg(time.Time{}, storeMsg))
					}
					return ""
				case "span-config":
					var rangeID int
					d.ScanArgs(t, "range-id", &rangeID)
					conf := parseSpanConfig(t, d)
					rangeConfigMap[roachpb.RangeID(rangeID)] = conf
					return ""
				case "range":
					var ranges []roachpb.RangeID
					for _, next := range strings.Split(d.Input, "\n") {
						rangeID, voters := parseRange(t, next)
						rangeMap[rangeID] = voters
						ranges = append(ranges, rangeID)
					}

					leaseStoreMsgs := map[roachpb.StoreID]*storeLeaseholderMsg{}
					stores := []roachpb.StoreID{}
					for _, rangeID := range ranges {
						conf, ok := rangeConfigMap[rangeID]
						if !ok {
							rangeConfigMap[rangeID] = conf
							conf = roachpb.TestingDefaultSpanConfig()
						}
						voters := rangeMap[rangeID]
						leaseStore := voters[0]
						leaseStoreMsg, ok := leaseStoreMsgs[leaseStore]
						if !ok {
							leaseStoreMsg = &storeLeaseholderMsg{StoreID: leaseStore}
							leaseStoreMsgs[leaseStore] = leaseStoreMsg
							stores = append(stores, leaseStore)
						}
						rMsg := rangeMsg{
							RangeID:  rangeID,
							start:    []byte(fmt.Sprintf("%v", rangeID)),
							end:      []byte(fmt.Sprintf("%v", rangeID+1)),
							replicas: []storeIDAndReplicaState{},
							conf:     conf,
						}
						for _, voter := range voters {
							rMsg.replicas = append(rMsg.replicas,
								storeIDAndReplicaState{
									StoreID: voter,
									replicaState: replicaState{
										replicaIDAndType: replicaIDAndType{
											ReplicaID:   roachpb.ReplicaID(voter),
											replicaType: replicaType{isLeaseholder: voter == leaseStore},
										},
									},
								})
						}
						leaseStoreMsg.ranges = append(leaseStoreMsg.ranges, rMsg)
					}
					for _, storeID := range stores {
						require.NoError(t, a.cs.processStoreLeaseMsg(time.Time{}, *leaseStoreMsgs[storeID]))
					}
					return fmt.Sprintf("%v", rangeMap)
				case "rebalance-lease":
					var storeID, rangeID int
					d.ScanArgs(t, "store-id", &storeID)
					d.ScanArgs(t, "range-id", &rangeID)

					stores := []roachpb.StoreID{}
					for _, store := range a.cs.stores {
						stores = append(stores, store.StoreID)
					}
					pl := makeStoreIDPostingList(stores)
					target, err := a.rebalanceLeaseTarget(ctx, roachpb.StoreID(storeID), rangeLoadMap[roachpb.RangeID(rangeID)], pl)
					if err != nil {
						return fmt.Sprintf("err: %v", err)
					}
					return fmt.Sprintf("target=%v", target)
				case "rebalance-replica":
					var storeID, rangeID int
					d.ScanArgs(t, "store-id", &storeID)
					d.ScanArgs(t, "range-id", &rangeID)
					stores := []roachpb.StoreID{}
					for _, store := range a.cs.stores {
						stores = append(stores, store.StoreID)
					}
					pl := makeStoreIDPostingList(stores)
					target, err := a.rebalanceReplicaTarget(ctx, roachpb.StoreID(storeID), rangeLoadMap[roachpb.RangeID(rangeID)], pl)
					if err != nil {
						return fmt.Sprintf("err: %v", err)
					}
					return fmt.Sprintf("target=%v", target)
				case "move-range":
					var storeID, rangeID int
					d.ScanArgs(t, "store-id", &storeID)
					d.ScanArgs(t, "range-id", &rangeID)
					stores := []roachpb.StoreID{}
					for _, store := range a.cs.stores {
						stores = append(stores, store.StoreID)
					}
					pl := makeStoreIDPostingList(stores)
					changes := []*pendingReplicaChange{}
					a.tryMovingRange(ctx, roachpb.StoreID(storeID), rangeLoadMap[roachpb.RangeID(rangeID)], pl, &changes)
					if len(changes) == 0 {
						return "no changes"
					}

					var buf strings.Builder
					buf.WriteString("changes\n")
					for i := range changes {
						fmt.Fprintf(&buf, "  %+v\n", changes[i])
					}
					return buf.String()
				case "rebalance-stores":
					changes := a.rebalanceStores(ctx)
					if len(changes) == 0 {
						return "no changes"
					}
					var buf strings.Builder
					buf.WriteString("changes\n")
					for i := range changes {
						fmt.Fprintf(&buf, "  %+v\n", changes[i])
					}
					return buf.String()
				default:
					return fmt.Sprintf("unknown command %s", d.Cmd)
				}
			})
	})
}

// TODO(kvoli):
// - diversity scoring bug
// - implement RebalanceStores
//   - RebalanceRange
//     - RebalanceRangeLease
//     - RebalanceRangeReplica
// - process NodeLoadResponse
//
// rebalanceStores -> []*pendingReplicaChange
//
// store store-id=... attrs=... locality-tiers=...
// store-load store-id=... load=(...) capacity=(...) secondary-load=(...)
// node-load node-id=... cpu-load=... cpu-capacity=...
//
//     node id=1 locality=...
// add
//     store id=1 node=1 attrs=...
//     range id=1 store=1 start=a end=b config=...
//         replica id=1 store=1 leaseholder=true type=voter paused=false lagging=false
//         replica id=2 store=2 leaseholder=false type=voter paused=false lagging=false
//     ...
// msg
//     node=1 seq=1 cpu=4 cpu_capacity=8
//         store=1 load=(cpu,writeBandwidth,byteSize,leaseCount) capacty=(...)
//             range=1 load=(...)     raft_cpu=1 type=voter
//             range=n load=(1,2,3,4) raft_cpu=1 type=voter
//         store=2 load=(1,2,3,4) capacity=(1,2,3,4)
//             range=1 load=(...)     raft_cpu=1 type=voter
//             range=n load=(...)     raft_cpu=1 type=voter
//             ...
//     node=2 seq=1 cpu=4 cpu_capacity=...
//         store=...
//             range=...
//
