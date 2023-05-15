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
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

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
		a := newAllocatorState(&manualTestClock{})
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
						require.NoError(t, a.cs.setStore(desc))
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
						require.NoError(t, a.cs.processStoreLoadMsg(time.Time{}, storeMsg, 1))
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
// - cluster state
//   - update to new interface
//   - testing
// - diversity scoring bug
// - rebalanceStores
//   - fill in
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
