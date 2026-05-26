// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaintegration

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/mmaprototype"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/datadriven"
)

// TestMakeStoreLoadMsg tests the end-to-end flow from StoreDescriptor through
// MakeStoreLoadMsg and ProcessStoreLoadMsg into the load summary.
func TestMakeStoreLoadMsg(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	alloc := mmaprototype.NewAllocatorState(
		timeutil.DefaultTimeSource{}, rand.New(rand.NewSource(42)),
	)

	datadriven.RunTest(t, datapathutils.TestDataPath(t, t.Name()),
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "set-store":
				var storeID, nodeID int
				d.ScanArgs(t, "store-id", &storeID)
				d.ScanArgs(t, "node-id", &nodeID)
				alloc.SetStore(mmaprototype.StoreAttributesAndLocality{
					StoreID: roachpb.StoreID(storeID),
					NodeID:  roachpb.NodeID(nodeID),
				})
				return ""

			case "make-store-load-msg":
				var storeID, nodeID, numStores int
				var cpuPerSecond int64
				var storesCPURate int64
				var nodeCPUUsage, nodeCPUCapacity int64

				d.ScanArgs(t, "store-id", &storeID)
				d.ScanArgs(t, "node-id", &nodeID)
				d.ScanArgs(t, "num-stores", &numStores)
				d.ScanArgs(t, "cpu-per-second", &cpuPerSecond)
				d.ScanArgs(t, "stores-cpu-rate", &storesCPURate)
				d.MaybeScanArgs(t, "node-cpu-usage", &nodeCPUUsage)
				d.MaybeScanArgs(t, "node-cpu-capacity", &nodeCPUCapacity)

				desc := roachpb.StoreDescriptor{
					StoreID: roachpb.StoreID(storeID),
					Node:    roachpb.NodeDescriptor{NodeID: roachpb.NodeID(nodeID)},
					Capacity: roachpb.StoreCapacity{
						CPUPerSecond: float64(cpuPerSecond),
					},
					NodeCapacity: roachpb.NodeCapacity{
						StoresCPURate:       storesCPURate,
						NumStores:           int32(numStores),
						NodeCPURateUsage:    nodeCPUUsage,
						NodeCPURateCapacity: nodeCPUCapacity,
					},
				}

				msg := MakeStoreLoadMsg(desc, 0)
				alloc.ProcessStoreLoadMsg(ctx, &msg)

				// Print the StoreLoadMsg in the same key=value format
				// parsed by parseStoreLoadMsg in cluster_state_test.go.
				return fmt.Sprintf(
					"store-id=%d node-id=%d"+
						" load=[%v,%v,%v]"+
						" capacity=[%v,%v,%v]"+
						" secondary-load=%v"+
						" load-time=0s"+
						" node-cpu-load=%v"+
						" node-cpu-capacity=%v\n",
					msg.StoreID, msg.NodeID,
					msg.Load[mmaprototype.CPURate],
					msg.Load[mmaprototype.WriteBandwidth],
					msg.Load[mmaprototype.ByteSize],
					msg.Capacity[mmaprototype.CPURate],
					msg.Capacity[mmaprototype.WriteBandwidth],
					msg.Capacity[mmaprototype.ByteSize],
					msg.SecondaryLoad[mmaprototype.LeaseCount],
					msg.NodeCPULoad,
					msg.NodeCPUCapacity,
				)

			case "load-summary":
				return alloc.LoadSummaryForAllStores(ctx)

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}
