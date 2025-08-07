// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaintegration

import (
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/mmaprototype"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/datadriven"
)

// TestDataDrivenStoreLoadMsg is a data-driven test for the store load functionality.
// It provides the following commands:
//
//   - "make-store-load-msg"
//     Creates a StoreLoadMsg from a StoreDescriptor.
//     Args: store_id=<int> cpu_per_second=<float> write_bytes_per_second=<int>
//     logical_bytes=<int> capacity=<int> available=<int> lease_count=<int> range_count=<int>
//     node_cpu_rate_capacity=<int> node_cpu_rate_usage=<int> stores_cpu_rate=<int> num_stores=<int>
//     timestamp=<int64>
func TestDataDrivenStoreLoadMsg(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const nodeID = 1

	datadriven.Walk(t, datapathutils.TestDataPath(t, "storeload"), func(t *testing.T, path string) {
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "make-store-load-msg":
				var storeID, writeBytesPerSecond, logicalBytes, capacity, available, leaseCount, rangeCount int
				var nodeCPURateCapacity, nodeCPURateUsage, storesCPURate, numStores, timestamp int
				var cpuPerSecond float64

				d.ScanArgs(t, "store_id", &storeID)
				d.ScanArgs(t, "cpu_per_second", &cpuPerSecond)
				d.ScanArgs(t, "write_bytes_per_second", &writeBytesPerSecond)
				d.ScanArgs(t, "logical_bytes", &logicalBytes)
				d.ScanArgs(t, "capacity", &capacity)
				d.ScanArgs(t, "available", &available)
				d.ScanArgs(t, "lease_count", &leaseCount)
				d.ScanArgs(t, "range_count", &rangeCount)
				d.ScanArgs(t, "node_cpu_rate_capacity", &nodeCPURateCapacity)
				d.ScanArgs(t, "node_cpu_rate_usage", &nodeCPURateUsage)
				d.ScanArgs(t, "stores_cpu_rate", &storesCPURate)
				d.ScanArgs(t, "num_stores", &numStores)
				d.ScanArgs(t, "timestamp", &timestamp)

				desc := roachpb.StoreDescriptor{
					StoreID: roachpb.StoreID(storeID),
					Node: roachpb.NodeDescriptor{
						NodeID: roachpb.NodeID(nodeID),
					},
					Capacity: roachpb.StoreCapacity{
						CPUPerSecond:        cpuPerSecond,
						WriteBytesPerSecond: float64(writeBytesPerSecond),
						LogicalBytes:        int64(logicalBytes),
						Capacity:            int64(capacity),
						Available:           int64(available),
						LeaseCount:          int32(leaseCount),
						RangeCount:          int32(rangeCount),
					},
					NodeCapacity: roachpb.NodeCapacity{
						NodeCPURateCapacity: int64(nodeCPURateCapacity),
						NodeCPURateUsage:    int64(nodeCPURateUsage),
						StoresCPURate:       int64(storesCPURate),
						NumStores:           int32(numStores),
					},
				}

				msg := MakeStoreLoadMsg(desc, int64(timestamp))
				return formatStoreLoadMsg(msg)
			default:
				d.Fatalf(t, "unknown command: %s", d.Cmd)
				return ""
			}
		})
	})
}

// formatStoreLoadMsg formats a StoreLoadMsg for output.
func formatStoreLoadMsg(msg mmaprototype.StoreLoadMsg) string {
	var sb strings.Builder
	_, _ = fmt.Fprintf(&sb, "s%d,n%d at %d:\n", msg.StoreID, msg.NodeID, msg.LoadTime.UnixNano())
	_, _ = fmt.Fprintf(&sb, "load[cpu]=%v\n", msg.Load[mmaprototype.CPURate])
	_, _ = fmt.Fprintf(&sb, "load[write]=%v\n", msg.Load[mmaprototype.WriteBandwidth])
	_, _ = fmt.Fprintf(&sb, "load[byte]=%v\n", msg.Load[mmaprototype.ByteSize])
	_, _ = fmt.Fprintf(&sb, "capacity[cpu]=%v\n", msg.Capacity[mmaprototype.CPURate])
	_, _ = fmt.Fprintf(&sb, "capacity[write]=%v\n", msg.Capacity[mmaprototype.WriteBandwidth])
	_, _ = fmt.Fprintf(&sb, "capacity[byte]=%v\n", msg.Capacity[mmaprototype.ByteSize])
	_, _ = fmt.Fprintf(&sb, "secondary_load[lease]=%v\n", msg.SecondaryLoad[mmaprototype.LeaseCount])
	_, _ = fmt.Fprintf(&sb, "secondary_load[replica]=%v\n", msg.SecondaryLoad[mmaprototype.ReplicaCount])
	return sb.String()
}
