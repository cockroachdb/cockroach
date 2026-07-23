// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaintegration

import (
	"fmt"
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/mmaprototype"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
)

func TestComputePhysicalStore(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	datadriven.RunTest(t, datapathutils.TestDataPath(t, t.Name()),
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "compute":
				var numStores int
				var storesCPUCores, nodeUsageCores, nodeCapCores float64
				d.ScanArgs(t, "num-stores", &numStores)
				d.ScanArgs(t, "stores-cpu", &storesCPUCores)
				d.ScanArgs(t, "node-cpu-usage", &nodeUsageCores)
				d.ScanArgs(t, "node-cpu-capacity", &nodeCapCores)

				var logicalGiB, usedGiB, availableGiB float64
				d.ScanArgs(t, "logical-gib", &logicalGiB)
				d.ScanArgs(t, "used-gib", &usedGiB)
				d.ScanArgs(t, "available-gib", &availableGiB)

				var writeBPS float64
				d.ScanArgs(t, "write-bytes-per-sec", &writeBPS)

				var storeCPUCores float64
				if d.HasArg("store-cpu-per-second") {
					d.ScanArgs(t, "store-cpu-per-second", &storeCPUCores)
				}

				var sqlGatewayCPUCores, sqlDistCPUCores float64
				if d.HasArg("sql-gateway-cpu") {
					d.ScanArgs(t, "sql-gateway-cpu", &sqlGatewayCPUCores)
				}
				if d.HasArg("sql-dist-cpu") {
					d.ScanArgs(t, "sql-dist-cpu", &sqlDistCPUCores)
				}

				const nsPerCore = 1e9
				const gib = 1 << 30

				desc := roachpb.StoreDescriptor{
					NodeCapacity: roachpb.NodeCapacity{
						NumStores:               int32(numStores),
						StoresCPURate:           int64(storesCPUCores * nsPerCore),
						NodeCPURateUsage:        int64(nodeUsageCores * nsPerCore),
						NodeCPURateCapacity:     int64(nodeCapCores * nsPerCore),
						SQLGatewayCPUNanoPerSec: int64(sqlGatewayCPUCores * nsPerCore),
						SQLDistCPUNanoPerSec:    int64(sqlDistCPUCores * nsPerCore),
					},
					Capacity: roachpb.StoreCapacity{
						CPUPerSecond:        storeCPUCores * nsPerCore,
						LogicalBytes:        int64(logicalGiB * gib),
						Used:                int64(usedGiB * gib),
						Available:           int64(availableGiB * gib),
						WriteBytesPerSecond: writeBPS,
					},
				}

				res := computePhysicalStore(desc)
				amp := ComputeAmplificationFactors(desc)

				fmtCores := func(v mmaprototype.LoadValue) string {
					return fmt.Sprintf("%.2f cores", float64(v)/nsPerCore)
				}
				fmtIBytes := func(v mmaprototype.LoadValue) string {
					b := math.Abs(float64(v))
					sign := ""
					if v < 0 {
						sign = "-"
					}
					switch {
					case b >= 1<<30:
						return fmt.Sprintf("%s%.2f GiB", sign, b/float64(1<<30))
					case b >= 1<<20:
						return fmt.Sprintf("%s%.2f MiB", sign, b/float64(1<<20))
					case b >= 1<<10:
						return fmt.Sprintf("%s%.2f KiB", sign, b/float64(1<<10))
					default:
						return fmt.Sprintf("%s%.2f B", sign, b)
					}
				}
				fmtBytes := func(v mmaprototype.LoadValue) string {
					return fmtIBytes(v)
				}
				fmtBytesPerSec := func(v mmaprototype.LoadValue) string {
					return fmtIBytes(v) + "/s"
				}
				fmtCap := func(v mmaprototype.LoadValue, fmtFn func(mmaprototype.LoadValue) string) string {
					if v == mmaprototype.UnknownCapacity {
						return "unknown"
					}
					return fmtFn(v)
				}

				return fmt.Sprintf(
					"cpu:             load=%s  capacity=%s  amp=%.4f\n"+
						"disk:            load=%s  capacity=%s  amp=%.4f\n"+
						"write-bandwidth: load=%s  capacity=%s  amp=%.4f\n"+
						"amplification-factors: [cpu=%.4f, disk=%.4f, write-bw=%.4f]\n",
					fmtCores(res.load[mmaprototype.CPURate]),
					fmtCap(res.capacity[mmaprototype.CPURate], fmtCores),
					res.amplificationFactors[mmaprototype.CPURate],
					fmtBytes(res.load[mmaprototype.ByteSize]),
					fmtCap(res.capacity[mmaprototype.ByteSize], fmtBytes),
					res.amplificationFactors[mmaprototype.ByteSize],
					fmtBytesPerSec(res.load[mmaprototype.WriteBandwidth]),
					fmtCap(res.capacity[mmaprototype.WriteBandwidth], fmtBytesPerSec),
					res.amplificationFactors[mmaprototype.WriteBandwidth],
					amp[mmaprototype.CPURate],
					amp[mmaprototype.ByteSize],
					amp[mmaprototype.WriteBandwidth],
				)

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}
