// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaintegration

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/mmaprototype"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

// computeStoreByteSizeCapacityWithOverhead uses (Total-Available)/Total as the
// disk fraction instead of Used/(Used+Available). This is the wrong model
// because Total-Available includes filesystem reserved blocks, ballast, and
// other non-store files on the same mount — all of which inflate the fraction
// and produce an unrealistically small capacity for stores with little data.
func computeStoreByteSizeCapacityWrong(
	logicalBytes mmaprototype.LoadValue, total int64, available int64,
) mmaprototype.LoadValue {
	var fullDiskFraction float64
	if total > 0 {
		fullDiskFraction = float64(total-available) / float64(total)
	}
	return computeStoreByteSizeCapacity(logicalBytes, fullDiskFraction, available)
}

func TestComputeStoreCPURateCapacity(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// All CPU inputs are in cores (1 core = 1e9 ns/s).
	const nsPerCore = 1e9

	datadriven.RunTest(t, datapathutils.TestDataPath(t, t.Name()),
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "scenario":
				// Ground-truth inputs describing what's ACTUALLY happening.
				var kvCPUCores float64      // Total KV CPU across all stores.
				var propOverhead float64    // CPU that scales with KV (dist SQL, RPC, compactions, etc.).
				var backgroundCores float64 // CPU that does NOT scale with KV (gateway SQL, GC, jobs, etc.).
				var nodeCapCores float64    // Node's total CPU capacity.
				var numStores int

				// SQL measurements available to sql_model (subsets of the above).
				var sqlGatewayCores float64 // Measured subset of background.
				var sqlDistCores float64    // Measured subset of proportional-overhead.

				d.ScanArgs(t, "kv-cpu", &kvCPUCores)
				d.ScanArgs(t, "proportional-overhead", &propOverhead)
				d.ScanArgs(t, "background", &backgroundCores)
				d.ScanArgs(t, "node-cpu-capacity", &nodeCapCores)
				d.ScanArgs(t, "num-stores", &numStores)
				d.ScanArgs(t, "sql-gateway-cpu", &sqlGatewayCores)
				d.ScanArgs(t, "sql-dist-cpu", &sqlDistCores)

				require.Greater(t, numStores, 0)
				require.Greater(t, nodeCapCores, 0.0)
				require.GreaterOrEqual(t, kvCPUCores, 0.0)
				require.GreaterOrEqual(t, propOverhead, 0.0)
				require.GreaterOrEqual(t, backgroundCores, 0.0)
				require.GreaterOrEqual(t, sqlGatewayCores, 0.0)
				require.GreaterOrEqual(t, sqlDistCores, 0.0)
				require.LessOrEqual(t, sqlGatewayCores, backgroundCores)
				require.LessOrEqual(t, sqlDistCores, propOverhead)

				// Derive node CPU usage from ground truth.
				nodeUsageCores := kvCPUCores + propOverhead + backgroundCores

				// Compute true capacity from ground truth.
				// true-mult = (kv + overhead) / kv: how much total CPU per unit of KV.
				// true-capacity = (nodeCapacity - background) / true-mult / numStores.
				var trueMult float64
				if kvCPUCores > 0 {
					trueMult = (kvCPUCores + propOverhead) / kvCPUCores
				} else {
					// No KV work → proportional overhead must also be 0.
					require.Equal(t, 0.0, propOverhead)
					trueMult = 1.0
				}
				available := max(0.0, nodeCapCores-backgroundCores)
				trueCapPerStore := available / trueMult / float64(numStores)

				// Build model inputs (what models actually see).
				in := storeCPURateCapacityInput{
					storesCPURate:       kvCPUCores * nsPerCore,
					nodeCPURateUsage:    nodeUsageCores * nsPerCore,
					nodeCPURateCapacity: nodeCapCores * nsPerCore,
					numStores:           int32(numStores),
				}

				// Run all three models.
				naiveCap := float64(computeStoreCPURateCapacity(in)) / nsPerCore
				cappedCap := computeCPUCapacityWithCap(in, func(_, _, _, _ float64) {}) / nsPerCore

				inSQL := in
				inSQL.sqlGatewayCPUNanoPerSec = sqlGatewayCores * nsPerCore
				inSQL.sqlDistCPUNanoPerSec = sqlDistCores * nsPerCore
				sqlCap := computeStoreCPURateCapacityWithSQL(inSQL, func(_, _, _ float64) {}) / nsPerCore

				// Format helpers.
				fmtError := func(modelCap float64) string {
					if trueCapPerStore <= 0 {
						return "   N/A"
					}
					pctErr := (modelCap - trueCapPerStore) / trueCapPerStore * 100
					// Round near-zero to avoid "-0.0%".
					if pctErr > -0.05 && pctErr < 0.05 {
						pctErr = 0
					}
					return fmt.Sprintf("%6.1f%%", pctErr)
				}

				// Build output.
				return fmt.Sprintf(
					"              node-cpu: %.2f = kv %.2f + overhead %.2f + background %.2f\n"+
						"correct:      capacity %6.2f\n"+
						"naive:        capacity %6.2f, capacity_err %s\n"+
						"capped_mult:  capacity %6.2f, capacity_err %s\n"+
						"sql_model:    capacity %6.2f, capacity_err %s\n",
					nodeUsageCores, kvCPUCores, propOverhead, backgroundCores,
					trueCapPerStore,
					naiveCap, fmtError(naiveCap),
					cappedCap, fmtError(cappedCap),
					sqlCap, fmtError(sqlCap))

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}

func TestComputeStoreByteSizeCapacity(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	datadriven.RunTest(t, datapathutils.TestDataPath(t, t.Name()),
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "compute":
				var logicalBytesStr, totalStr, usedStr, availableStr string

				d.ScanArgs(t, "logical-bytes", &logicalBytesStr)
				d.ScanArgs(t, "total", &totalStr)
				d.ScanArgs(t, "used", &usedStr)
				d.ScanArgs(t, "available", &availableStr)

				logicalBytes, err := humanizeutil.ParseBytes(logicalBytesStr)
				require.NoError(t, err)
				total, err := humanizeutil.ParseBytes(totalStr)
				require.NoError(t, err)
				used, err := humanizeutil.ParseBytes(usedStr)
				require.NoError(t, err)
				available, err := humanizeutil.ParseBytes(availableStr)
				require.NoError(t, err)

				sc := roachpb.StoreCapacity{
					Capacity:  total,
					Used:      used,
					Available: available,
				}
				fractionUsed := sc.FractionUsed()

				// Current model: uses FractionUsed() = Used/(Used+Available).
				result := computeStoreByteSizeCapacity(
					mmaprototype.LoadValue(logicalBytes), fractionUsed, available,
				)
				// Wrong model: uses (Total-Available)/Total.
				wrongResult := computeStoreByteSizeCapacityWrong(
					mmaprototype.LoadValue(logicalBytes), total, available,
				)

				fmtUtil := func(load int64, cap mmaprototype.LoadValue) string {
					if cap > 0 {
						return fmt.Sprintf("%.2f%%", float64(load)/float64(cap)*100)
					}
					return "N/A"
				}
				return fmt.Sprintf(
					"fraction-used: %.4f (via Used) vs %.4f (via Total-Available)\nkv-capacity: %s (kv-util: %s, available: %s)\nkv-capacity(wrong): %s (kv-util: %s, available: %s)\n",
					fractionUsed, float64(total-available)/float64(total),
					humanizeutil.IBytes(int64(result)), fmtUtil(logicalBytes, result), humanizeutil.IBytes(available),
					humanizeutil.IBytes(int64(wrongResult)), fmtUtil(logicalBytes, wrongResult), humanizeutil.IBytes(available),
				)

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}
