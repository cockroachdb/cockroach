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
			case "compute":
				var storeCPUCores float64
				var nodeUsageCores, nodeCapCores float64
				var storesCPUCores float64
				var numStores int

				d.ScanArgs(t, "store-load", &storeCPUCores)
				d.ScanArgs(t, "node-cpu-usage", &nodeUsageCores)
				d.ScanArgs(t, "node-cpu-capacity", &nodeCapCores)
				d.ScanArgs(t, "total-stores-cpu-usage", &storesCPUCores)
				d.ScanArgs(t, "num-stores", &numStores)

				result := computeStoreCPURateCapacity(
					mmaprototype.LoadValue(storeCPUCores*nsPerCore),
					nodeUsageCores*nsPerCore,
					nodeCapCores*nsPerCore,
					storesCPUCores*nsPerCore,
					int32(numStores),
				)
				capacityCores := float64(result) / nsPerCore
				fairShareCores := nodeCapCores / float64(numStores)

				var storeUtil string
				if result > 0 {
					storeUtil = fmt.Sprintf("%.2f%%", storeCPUCores/capacityCores*100)
				} else {
					storeUtil = "N/A"
				}
				return fmt.Sprintf(
					"kv-capacity: %.2f cores (total: %.2f cores)\nkv-util: %s (%.2f/%.2f cores)\n",
					capacityCores, fairShareCores,
					storeUtil, storeCPUCores, capacityCores,
				)

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
