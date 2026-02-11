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

	fmtUtil := func(load, cap float64) string {
		return fmt.Sprintf("%.2f%%", load/cap*100)
	}

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

				totalPerStore := nodeCapCores / float64(numStores)

				// Naive model: assumes all node CPU scales directly with store work.
				naiveResult := computeStoreCPURateCapacity(
					mmaprototype.LoadValue(storeCPUCores*nsPerCore),
					nodeUsageCores*nsPerCore,
					nodeCapCores*nsPerCore,
					storesCPUCores*nsPerCore,
					int32(numStores),
				)
				naiveCap := float64(naiveResult) / nsPerCore

				// Capped model: caps the indirect overhead multiplier.
				var cappedMult, bgLoad, mmaShare, mmaDirect float64
				cappedCapNs := computeCPUCapacityWithCap(
					mmaprototype.LoadValue(storeCPUCores*nsPerCore),
					storesCPUCores*nsPerCore,
					nodeUsageCores*nsPerCore,
					nodeCapCores*nsPerCore,
					int32(numStores),
					func(mult, backgroundLoad, mmaShareOfCapacity, mmaDirectCapacity float64) {
						cappedMult = mult
						bgLoad = backgroundLoad / nsPerCore
						mmaShare = mmaShareOfCapacity / nsPerCore
						mmaDirect = mmaDirectCapacity / nsPerCore
					},
				)
				cappedCap := cappedCapNs / nsPerCore

				var cappedSteps string
				if numStores <= 0 {
					cappedSteps = fmt.Sprintf(
						"capped_mult: (early return: num-stores=%d)\n"+
							"  kv-capacity: %.2f cores, kv-util: %s (%.2f/%.2f cores)\n",
						numStores,
						cappedCap, fmtUtil(storeCPUCores, cappedCap), storeCPUCores, cappedCap,
					)
				} else if nodeCapCores <= 0 {
					cappedSteps = fmt.Sprintf(
						"capped_mult: (node-cpu-capacity unknown, assuming 50%% util)\n"+
							"  kv-capacity: %.2f cores, kv-util: %s (%.2f/%.2f cores)\n",
						cappedCap, fmtUtil(storeCPUCores, cappedCap), storeCPUCores, cappedCap,
					)
				} else {
					cappedSteps = fmt.Sprintf(
						"capped_mult: (steps below)\n"+
							"  mult        = max(1, min(%.2f/%.2f, %.0f)) = %.1f\n"+
							"  background  = max(0, %.2f - %.2f*%.1f) = %.2f cores\n"+
							"  mma-share   = %.2f - %.2f = %.2f cores\n"+
							"  mma-direct  = %.2f / %.1f = %.2f cores\n"+
							"  per-store   = %.2f / %d = %.2f cores\n"+
							"  kv-capacity: %.2f cores, kv-util: %s (%.2f/%.2f cores)\n",
						nodeUsageCores, storesCPUCores, cpuIndirectOverheadMultiplier, cappedMult,
						nodeUsageCores, storesCPUCores, cappedMult, bgLoad,
						nodeCapCores, bgLoad, mmaShare,
						mmaShare, cappedMult, mmaDirect,
						mmaDirect, numStores, cappedCap,
						cappedCap, fmtUtil(storeCPUCores, cappedCap), storeCPUCores, cappedCap,
					)
				}

				return fmt.Sprintf(
					"              node-cpu-capacity/num-stores: %.2f cores/store\n"+
						"naive:        kv-capacity: %.2f cores, kv-util: %s (%.2f/%.2f cores)\n"+
						"%s",
					totalPerStore,
					naiveCap, fmtUtil(storeCPUCores, naiveCap), storeCPUCores, naiveCap,
					cappedSteps,
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
