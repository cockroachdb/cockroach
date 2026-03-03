// Copyright 2025 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func TestComputeStoreCPURateCapacity(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// All CPU inputs are in cores (1 core = 1e9 ns/s).
	const nsPerCore = 1e9

	// Accumulators for mean |capacity_err| across scenarios.
	var naiveAbsErrs, cappedAbsErrs []float64

	datadriven.RunTest(t, datapathutils.TestDataPath(t, t.Name()),
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "scenario":
				// Ground-truth inputs: the true breakdown of CPU on the node.
				var kvCPUCores float64   // direct KV replica CPU (= storesCPURate)
				var propOverhead float64 // CPU proportional to KV (DistSQL, RPC, compactions)
				var bgCores float64      // CPU independent of KV (gateway SQL, GC, jobs)
				var nodeCapCores float64 // total CPU capacity of the node
				var numStores int        // number of stores on the node

				d.ScanArgs(t, "kv-cpu", &kvCPUCores)
				d.ScanArgs(t, "proportional-overhead", &propOverhead)
				d.ScanArgs(t, "background", &bgCores)
				d.ScanArgs(t, "node-cpu-capacity", &nodeCapCores)
				d.ScanArgs(t, "num-stores", &numStores)

				// Input validations.
				require.Greater(t, numStores, 0, "num-stores must be >= 1")
				require.Greater(t, nodeCapCores, 0.0, "node-cpu-capacity must be > 0")
				require.GreaterOrEqual(t, kvCPUCores, 0.0, "kv-cpu must be >= 0")
				require.GreaterOrEqual(t, propOverhead, 0.0, "proportional-overhead must be >= 0")
				require.GreaterOrEqual(t, bgCores, 0.0, "background must be >= 0")

				// Derive node usage from ground truth.
				nodeUsageCores := kvCPUCores + propOverhead + bgCores

				// Compute true capacity from ground truth.
				// true-mult is the ratio of total KV-proportional CPU to direct KV CPU.
				// true-capacity = (node-capacity - background) / true-mult / num-stores
				var trueMult float64
				if kvCPUCores == 0 {
					trueMult = 1.0 // no KV work, assume multiplier of 1
				} else {
					trueMult = (kvCPUCores + propOverhead) / kvCPUCores
				}
				trueCapPerStore := max(0, (nodeCapCores-bgCores)/trueMult/float64(numStores))

				// Build model input (models only see aggregate metrics, not the
				// ground-truth breakdown).
				in := storeCPURateCapacityInput{
					storesCPURate:       kvCPUCores * nsPerCore,
					nodeCPURateUsage:    nodeUsageCores * nsPerCore,
					nodeCPURateCapacity: nodeCapCores * nsPerCore,
					numStores:           int32(numStores),
				}

				// Run models.
				naiveCap := float64(computeStoreCPURateCapacityNaive(in)) / nsPerCore
				cappedCap := computeCPUCapacityWithCap(in) / nsPerCore
				physDesc := roachpb.StoreDescriptor{
					NodeCapacity: roachpb.NodeCapacity{
						NumStores:           int32(numStores),
						StoresCPURate:       int64(kvCPUCores * nsPerCore),
						NodeCPURateUsage:    int64(nodeUsageCores * nsPerCore),
						NodeCPURateCapacity: int64(nodeCapCores * nsPerCore),
					},
				}
				physResult := computePhysicalCPU(physDesc)
				physLoad := float64(physResult.load) / nsPerCore
				physCap := float64(physResult.capacity) / nsPerCore

				// Compute error percentage.
				// Negative = pessimistic (underestimates capacity, safer).
				// Positive = optimistic (overestimates capacity, dangerous).
				errPct := func(modelCap float64) (pct float64, inf bool) {
					if trueCapPerStore == 0 {
						if modelCap == 0 {
							return 0, false
						}
						return 0, true // +Inf
					}
					return (modelCap - trueCapPerStore) / trueCapPerStore * 100, false
				}
				fmtErr := func(modelCap float64) string {
					pct, inf := errPct(modelCap)
					if inf {
						return "+Inf%"
					}
					if math.Abs(pct) < 0.005 {
						return "0.00%"
					}
					return fmt.Sprintf("%+.2f%%", pct)
				}

				// Accumulate |err| for mean calculation (skip +Inf).
				if naivePct, inf := errPct(naiveCap); !inf {
					naiveAbsErrs = append(naiveAbsErrs, math.Abs(naivePct))
				}
				if cappedPct, inf := errPct(cappedCap); !inf {
					cappedAbsErrs = append(cappedAbsErrs, math.Abs(cappedPct))
				}

				return fmt.Sprintf(
					"node-cpu: %.2f used / %.2f capacity (%.2f kv + %.2f proportional + %.2f background)\n"+
						"truth:    kv-capacity: %.2f cores/store (true-mult: %.2f)\n"+
						"naive:    kv-capacity: %.2f cores/store, capacity_err: %s\n"+
						"capped:   kv-capacity: %.2f cores/store, capacity_err: %s\n"+
						"physical: load: %.2f capacity: %.2f (util: %.2f%%, amp-factor: %.2f)\n",
					nodeUsageCores, nodeCapCores, kvCPUCores, propOverhead, bgCores,
					trueCapPerStore, trueMult,
					naiveCap, fmtErr(naiveCap),
					cappedCap, fmtErr(cappedCap),
					physLoad, physCap, physLoad/physCap*100, float64(physResult.amplificationFactor),
				)

			case "mean":
				meanOf := func(vals []float64) float64 {
					if len(vals) == 0 {
						return 0
					}
					sum := 0.0
					for _, v := range vals {
						sum += v
					}
					return sum / float64(len(vals))
				}
				return fmt.Sprintf(
					"Mean |capacity_err| across %d scenarios: naive: %.1f%%  capped: %.1f%%\n",
					len(naiveAbsErrs), meanOf(naiveAbsErrs), meanOf(cappedAbsErrs),
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

				// Legacy model: uses FractionUsed() = Used/(Used+Available).
				result := computeStoreByteSizeCapacity(
					mmaprototype.LoadValue(logicalBytes), fractionUsed, available,
				)
				// Wrong model: uses (Total-Available)/Total.
				wrongResult := computeStoreByteSizeCapacityNaive(
					mmaprototype.LoadValue(logicalBytes), total, available,
				)

				// Physical model: load=Used, capacity=Used+Available.
				physDesc := roachpb.StoreDescriptor{
					Capacity: roachpb.StoreCapacity{
						LogicalBytes: logicalBytes,
						Used:         used,
						Available:    available,
					},
				}
				physResult := computePhysicalDisk(physDesc)

				fmtUtil := func(load int64, cap mmaprototype.LoadValue) string {
					if cap > 0 {
						return fmt.Sprintf("%.2f%%", float64(load)/float64(cap)*100)
					}
					return "N/A"
				}
				return fmt.Sprintf(
					"fraction-used: %.4f (via Used) vs %.4f (via Total-Available)\n"+
						"legacy-capacity: %s (kv-util: %s, available: %s)\n"+
						"legacy-capacity(wrong): %s (kv-util: %s, available: %s)\n"+
						"physical: load=%s capacity=%s (util: %s, amp-factor: %.2f)\n",
					fractionUsed, float64(total-available)/float64(total),
					humanizeutil.IBytes(int64(result)), fmtUtil(logicalBytes, result), humanizeutil.IBytes(available),
					humanizeutil.IBytes(int64(wrongResult)), fmtUtil(logicalBytes, wrongResult), humanizeutil.IBytes(available),
					humanizeutil.IBytes(int64(physResult.load)),
					humanizeutil.IBytes(int64(physResult.capacity)),
					fmtUtil(int64(physResult.load), physResult.capacity),
					float64(physResult.amplificationFactor),
				)

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}
