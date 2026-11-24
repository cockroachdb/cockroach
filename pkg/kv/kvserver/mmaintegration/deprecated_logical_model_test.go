// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaintegration

import (
	"context"
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

// storeCPURateCapacityInput holds the inputs needed to compute per-store CPU
// capacity. Using a struct avoids accidentally swapping the order of parameters
// when passing them to functions.
type storeCPURateCapacityInput struct {
	// storesCPURate is the aggregated CPU usage across all stores on the node
	// (sum of per-store CPU load usage) in ns/sec.
	storesCPURate float64
	// nodeCPURateUsage is the total CPU usage of the node (from OS-level
	// metrics) in ns/sec.
	nodeCPURateUsage float64
	// nodeCPURateCapacity is the total CPU capacity of the node (from OS-level
	// metrics) in ns/sec.
	nodeCPURateCapacity float64
	// numStores is the number of stores on the node.
	numStores int32
}

// computeStoreByteSizeCapacity computes the byte size (disk) capacity for a
// store. The load is LogicalBytes (uncompressed) and the capacity is expressed
// in the same LogicalBytes-space, so that load/capacity recovers the actual
// disk utilization.
//
// The two cases are:
//
//  1. Normal: logicalBytes > 0 and diskFractionUsed >= 0.01.
//     capacity = logicalBytes / diskFractionUsed.
//     This ensures that logicalBytes/capacity equals the observed physical
//     disk utilization, encoding the space amplification factor
//     (physical/logical) into the capacity.
//
//  2. Empty/new store: logicalBytes is 0 or diskFractionUsed < 0.01.
//     We fall back to availableBytes (compressed/physical), which is
//     pessimistic since LogicalBytes are uncompressed.
//
// diskFractionUsed should be computed via StoreCapacity.FractionUsed(), which
// prefers Used/(Available+Used) to scope utilization to the store's own data.
// Available does not compensate for the ballast, so utilization will look
// slightly higher than actual. This is acceptable since the ballast is small
// (default 1% of capacity) and is for emergency use.
func computeStoreByteSizeCapacity(
	logicalBytes mmaprototype.LoadValue, diskFractionUsed float64, availableBytes int64,
) mmaprototype.LoadValue {
	almostZeroUtil := diskFractionUsed < 0.01
	if logicalBytes == 0 || almostZeroUtil {
		// Has no ranges or is almost empty. This is likely a new store. Since
		// LogicalBytes are uncompressed, we start with the compressed available,
		// which is desirably pessimistic.
		return mmaprototype.LoadValue(availableBytes)
	}
	// Normal case. The store has some ranges, and is not almost empty.
	return mmaprototype.LoadValue(float64(logicalBytes) / diskFractionUsed)
}

// cpuCapacityFloorPerStore is the minimum per-store CPU capacity (in ns/s)
// returned by computeCPUCapacityWithCap. The capacity formula is:
//
//	mult = clamp(nodeCPURateUsage / storesCPURate, 1, maxCPUAmplification)
//	backgroundLoad = nodeCPURateUsage - storesCPURate * mult
//	capacity = (nodeCPURateCapacity - backgroundLoad) / mult
//
// As background load grows (SQL gateway, GC, etc.), capacity shrinks
// toward 0 while storesCPURate stays constant, causing utilization
// (load/capacity) to spike to infinity. The floor prevents this.
//
// Example: 16-core node, 1 store, storesCPURate=2, mult clamped to
// maxCPUAmplification=3 (implicit mult exceeds cap in all rows):
//
//	bg = nodeCPURateUsage - storesCPURate*mult
//	cap = (nodeCPURateCapacity - bg) / mult
//
//	usage=10     bg=4      cap=(16-4)/3    = 4.0 cores
//	usage=14     bg=8      cap=(16-8)/3    = 2.67 cores
//	usage=16     bg=10     cap=(16-10)/3   = 2.0 cores   (node saturated)
//	usage=21.7   bg=15.7   cap=(16-15.7)/3 = 0.1 cores
//	usage=21.91  bg=15.91  cap=(16-15.91)/3= 0.03 cores  (without floor)
//	usage=22     bg=16     cap=(16-16)/3   = 0.0 cores   (without floor)
//
// The floor keeps capacity at 0.1 cores - small enough that the store
// looks nearly saturated, large enough to keep utilization finite.
//
// Note: due to flooring, severe overloads become indistinguishable (load=2 over
// a 0.1-core floor is 20x util regardless of how much background exists). This
// is acceptable because MMA has no stronger action beyond 20x.
const cpuCapacityFloorPerStore = 0.1 * 1e9 // 0.1 cores in ns/s

// computeCPUCapacityWithCap computes per-store CPU capacity using a clamped
// multiplier. Unlike the naive model (computeStoreCPURateCapacityNaive in
// legacy_model_test.go, which assumes all node CPU usage is caused by
// MMA-tracked store work), this model distinguishes between MMA-attributed
// load and background load.
//
// The formula:
//
//  1. clampedMult = clamp(nodeCPURateUsage / storesCPURate, 1, maxCPUAmplification)
//     When the implicit multiplier is low (<= cap), we assume all non-store
//     CPU is indirectly caused by store work (e.g. RPC overhead, compactions)
//     and will scale linearly with MMA's direct load.
//     When the implicit multiplier exceeds the cap, we stop attributing
//     unrelated CPU consumption (SQL gateway work, GC, background jobs) to
//     the stores.
//
//  2. backgroundLoad = nodeCPURateUsage - storesCPURate * clampedMult
//     The portion of node CPU usage that we assume is independent of
//     MMA-tracked work. This is zero when the implicit multiplier is below
//     the cap, and grows as auxiliary CPU usage increases.
//
//  3. mmaShareOfCapacity = nodeCPURateCapacity - backgroundLoad
//     The number of cores available for MMA-related work (both direct store
//     load and its indirect overhead).
//
//  4. mmaDirectCapacity = mmaShareOfCapacity / clampedMult
//     The capacity in terms of direct MMA load. Dividing by the multiplier
//     accounts for the indirect overhead: each unit of direct load consumes
//     clampedMult units of actual CPU.
//
//  5. storeCapacity = mmaDirectCapacity / numStores
//     Split evenly across stores on the node.
//
// Relationship to the naive model:
//
// The naive model (computeStoreCPURateCapacityNaive in legacy_model_test.go)
// attributes all node CPU to MMA-tracked store work. It maintains an
// invariant that lets node-level utilization be recovered from store values
// alone:
//
//	sum(store loads) / sum(store capacities) = NodeCPURateUsage / NodeCPURateCapacity
//
// That model is the special case where cap = infinity (clamping never
// activates). When clamping does activate (background > 0), this function
// mistakenly breaks the invariant. Example with one store
// (storesCPURate=2, NodeCPURateUsage=10, NodeCPURateCapacity=16, cap=3):
//
//	Naive:  capacity = 2 / (10/16) = 3.2   load/cap = 2/3.2 = 0.625 = 10/16  (matches)
//	Capped: capacity = (16 - 4) / 3 = 4    load/cap = 2/4   = 0.5   != 10/16 (breaks)
//
// The capped model reports lower store utilization because it no longer
// charges stores for background CPU they do not control. The trade-off is
// that node-level overload detection cannot sum store values to recover
// physical utilization. MakeStoreLoadMsg addresses this by passing
// physical node-level CPU (NodeCPURateUsage, NodeCPURateCapacity) through
// StoreLoadMsg so that node-level overload detection can use them directly.
//
// When no clamping occurs (implicit mult in [1, cap]), backgroundLoad = 0
// and this function is equivalent to the naive model.
//
// When storesCPURate is 0 (no replicas reporting CPU yet), we use the limiting
// behavior: MMA attributes none of the node usage to itself (all is
// background), and divides the idle capacity by the multiplier across stores.
func computeCPUCapacityWithCap(in storeCPURateCapacityInput) (capacity float64) {
	defer func() {
		capacity = max(cpuCapacityFloorPerStore, capacity)
	}()

	mult := 0.0
	if in.numStores <= 0 || in.nodeCPURateCapacity <= 0 {
		log.KvDistribution.Fatalf(context.Background(),
			"numStores and nodeCPURateCapacity must be > 0")
	}

	if in.storesCPURate <= 0 {
		// When MMA has zero load, the implicit multiplier is infinite, so we
		// use the cap. MMA attributes 0 * cap = 0 of the node usage to itself,
		// meaning all node usage is "background". MMA gets the remaining idle
		// capacity, scaled down by the multiplier.
		mult = maxCPUAmplification
	} else {
		// Compute the implicit multiplier and clamp it to [1, cap].
		// - Clamping from above prevents pathological behavior when MMA tracks
		//   little load but the node has high CPU usage from other sources.
		// - Clamping from below (at 1) handles the case where MMA load exceeds
		//   node usage (shouldn't happen, but can due to measurement lag).
		//   Without this, MMA would get unreasonably high capacity.
		implicitMult := in.nodeCPURateUsage / in.storesCPURate
		mult = max(1, min(implicitMult, maxCPUAmplification))
	}

	// Background load is the portion of node usage NOT attributed to MMA.
	// This is clamped to be non-negative.
	mmaAttributedLoad := in.storesCPURate * mult
	backgroundLoad := max(0.0, in.nodeCPURateUsage-mmaAttributedLoad)

	// MMA's share of capacity is what remains after background load.
	// Clamp to non-negative to handle overloaded nodes where backgroundLoad
	// exceeds nodeCPURateCapacity.
	mmaShareOfCapacity := max(0.0, in.nodeCPURateCapacity-backgroundLoad)

	// MMA's direct capacity is scaled down by the multiplier to account
	// for indirect overhead.
	mmaDirectCapacity := mmaShareOfCapacity / mult

	// Divide evenly across stores.
	capacity = mmaDirectCapacity / float64(in.numStores)
	return capacity
}

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
						"capped:   kv-capacity: %.2f cores/store, capacity_err: %s\n",
					nodeUsageCores, nodeCapCores, kvCPUCores, propOverhead, bgCores,
					trueCapPerStore, trueMult,
					naiveCap, fmtErr(naiveCap),
					cappedCap, fmtErr(cappedCap),
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

				// Current model: uses FractionUsed() = Used/(Used+Available).
				result := computeStoreByteSizeCapacity(
					mmaprototype.LoadValue(logicalBytes), fractionUsed, available,
				)
				// Wrong model: uses (Total-Available)/Total.
				wrongResult := computeStoreByteSizeCapacityNaive(
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
