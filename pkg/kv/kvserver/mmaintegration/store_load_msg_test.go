// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaintegration

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/mmaprototype"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/datadriven"
)

// TestCPUCapacityFormulas explores an alternative formula for computing MMA CPU
// capacity that caps the maximum multiplier between MMA-tracked load and total
// node CPU usage.
//
// Problem with the current approach:
// The current formula assumes ALL node CPU usage is caused (directly or
// indirectly) by MMA-tracked work. This breaks down when:
// - MMA tracks very little load (e.g., few/no replicas)
// - But the node has significant CPU usage from other sources (SQL, RPC, GC)
//
// Example: MMA tracks 1 core, node uses 4 cores total. Current formula
// assigns capacity such that MMA is at the same utilization as the node,
// meaning MMA gets capacity of 1 core (at 100% util). But clearly, the other
// 3 cores of usage aren't all caused by that 1 core of MMA work.
//
// Proposed improvement:
// Cap the multiplier so MMA only assumes responsibility for a bounded amount
// of "other" load. For example, with maxMult=3, MMA assumes it causes at most
// 2x its direct load in indirect load (total 3x).
//
// The datadriven test uses the following command:
//
//	compute stores_cpu=<cores> node_usage=<cores> node_capacity=<cores> [num_stores=<n>] [max_mult=<m>]
//
// It outputs a comparison of the current formula vs the proposed capped formula.
func TestCPUCapacityFormulas(t *testing.T) {
	datadriven.RunTest(t, "testdata/cpu_capacity_formulas", func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "compute":
			return runComputeCmd(t, d)
		default:
			t.Fatalf("unknown command: %s", d.Cmd)
			return ""
		}
	})
}

func runComputeCmd(t *testing.T, d *datadriven.TestData) string {
	// Parse arguments
	var storesCPU, nodeUsage, nodeCapacity float64
	numStores := int32(1)
	maxMult := 2.0

	for _, arg := range d.CmdArgs {
		switch arg.Key {
		case "stores_cpu":
			storesCPU = mustParseFloat(t, arg.Vals[0])
		case "node_usage":
			nodeUsage = mustParseFloat(t, arg.Vals[0])
		case "node_capacity":
			nodeCapacity = mustParseFloat(t, arg.Vals[0])
		case "num_stores":
			numStores = int32(mustParseInt(t, arg.Vals[0]))
		case "max_mult":
			maxMult = mustParseFloat(t, arg.Vals[0])
		}
	}

	if nodeCapacity == 0 {
		t.Fatalf("node_capacity must be specified and > 0")
	}

	var out strings.Builder

	// Current formula (matches MakeStoreLoadMsg behavior).
	// The multiplier is node_usage / stores_cpu (uncapped).
	// When storesCPU is zero, mult is +Inf which is interesting to show.
	currentMult := nodeUsage / storesCPU
	var currentCap, currentUtil float64
	if storesCPU > 0 && nodeUsage > 0 {
		cpuUtil := nodeUsage / nodeCapacity
		currentCap = (storesCPU / cpuUtil) / float64(numStores)
		currentUtil = (storesCPU / float64(numStores)) / currentCap
	}

	// Capped formula
	cappedCapNanos, cappedMult := computeCPUCapacityWithCap(
		int64(storesCPU*1e9),
		int64(nodeUsage*1e9),
		int64(nodeCapacity*1e9),
		numStores,
		maxMult,
	)
	cappedCap := cappedCapNanos / 1e9
	var cappedUtil float64
	if cappedCap > 0 {
		cappedUtil = (storesCPU / float64(numStores)) / cappedCap
	}

	// Tabular output: current vs capped side by side.
	fmt.Fprintf(&out, "cur    capped\n")
	fmt.Fprintf(&out, "%05.2f  %05.2f  mult\n", currentMult, cappedMult)
	fmt.Fprintf(&out, "%05.2f  %05.2f  cap\n", currentCap, cappedCap)
	fmt.Fprintf(&out, "%5.2f%% %5.2f%% util\n", currentUtil*100, cappedUtil*100)

	// Verify invariant for capped formula: at full MMA capacity, node should
	// also be at full capacity. Only print if it FAILS.
	if storesCPU > 0 && cappedCap > 0 {
		backgroundLoad := max(0, nodeUsage-storesCPU*cappedMult)
		mmaDirectCapTotal := cappedCap * float64(numStores)
		nodeUsageAtFull := backgroundLoad + mmaDirectCapTotal*cappedMult
		if abs(nodeUsageAtFull-nodeCapacity) >= 0.001 {
			fmt.Fprintf(&out, "INVARIANT VIOLATED: at full MMA cap, node=%.2f (want %.2f)\n",
				nodeUsageAtFull, nodeCapacity)
		}
	}

	return out.String()
}

func mustParseFloat(t *testing.T, s string) float64 {
	t.Helper()
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		t.Fatalf("invalid float %q: %v", s, err)
	}
	return v
}

func mustParseInt(t *testing.T, s string) int {
	t.Helper()
	v, err := strconv.Atoi(s)
	if err != nil {
		t.Fatalf("invalid int %q: %v", s, err)
	}
	return v
}

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

// computeCPUCapacityWithCap computes per-store CPU capacity using a clamped multiplier.
//
// The formula:
//  1. clampedMult = clamp(nodeCPURateUsage / storesCPURate, 1, maxMultiplier)
//     In the first case, the CPU load that isn't tracked by MMA is small enough
//     to assume (pessimistically) that all of it is caused by MMA-tracked work,
//     and that it is going to scale linearly with MMA's direct load.
//     In the second case, there is so much auxiliary load that we do not
//     make this assumption and instead assume that one unit of MMA load will
//     cause two units of auxiliary load, but that anything in excess of that
//     is background load that is independent of MMA decision making.
//  2. backgroundLoad = nodeCPURateUsage - storesCPURate * cappedMult
//     This is the load that we assume is "fixed" on the node without any
//     relationship with MMA-related work.
//  3. mmaShareOfCapacity = nodeCPURateCapacity - backgroundLoad
//     This is the capacity (basically the number of cores) that we can fill
//     with MMA-related work. It's not the capacity we'll hand to MMA yet because
//     we need to still account for the "indirect" load that MMA will cause.
//  4. mmaDirectCapacity = mmaShareOfCapacity / cappedMult
//  5. storeCapacity = mmaDirectCapacity / numStores
//
// The key insight is that `backgroundLoad` represents CPU usage that is NOT
// attributed to MMA work (assumed constant), while MMA is responsible for
// the remainder which scales linearly with MMA's direct load.
//
// When storesCPURate is 0, we use the limiting behavior: MMA attributes 0 of
// the node usage to itself (all is background), leaving the idle capacity
// available, scaled by maxMultiplier.
func computeCPUCapacityWithCap(
	storesCPURate, nodeCPURateUsage, nodeCPURateCapacity int64,
	numStores int32,
	maxMultiplier float64,
) (capacity float64, mult float64) {
	if nodeCPURateCapacity == 0 || numStores == 0 {
		return 0, 0
	}

	if storesCPURate == 0 {
		// When MMA has zero load, the implicit multiplier is infinite, so we
		// use maxMultiplier. MMA attributes 0 * maxMult = 0 of the node usage
		// to itself, meaning all node usage is "background". MMA gets the
		// remaining idle capacity, scaled by maxMultiplier.
		mult = maxMultiplier
	} else {
		// Compute the implicit multiplier and clamp it to [1, maxMultiplier].
		// - Clamping from above prevents pathological behavior when MMA tracks
		//   little load but the node has high CPU usage from other sources.
		// - Clamping from below (at 1) handles the case where MMA load exceeds
		//   node usage (shouldn't happen, but can due to measurement lag).
		//   Without this, MMA would get unreasonably high capacity.
		implicitMult := float64(nodeCPURateUsage) / float64(storesCPURate)
		mult = max(1, min(implicitMult, maxMultiplier))
	}

	// Background load is the portion of node usage NOT attributed to MMA.
	// This is clamped to be non-negative.
	mmaAttributedLoad := float64(storesCPURate) * mult
	backgroundLoad := max(0, float64(nodeCPURateUsage)-mmaAttributedLoad)

	// MMA's share of capacity is what remains after background load.
	mmaShareOfCapacity := float64(nodeCPURateCapacity) - backgroundLoad

	// MMA's direct capacity is scaled down by the multiplier.
	mmaDirectCapacity := mmaShareOfCapacity / mult

	// Divide evenly across stores.
	capacity = mmaDirectCapacity / float64(numStores)
	return capacity, mult
}

// TestCompareWithMakeStoreLoadMsg verifies that our "current formula" computation
// matches the actual MakeStoreLoadMsg implementation.
func TestCompareWithMakeStoreLoadMsg(t *testing.T) {
	desc := roachpb.StoreDescriptor{
		StoreID: 1,
		Node:    roachpb.NodeDescriptor{NodeID: 1},
		Capacity: roachpb.StoreCapacity{
			CPUPerSecond: 2e9, // 2 cores
		},
	}
	desc.NodeCapacity = roachpb.NodeCapacity{
		StoresCPURate:       2e9,
		NumStores:           1,
		NodeCPURateCapacity: 16e9,
		NodeCPURateUsage:    8e9,
	}

	msg := MakeStoreLoadMsg(desc, 0)
	actualCapacity := float64(msg.Capacity[mmaprototype.CPURate])

	// Current formula: capacity = storesCPU / cpuUtil / numStores
	cpuUtil := float64(desc.NodeCapacity.NodeCPURateUsage) / float64(desc.NodeCapacity.NodeCPURateCapacity)
	expectedCapacity := float64(desc.NodeCapacity.StoresCPURate) / cpuUtil / float64(desc.NodeCapacity.NumStores)

	if abs(actualCapacity-expectedCapacity) > 1e6 {
		t.Errorf("MakeStoreLoadMsg capacity %.2f != expected %.2f", actualCapacity/1e9, expectedCapacity/1e9)
	}
}
