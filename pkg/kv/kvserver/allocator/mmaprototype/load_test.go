// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaprototype

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/dd"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

type storeLoadAndNodeID struct {
	nodeID    roachpb.NodeID
	storeLoad *storeLoad
}

type testLoadInfoProvider struct {
	t                  *testing.T
	b                  strings.Builder
	sloads             map[roachpb.StoreID]storeLoadAndNodeID
	nloads             map[roachpb.NodeID]*NodeLoad
	returnedLoadSeqNum uint64
}

func (p *testLoadInfoProvider) getStoreReportedLoad(
	storeID roachpb.StoreID,
) (roachpb.NodeID, *storeLoad) {
	sl, ok := p.sloads[storeID]
	require.True(p.t, ok)
	return sl.nodeID, sl.storeLoad
}

func (p *testLoadInfoProvider) getNodeReportedLoad(nodeID roachpb.NodeID) *NodeLoad {
	nl, ok := p.nloads[nodeID]
	require.True(p.t, ok)
	return nl
}

func (p *testLoadInfoProvider) computeLoadSummary(
	context.Context, roachpb.StoreID, *meanStoreLoad, *meanNodeLoad, mmaLogger,
) storeLoadSummary {
	fmt.Fprintf(&p.b, "called computeLoadSummary: returning seqnum %d", p.returnedLoadSeqNum)
	return storeLoadSummary{
		loadSeqNum: p.returnedLoadSeqNum,
	}
}

func TestLoadSummaryForDimension(t *testing.T) {
	ctx := context.Background()
	const (
		dummyStoreID roachpb.StoreID = 1
		dummyNodeID  roachpb.NodeID  = 1

		vCPU LoadValue = 1_000_000_000 // 1 vCPU = 1e9 ns/s
	)
	// Derive test parameters from the floor so tests don't hardcode floor
	// values. meanLoad is set to 0, so the denominator is exactly the floor
	// and fractionAbove = load / floor. We compute exact thresholds using
	// floating-point multiplication rounded up via +1, since the floor may
	// not be evenly divisible.
	wbFloor := writeBandwidthSignificanceFloor
	// Exact 5% and 10% thresholds (rounded up to ensure >= threshold).
	wbNoChangeDelta := LoadValue(math.Ceil(float64(wbFloor) * 0.05))
	wbOverloadDelta := LoadValue(math.Ceil(float64(wbFloor) * 0.1))

	testCases := []struct {
		name     string
		dim      LoadDimension
		load     LoadValue
		capacity LoadValue
		meanLoad LoadValue
		meanUtil float64
		expected loadSummary
	}{
		//
		// WriteBandwidth (UnknownCapacity).
		// Tests the denominator clamp: denom = max(meanLoad, wbFloor).
		// meanLoad is 0 so the denominator is exactly wbFloor.
		//
		{
			// Just below the 5% boundary → loadNormal.
			name:     "WB below 5pct loadNormal",
			dim:      WriteBandwidth,
			load:     wbNoChangeDelta - 1,
			capacity: UnknownCapacity,
			meanLoad: 0,
			expected: loadNormal,
		},
		{
			// At the 5% boundary → loadNoChange.
			name:     "WB at 5pct loadNoChange",
			dim:      WriteBandwidth,
			load:     wbNoChangeDelta,
			capacity: UnknownCapacity,
			meanLoad: 0,
			expected: loadNoChange,
		},
		{
			// Just above the 10% boundary → overloadSlow.
			name:     "WB above 10pct overloadSlow",
			dim:      WriteBandwidth,
			load:     wbOverloadDelta + 1,
			capacity: UnknownCapacity,
			meanLoad: 0,
			expected: overloadSlow,
		},
		{
			// Just below the -10% boundary → loadLow.
			name:     "WB below -10pct loadLow",
			dim:      WriteBandwidth,
			load:     -wbOverloadDelta - 1,
			capacity: UnknownCapacity,
			meanLoad: 0,
			expected: loadLow,
		},
		{
			// Mean above floor, clamp inactive (denom = meanLoad, not floor).
			// Just above 10%: delta / meanLoad > 0.1 → overloadSlow.
			name:     "WB high mean clamp inactive",
			dim:      WriteBandwidth,
			load:     wbFloor*20 + wbFloor*2 + 1,
			capacity: UnknownCapacity,
			meanLoad: wbFloor * 20,
			expected: overloadSlow,
		},
		{
			// Mean = 0: denominator falls back to floor (no division by zero).
			// Small load / floor is well under 5% → loadNormal.
			name:     "WB mean zero floor fallback",
			dim:      WriteBandwidth,
			load:     wbFloor / 100,
			capacity: UnknownCapacity,
			meanLoad: 0,
			expected: loadNormal,
		},
		//
		// CPURate (capacity = 2 vCPU).
		// CPU below 5% utilization is not worth rebalancing, so the
		// result is capped at loadNormal. Above 90% is overloadUrgent.
		//
		{
			// Using < 5% of CPU capacity → not worth rebalancing.
			name:     "CPU below 5pct capped",
			dim:      CPURate,
			load:     vCPU/10 - 1,
			capacity: 2 * vCPU,
			meanLoad: vCPU / 20,
			meanUtil: 0.04,
			expected: loadNormal,
		},
		{
			// Using exactly 5% of CPU capacity → rebalancing allowed.
			// Utilization 0.05 vs mean 0.025, clamped denom 0.05 →
			// fractionAbove = 0.5 → overloadSlow.
			name:     "CPU at 5pct uncapped",
			dim:      CPURate,
			load:     vCPU / 10,
			capacity: 2 * vCPU,
			meanLoad: vCPU / 20,
			meanUtil: 0.025,
			expected: overloadSlow,
		},
		{
			// Using > 90% of CPU capacity → overloadUrgent.
			name:     "CPU above 90pct overloadUrgent",
			dim:      CPURate,
			load:     9*vCPU/5 + 1,
			capacity: 2 * vCPU,
			meanLoad: vCPU,
			meanUtil: 0.5,
			expected: overloadUrgent,
		},
		//
		// ByteSize (capacity = 100 mib).
		// Disk below 50% full is not worth rebalancing for ByteSize,
		// so the result is capped at loadNormal. Above 90% is
		// overloadUrgent.
		//
		{
			// Disk < 50% full → not worth rebalancing.
			name:     "ByteSize below 50pct capped",
			dim:      ByteSize,
			load:     50*byteSizeSignificanceFloor - 1,
			capacity: 100 * byteSizeSignificanceFloor,
			meanLoad: 20 * byteSizeSignificanceFloor,
			meanUtil: 0.2,
			expected: loadNormal,
		},
		{
			// Disk exactly 50% full → rebalancing allowed.
			// Utilization 0.5 vs mean 0.4 → 25% above → overloadSlow.
			name:     "ByteSize at 50pct uncapped",
			dim:      ByteSize,
			load:     50 * byteSizeSignificanceFloor,
			capacity: 100 * byteSizeSignificanceFloor,
			meanLoad: 40 * byteSizeSignificanceFloor,
			meanUtil: 0.4,
			expected: overloadSlow,
		},
		{
			// Disk > 90% full → overloadUrgent.
			name:     "ByteSize above 90pct overloadUrgent",
			dim:      ByteSize,
			load:     90*byteSizeSignificanceFloor + 1,
			capacity: 100 * byteSizeSignificanceFloor,
			meanLoad: 50 * byteSizeSignificanceFloor,
			meanUtil: 0.5,
			expected: overloadUrgent,
		},
		//
		// Edge cases.
		//
		{
			// Adjusted load can be negative. A large negative delta
			// relative to the floor → loadLow.
			name:     "WB negative load",
			dim:      WriteBandwidth,
			load:     -wbFloor*10/100 - 1,
			capacity: UnknownCapacity,
			meanLoad: 0,
			expected: loadLow,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := loadSummaryForDimension(
				ctx, dummyStoreID, dummyNodeID,
				tc.dim, tc.load, tc.capacity, tc.meanLoad, tc.meanUtil,
				makeMMALogger(false /* verboseToInfof */),
			)
			require.Equal(t, tc.expected, got,
				"dim=%v load=%d meanLoad=%d capacity=%d",
				tc.dim, tc.load, tc.meanLoad, tc.capacity)
		})
	}
}

func TestMeansMemo(t *testing.T) {
	interner := newStringInterner()
	cm := newConstraintMatcher(interner)
	storeMap := map[roachpb.StoreID]StoreAttributesAndLocality{}
	loadProvider := &testLoadInfoProvider{
		t:      t,
		sloads: map[roachpb.StoreID]storeLoadAndNodeID{},
		nloads: map[roachpb.NodeID]*NodeLoad{},
	}
	mm := newMeansMemo(loadProvider, cm)
	var mss *meansForStoreSet
	datadriven.RunTest(t, "testdata/means_memo",
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "store":
				for _, line := range strings.Split(d.Input, "\n") {
					sal := parseStoreAttributedAndLocality(t, strings.TrimSpace(line))
					cm.setStore(sal.withNodeTier())
					storeMap[sal.StoreID] = sal
				}
				return ""

			case "store-load":
				storeID := dd.ScanArg[roachpb.StoreID](t, d, "store-id")
				sal, ok := storeMap[storeID]
				require.True(t, ok)
				var cpuLoad, wbLoad, bsLoad int64
				d.ScanArgs(t, "load", &cpuLoad, &wbLoad, &bsLoad)
				var cpuCapacity, wbCapacity, bsCapacity int64
				d.ScanArgs(t, "capacity", &cpuCapacity, &wbCapacity, &bsCapacity)
				leaseCountLoad := dd.ScanArg[LoadValue](t, d, "secondary-load")
				sLoad := &storeLoad{
					reportedLoad: LoadVector{LoadValue(cpuLoad), LoadValue(wbLoad), LoadValue(bsLoad)},
					capacity: LoadVector{
						LoadValue(cpuCapacity), LoadValue(wbCapacity), LoadValue(bsCapacity)},
					reportedSecondaryLoad: SecondaryLoadVector{leaseCountLoad},
				}
				for i := range sLoad.capacity {
					if sLoad.capacity[i] < 0 {
						sLoad.capacity[i] = UnknownCapacity
					}
				}
				loadProvider.sloads[storeID] = storeLoadAndNodeID{
					nodeID:    sal.NodeID,
					storeLoad: sLoad,
				}

				return ""

			case "node-load":
				nLoad := &NodeLoad{
					NodeID:          dd.ScanArg[roachpb.NodeID](t, d, "node-id"),
					NodeCPULoad:     dd.ScanArg[LoadValue](t, d, "cpu-load"),
					NodeCPUCapacity: dd.ScanArg[LoadValue](t, d, "cpu-capacity"),
				}
				loadProvider.nloads[nLoad.NodeID] = nLoad
				return ""

			case "get-means":
				var disj constraintsDisj
				for _, line := range strings.Split(d.Input, "\n") {
					parts := strings.Fields(line)
					if len(parts) == 0 {
						continue
					}
					cc := parseConstraints(t, parts)
					disj = append(disj, interner.internConstraintsConj(cc))
				}
				var ok bool
				mss, ok = mm.getMeans(disj)
				if !ok {
					return "ok: false (no stores match)\n"
				}
				var b strings.Builder
				fmt.Fprintf(&b, "stores: ")
				printPostingList(&b, mss.stores)
				fmt.Fprintf(&b, "\nstore-means (load,cap,util): ")
				for i := range mss.storeLoad.load {
					switch LoadDimension(i) {
					case CPURate:
						fmt.Fprintf(&b, "cpu: ")
					case WriteBandwidth:
						fmt.Fprintf(&b, " write-bw: ")
					case ByteSize:
						fmt.Fprintf(&b, " bytes: ")
					}
					capacity := mss.storeLoad.capacity[i]
					var capStr string
					if capacity == UnknownCapacity {
						capStr = "unknown"
					} else {
						capStr = fmt.Sprintf("%d", capacity)
					}
					fmt.Fprintf(&b, "(%d, %s, %.2f)", mss.storeLoad.load[i],
						capStr, mss.storeLoad.util[i])
				}
				fmt.Fprintf(&b, "\n   secondary-load: %d\n", mss.storeLoad.secondaryLoad)
				fmt.Fprintf(&b, "node-mean cpu (load,cap,util): (%d, %d, %.2f)\n", mss.nodeLoad.loadCPU,
					mss.nodeLoad.capacityCPU, mss.nodeLoad.utilCPU)
				return b.String()

			case "get-store-summary":
				storeID := dd.ScanArg[roachpb.StoreID](t, d, "store-id")
				loadSeqNum := dd.ScanArg[uint64](t, d, "load-seq-num")
				loadProvider.returnedLoadSeqNum = loadSeqNum
				_ = mm.getStoreLoadSummary(context.Background(), mss, storeID, loadSeqNum)
				rv := loadProvider.b.String()
				loadProvider.b.Reset()
				return rv

			case "clear":
				mm.clear()
				return ""

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}

// TestComputeMeansForStoreSetEmpty verifies that computeMeansForStoreSet
// returns ok=false (rather than panicking) when given no stores. This is the
// empty-set contract relied on by getMeans and by callers that may receive a
// constraint conjunction matching no stores.
func TestComputeMeansForStoreSetEmpty(t *testing.T) {
	loadProvider := &testLoadInfoProvider{
		t:      t,
		sloads: map[roachpb.StoreID]storeLoadAndNodeID{},
		nloads: map[roachpb.NodeID]*NodeLoad{},
	}
	scratchNodes := map[roachpb.NodeID]*NodeLoad{}
	scratchStores := map[roachpb.StoreID]struct{}{}

	for _, name := range []string{"nil slice", "empty slice"} {
		t.Run(name, func(t *testing.T) {
			var stores []roachpb.StoreID
			if name == "empty slice" {
				stores = []roachpb.StoreID{}
			}
			means, ok := computeMeansForStoreSet(loadProvider, stores, scratchNodes, scratchStores)
			require.False(t, ok)
			require.Equal(t, meansLoad{}, means)
		})
	}
}
