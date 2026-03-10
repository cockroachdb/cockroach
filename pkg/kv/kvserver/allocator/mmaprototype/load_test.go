// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaprototype

import (
	"context"
	"fmt"
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
	context.Context, roachpb.StoreID, *meanStoreLoad, *meanNodeLoad,
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

		kib  LoadValue = 1 << 10
		mib  LoadValue = 1 << 20
		vCPU LoadValue = 1_000_000_000 // 1 vCPU = 1e9 ns/s
	)
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
		// WriteBandwidth (UnknownCapacity, significance floor = 5 mib).
		// Tests the denominator clamp: denom = max(meanLoad, 5 mib).
		// With denom = 5 mib, the fraction thresholds translate to:
		//   5% (loadNoChange) = 256 kib,  10% (overloadSlow) = 512 kib.
		//
		{
			// Just below the 5% boundary (loadNoChange requires >= 5%):
			// (256 kib - 1) / 5 mib < 0.05 → loadNormal.
			name:     "WB below 5pct loadNormal",
			dim:      WriteBandwidth,
			load:     1*mib + 256*kib - 1,
			capacity: UnknownCapacity,
			meanLoad: 1 * mib,
			expected: loadNormal,
		},
		{
			// At the 5% boundary:
			// 256 kib / 5 mib = 0.05 exactly → loadNoChange.
			name:     "WB at 5pct loadNoChange",
			dim:      WriteBandwidth,
			load:     1*mib + 256*kib,
			capacity: UnknownCapacity,
			meanLoad: 1 * mib,
			expected: loadNoChange,
		},
		{
			// Just above the 10% boundary (overloadSlow requires > 10%):
			// (512 kib + 1) / 5 mib > 0.1 → overloadSlow.
			name:     "WB above 10pct overloadSlow",
			dim:      WriteBandwidth,
			load:     1*mib + 512*kib + 1,
			capacity: UnknownCapacity,
			meanLoad: 1 * mib,
			expected: overloadSlow,
		},
		{
			// Just below the -10% boundary (loadLow requires < -10%):
			// -(512 kib + 1) / 5 mib < -0.1 → loadLow.
			name:     "WB below -10pct loadLow",
			dim:      WriteBandwidth,
			load:     2*mib - 512*kib - 1,
			capacity: UnknownCapacity,
			meanLoad: 2 * mib,
			expected: loadLow,
		},
		{
			// Mean above floor, clamp inactive (denom = meanLoad, not floor).
			// Just above 10%: (10 mib + 1) / 100 mib > 0.1 → overloadSlow.
			name:     "WB high mean clamp inactive",
			dim:      WriteBandwidth,
			load:     100*mib + 10*mib + 1,
			capacity: UnknownCapacity,
			meanLoad: 100 * mib,
			expected: overloadSlow,
		},
		{
			// Mean = 0: denominator falls back to floor (no division by zero).
			// 100 kib / 5 mib ≈ 0.019 → loadNormal.
			name:     "WB mean zero floor fallback",
			dim:      WriteBandwidth,
			load:     100 * kib,
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
			// 50ms/s above mean on a 100ms/s floor → overloadSlow.
			name:     "CPU at 5pct uncapped",
			dim:      CPURate,
			load:     vCPU / 10,
			capacity: 2 * vCPU,
			meanLoad: vCPU / 20,
			meanUtil: 0.05,
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
		{
			// Unknown CPU capacity uses the 100ms/s significance floor.
			// Delta of ~10ms/s is just under 10% of the floor →
			// loadNoChange. Without the floor, the same delta would be
			// ~100% of the 10ms/s mean → overloadSlow.
			name:     "CPU unknown capacity floor",
			dim:      CPURate,
			load:     2*vCPU/100 - 1,
			capacity: UnknownCapacity,
			meanLoad: vCPU / 100,
			expected: loadNoChange,
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
			load:     50*mib - 1,
			capacity: 100 * mib,
			meanLoad: 20 * mib,
			meanUtil: 0.2,
			expected: loadNormal,
		},
		{
			// Disk exactly 50% full → rebalancing allowed.
			// 10 mib above mean on a 40 mib denom → overloadSlow.
			name:     "ByteSize at 50pct uncapped",
			dim:      ByteSize,
			load:     50 * mib,
			capacity: 100 * mib,
			meanLoad: 40 * mib,
			meanUtil: 0.5,
			expected: overloadSlow,
		},
		{
			// Disk > 90% full → overloadUrgent.
			name:     "ByteSize above 90pct overloadUrgent",
			dim:      ByteSize,
			load:     90*mib + 1,
			capacity: 100 * mib,
			meanLoad: 50 * mib,
			meanUtil: 0.5,
			expected: overloadUrgent,
		},
		{
			// Unknown ByteSize capacity uses the 100 mib significance floor.
			// Delta of ~10 mib is just under 10% of the floor →
			// loadNoChange. Without the floor, the same delta would be
			// ~100% of the 10 mib mean → overloadSlow.
			name:     "ByteSize unknown capacity floor",
			dim:      ByteSize,
			load:     20*mib - 1,
			capacity: UnknownCapacity,
			meanLoad: 10 * mib,
			expected: loadNoChange,
		},
		//
		// Edge cases.
		//
		{
			// Adjusted load can be negative. -512 kib is 2.5 mib below
			// the 2 mib mean: -2.5 mib / 5 mib = -0.50 → loadLow.
			name:     "WB negative load",
			dim:      WriteBandwidth,
			load:     -512 * kib,
			capacity: UnknownCapacity,
			meanLoad: 2 * mib,
			expected: loadLow,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := loadSummaryForDimension(
				ctx, dummyStoreID, dummyNodeID,
				tc.dim, tc.load, tc.capacity, tc.meanLoad, tc.meanUtil,
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
				mss = mm.getMeans(disj)
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
