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
					cm.setStore(sal)
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
					NodeID:      dd.ScanArg[roachpb.NodeID](t, d, "node-id"),
					ReportedCPU: dd.ScanArg[LoadValue](t, d, "cpu-load"),
					CapacityCPU: dd.ScanArg[LoadValue](t, d, "cpu-capacity"),
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
