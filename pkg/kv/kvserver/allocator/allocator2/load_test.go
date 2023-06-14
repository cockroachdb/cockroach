// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package allocator2

import (
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

type testLoadInfoProvider struct {
	t                  *testing.T
	b                  strings.Builder
	sloads             map[roachpb.StoreID]*storeLoad
	nloads             map[roachpb.NodeID]*nodeLoad
	returnedLoadSeqNum uint64
}

func (p *testLoadInfoProvider) getStoreReportedLoad(storeID roachpb.StoreID) *storeLoad {
	sl, ok := p.sloads[storeID]
	require.True(p.t, ok)
	return sl
}

func (p *testLoadInfoProvider) getNodeReportedLoad(nodeID roachpb.NodeID) *nodeLoad {
	nl, ok := p.nloads[nodeID]
	require.True(p.t, ok)
	return nl
}

func (p *testLoadInfoProvider) computeLoadSummary(
	roachpb.StoreID, *meanStoreLoad, *meanNodeLoad,
) storeLoadSummary {
	fmt.Fprintf(&p.b, "called computeLoadSummary: returning seqnum %d", p.returnedLoadSeqNum)
	return storeLoadSummary{
		loadSeqNum: p.returnedLoadSeqNum,
	}
}

func TestMeansMemo(t *testing.T) {
	interner := newStringInterner()
	cm := newConstraintMatcher(interner)
	storeMap := map[roachpb.StoreID]roachpb.StoreDescriptor{}
	loadProvider := &testLoadInfoProvider{
		t:      t,
		sloads: map[roachpb.StoreID]*storeLoad{},
		nloads: map[roachpb.NodeID]*nodeLoad{},
	}
	mm := newMeansMemo(loadProvider, cm)
	var mss *meansForStoreSet
	datadriven.RunTest(t, "testdata/means_memo",
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "store":
				for _, next := range strings.Split(d.Input, "\n") {
					desc := parseStoreDescriptor(t, strings.TrimSpace(next))
					cm.setStore(desc)
					storeMap[desc.StoreID] = desc
				}
				return ""

			case "store-load":
				for _, next := range strings.Split(d.Input, "\n") {
					sLoad := parseStoreLoad(t, next)
					desc, ok := storeMap[sLoad.StoreID]
					require.True(t, ok)
					sLoad.StoreDescriptor = desc
					sLoad.NodeID = desc.Node.NodeID
					loadProvider.sloads[sLoad.StoreID] = &sLoad
				}
				return ""

			case "node-load":
				for _, next := range strings.Split(d.Input, "\n") {
					nLoad := parseNodeLoad(t, next)
					loadProvider.nloads[nLoad.nodeID] = &nLoad
				}
				return ""

			case "get-means":
				var disj constraintsDisj
				for _, line := range strings.Split(d.Input, "\n") {
					parts := strings.Fields(line)
					if len(parts) == 0 {
						continue
					}
					cc := parseConstraints(t, parts)
					disj = append(disj, interner.internConstraints(cc))
				}
				mss = mm.getMeans(disj)
				var b strings.Builder
				fmt.Fprintf(&b, "stores: ")
				printPostingList(&b, mss.stores)
				fmt.Fprintf(&b, "\nstore-means (load,cap,util): ")
				for i := range mss.storeLoad.load {
					switch loadDimension(i) {
					case cpu:
						fmt.Fprintf(&b, "cpu: ")
					case writeBandwidth:
						fmt.Fprintf(&b, " write-bw: ")
					case byteSize:
						fmt.Fprintf(&b, " bytes: ")
					}
					capacity := mss.storeLoad.capacity[i]
					var capStr string
					if capacity == parentCapacity {
						capStr = "parent"
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
				var storeID int
				d.ScanArgs(t, "store-id", &storeID)
				var loadSeqNum uint64
				d.ScanArgs(t, "load-seq-num", &loadSeqNum)
				loadProvider.returnedLoadSeqNum = loadSeqNum
				_ = mm.getStoreLoadSummary(mss, roachpb.StoreID(storeID), loadSeqNum)
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
