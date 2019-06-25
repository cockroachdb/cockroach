// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package gossip

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

func TestNodeSetMaxSize(t *testing.T) {
	defer leaktest.AfterTest(t)()
	nodes := makeNodeSet(1, metric.NewGauge(metric.Metadata{Name: ""}))
	if !nodes.hasSpace() {
		t.Error("set should have space")
	}
	nodes.addNode(roachpb.NodeID(1))
	if nodes.hasSpace() {
		t.Error("set should have no space")
	}
}

func TestNodeSetHasNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	nodes := makeNodeSet(2, metric.NewGauge(metric.Metadata{Name: ""}))
	node := roachpb.NodeID(1)
	if nodes.hasNode(node) {
		t.Error("node wasn't added and should not be valid")
	}
	// Add node and verify it's valid.
	nodes.addNode(node)
	if !nodes.hasNode(node) {
		t.Error("empty node wasn't added and should not be valid")
	}
}

func TestNodeSetAddAndRemoveNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	nodes := makeNodeSet(2, metric.NewGauge(metric.Metadata{Name: ""}))
	node0 := roachpb.NodeID(1)
	node1 := roachpb.NodeID(2)
	nodes.addNode(node0)
	nodes.addNode(node1)
	if !nodes.hasNode(node0) || !nodes.hasNode(node1) {
		t.Error("failed to locate added nodes")
	}
	nodes.removeNode(node0)
	if nodes.hasNode(node0) || !nodes.hasNode(node1) {
		t.Error("failed to remove node0", nodes)
	}
	nodes.removeNode(node1)
	if nodes.hasNode(node0) || nodes.hasNode(node1) {
		t.Error("failed to remove node1", nodes)
	}
}

func TestNodeSetFilter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	nodes1 := makeNodeSet(2, metric.NewGauge(metric.Metadata{Name: ""}))
	node0 := roachpb.NodeID(1)
	node1 := roachpb.NodeID(2)
	nodes1.addNode(node0)
	nodes1.addNode(node1)

	nodes2 := makeNodeSet(1, metric.NewGauge(metric.Metadata{Name: ""}))
	nodes2.addNode(node1)

	filtered := nodes1.filter(func(a roachpb.NodeID) bool {
		return !nodes2.hasNode(a)
	})
	if filtered.len() != 1 || filtered.hasNode(node1) || !filtered.hasNode(node0) {
		t.Errorf("expected filter to leave node0: %+v", filtered)
	}
}

func TestNodeSetAsSlice(t *testing.T) {
	defer leaktest.AfterTest(t)()
	nodes := makeNodeSet(2, metric.NewGauge(metric.Metadata{Name: ""}))
	node0 := roachpb.NodeID(1)
	node1 := roachpb.NodeID(2)
	nodes.addNode(node0)
	nodes.addNode(node1)

	nodeArr := nodes.asSlice()
	if len(nodeArr) != 2 {
		t.Error("expected slice of length 2:", nodeArr)
	}
	if (nodeArr[0] != node0 && nodeArr[0] != node1) ||
		(nodeArr[1] != node1 && nodeArr[1] != node0) {
		t.Error("expected slice to contain both node0 and node1:", nodeArr)
	}
}
