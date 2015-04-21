// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package gossip

import (
	"testing"

	"github.com/cockroachdb/cockroach/proto"
)

func TestNodeSetMaxSize(t *testing.T) {
	nodes := newNodeSet(1)
	if !nodes.hasSpace() {
		t.Error("set should have space")
	}
	nodes.addNode(proto.NodeID(1))
	if nodes.hasSpace() {
		t.Error("set should have no space")
	}
}

func TestNodeSetHasNode(t *testing.T) {
	nodes := newNodeSet(2)
	node := proto.NodeID(1)
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
	nodes := newNodeSet(2)
	node0 := proto.NodeID(1)
	node1 := proto.NodeID(2)
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
	nodes1 := newNodeSet(2)
	node0 := proto.NodeID(1)
	node1 := proto.NodeID(2)
	nodes1.addNode(node0)
	nodes1.addNode(node1)

	nodes2 := newNodeSet(1)
	nodes2.addNode(node1)

	filtered := nodes1.filter(func(a proto.NodeID) bool {
		return !nodes2.hasNode(a)
	})
	if filtered.len() != 1 || filtered.hasNode(node1) || !filtered.hasNode(node0) {
		t.Errorf("expected filter to leave node0: %+v", filtered)
	}
}

func TestNodeSetAsSlice(t *testing.T) {
	nodes := newNodeSet(2)
	node0 := proto.NodeID(1)
	node1 := proto.NodeID(2)
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
