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
	"math/rand"

	"github.com/cockroachdb/cockroach/proto"
)

// A nodeSet keeps a set of nodes and provides simple node-matched
// management functions. nodeSet is not thread safe.
type nodeSet struct {
	nodes   map[proto.NodeID]struct{} // Set of proto.NodeID
	maxSize int                       // Maximum size of set
}

func newNodeSet(maxSize int) *nodeSet {
	return &nodeSet{
		nodes:   make(map[proto.NodeID]struct{}),
		maxSize: maxSize,
	}
}

// hasSpace returns whether there are fewer than maxSize nodes
// in the nodes slice.
func (as *nodeSet) hasSpace() bool {
	return len(as.nodes) < as.maxSize
}

// len returns the number of nodes in the set.
func (as *nodeSet) len() int {
	return len(as.nodes)
}

// asSlice returns the nodes as a slice.
func (as *nodeSet) asSlice() []proto.NodeID {
	arr := make([]proto.NodeID, len(as.nodes))
	var count int
	for node := range as.nodes {
		arr[count] = node
		count++
	}
	return arr
}

// selectRandom returns a random node from the set. Returns 0 if
// there are no nodes to select.
func (as *nodeSet) selectRandom() proto.NodeID {
	idx := rand.Intn(len(as.nodes))
	var count = 0
	for node := range as.nodes {
		if count == idx {
			return node
		}
		count++
	}
	return 0
}

// filter returns an nodeSet of nodes which return true when
// passed to the supplied filter function filterFn. filterFn should
// return true to keep an node and false to remove an node.
func (as *nodeSet) filter(filterFn func(node proto.NodeID) bool) *nodeSet {
	avail := newNodeSet(as.maxSize)
	for node := range as.nodes {
		if filterFn(node) {
			avail.addNode(node)
		}
	}
	return avail
}

// hasNode verifies that the supplied node matches a node
// in the slice.
func (as *nodeSet) hasNode(node proto.NodeID) bool {
	_, ok := as.nodes[node]
	return ok
}

// addNode adds the node to the nodes set.
func (as *nodeSet) addNode(node proto.NodeID) {
	as.nodes[node] = struct{}{}
}

// removeNode removes the node from the nodes set.
func (as *nodeSet) removeNode(node proto.NodeID) {
	delete(as.nodes, node)
}
