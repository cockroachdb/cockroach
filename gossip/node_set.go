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
// permissions and limitations under the License.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package gossip

import "github.com/cockroachdb/cockroach/roachpb"

// A nodeSet keeps a set of nodes and provides simple node-matched
// management functions. nodeSet is not thread safe.
type nodeSet struct {
	nodes   map[roachpb.NodeID]struct{} // Set of roachpb.NodeID
	maxSize int                         // Maximum size of set
}

func makeNodeSet(maxSize int) nodeSet {
	return nodeSet{
		nodes:   make(map[roachpb.NodeID]struct{}),
		maxSize: maxSize,
	}
}

// hasSpace returns whether there are fewer than maxSize nodes
// in the nodes slice.
func (as nodeSet) hasSpace() bool {
	return len(as.nodes) < as.maxSize
}

// len returns the number of nodes in the set.
func (as nodeSet) len() int {
	return len(as.nodes)
}

// asSlice returns the nodes as a slice.
func (as nodeSet) asSlice() []roachpb.NodeID {
	slice := make([]roachpb.NodeID, 0, len(as.nodes))
	for node := range as.nodes {
		slice = append(slice, node)
	}
	return slice
}

// filter returns an nodeSet of nodes which return true when
// passed to the supplied filter function filterFn. filterFn should
// return true to keep an node and false to remove an node.
func (as nodeSet) filter(filterFn func(node roachpb.NodeID) bool) nodeSet {
	avail := makeNodeSet(as.maxSize)
	for node := range as.nodes {
		if filterFn(node) {
			avail.addNode(node)
		}
	}
	return avail
}

// hasNode verifies that the supplied node matches a node
// in the slice.
func (as nodeSet) hasNode(node roachpb.NodeID) bool {
	_, ok := as.nodes[node]
	return ok
}

// setMaxSize adjusts the maximum size allowed for the node set.
func (as *nodeSet) setMaxSize(maxSize int) {
	if as.maxSize != maxSize {
		as.maxSize = maxSize
	}
}

// addNode adds the node to the nodes set.
func (as *nodeSet) addNode(node roachpb.NodeID) {
	as.nodes[node] = struct{}{}
}

// removeNode removes the node from the nodes set.
func (as *nodeSet) removeNode(node roachpb.NodeID) {
	delete(as.nodes, node)
}
