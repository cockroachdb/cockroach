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
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

// A nodeSet keeps a set of nodes and provides simple node-matched
// management functions. nodeSet is not thread safe.
type nodeSet struct {
	nodes        map[roachpb.NodeID]struct{} // Set of roachpb.NodeID
	placeholders int                         // Number of nodes whose ID we don't know yet.
	maxSize      int                         // Maximum size of set
	gauge        *metric.Gauge               // Gauge for the number of nodes in the set.
}

func makeNodeSet(maxSize int, gauge *metric.Gauge) nodeSet {
	return nodeSet{
		nodes:   make(map[roachpb.NodeID]struct{}),
		maxSize: maxSize,
		gauge:   gauge,
	}
}

// hasSpace returns whether there are fewer than maxSize nodes
// in the nodes slice.
func (as nodeSet) hasSpace() bool {
	return as.len() < as.maxSize
}

// len returns the number of nodes in the set.
func (as nodeSet) len() int {
	return len(as.nodes) + as.placeholders
}

// asSlice returns the nodes as a slice.
func (as nodeSet) asSlice() []roachpb.NodeID {
	slice := make([]roachpb.NodeID, 0, len(as.nodes))
	for node := range as.nodes {
		slice = append(slice, node)
	}
	return slice
}

// filter returns a nodeSet containing the nodes which return true when passed
// to the supplied filter function filterFn. filterFn should return true to
// keep a node and false to remove a node. The new nodeSet has a separate
// gauge object from the parent.
func (as nodeSet) filter(filterFn func(node roachpb.NodeID) bool) nodeSet {
	avail := makeNodeSet(as.maxSize,
		metric.NewGauge(metric.Metadata{Name: "TODO(marc)", Help: "TODO(marc)"}))
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
	as.maxSize = maxSize
}

// addNode adds the node to the nodes set.
func (as *nodeSet) addNode(node roachpb.NodeID) {
	// Account for duplicates by including them in the placeholders tally.
	// We try to avoid duplicate gossip connections, but don't guarantee that
	// they never occur.
	if !as.hasNode(node) {
		as.nodes[node] = struct{}{}
	} else {
		as.placeholders++
	}
	as.updateGauge()
}

// removeNode removes the node from the nodes set.
func (as *nodeSet) removeNode(node roachpb.NodeID) {
	// Parallel the logic in addNode. If we've already removed the given
	// node ID, it's because we had more than one of them.
	if as.hasNode(node) {
		delete(as.nodes, node)
	} else {
		as.placeholders--
	}
	as.updateGauge()
}

// addPlaceholder adds another node to the set of tracked nodes, but is
// intended for nodes whose IDs we don't know at the time of adding.
// resolvePlaceholder should be called once we know the ID.
func (as *nodeSet) addPlaceholder() {
	as.placeholders++
	as.updateGauge()
}

// resolvePlaceholder adds another node to the set of tracked nodes, but is
// intended for nodes whose IDs we don't know at the time of adding.
func (as *nodeSet) resolvePlaceholder(node roachpb.NodeID) {
	as.placeholders--
	as.addNode(node)
}

func (as *nodeSet) updateGauge() {
	if as.placeholders < 0 {
		log.Fatalf(context.TODO(),
			"nodeSet.placeholders should never be less than 0; gossip logic is broken %+v", as)
	}
	as.gauge.Update(int64(as.len()))
}
