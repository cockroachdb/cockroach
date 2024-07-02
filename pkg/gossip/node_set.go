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
	// Account for duplicates by tracking a count for each node ID rather than
	// just a hash set. We try to avoid duplicate gossip connections, but don't
	// guarantee that they never occur so it pays to actually track them.
	nodes        map[roachpb.NodeID]int // Count of connections to each node
	placeholders int                    // Number of nodes whose ID we don't know yet.
	maxSize      int                    // Maximum size of set
	gauge        *metric.Gauge          // Gauge for the number of nodes in the set.
}

func makeNodeSet(maxSize int, gauge *metric.Gauge) nodeSet {
	return nodeSet{
		nodes:   make(map[roachpb.NodeID]int),
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
	totalNodes := 0
	for _, count := range as.nodes {
		totalNodes += count
	}
	return totalNodes + as.placeholders
}

// asSlice returns the nodes as a slice (excluding any placeholders that
// haven't yet resolved to an ID).
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
	for node, count := range as.nodes {
		for i := 0; i < count; i++ {
			if filterFn(node) {
				avail.addNode(node)
			}
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
	as.nodes[node]++
	as.updateGauge()
}

// removeNode removes the node from the nodes set.
func (as *nodeSet) removeNode(node roachpb.NodeID) {
	// It shouldn't ever be the case that we're missing a node from the map, but
	// be defensive and treat it as a placeholder if we do.
	if count, ok := as.nodes[node]; ok {
		if count == 1 {
			delete(as.nodes, node)
		} else {
			as.nodes[node]--
		}
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
