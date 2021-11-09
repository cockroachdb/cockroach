// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scgraph

import (
	"sort"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
)

// NodeIterator is used to iterate nodes. Return iterutil.StopIteration to
// return early with no error.
type NodeIterator func(n *scpb.Node) error

// ForEachNode iterates the nodes in the graph.
func (g *Graph) ForEachNode(it NodeIterator) error {
	for _, m := range g.targetNodes {
		for i := 0; i < scpb.NumStatus; i++ {
			if ts, ok := m[scpb.Status(i)]; ok {
				if err := it(ts); err != nil {
					if iterutil.Done(err) {
						err = nil
					}
					return err
				}
			}
		}
	}
	return nil
}

// EdgeIterator is used to iterate edges. Return iterutil.StopIteration to
// return early with no error.
type EdgeIterator func(e Edge) error

// ForEachEdge iterates the edges in the graph.
func (g *Graph) ForEachEdge(it EdgeIterator) error {
	for _, e := range g.edges {
		if err := it(e); err != nil {
			if iterutil.Done(err) {
				err = nil
			}
			return err
		}
	}
	return nil
}

// DepEdgeIterator is used to iterate dep edges. Return iterutil.StopIteration
// to return early with no error.
type DepEdgeIterator func(de *DepEdge) error

// ForEachDepEdgeFrom iterates the dep edges in the graph.
func (g *Graph) ForEachDepEdgeFrom(n *scpb.Node, it DepEdgeIterator) error {
	edges := g.nodeDepEdgesFrom[n]
	for _, e := range edges {
		if err := it(e); err != nil {
			if iterutil.Done(err) {
				err = nil
			}
			return err
		}
	}
	return nil
}

// ForEachDepEdgeFromOrdered iterates the dep edges in the graph,
// sorting them based on the target index.
func (g *Graph) ForEachDepEdgeFromOrdered(n *scpb.Node, it DepEdgeIterator) error {
	edges := g.nodeDepEdgesFrom[n]
	// Order edges based on the target indexes.
	sortedEdges := make([]*DepEdge, len(edges))
	copy(sortedEdges, edges)
	sort.SliceStable(sortedEdges, func(i, j int) bool {
		return g.targetIdxMap[sortedEdges[i].to.Target] < g.targetIdxMap[sortedEdges[j].to.Target]
	})
	for _, e := range sortedEdges {
		if err := it(e); err != nil {
			if iterutil.Done(err) {
				err = nil
			}
			return err
		}
	}
	return nil
}
