// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scgraph

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
)

// NodeIterator is used to iterate nodes. Return iterutil.StopIteration to
// return early with no error.
type NodeIterator func(n *screl.Node) error

// ForEachNode iterates the nodes in the graph.
func (g *Graph) ForEachNode(it NodeIterator) error {
	for _, m := range g.targetNodes {
		for i := range scpb.Status_name {
			if ts, ok := m[scpb.Status(i)]; ok {
				if err := it(ts); err != nil {
					return iterutil.Map(err)
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
	for _, e := range g.opEdges {
		if err := it(e); err != nil {
			return iterutil.Map(err)
		}
	}
	return g.depEdges.iterate(func(de *DepEdge) error {
		return it(de)
	})
}

// DepEdgeIterator is used to iterate dep edges. Return iterutil.StopIteration
// to return early with no error.
type DepEdgeIterator func(de *DepEdge) error

// ForEachDepEdgeFrom iterates the dep edges in the graph with the selected
// source.
func (g *Graph) ForEachDepEdgeFrom(n *screl.Node, it DepEdgeIterator) (err error) {
	return g.depEdges.iterateFromNode(n, it)
}

// ForEachDepEdgeTo iterates the dep edges in the graph with the selected
// destination.
func (g *Graph) ForEachDepEdgeTo(n *screl.Node, it DepEdgeIterator) (err error) {
	return g.depEdges.iterateToNode(n, it)
}
