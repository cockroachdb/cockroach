// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scopt

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/scgraph"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
)

type deleteNodeOpt struct {
	query       *rel.Query
	edgeFromVar rel.Var
}

type registry struct {
	deleteQueries []*deleteNodeOpt
}

var optRegistry registry

// registerNoOpEdges adds a graph query that will label any
// edges starting from this node a no-op.
func registerNoOpEdges(edgeFromVar rel.Var, query *rel.Query) {
	optRegistry.deleteQueries = append(optRegistry.deleteQueries,
		&deleteNodeOpt{
			query:       query,
			edgeFromVar: edgeFromVar,
		})
}

func targetNodeVars(el rel.Var) (element, target, node rel.Var) {
	return el, el + "-target", el + "-node"
}

// OptimizeGraph given a graph this code will optimize it to eliminate
// unnecessary nodes. A new mutated graph will be returned based on the
// original with these any transformations / optimizations applied.
func OptimizeGraph(graph *scgraph.Graph) (*scgraph.Graph, error) {
	db := graph.Database()
	nodesToMark := make(map[*screl.Node]struct{})
	for _, delete := range optRegistry.deleteQueries {
		err := delete.query.Iterate(db, func(r rel.Result) error {
			nodesToMark[r.Var(delete.edgeFromVar).(*screl.Node)] = struct{}{}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	// Mark any op edges from these nodes as no-op.
	mutatedGraph := graph.ShallowClone()
	for node := range nodesToMark {
		if edge, ok := graph.GetOpEdgeFrom(node); ok {
			mutatedGraph.MarkAsNoOp(edge)
		}
	}
	return mutatedGraph, nil
}
