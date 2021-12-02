// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package opgen

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scgraph"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
)

type registry struct {
	targets []target
}

var opRegistry = &registry{}

// BuildGraph constructs a graph with operation edges populated from an initial
// state.
func BuildGraph(initial scpb.State) (*scgraph.Graph, error) {
	return opRegistry.buildGraph(initial)
}

func (r *registry) buildGraph(initial scpb.State) (*scgraph.Graph, error) {
	g, err := scgraph.New(initial)
	if err != nil {
		return nil, err
	}
	// Iterate through each match of initial state target's to target rules
	// and apply the relevant op edges to thr graph. Copy out the elements
	// to not mutate the database in place.
	type toAdd struct {
		transition
		n *scpb.Node
	}
	var edgesToAdd []toAdd
	for _, t := range r.targets {
		edgesToAdd = edgesToAdd[:0]
		if err := t.iterateFunc(g.Database(), func(n *scpb.Node) error {
			var in bool
			for _, op := range t.transitions {
				if in = in || op.from == n.Status; in {
					edgesToAdd = append(edgesToAdd, toAdd{
						transition: op,
						n:          n,
					})
				}
			}
			return nil
		}); err != nil {
			return nil, err
		}
		for _, op := range edgesToAdd {
			metadata := g.GetMetadataFromTarget(op.n.Target)
			if err := g.AddOpEdges(
				op.n.Target, op.from, op.to, op.revertible, op.minPhase, op.ops(op.n.Element(), &metadata)...,
			); err != nil {
				return nil, err
			}
		}

	}
	return g, nil
}
