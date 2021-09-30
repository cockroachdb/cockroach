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
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/errors"
)

type registry struct {
	targets []target
}

var opRegistry = &registry{}

// BuildGraph constructs a graph with operation edges populated from an initial
// state and phase.
//
// TODO(ajwerner): Generate the stages for all of the phases as it will make
// debugging easier.
func BuildGraph(currentPhase scop.Phase, initial scpb.State) (*scgraph.Graph, error) {
	return opRegistry.buildGraph(currentPhase, initial)
}

func (r *registry) buildGraph(currentPhase scop.Phase, initial scpb.State) (*scgraph.Graph, error) {
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

					// TODO(ajwerner): Consider moving this code out into the transition
					// construction rather than here at runtime. This should not be
					// possible to hit.
					if op.to == scpb.Status_UNKNOWN {
						return errors.AssertionFailedf(
							"illegal transition to %s on (%T, %v) to %v",
							scpb.Status_UNKNOWN, n.Target.Element(), n.Target.Direction, op.from,
						)
					}

					// Note that if this phase is not yet possible, we still want a node
					// in the graph in order to make the dependency rules work out. This
					// is very subtle. For now, we rewrite the transition to construct a
					// node which is unreachable.
					//
					// TODO(ajwerner): Figure out something more elegant here by moving
					// the min-phase checking up to the planning layer rather than the
					// operation generation layer.
					if op.minPhase > currentPhase {
						// make this a no-op edge but add the node
						op.from = op.to
						op.ops = func(element scpb.Element) []scop.Op { return nil }
					}

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
			if err := g.AddOpEdges(
				op.n.Target, op.from, op.to, op.revertible, op.ops(op.n.Element())...,
			); err != nil {
				return nil, err
			}
		}

	}
	return g, nil
}

// register constructs the rules for a given target.
// Intending to be called during init, register panics on any error.
func (r *registry) register(
	e scpb.Element, dir scpb.Target_Direction, initialStatus scpb.Status, specs ...transitionSpec,
) {
	r.targets = append(r.targets, makeTarget(e, dir, initialStatus, specs...))
}
