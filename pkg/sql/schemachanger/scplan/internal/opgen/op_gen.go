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
	"context"
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/scgraph"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

type registry struct {
	targets []target
}

var opRegistry = &registry{}

// BuildGraph constructs a graph with operation edges populated from an initial
// state.
func BuildGraph(cs scpb.CurrentState) (*scgraph.Graph, error) {
	return opRegistry.buildGraph(cs)
}

func (r *registry) buildGraph(cs scpb.CurrentState) (_ *scgraph.Graph, err error) {
	start := timeutil.Now()
	defer func() {
		if err != nil || !log.V(2) {
			return
		}
		log.Infof(context.TODO(), "operation graph generation took %v", timeutil.Since(start))
	}()
	g, err := scgraph.New(cs)
	if err != nil {
		return nil, err
	}
	// Iterate through each match of initial state target's to target rules
	// and apply the relevant op edges to the graph. Copy out the elements
	// to not mutate the database in place.
	type toAdd struct {
		transition
		n *screl.Node
	}
	var edgesToAdd []toAdd
	md := makeTargetsWithElementMap(cs)
	for _, t := range r.targets {
		edgesToAdd = edgesToAdd[:0]
		if err := t.iterateFunc(g.Database(), func(n *screl.Node) error {
			status := n.CurrentStatus
			for _, op := range t.transitions {
				if op.from == status {
					edgesToAdd = append(edgesToAdd, toAdd{
						transition: op,
						n:          n,
					})
					status = op.to
				}
			}
			return nil
		}); err != nil {
			return nil, err
		}
		for _, e := range edgesToAdd {
			var ops []scop.Op
			if e.ops != nil {
				ops = e.ops(e.n.Element(), &md)
			}
			if err := g.AddOpEdges(
				e.n.Target, e.from, e.to, e.revertible, e.canFail, e.minPhase, ops...,
			); err != nil {
				return nil, err
			}
		}

	}
	return g, nil
}

// InitialStatus returns the status at the source of an op-edge path.
func InitialStatus(e scpb.Element, target scpb.Status) scpb.Status {
	if t, found := findTarget(e, target); found {
		return t.transitions[0].from
	}
	return scpb.Status_UNKNOWN
}

// NextStatus returns the status succeeding the current one for the element
// and target status, if the corresponding op edge exists.
func NextStatus(e scpb.Element, target, current scpb.Status) scpb.Status {
	if t, found := findTarget(e, target); found {
		for _, tt := range t.transitions {
			if tt.from == current {
				return tt.to
			}
		}
	}
	return scpb.Status_UNKNOWN
}

func findTarget(e scpb.Element, s scpb.Status) (_ target, found bool) {
	et := reflect.TypeOf(e)
	for _, t := range opRegistry.targets {
		if t.status != s {
			continue
		}
		if reflect.TypeOf(t.e) != et {
			continue
		}
		return t, true /* found */

	}
	return target{}, false /* found */
}
