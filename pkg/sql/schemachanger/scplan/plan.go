// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scplan

import (
	"sort"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scgraph"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/deprules"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/opgen"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/errors"
)

// Params holds the arguments for planning.
type Params struct {
	// ExecutionPhase indicates the phase that the plan should be constructed for.
	ExecutionPhase scop.Phase
	// CreatedDescriptorIDs contains IDs for new descriptors created by the same
	// schema changer (i.e., earlier in the same transaction). New descriptors
	// can have most of their schema changes fully executed in the same
	// transaction.
	//
	// This doesn't do anything right now.
	CreatedDescriptorIDs catalog.DescriptorIDSet
}

// A Plan is a schema change plan, primarily containing ops to be executed that
// are partitioned into stages.
type Plan struct {
	Params  Params
	Initial scpb.State
	Graph   *scgraph.Graph
	Stages  []Stage
}

// A Stage is a sequence of ops to be executed "together" as part of a schema
// change.
//
// Stages also contain the state before and after the execution of the ops in
// the stage, reflecting the fact that any set of ops can be thought of as a
// transition from one state to another.
type Stage struct {
	Before, After scpb.State
	Ops           scop.Ops
	Revertible    bool
}

// MakePlan generates a Plan for a particular phase of a schema change, given
// the initial state for a set of targets.
func MakePlan(initial scpb.State, params Params) (_ Plan, err error) {
	defer func() {
		if r := recover(); r != nil {
			rAsErr, ok := r.(error)
			if !ok {
				rAsErr = errors.Errorf("panic during MakePlan: %v", r)
			}
			err = errors.CombineErrors(err, rAsErr)
		}
	}()

	g, err := opgen.BuildGraph(params.ExecutionPhase, initial)
	if err != nil {
		return Plan{}, err
	}
	if err := deprules.Apply(g); err != nil {
		return Plan{}, err
	}

	stages := buildStages(initial, g, params)
	return Plan{
		Params:  params,
		Initial: initial,
		Graph:   g,
		Stages:  stages,
	}, nil
}

// validateStages sanity checks stages to ensure no
// invalid execution plans are made.
func validateStages(stages []Stage) {
	revertibleAllowed := true
	for idx, stage := range stages {
		if !stage.Revertible {
			revertibleAllowed = false
		}
		if stage.Revertible && !revertibleAllowed {
			panic(errors.AssertionFailedf(
				"invalid execution plan revertability flipped at stage (%d): %v", idx, stage))
		}
	}
}

func buildStages(init scpb.State, g *scgraph.Graph, params Params) []Stage {
	// Fetch the order of the graph, which will be used to
	// evaluating edges in topological order.
	nodeRanks, err := g.GetNodeRanks()
	if err != nil {
		panic(err)
	}
	// TODO(ajwerner): deal with the case where the target status was
	// fulfilled by something that preceded the initial state.
	cur := init
	fulfilled := map[*scpb.Node]struct{}{}
	filterUnsatisfiedEdgesStep := func(edges []*scgraph.OpEdge) ([]*scgraph.OpEdge, bool) {
		candidates := make(map[*scpb.Node]struct{})
		for _, e := range edges {
			candidates[e.To()] = struct{}{}
		}
		// Check to see if the current set of edges will have their dependencies met
		// if they are all run. Any which will not must be pruned. This greedy
		// algorithm works, but a justification is in order.
		failed := map[*scgraph.OpEdge]struct{}{}
		for _, e := range edges {
			_ = g.ForEachDepEdgeFrom(e.To(), func(de *scgraph.DepEdge) error {
				_, isFulfilled := fulfilled[de.To()]
				_, isCandidate := candidates[de.To()]
				if isFulfilled || isCandidate {
					return nil
				}
				failed[e] = struct{}{}
				return iterutil.StopIteration()
			})
		}
		if len(failed) == 0 {
			return edges, true
		}
		truncated := edges[:0]
		for _, e := range edges {
			if _, found := failed[e]; !found {
				truncated = append(truncated, e)
			}
		}
		return truncated, false
	}
	filterUnsatisfiedEdges := func(edges []*scgraph.OpEdge) ([]*scgraph.OpEdge, bool) {
		for len(edges) > 0 {
			if filtered, done := filterUnsatisfiedEdgesStep(edges); !done {
				edges = filtered
			} else {
				return filtered, true
			}
		}
		return edges, false
	}
	buildStageType := func(edges []*scgraph.OpEdge) (Stage, bool) {
		edges, ok := filterUnsatisfiedEdges(edges)
		if !ok {
			return Stage{}, false
		}
		sort.SliceStable(edges,
			func(i, j int) bool {
				// Higher ranked edges should go first.
				return nodeRanks[edges[i].To()] > nodeRanks[edges[j].To()]
			})

		next := shallowCopy(cur)
		isStageRevertible := true
		var ops []scop.Op
		for revertible := 1; revertible >= 0; revertible-- {
			isStageRevertible = revertible == 1
			for _, e := range edges {
				for i, ts := range cur.Nodes {
					if e.From() == ts && isStageRevertible == e.Revertible() {
						next.Nodes[i] = e.To()
						ops = append(ops, e.Op()...)
						break
					}
				}
			}
			// If we added non-revertible stages
			// then this stage is done
			if len(ops) != 0 {
				break
			}
		}
		return Stage{
			Before:     cur,
			After:      next,
			Ops:        scop.MakeOps(ops...),
			Revertible: isStageRevertible,
		}, true
	}

	var stages []Stage
	for {
		// Note that the current nodes are fulfilled for the sake of dependency
		// checking.
		for _, ts := range cur.Nodes {
			fulfilled[ts] = struct{}{}
		}

		// Extract the set of op edges for the current stage.
		var opEdges []*scgraph.OpEdge
		for _, t := range cur.Nodes {
			// TODO(ajwerner): improve the efficiency of this lookup.
			// Look for an opEdge from this node. Then, for the other side
			// of the opEdge, look for dependencies.
			if oe, ok := g.GetOpEdgeFrom(t); ok {
				opEdges = append(opEdges, oe)
			}
		}

		// Group the op edges a per-type basis.
		opTypes := make(map[scop.Type][]*scgraph.OpEdge)
		for _, oe := range opEdges {
			opTypes[oe.Type()] = append(opTypes[oe.Type()], oe)
		}

		// Greedily attempt to find a stage which can be executed. This is sane
		// because once a dependency is met, it never becomes unmet.
		var didSomething bool
		var s Stage
		for _, typ := range []scop.Type{
			scop.MutationType,
			scop.BackfillType,
			scop.ValidationType,
		} {
			if s, didSomething = buildStageType(opTypes[typ]); didSomething {
				break
			}
		}
		if !didSomething {
			break
		}
		stages = append(stages, s)
		cur = s.After
	}
	validateStages(stages)
	return stages
}

// shallowCopy creates a shallow copy of the passed state. Importantly, it
// retains copies to the same underlying nodes while allocating new backing
// slices.
func shallowCopy(cur scpb.State) scpb.State {
	return scpb.State{
		Nodes: append(
			make([]*scpb.Node, 0, len(cur.Nodes)),
			cur.Nodes...,
		),
		Statements: append(
			make([]*scpb.Statement, 0, len(cur.Statements)),
			cur.Statements...,
		),
		Authorization: cur.Authorization,
	}
}
