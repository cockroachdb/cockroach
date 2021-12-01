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

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scgraph"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/deprules"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/opgen"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/scopt"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/errors"
)

// Params holds the arguments for planning.
type Params struct {
	// ExecutionPhase indicates the phase that the plan should be constructed for.
	ExecutionPhase scop.Phase
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
func MakePlan(initial scpb.State, params Params) (p Plan, err error) {
	defer func() {
		if r := recover(); r != nil {
			rAsErr, ok := r.(error)
			if !ok {
				rAsErr = errors.Errorf("panic during MakePlan: %v", r)
			}
			err = errors.CombineErrors(err, rAsErr)
		}
	}()

	p = Plan{
		Initial: initial,
		Params:  params,
	}
	g, err := opgen.BuildGraph(initial)
	if err != nil {
		return p, err
	}
	p.Graph = g
	if err := deprules.Apply(g); err != nil {
		return p, err
	}
	optimizedGraph, err := scopt.OptimizePlan(g)
	if err != nil {
		return p, err
	}
	p.Graph = optimizedGraph
	p.Stages = buildStages(initial, params.ExecutionPhase, optimizedGraph)
	// TODO(fqazi): Enforce here that only one post commit stage will be generated
	// Inside OpGen we will enforce that only a single transition can occur in
	// post commit.
	if err = validateStages(p.Stages); err != nil {
		return p, errors.WithAssertionFailure(errors.Wrapf(err, "invalid execution plan"))
	}
	return p, nil
}

// validateStages sanity checks stages to ensure no
// invalid execution plans are made.
func validateStages(stages []Stage) error {
	revertibleAllowed := true
	for idx, stage := range stages {
		if !stage.Revertible {
			revertibleAllowed = false
		}
		if stage.Revertible && !revertibleAllowed {
			return errors.Errorf("stage %d of %d is unexpectedly marked as revertible: %v",
				idx+1, len(stages), stage)
		}
	}
	return nil
}

func buildStages(init scpb.State, phase scop.Phase, g *scgraph.Graph) []Stage {
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
		candidates := make(map[*scpb.Node]*scgraph.OpEdge)
		for _, e := range edges {
			candidates[e.To()] = e
		}
		// Check to see if the current set of edges will have their dependencies met
		// if they are all run. Any which will not must be pruned. This greedy
		// algorithm works, but a justification is in order.
		failed := map[*scgraph.OpEdge]struct{}{}
		for _, e := range edges {
			if !e.IsPhaseSatisfied(phase) {
				failed[e] = struct{}{}
			}
		}
		for _, e := range edges {
			if err := g.ForEachDepEdgeFrom(e.To(), func(de *scgraph.DepEdge) error {
				_, isFulfilled := fulfilled[de.To()]
				_, isCandidate := candidates[de.To()]
				if de.Kind() == scgraph.SameStage && isFulfilled {
					// This is bad, we have a happens-after relationship, and it has
					// already happened.
					return errors.AssertionFailedf("failed to satisfy %v->%v (%s) dependency",
						screl.NodeString(de.From()), screl.NodeString(de.To()), de.Name())
				}
				if isFulfilled || isCandidate {
					return nil
				}
				failed[e] = struct{}{}
				return iterutil.StopIteration()
			}); err != nil {
				panic(err)
			}
		}
		// Ensure that all SameStage DepEdges are met appropriately.
		for _, e := range edges {
			if err := g.ForEachSameStageDepEdgeTo(e.To(), func(de *scgraph.DepEdge) error {
				if _, isFulfilled := fulfilled[de.From()]; isFulfilled {
					// This is bad, we have a happens-after relationship, and it has
					// already happened.
					return errors.AssertionFailedf("failed to satisfy %v->%v (%s) dependency",
						screl.NodeString(de.From()), screl.NodeString(de.To()), de.Name())
				}
				fromCandidate, fromIsCandidate := candidates[de.From()]
				if !fromIsCandidate {
					failed[e] = struct{}{}
					return iterutil.StopIteration()
				}
				_, fromIsFailed := failed[fromCandidate]
				if fromIsFailed {
					failed[e] = struct{}{}
					return iterutil.StopIteration()
				}
				if _, eIsFailed := failed[e]; eIsFailed {
					failed[fromCandidate] = struct{}{}
				}
				return nil
			}); err != nil {
				panic(err)
			}
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
	isRevertiblePreferred := true
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
		var ops []scop.Op
		var isStageRevertible bool
		revertible := []bool{true, false}
		if !isRevertiblePreferred {
			revertible = []bool{false}
		}
		for _, isStageRevertible = range revertible {
			for _, e := range edges {
				for i, ts := range cur.Nodes {
					if e.From() == ts && (!isRevertiblePreferred || isStageRevertible == e.Revertible()) {
						next.Nodes[i] = e.To()
						// TODO(fqazi): MakePlan should never emit empty stages, they are harmless
						// but a side effect of OpEdge filtering. We need to adjust the algorithm
						// here to run multiple transitions in a stage when planning.

						// If this edge has been marked as a no-op, then a state transition
						// can happen without executing anything.
						if !g.IsOpEdgeOptimizedOut(e) {
							ops = append(ops, e.Op()...)
						}
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
		if isRevertiblePreferred && !isStageRevertible {
			isRevertiblePreferred = false
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
