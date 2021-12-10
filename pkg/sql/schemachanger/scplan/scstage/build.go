// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scstage

import (
	"sort"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scgraph"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/errors"
)

// BuildStages builds the plan's stages.
func BuildStages(init scpb.State, phase scop.Phase, g *scgraph.Graph) []Stage {
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
		for _, isStageRevertible = range []bool{true, false} {
			if isStageRevertible && !isRevertiblePreferred {
				continue
			}
			hasTransitions := false
			for _, e := range edges {
				for i, ts := range cur.Nodes {
					if e.From() == ts && (!isStageRevertible || e.Revertible()) {
						next.Nodes[i] = e.To()
						hasTransitions = true
						// If this edge has been marked as a no-op, then a state transition
						// can happen without executing anything.
						if !g.IsOpEdgeOptimizedOut(e) {
							ops = append(ops, e.Op()...)
						}
						break
					}
				}
			}
			if hasTransitions {
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
			Phase:      phase,
		}, true
	}

	// Build stages for this phase and all subsequent phases.
	var stages []Stage
	for ; phase <= scop.PostCommitPhase; phase++ {
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
	}

	return decorateStages(stages)
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
