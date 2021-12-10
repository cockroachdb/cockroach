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
func BuildStages(init scpb.State, phase scop.Phase, g *scgraph.Graph) (stages []Stage) {
	b := makeBuildContext(init, phase, g)
	// Build stages for this phase and all subsequent phases.
	for b.phase <= scop.PostCommitPhase {
		// Note that the current nodes are fulfilled for the sake of dependency
		// checking.
		for _, ts := range b.state.Nodes {
			b.fulfilled[ts] = struct{}{}
		}

		// Extract the set of op edges for the current stage.
		var opEdges []*scgraph.OpEdge
		for _, t := range b.state.Nodes {
			// TODO(ajwerner): improve the efficiency of this lookup.
			// Look for an opEdge from this node. Then, for the other side
			// of the opEdge, look for dependencies.
			if oe, ok := b.g.GetOpEdgeFrom(t); ok {
				opEdges = append(opEdges, oe)
			}
		}

		if s := b.tryBuildStage(opEdges); s != nil {
			stages = append(stages, *s)
			b.state = s.After
			b.isRevertiblePreferred = b.isRevertiblePreferred && s.Revertible
		} else {
			// No further progress is possible in this phase, move on to the next one.
			b.phase++
		}
	}

	return decorateStages(stages)
}

// buildCtx contains the necessary context for building a stage.
// When building a stage, this struct is read-only.
type buildCtx struct {
	g                     *scgraph.Graph
	nodeRanks             map[*scpb.Node]int
	state                 scpb.State
	phase                 scop.Phase
	isRevertiblePreferred bool
	fulfilled             map[*scpb.Node]struct{}
}

// makeBuildContext is the constructor for buildCtx.
func makeBuildContext(init scpb.State, phase scop.Phase, g *scgraph.Graph) buildCtx {
	// Fetch the ranks of the nodes, which will be used to evaluate edges
	// in topological order.
	nodeRanks, err := g.GetNodeRanks()
	if err != nil {
		panic(err)
	}
	return buildCtx{
		g:                     g,
		phase:                 phase,
		isRevertiblePreferred: true,
		nodeRanks:             nodeRanks,
		// TODO(ajwerner): deal with the case where the target status was
		// fulfilled by something that preceded the initial state.
		state:     init,
		fulfilled: make(map[*scpb.Node]struct{}, len(nodeRanks)),
	}
}

// tryBuildStage tries to build a stage using the provided op-edges.
// This is done on a best-effort basis, if not possible nil is returned.
func (b buildCtx) tryBuildStage(edges []*scgraph.OpEdge) *Stage {
	// Group the op edges a per-type basis.
	opTypes := make(map[scop.Type][]*scgraph.OpEdge)
	for _, oe := range edges {
		opTypes[oe.Type()] = append(opTypes[oe.Type()], oe)
	}

	// Greedily attempt to find a stage which can be executed. This is sane
	// because once a dependency is met, it never becomes unmet.
	for _, typ := range []scop.Type{
		scop.MutationType,
		scop.BackfillType,
		scop.ValidationType,
	} {
		if filteredEdges := b.filterUnsatisfiedEdges(opTypes[typ]); len(filteredEdges) > 0 {
			s := b.makeStage(filteredEdges)
			return &s
		}
	}
	return nil
}

func (b buildCtx) filterUnsatisfiedEdges(edges []*scgraph.OpEdge) []*scgraph.OpEdge {
	for len(edges) > 0 {
		failed := b.collectFailedEdges(edges)
		if len(failed) == 0 {
			return edges
		}

		// Remove all failed edges from the edges slice.
		filtered := edges[:0]
		for _, e := range edges {
			if _, found := failed[e]; !found {
				filtered = append(filtered, e)
			}
		}
		edges = filtered
	}
	return nil
}

func (b buildCtx) collectFailedEdges(
	edges []*scgraph.OpEdge,
) (failed map[*scgraph.OpEdge]struct{}) {
	candidates := make(map[*scpb.Node]*scgraph.OpEdge)
	for _, e := range edges {
		candidates[e.To()] = e
	}
	// Check to see if the current set of edges will have their dependencies met
	// if they are all run. Any which will not must be pruned. This greedy
	// algorithm works, but a justification is in order.
	failed = make(map[*scgraph.OpEdge]struct{}, len(edges))
	for _, e := range edges {
		if !e.IsPhaseSatisfied(b.phase) {
			failed[e] = struct{}{}
		}
	}
	for _, e := range edges {
		if err := b.g.ForEachDepEdgeTo(e.To(), func(de *scgraph.DepEdge) error {
			if _, isFulfilled := b.fulfilled[de.From()]; isFulfilled {
				// Dependency source node has already been fulfilled in an earlier
				// stage.
				if de.Kind() == scgraph.SameStagePrecedence {
					// This is bad, the dependency requires the destination node to be
					// fulfilled in the same stage, which is impossible in this case.
					return errors.AssertionFailedf("failed to satisfy %v->%v (%s) dependency",
						screl.NodeString(de.From()), screl.NodeString(de.To()), de.Name())
				}
				return nil
			}
			if _, isCandidate := candidates[de.From()]; isCandidate {
				// Dependency source and destination nodes will both be fulfilled in the
				// same stage.
				return nil
			}
			// The candidate op edge must be rejected.
			failed[e] = struct{}{}
			return iterutil.StopIteration()
		}); err != nil {
			panic(err)
		}
	}
	// Ensure that all SameStagePrecedence DepEdges are met appropriately.
	for _, e := range edges {
		if err := b.g.ForEachDepEdgeFrom(e.To(), func(de *scgraph.DepEdge) error {
			if de.Kind() != scgraph.SameStagePrecedence {
				// Only look at same-stage dependency edges.
				return nil
			}
			if _, isFulfilled := b.fulfilled[de.To()]; isFulfilled {
				// This is bad, the dependency requires the source node to be
				// fulfilled in the same stage as the destination, which is impossible
				// in this case because the destination has already been fulfilled.
				return errors.AssertionFailedf("failed to satisfy %v->%v (%s) dependency",
					screl.NodeString(de.From()), screl.NodeString(de.To()), de.Name())
			}
			toCandidate, toIsCandidate := candidates[de.To()]
			if !toIsCandidate {
				// The candidate op edge is rejected because the dependency destination
				// node will not be fulfilled in this stage, due to the op edge leading
				// to it not being in the set of candidate op edges for this stage.
				//
				// As a result the dependency source node, which is also the candidate
				// edge destination node, cannot possibly be fulfilled in this stage
				// either.
				failed[e] = struct{}{}
				return iterutil.StopIteration()
			}
			_, toIsFailed := failed[toCandidate]
			if toIsFailed {
				// The candidate op edge is rejected because the dependency destination
				// node will not be fulfilled in this stage, due to the op edge leading
				// to it being a candidate op edge which has been rejected.
				//
				// As a result the dependency source node, which is also the candidate
				// edge destination node, cannot possibly be fulfilled in this stage
				// either.
				failed[e] = struct{}{}
				return iterutil.StopIteration()
			}
			if _, eIsFailed := failed[e]; eIsFailed {
				// If the candidate op edge has already been rejected due to unmet phase
				// or precedence requirements, then the op edge leading to the
				// dependency destination node must also be rejected because the
				// same-stage constraint cannot possibly be satisfied.
				failed[toCandidate] = struct{}{}
			}
			return nil
		}); err != nil {
			panic(err)
		}
	}
	return failed
}

func (b buildCtx) makeStage(filteredEdges []*scgraph.OpEdge) Stage {
	sort.SliceStable(filteredEdges,
		func(i, j int) bool {
			// Higher ranked edges should go first.
			return b.nodeRanks[filteredEdges[i].To()] > b.nodeRanks[filteredEdges[j].To()]
		})

	s := Stage{
		Before: b.state,
		After:  shallowCopy(b.state),
		Phase:  b.phase,
	}
	var ops []scop.Op
	for _, s.Revertible = range []bool{true, false} {
		if s.Revertible && !b.isRevertiblePreferred {
			continue
		}
		hasTransitions := false
		for _, e := range filteredEdges {
			for i, ts := range b.state.Nodes {
				if e.From() == ts && (!s.Revertible || e.Revertible()) {
					s.After.Nodes[i] = e.To()
					hasTransitions = true
					// If this edge has been marked as a no-op, then a state transition
					// can happen without executing anything.
					if !b.g.IsOpEdgeOptimizedOut(e) {
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
	s.Ops = scop.MakeOps(ops...)
	return s
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
