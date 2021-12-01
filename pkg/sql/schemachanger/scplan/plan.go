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
	"fmt"
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
	stages  []Stage
}

// StagesForCurrentPhase returns the stages in the execution phase specified in
// the plan params.
func (p Plan) StagesForCurrentPhase() []Stage {
	for i, s := range p.stages {
		if s.Phase > p.Params.ExecutionPhase {
			return p.stages[:i]
		}
	}
	return p.stages
}

// StagesForAllPhases returns the stages in the execution phase specified in
// the plan params and also all subsequent stages.
func (p Plan) StagesForAllPhases() []Stage {
	return p.stages
}

// A Stage is a sequence of ops to be executed "together" as part of a schema
// change.
//
// stages also contain the state before and after the execution of the ops in
// the stage, reflecting the fact that any set of ops can be thought of as a
// transition from one state to another.
type Stage struct {
	Before, After          scpb.State
	Ops                    scop.Ops
	Revertible             bool
	Phase                  scop.Phase
	Ordinal, StagesInPhase int
}

func (s Stage) String() string {
	revertible := ""
	if !s.Revertible {
		revertible = "non-revertible "
	}
	if s.Ops == nil {
		return fmt.Sprintf("%s %sstage %d of %d with no ops",
			s.Phase.String(), revertible, s.Ordinal, s.StagesInPhase)
	}
	return fmt.Sprintf("%s %sstage %d of %d with %d %s ops",
		s.Phase.String(), revertible, s.Ordinal, s.StagesInPhase, len(s.Ops.Slice()), s.Ops.Type().String())
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

	p.stages = buildStages(initial, params.ExecutionPhase, optimizedGraph)
	if err = validateStages(p.stages); err != nil {
		return p, errors.WithAssertionFailure(errors.Wrapf(err, "invalid basic execution plan"))
	}
	p.stages = collapseStages(p.stages)
	if err = validateStages(p.stages); err != nil {
		return p, errors.WithAssertionFailure(errors.Wrapf(err, "invalid improved execution plan"))
	}
	return p, nil
}

// validateStages checks that the plan is valid.
func validateStages(stages []Stage) error {
	if len(stages) == 0 {
		return nil
	}

	// Check that the terminal node statuses are valid for their target direction.
	initial := stages[0].Before.Nodes
	final := stages[len(stages)-1].After.Nodes
	if nInitial, nFinal := len(initial), len(final); nInitial != nFinal {
		return errors.Errorf("initial state has %d nodes, final state has %d", nInitial, nFinal)
	}
	for i, initialNode := range initial {
		finalNode := final[i]
		initialTarget, finalTarget := initialNode.Target, finalNode.Target
		if initialTarget != finalTarget {
			return errors.Errorf("target mismatch between initial and final nodes at index %d: %s != %s",
				i, initialTarget.String(), finalTarget.String())
		}

		e := initialTarget.Element()
		switch initialTarget.Direction {
		case scpb.Target_ADD:
			if finalNode.Status != scpb.Status_PUBLIC {
				return errors.Errorf("final status is %s instead of public at index %d for adding %T %s",
					finalNode.Status, i, e, e)
			}
		case scpb.Target_DROP:
			if finalNode.Status != scpb.Status_ABSENT {
				return errors.Errorf("final status is %s instead of absent at index %d for dropping %T %s",
					finalNode.Status, i, e, e)
			}
		}
	}

	// Check that phases are monotonically increasing.
	currentPhase := scop.StatementPhase
	for _, stage := range stages {
		if stage.Phase < currentPhase {
			return errors.Errorf("%s: preceded by %s stage",
				stage.String(), currentPhase)
		}
	}

	// Check that revertibility is monotonically decreasing.
	revertibleAllowed := true
	for _, stage := range stages {
		if !stage.Revertible {
			revertibleAllowed = false
		}
		if stage.Revertible && !revertibleAllowed {
			return errors.Errorf("%s: preceded by non-revertible stage",
				stage.String())
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

// collapseStages collapses stages with no ops together whenever possible.
func collapseStages(stages []Stage) (collapsedStages []Stage) {
	if len(stages) <= 1 {
		return stages
	}

	accumulator := stages[0]
	for _, s := range stages[1:] {
		if isCollapsible(accumulator, s) {
			accumulator.After = s.After
			accumulator.Revertible = s.Revertible
			if s.Ops != nil {
				accumulator.Ops = s.Ops
			} else if accumulator.Ops == nil {
				panic("missing ops")
			}
		} else {
			collapsedStages = append(collapsedStages, accumulator)
			accumulator = s
		}
	}
	collapsedStages = append(collapsedStages, accumulator)
	return decorateStages(collapsedStages)
}

// decorateStages decorates stages with position in plan.
func decorateStages(stages []Stage) []Stage {
	if len(stages) == 0 {
		return nil
	}
	phaseMap := map[scop.Phase][]int{}
	for i, s := range stages {
		phaseMap[s.Phase] = append(phaseMap[s.Phase], i)
	}
	for _, indexes := range phaseMap {
		for i, j := range indexes {
			s := &stages[j]
			s.Ordinal = i + 1
			s.StagesInPhase = len(indexes)
		}
	}
	return stages
}

// isCollapsible returns true iff s can be collapsed into acc. The collapsed
// state will have:
//   - the Before state from acc,
//   - the After state from s,
//   - the Revertible flag from s.
//
// The collapse rules are:
// 1. Both states must be in the same phase.
// 2. If acc doesn't have ops and s does, we can collapse acc and s together.
//    This means we lose the ability to revert the acc state, but since it
//    doesn't have any ops this effectively doesn't matter.
// 3. If s doesn't have ops, then it can be collapsed into acc. However,
//    unlike the previous case, assuming acc is revertible, we don't want to
//    lose that property when s isn't revertible.
func isCollapsible(acc Stage, s Stage) bool {
	if acc.Phase != s.Phase {
		return false
	}
	// At this point acc and s are in the same phase, we can consider collapsing.
	if acc.Ops == nil {
		return true
	}
	if s.Ops != nil {
		return false
	}
	// At this point, acc has ops, s has none, and they are in the same phase.
	// From now on, collapsibility will be determined by revertibility.
	if !acc.Revertible {
		// If acc is not revertible, then nothing after can be anyway.
		return true
	}
	// If acc is revertible, only collapse no-op stage s into it if it's also
	// revertible, otherwise acc will be made non-revertible. We don't want that
	// because it has ops and s doesn't.
	return s.Revertible
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
