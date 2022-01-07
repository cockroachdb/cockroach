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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scgraph"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/errors"
)

// A Stage is a sequence of ops to be executed "together" as part of a schema
// change.
//
// stages also contain the state before and after the execution of the ops in
// the stage, reflecting the fact that any set of ops can be thought of as a
// transition from one state to another.
type Stage struct {
	// Before and After are the states before and after the stage gets executed.
	Before, After scpb.CurrentState

	// EdgeOps and ExtraOps are the collected ops in this stage:
	// - EdgeOps contains the ops originating from op-edges, that is to say, state
	//   transitions from the Before to the After states.
	// - ExtraOps contains ops unrelated to the stage states but which are
	//   required for the execution of the schema changer framework itself. This
	//   includes, notably, creating the schema changer job, updating its
	//   progress, and so forth.
	EdgeOps, ExtraOps []scop.Op

	// Phase describes the context in which the stage is to be executed: is it
	// during the statement's transaction? is it asynchronous, in the subsequent
	// schema changer job?
	Phase scop.Phase

	// Ordinal and StagesInPhase describe where the stage lies in the Phase, with
	// relation to the other stages in the same phase. Note that Ordinal starts
	// counting at 1. This is because this data is mainly useful for debugging.
	Ordinal, StagesInPhase int
}

// Type returns the type of the operations in this stage.
func (s Stage) Type() scop.Type {
	return s.EdgeOps[0].Type()
}

// Ops returns the operations in this stage.
func (s Stage) Ops() []scop.Op {
	ops := make([]scop.Op, 0, len(s.EdgeOps)+len(s.ExtraOps))
	ops = append(ops, s.EdgeOps...)
	ops = append(ops, s.ExtraOps...)
	return ops
}

// String returns a short string representation of this stage.
func (s Stage) String() string {
	ops := "no ops"
	if n := len(s.Ops()); n > 0 {
		ops = fmt.Sprintf("%d %s ops", n, s.Type())
	}
	return fmt.Sprintf("%s stage %d of %d with %s",
		s.Phase.String(), s.Ordinal, s.StagesInPhase, ops)
}

// ValidateStages checks that the plan is valid.
func ValidateStages(stages []Stage, g *scgraph.Graph) error {
	if len(stages) == 0 {
		return nil
	}

	// Check that each stage has internally-consistent states.
	for _, stage := range stages {
		if err := validateInternalStageStates(stage); err != nil {
			return errors.Wrapf(err, "%s", stage)
		}
	}

	// Check that the stage states all line up correctly from one to the next.
	for i := range stages {
		if i == 0 {
			continue
		}
		if err := validateAdjacentStagesStates(stages[i-1], stages[i]); err != nil {
			return errors.Wrapf(err, "stages %d and %d of %d", i, i+1, len(stages))
		}
	}

	// Check that the final state is valid.
	final := stages[len(stages)-1].After.Nodes
	for i, node := range final {
		if node.TargetStatus != node.Status {
			return errors.Errorf("final status is %s instead of %s at index %d for adding %+v",
				node.Status, node.TargetStatus, i, node.Element())
		}
	}

	// Check that phases are monotonically increasing.
	currentPhase := scop.EarliestPhase
	for _, stage := range stages {
		if stage.Phase < currentPhase {
			return errors.Errorf("%s: preceded by %s stage",
				stage.String(), currentPhase)
		}
	}

	// Check stage internal subgraph consistency.
	for _, stage := range stages {
		if err := validateStageSubgraph(stage, g); err != nil {
			return errors.Wrapf(err, "%s", stage.String())
		}
	}
	return nil
}

func validateInternalStageStates(stage Stage) error {
	before := stage.Before.Nodes
	after := stage.After.Nodes
	if na, nb := len(after), len(before); na != nb {
		return errors.Errorf("Before state has %d nodes and After state has %d nodes",
			nb, na)
	}
	for j := range before {
		beforeTarget, afterTarget := before[j].Target, after[j].Target
		if ea, eb := afterTarget.Element(), beforeTarget.Element(); ea != eb {
			return errors.Errorf("target at index %d has Before element %+v and After element %+v",
				j, eb, ea)
		}
		if ta, tb := afterTarget.TargetStatus, beforeTarget.TargetStatus; ta != tb {
			return errors.Errorf("target at index %d has Before status %s and After status %s",
				j, tb, ta)
		}
	}
	return nil
}

func validateAdjacentStagesStates(previous, next Stage) error {
	after := previous.After.Nodes
	before := next.Before.Nodes
	if na, nb := len(after), len(before); na != nb {
		return errors.Errorf("node count mismatch: %d != %d",
			na, nb)
	}
	for j, beforeNode := range before {
		afterNode := after[j]
		if sa, sb := afterNode.Status, beforeNode.Status; sa != sb {
			return errors.Errorf("node status mismatch at index %d: %s != %s",
				j, afterNode.Status.String(), beforeNode.Status.String())
		}
		if ta, tb := afterNode.TargetStatus, beforeNode.TargetStatus; ta != tb {
			return errors.Errorf("target status mismatch at index %d: %s != %s",
				j, ta.String(), tb.String())
		}
		if ea, eb := afterNode.Element(), beforeNode.Element(); ea != eb {
			return errors.Errorf("target element mismatch at index %d: %+v != %+v",
				j, ea, eb)
		}
	}
	return nil
}

func validateStageSubgraph(stage Stage, g *scgraph.Graph) error {
	// Transform the ops in a non-repeating sequence of their original op edges.
	var queue []*scgraph.OpEdge
	for _, op := range stage.EdgeOps {
		oe := g.GetOpEdgeFromOp(op)
		if oe == nil {
			// This shouldn't happen.
			return errors.Errorf("cannot find op edge for op %s", op)
		}
		if len(queue) == 0 || queue[len(queue)-1] != oe {
			queue = append(queue, oe)
		}
	}

	// Build the initial set of fulfilled nodes by traversing the graph
	// recursively and backwards.
	fulfilled := map[*scpb.Node]bool{}
	current := append([]*scpb.Node{}, stage.Before.Nodes...)
	{
		edgesTo := make(map[*scpb.Node][]scgraph.Edge, g.Order())
		_ = g.ForEachEdge(func(e scgraph.Edge) error {
			edgesTo[e.To()] = append(edgesTo[e.To()], e)
			return nil
		})
		var dfs func(n *scpb.Node)
		dfs = func(n *scpb.Node) {
			if _, found := fulfilled[n]; found {
				return
			}
			fulfilled[n] = true
			for _, e := range edgesTo[n] {
				dfs(e.From())
			}
		}
		for _, n := range current {
			dfs(n)
		}
	}

	// Check that the precedence constraints are satisfied by walking from the
	// initial state towards the final state of the stage.
	//
	// Outer loop of our state machine which attempts to progress towards the
	// final state.
	for hasProgressed := true; hasProgressed; {
		hasProgressed = false
		// Try to make progress for each target.
		for i, n := range current {
			if n.Status == stage.After.Nodes[i].Status {
				// We're done for this target.
				continue
			}
			oe, ok := g.GetOpEdgeFrom(n)
			if !ok {
				// This shouldn't happen.
				return errors.Errorf("cannot find op-edge path from %s to %s",
					screl.NodeString(stage.Before.Nodes[i]), screl.NodeString(stage.After.Nodes[i]))
			}

			// Prevent making progress on this target if there are unmet dependencies.
			var hasUnmetDeps bool
			if err := g.ForEachDepEdgeTo(oe.To(), func(de *scgraph.DepEdge) error {
				hasUnmetDeps = hasUnmetDeps || !fulfilled[de.From()]
				return nil
			}); err != nil {
				return err
			}
			if hasUnmetDeps {
				continue
			}

			// Prevent making progress on this target unless this op edge has been
			// marked as no-op or is the next in the queue.
			if len(queue) > 0 && oe == queue[0] {
				queue = queue[1:]
			} else if !g.IsNoOp(oe) {
				continue
			}

			current[i] = oe.To()
			fulfilled[oe.To()] = true
			hasProgressed = true
		}
	}
	// When we stop making progress we expect to have reached the After state.
	for i, n := range current {
		if n != stage.After.Nodes[i] {
			return errors.Errorf("internal inconsistency, ended in non-terminal node %s after walking the graph",
				screl.NodeString(stage.After.Nodes[i]))
		}
	}

	return nil
}
