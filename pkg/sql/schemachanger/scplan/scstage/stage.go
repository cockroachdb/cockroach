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

// ValidateStages checks that the plan is valid.
func ValidateStages(stages []Stage, g *scgraph.Graph) error {
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

	revertibleAllowed := true
	for _, stage := range stages {
		// Check that revertibility is monotonically decreasing.
		if !stage.Revertible {
			revertibleAllowed = false
		}
		if stage.Revertible && !revertibleAllowed {
			return errors.Errorf("%s: preceded by non-revertible stage",
				stage.String())
		}

		// Check stage internal consistency.
		if err := validateStage(stage, g); err != nil {
			return errors.Wrapf(err, "%s", stage.String())
		}
	}
	return nil
}

func validateStage(stage Stage, g *scgraph.Graph) error {
	// Transform the ops in a non-repeating sequence of their original op edges.
	var queue []*scgraph.OpEdge
	if stage.Ops != nil {
		for _, op := range stage.Ops.Slice() {
			oe := g.GetOpEdgeFromOp(op)
			if oe == nil {
				continue
			}
			if len(queue) == 0 || queue[len(queue)-1] != oe {
				queue = append(queue, oe)
			}
		}
	}

	// Check that the precedence constraints are satisfied by walking from the
	// initial state towards the final state of the stage.
	fulfilled := map[*scpb.Node]bool{}
	current := append([]*scpb.Node{}, stage.Before.Nodes...)
	for _, n := range current {
		fulfilled[n] = true
	}
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
				return errors.Errorf("%s: cannot find op-edge path from %s to %s",
					stage, screl.NodeString(stage.Before.Nodes[i]), screl.NodeString(stage.After.Nodes[i]))
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
			// optimized away or is the next in the queue.
			if len(queue) > 0 && oe == queue[0] {
				queue = queue[1:]
			} else if !g.IsOpEdgeOptimizedOut(oe) {
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
			return errors.Errorf("%s: internal inconsistency, expected %s after walking the graph in direction %s, ended in %s",
				stage, screl.NodeString(stage.After.Nodes[i]), n.Direction.String(), n.Status)
		}
	}

	return nil
}
