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

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
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
func ValidateStages(stages []Stage) error {
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
