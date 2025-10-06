// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scstage

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/scgraph"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// A Stage is a sequence of ops to be executed "together" as part of a schema
// change. Stages also contain the statuses before and after the execution of
// the ops in the stage, reflecting the fact that any set of ops can be thought
// of as a transition from one state to another.
type Stage struct {
	// Before and After are the states before and after the stage gets executed.
	Before, After []scpb.Status

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

var _ redact.SafeFormatter = Stage{}

func (s Stage) SafeFormat(p redact.SafePrinter, _ rune) {
	p.Printf("%s stage %d of %d with ",
		s.Phase, s.Ordinal, s.StagesInPhase)
	if n := len(s.Ops()); n > 1 {
		p.Printf("%d %s ops", n, s.Type())
	} else if n == 1 {
		p.Printf("1 %s op", s.Type())
	} else {
		p.Printf("no ops")
	}
}

// Type returns the type of the operations in this stage.
func (s Stage) Type() scop.Type {
	if len(s.EdgeOps) == 0 {
		return scop.MutationType
	}
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
	if n := len(s.Ops()); n > 1 {
		ops = fmt.Sprintf("%d %s ops", n, s.Type())
	} else if n == 1 {
		ops = fmt.Sprintf("1 %s op", s.Type())
	}
	return fmt.Sprintf("%s stage %d of %d with %s",
		s.Phase.String(), s.Ordinal, s.StagesInPhase, ops)
}

// IsResetPreCommitStage returns true iff this is the first stage in the
// pre-commit phase, also known as the reset stage, which is special in that
// the elements transition _away_ from their targets instead of towards them.
func (s Stage) IsResetPreCommitStage() bool {
	return s.Phase == scop.PreCommitPhase && s.Ordinal == 1
}

// ValidateStages checks that the plan is valid.
func ValidateStages(ts scpb.TargetState, stages []Stage, g *scgraph.Graph) error {
	if len(stages) == 0 {
		return nil
	}

	// Check that each stage has internally-consistent states.
	for _, stage := range stages {
		if na, nb := len(stage.After), len(stage.Before); na != nb {
			return errors.Errorf("%s: Before state has %d nodes and After state has %d nodes",
				stage, nb, na)
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
	final := stages[len(stages)-1].After
	for i, actual := range final {
		expected := ts.Targets[i].TargetStatus
		if actual != expected {
			return errors.Errorf("final status is %s instead of %s at index %d for adding %s",
				actual, expected, i, screl.ElementString(ts.Targets[i].Element()))
		}
	}

	// Check that phases are monotonically increasing.
	currentPhase := scop.EarliestPhase
	for _, stage := range stages {
		if stage.Phase < currentPhase {
			return errors.Errorf("%s: preceded by %s stage",
				stage.String(), currentPhase)
		}
		currentPhase = stage.Phase
	}

	// Check stage internal subgraph consistency.
	for _, stage := range stages {
		if err := validateStageSubgraph(ts, stage, g); err != nil {
			return errors.Wrapf(err, "%s", stage.String())
		}
	}
	return nil
}

func validateAdjacentStagesStates(previous, next Stage) error {
	if na, nb := len(previous.After), len(next.Before); na != nb {
		return errors.Errorf("node count mismatch: %d != %d",
			na, nb)
	}
	for j, before := range next.Before {
		after := previous.After[j]
		if before != after {
			return errors.Errorf("node status mismatch at index %d: %s != %s",
				j, after.String(), before.String())
		}
	}
	return nil
}

func validateStageSubgraph(ts scpb.TargetState, stage Stage, g *scgraph.Graph) error {
	if stage.IsResetPreCommitStage() {
		// Ignore the reset stage, which is the only one where we travel backwards
		// in the graph.
		return nil
	}

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

	type beforeOrDuring int
	const (
		_ beforeOrDuring = iota
		before
		during
	)

	// Build the initial set of fulfilled nodes by traversing the graph
	// recursively and backwards.
	fulfilled := map[*screl.Node]beforeOrDuring{}
	current := make([]*screl.Node, len(ts.Targets))
	for i, status := range stage.Before {
		t := &ts.Targets[i]
		n, ok := g.GetNode(t, status)
		if !ok {
			// This shouldn't happen.
			return errors.Errorf("cannot find starting node for %s", screl.ElementString(t.Element()))
		}
		current[i] = n
	}
	for _, n := range current {
		for {
			fulfilled[n] = before
			oe, ok := g.GetOpEdgeTo(n)
			if !ok {
				break
			}
			n = oe.From()
		}
	}

	if stage.Phase == scop.StatementPhase {
		// We can't validate the statement phase stages more deeply because
		// the stage only contains a subset of the ops that are otherwise
		// on its corresponding op-edges.
		return nil
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
			if n.CurrentStatus == stage.After[i] {
				// We're done for this target.
				continue
			}
			oe, ok := g.GetOpEdgeFrom(n)
			if !ok {
				// This shouldn't happen.
				return errors.Errorf("cannot find op-edge path from %s to %s for %s",
					stage.Before[i], stage.After[i], screl.ElementString(ts.Targets[i].Element()))
			}

			// Prevent making progress on this target if there are unmet dependencies.
			var hasUnmetDeps bool
			if err := g.ForEachDepEdgeTo(oe.To(), func(de *scgraph.DepEdge) error {
				if _, isFulfilled := fulfilled[de.From()]; !isFulfilled {
					hasUnmetDeps = true
				}
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
			}

			current[i] = oe.To()
			fulfilled[oe.To()] = during
			hasProgressed = true

			// Check same-stage or previous-stage constraints.
			if err := g.ForEachDepEdgeTo(oe.To(), func(de *scgraph.DepEdge) error {
				switch fulfilled[de.From()] {
				case before:
					if de.Kind() == scgraph.SameStagePrecedence {
						return errors.Errorf("%s not reached in same stage as %s, violates rule in %s",
							de.From(), oe.To(), de.RuleNames())
					}
				case during:
					if de.Kind() == scgraph.PreviousStagePrecedence || de.Kind() == scgraph.PreviousTransactionPrecedence {
						return errors.Errorf("%s reached in same stage as %s, violates rule in %s",
							de.From(), oe.To(), de.RuleNames())
					}
				}
				return nil
			}); err != nil {
				return err
			}
		}
	}
	// When we stop making progress we expect to have reached the After state.
	for i, n := range current {
		if n.CurrentStatus != stage.After[i] {
			return errors.Errorf("internal inconsistency, "+
				"element %s targets %s and should transition from %s to %s in this stage, "+
				"but walking the graph blocks at %s",
				screl.ElementString(ts.Targets[i].Element()),
				ts.Targets[i].TargetStatus,
				stage.Before[i],
				stage.After[i],
				n.CurrentStatus,
			)
		}
	}

	return nil
}
