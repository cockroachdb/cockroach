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
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/scgraph"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/errors"
)

// BuildStages builds the plan's stages for this and all subsequent phases.
func BuildStages(
	init scpb.CurrentState, phase scop.Phase, g *scgraph.Graph, scJobIDSupplier func() jobspb.JobID,
) []Stage {
	newBuildState := func(isRevertibilityIgnored bool) *buildState {
		b := buildState{
			g:                      g,
			phase:                  phase,
			state:                  init.ShallowCopy(),
			fulfilled:              make(map[*scpb.Node]struct{}, g.Order()),
			scJobIDSupplier:        scJobIDSupplier,
			isRevertibilityIgnored: isRevertibilityIgnored,
		}
		for _, n := range init.Nodes {
			b.fulfilled[n] = struct{}{}
		}
		return &b
	}

	// Try building stages while ignoring revertibility constraints.
	// This is fine as long as there are no post-commit stages.
	stages := buildStages(newBuildState(true /* isRevertibilityIgnored */))
	if n := len(stages); n > 0 && stages[n-1].Phase > scop.PreCommitPhase {
		stages = buildStages(newBuildState(false /* isRevertibilityIgnored */))
	}
	return decorateStages(stages)
}

func buildStages(b *buildState) (stages []Stage) {
	// Build stages until reaching the terminal state.
	for !b.isStateTerminal(b.state) {
		// Generate a stage builder which can make progress.
		sb := b.makeStageBuilder()
		for !sb.canMakeProgress() {
			// When no further progress is possible, move to the next phase and try
			// again, until progress is possible. We haven't reached the terminal
			// state yet, so this is guaranteed (barring any horrible bugs).
			if b.phase == scop.PreCommitPhase {
				// This is a special case.
				// We need to move to the post-commit phase, but this will require
				// creating a schema changer job, which in turn will require this
				// otherwise-empty pre-commit stage.
				break
			}
			if b.phase == scop.LatestPhase {
				// This should never happen, we should always be able to make forward
				// progress because we haven't reached the terminal state yet.
				panic(errors.AssertionFailedf("unable to make progress"))
			}
			b.phase++
			sb = b.makeStageBuilder()
		}
		// Build the stage.
		stages = append(stages, sb.build())
		// Update the build state with this stage's progress.
		for n := range sb.fulfilling {
			b.fulfilled[n] = struct{}{}
		}
		b.state = sb.after()
		switch b.phase {
		case scop.StatementPhase, scop.PreCommitPhase:
			// These phases can only have at most one stage each.
			b.phase++
		}
	}
	return stages
}

// buildState contains the global build state for building the stages.
// Only the buildStages function mutates it, it's read-only everywhere else.
type buildState struct {
	g                      *scgraph.Graph
	scJobIDSupplier        func() jobspb.JobID
	isRevertibilityIgnored bool

	state     scpb.CurrentState
	phase     scop.Phase
	fulfilled map[*scpb.Node]struct{}
}

// isStateTerminal returns true iff the state is terminal, according to the
// graph.
func (b buildState) isStateTerminal(state scpb.CurrentState) bool {
	for _, n := range state.Nodes {
		if _, found := b.g.GetOpEdgeFrom(n); found {
			return false
		}
	}
	return true
}

// makeStageBuilder returns a stage builder with an operation type for which
// progress can be made. Defaults to the mutation type if none make progress.
func (b buildState) makeStageBuilder() (sb stageBuilder) {
	opTypes := []scop.Type{scop.BackfillType, scop.ValidationType, scop.MutationType}
	switch b.phase {
	case scop.StatementPhase, scop.PreCommitPhase:
		// We don't allow expensive operations pre-commit.
		opTypes = []scop.Type{scop.MutationType}
	}
	for _, opType := range opTypes {
		sb = b.makeStageBuilderForType(opType)
		if sb.canMakeProgress() {
			break
		}
	}
	return sb
}

// makeStageBuilderForType creates and populates a stage builder for the given
// op type.
func (b buildState) makeStageBuilderForType(opType scop.Type) stageBuilder {
	sb := stageBuilder{
		bs:         b,
		opType:     opType,
		current:    make([]currentTargetState, len(b.state.Nodes)),
		fulfilling: map[*scpb.Node]struct{}{},
	}
	for i, n := range b.state.Nodes {
		t := sb.makeCurrentTargetState(n)
		sb.current[i] = t
	}
	// Greedily try to make progress by going down op edges when possible.
	for isDone := false; !isDone; {
		isDone = true
		for i, t := range sb.current {
			if t.e == nil {
				continue
			}
			if sb.hasUnmetInboundDeps(t.e.To()) {
				continue
			}
			if sb.hasUnmeetableOutboundDeps(t.e.To()) {
				continue
			}
			sb.opEdges = append(sb.opEdges, t.e)
			sb.fulfilling[t.e.To()] = struct{}{}
			sb.current[i] = sb.nextTargetState(t)
			isDone = false
		}
	}
	return sb
}

// stageBuilder contains the state for building one stage.
type stageBuilder struct {
	bs         buildState
	opType     scop.Type
	current    []currentTargetState
	fulfilling map[*scpb.Node]struct{}
	opEdges    []*scgraph.OpEdge
}

type currentTargetState struct {
	n *scpb.Node
	e *scgraph.OpEdge

	// hasOpEdgeWithOps is true iff this stage already includes an op edge with
	// ops for this target.
	hasOpEdgeWithOps bool
}

func (sb stageBuilder) makeCurrentTargetState(n *scpb.Node) currentTargetState {
	e, found := sb.bs.g.GetOpEdgeFrom(n)
	if !found || !sb.isOutgoingOpEdgeAllowed(e) {
		return currentTargetState{n: n}
	}
	return currentTargetState{
		n:                n,
		e:                e,
		hasOpEdgeWithOps: !sb.bs.g.IsNoOp(e),
	}
}

// isOutgoingOpEdgeAllowed returns false iff there is something preventing using
// that op edge in this current stage.
func (sb stageBuilder) isOutgoingOpEdgeAllowed(e *scgraph.OpEdge) bool {
	if _, isFulfilled := sb.bs.fulfilled[e.To()]; isFulfilled {
		panic(errors.AssertionFailedf(
			"node %s is unexpectedly already fulfilled in a previous stage",
			screl.NodeString(e.To())))
	}
	if _, isCandidate := sb.fulfilling[e.To()]; isCandidate {
		panic(errors.AssertionFailedf(
			"node %s is unexpectedly already scheduled to be fulfilled in the upcoming stage",
			screl.NodeString(e.To())))
	}
	if e.Type() != sb.opType {
		return false
	}
	if !e.IsPhaseSatisfied(sb.bs.phase) {
		return false
	}
	if !sb.bs.isRevertibilityIgnored && sb.bs.phase == scop.PostCommitPhase && !e.Revertible() {
		return false
	}
	return true
}

// canMakeProgress returns true if the stage built by this builder will make
// progress.
func (sb stageBuilder) canMakeProgress() bool {
	return len(sb.opEdges) > 0
}

func (sb stageBuilder) nextTargetState(t currentTargetState) currentTargetState {
	next := sb.makeCurrentTargetState(t.e.To())
	if t.hasOpEdgeWithOps {
		if next.hasOpEdgeWithOps {
			// Prevent having more than one non-no-op op edge per target in a
			// stage. This upholds the 2-version invariant.
			// TODO(postamar): uphold the 2-version invariant using dep rules instead.
			next.e = nil
		} else {
			next.hasOpEdgeWithOps = true
		}
	}
	return next
}

func (sb stageBuilder) hasUnmetInboundDeps(n *scpb.Node) (ret bool) {
	_ = sb.bs.g.ForEachDepEdgeTo(n, func(de *scgraph.DepEdge) error {
		if sb.isUnmetInboundDep(de) {
			ret = true
			return iterutil.StopIteration()
		}
		return nil
	})
	return ret
}

func (sb *stageBuilder) isUnmetInboundDep(de *scgraph.DepEdge) bool {
	_, fromIsFulfilled := sb.bs.fulfilled[de.From()]
	_, fromIsCandidate := sb.fulfilling[de.From()]
	switch de.Kind() {
	case scgraph.Precedence:
		// True iff the source node has not been fulfilled in an earlier stage
		// and also iff it's not (yet?) scheduled to be fulfilled in this stage.
		return !fromIsFulfilled && !fromIsCandidate

	case scgraph.SameStagePrecedence:
		if fromIsFulfilled {
			// The dependency requires the source node to be fulfilled in the same
			// stage as the destination, which is impossible at this point because
			// it has already been fulfilled in an earlier stage.
			// This should never happen.
			break
		}
		// True iff the source node has not been fulfilled in an earlier stage and
		// and also iff it's not (yet?) scheduled to be fulfilled in this stage.
		return !fromIsCandidate

	default:
		panic(errors.AssertionFailedf("unknown dep edge kind %q", de.Kind()))
	}
	// The dependency constraint is somehow unsatisfiable.
	panic(errors.AssertionFailedf("failed to satisfy %s rule %q",
		de.String(), de.Name()))
}

func (sb stageBuilder) hasUnmeetableOutboundDeps(n *scpb.Node) (ret bool) {
	candidates := make(map[*scpb.Node]int, len(sb.current))
	for i, t := range sb.current {
		if t.e != nil {
			candidates[t.e.To()] = i
		}
	}
	visited := make(map[*scpb.Node]bool)
	var visit func(n *scpb.Node)
	visit = func(n *scpb.Node) {
		if ret || visited[n] {
			return
		}
		visited[n] = true
		if _, isFulfilled := sb.bs.fulfilled[n]; isFulfilled {
			// This should never happen.
			panic(errors.AssertionFailedf("%s should not yet be fulfilled",
				screl.NodeString(n)))
		}
		if _, isFulfilling := sb.bs.fulfilled[n]; isFulfilling {
			// This should never happen.
			panic(errors.AssertionFailedf("%s should not yet be scheduled for this stage",
				screl.NodeString(n)))
		}
		if _, isCandidate := candidates[n]; !isCandidate {
			ret = true
			return
		}
		_ = sb.bs.g.ForEachDepEdgeTo(n, func(de *scgraph.DepEdge) error {
			if ret {
				return iterutil.StopIteration()
			}
			if visited[de.From()] {
				return nil
			}
			if !sb.isUnmetInboundDep(de) {
				return nil
			}
			if de.Kind() == scgraph.SameStagePrecedence {
				visit(de.From())
			} else {
				ret = true
			}
			return nil
		})
		_ = sb.bs.g.ForEachDepEdgeFrom(n, func(de *scgraph.DepEdge) error {
			if ret {
				return iterutil.StopIteration()
			}
			if visited[de.To()] {
				return nil
			}
			if de.Kind() == scgraph.SameStagePrecedence {
				visit(de.To())
			}
			return nil
		})
	}
	visit(n)
	return ret
}

func (sb stageBuilder) build() Stage {
	s := Stage{
		Before: sb.bs.state,
		After:  sb.after(),
		Phase:  sb.bs.phase,
	}
	for _, e := range sb.opEdges {
		if sb.bs.g.IsNoOp(e) {
			continue
		}
		s.EdgeOps = append(s.EdgeOps, e.Op()...)
	}
	// Decorate stage with job-related operations.
	// TODO(ajwerner): Rather than adding this above the opgen layer, it'd be
	// better to do it as part of graph generation. We could treat the job as
	// and the job references as elements and track their relationships. The
	// oddity here is that the job gets both added and removed. In practice, this
	// may prove to be a somewhat common pattern in other cases: consider the
	// intermediate index needed when adding and dropping columns as part of the
	// same transaction.
	switch s.Phase {
	case scop.PreCommitPhase:
		// If this pre-commit stage is non-terminal, this means there will be at
		// least one post-commit stage, so we need to create a schema changer job
		// and update references for the affected descriptors.
		if !sb.bs.isStateTerminal(s.After) {
			s.ExtraOps = append(sb.addJobReferenceOps(s.After), sb.createSchemaChangeJobOp(s.After))
		}
	case scop.PostCommitPhase, scop.PostCommitNonRevertiblePhase:
		if sb.opType == scop.MutationType {
			if sb.bs.isStateTerminal(s.After) {
				// The terminal mutation stage needs to remove references to the schema
				// changer job in the affected descriptors.
				s.ExtraOps = sb.removeJobReferenceOps(s.After)
			}
			// Post-commit mutation stages all update the progress of the schema
			// changer job.
			s.ExtraOps = append(s.ExtraOps, sb.updateJobProgressOp(s.After))
		}
	}
	return s
}

func (sb stageBuilder) after() scpb.CurrentState {
	state := sb.bs.state.ShallowCopy()
	for i, t := range sb.current {
		state.Nodes[i] = t.n
	}
	return state
}

func (sb stageBuilder) createSchemaChangeJobOp(state scpb.CurrentState) scop.Op {
	return &scop.CreateDeclarativeSchemaChangerJob{
		JobID:       sb.bs.scJobIDSupplier(),
		TargetState: state.DeepCopy().TargetState,
		Statuses:    state.Statuses(),
	}
}

func (sb stageBuilder) addJobReferenceOps(state scpb.CurrentState) []scop.Op {
	jobID := sb.bs.scJobIDSupplier()
	return generateOpsForJobIDs(
		screl.GetDescIDs(state.TargetState),
		jobID,
		func(descID descpb.ID, id jobspb.JobID) scop.Op {
			return &scop.AddJobReference{DescriptorID: descID, JobID: jobID}
		},
	)
}

func (sb stageBuilder) updateJobProgressOp(state scpb.CurrentState) scop.Op {
	return &scop.UpdateSchemaChangerJob{
		JobID:           sb.bs.scJobIDSupplier(),
		Statuses:        state.Statuses(),
		IsNonCancelable: sb.bs.phase >= scop.PostCommitNonRevertiblePhase,
	}
}

func (sb stageBuilder) removeJobReferenceOps(state scpb.CurrentState) []scop.Op {
	jobID := sb.bs.scJobIDSupplier()
	return generateOpsForJobIDs(
		screl.GetDescIDs(state.TargetState),
		jobID,
		func(descID descpb.ID, id jobspb.JobID) scop.Op {
			return &scop.RemoveJobReference{DescriptorID: descID, JobID: jobID}
		},
	)
}

func generateOpsForJobIDs(
	descIDs []descpb.ID, jobID jobspb.JobID, f func(descID descpb.ID, id jobspb.JobID) scop.Op,
) []scop.Op {
	ops := make([]scop.Op, len(descIDs))
	for i, descID := range descIDs {
		ops[i] = f(descID, jobID)
	}
	return ops
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
