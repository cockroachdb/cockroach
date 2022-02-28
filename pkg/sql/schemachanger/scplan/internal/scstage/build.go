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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/scgraph"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/errors"
)

// BuildStages builds the plan's stages for this and all subsequent phases.
// Note that the scJobIDSupplier function is idempotent, and must return the
// same value for all calls.
func BuildStages(
	init scpb.CurrentState, phase scop.Phase, g *scgraph.Graph, scJobIDSupplier func() jobspb.JobID,
) []Stage {
	c := buildContext{
		rollback:               init.InRollback,
		g:                      g,
		scJobIDSupplier:        scJobIDSupplier,
		isRevertibilityIgnored: true,
		targetState:            init.TargetState,
		startingStatuses:       init.Current,
		startingPhase:          phase,
	}

	// Try building stages while ignoring revertibility constraints.
	// This is fine as long as there are no post-commit stages.
	stages := buildStages(c)
	if n := len(stages); n > 0 && stages[n-1].Phase > scop.PreCommitPhase {
		c.isRevertibilityIgnored = false
		stages = buildStages(c)
	}
	return decorateStages(stages)
}

// buildContext contains the global constants for building the stages.
// Only the BuildStages function mutates it, it's read-only everywhere else.
type buildContext struct {
	rollback               bool
	g                      *scgraph.Graph
	scJobIDSupplier        func() jobspb.JobID
	isRevertibilityIgnored bool
	targetState            scpb.TargetState
	startingStatuses       []scpb.Status
	startingPhase          scop.Phase
}

func buildStages(bc buildContext) (stages []Stage) {
	// Initialize the build state for this buildContext.
	bs := buildState{
		incumbent: make([]scpb.Status, len(bc.startingStatuses)),
		phase:     bc.startingPhase,
		fulfilled: make(map[*screl.Node]struct{}, bc.g.Order()),
	}
	for i, n := range bc.nodes(bc.startingStatuses) {
		bs.incumbent[i] = n.CurrentStatus
		bs.fulfilled[n] = struct{}{}
	}
	// Build stages until reaching the terminal state.
	for !bc.isStateTerminal(bs.incumbent) {
		// Generate a stage builder which can make progress.
		sb := bc.makeStageBuilder(bs)
		for !sb.canMakeProgress() {
			// When no further progress is possible, move to the next phase and try
			// again, until progress is possible. We haven't reached the terminal
			// state yet, so this is guaranteed (barring any horrible bugs).
			if bs.phase == scop.PreCommitPhase {
				// This is a special case.
				// We need to move to the post-commit phase, but this will require
				// creating a schema changer job, which in turn will require this
				// otherwise-empty pre-commit stage.
				break
			}
			if bs.phase == scop.LatestPhase {
				// This should never happen, we should always be able to make forward
				// progress because we haven't reached the terminal state yet.
				panic(errors.AssertionFailedf("unable to make progress"))
			}
			bs.phase++
			sb = bc.makeStageBuilder(bs)
		}
		// Build the stage.
		stage := sb.build()
		stages = append(stages, stage)
		// Update the build state with this stage's progress.
		for n := range sb.fulfilling {
			bs.fulfilled[n] = struct{}{}
		}
		bs.incumbent = stage.After
		switch bs.phase {
		case scop.StatementPhase, scop.PreCommitPhase:
			// These phases can only have at most one stage each.
			bs.phase++
		}
	}
	return stages
}

// buildState contains the global build state for building the stages.
// Only the buildStages function mutates it, it's read-only everywhere else.
type buildState struct {
	incumbent []scpb.Status
	phase     scop.Phase
	fulfilled map[*screl.Node]struct{}
}

// isStateTerminal returns true iff the state is terminal, according to the
// graph.
func (bc buildContext) isStateTerminal(current []scpb.Status) bool {
	for _, n := range bc.nodes(current) {
		if _, found := bc.g.GetOpEdgeFrom(n); found {
			return false
		}
	}
	return true
}

// makeStageBuilder returns a stage builder with an operation type for which
// progress can be made. Defaults to the mutation type if none make progress.
func (bc buildContext) makeStageBuilder(bs buildState) (sb stageBuilder) {
	opTypes := []scop.Type{scop.BackfillType, scop.ValidationType, scop.MutationType}
	switch bs.phase {
	case scop.StatementPhase, scop.PreCommitPhase:
		// We don't allow expensive operations pre-commit.
		opTypes = []scop.Type{scop.MutationType}
	}
	for _, opType := range opTypes {
		sb = bc.makeStageBuilderForType(bs, opType)
		if sb.canMakeProgress() {
			break
		}
	}
	return sb
}

// makeStageBuilderForType creates and populates a stage builder for the given
// op type.
func (bc buildContext) makeStageBuilderForType(bs buildState, opType scop.Type) stageBuilder {
	sb := stageBuilder{
		bc:         bc,
		bs:         bs,
		opType:     opType,
		current:    make([]currentTargetState, len(bc.targetState.Targets)),
		fulfilling: map[*screl.Node]struct{}{},
	}
	for i, n := range bc.nodes(bs.incumbent) {
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
	bc         buildContext
	bs         buildState
	opType     scop.Type
	current    []currentTargetState
	fulfilling map[*screl.Node]struct{}
	opEdges    []*scgraph.OpEdge
}

type currentTargetState struct {
	n *screl.Node
	e *scgraph.OpEdge

	// hasOpEdgeWithOps is true iff this stage already includes an op edge with
	// ops for this target.
	hasOpEdgeWithOps bool
}

func (sb stageBuilder) makeCurrentTargetState(n *screl.Node) currentTargetState {
	e, found := sb.bc.g.GetOpEdgeFrom(n)
	if !found || !sb.isOutgoingOpEdgeAllowed(e) {
		return currentTargetState{n: n}
	}
	return currentTargetState{
		n:                n,
		e:                e,
		hasOpEdgeWithOps: !sb.bc.g.IsNoOp(e),
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
	if !sb.bc.isRevertibilityIgnored && sb.bs.phase == scop.PostCommitPhase && !e.Revertible() {
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

func (sb stageBuilder) hasUnmetInboundDeps(n *screl.Node) (ret bool) {
	_ = sb.bc.g.ForEachDepEdgeTo(n, func(de *scgraph.DepEdge) error {
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

func (sb stageBuilder) hasUnmeetableOutboundDeps(n *screl.Node) (ret bool) {
	candidates := make(map[*screl.Node]int, len(sb.current))
	for i, t := range sb.current {
		if t.e != nil {
			candidates[t.e.To()] = i
		}
	}
	visited := make(map[*screl.Node]bool)
	var visit func(n *screl.Node)
	visit = func(n *screl.Node) {
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
		_ = sb.bc.g.ForEachDepEdgeTo(n, func(de *scgraph.DepEdge) error {
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
		_ = sb.bc.g.ForEachDepEdgeFrom(n, func(de *scgraph.DepEdge) error {
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
	after := make([]scpb.Status, len(sb.current))
	for i, t := range sb.current {
		after[i] = t.n.CurrentStatus
	}
	s := Stage{
		Before: sb.bs.incumbent,
		After:  after,
		Phase:  sb.bs.phase,
	}
	for _, e := range sb.opEdges {
		if sb.bc.g.IsNoOp(e) {
			continue
		}
		s.EdgeOps = append(s.EdgeOps, e.Op()...)
	}
	jobOps := sb.computeExtraJobOps(s, after)
	s.ExtraOps = jobOps
	return s
}

// Decorate stage with job-related operations.
//
// TODO(ajwerner): Rather than adding this above the opgen layer, it may be
// better to do it as part of graph generation. We could treat the job as
// and the job references as elements and track their relationships. The
// oddity here is that the job gets both added and removed. In practice, this
// may prove to be a somewhat common pattern in other cases: consider the
// intermediate index needed when adding and dropping columns as part of the
// same transaction.
func (sb stageBuilder) computeExtraJobOps(s Stage, after []scpb.Status) []scop.Op {
	switch s.Phase {
	case scop.PreCommitPhase:
		// If this pre-commit stage is non-terminal, this means there will be at
		// least one post-commit stage, so we need to create a schema changer job
		// and update references for the affected descriptors.
		if !sb.bc.isStateTerminal(after) {
			const initialize = true
			return append(sb.bc.setJobStateOnDescriptorOps(initialize, after),
				sb.bc.createSchemaChangeJobOp())
		}
		return nil
	case scop.PostCommitPhase, scop.PostCommitNonRevertiblePhase:
		if sb.opType != scop.MutationType {
			return nil
		}
		var ops []scop.Op
		if sb.bc.isStateTerminal(after) {
			// The terminal mutation stage needs to remove references to the schema
			// changer job in the affected descriptors.
			ops = sb.bc.removeJobReferenceOps()
		} else {
			const initialize = false
			ops = sb.bc.setJobStateOnDescriptorOps(initialize, after)
		}
		// If we just moved to a non-cancelable phase, we need to tell the job
		// that it cannot be canceled. Ideally we'd do this just once.
		//
		// TODO(ajwerner): Track the previous stage's phase so we can know when
		// we need to emit this op. Also, this is buggy, we need to mark the job
		// as non-cancelable *before* we start to execute the first NonRevertible
		// phase.
		ops = append(ops, sb.bc.updateJobProgressOp(s.Phase > scop.PostCommitPhase))
		return ops
	default:
		return nil
	}
}

func (bc buildContext) createSchemaChangeJobOp() scop.Op {
	return &scop.CreateSchemaChangerJob{
		JobID:         bc.scJobIDSupplier(),
		Statements:    bc.targetState.Statements,
		Authorization: bc.targetState.Authorization,
		DescriptorIDs: screl.AllTargetDescIDs(bc.targetState).Ordered(),
	}
}

func (bc buildContext) updateJobProgressOp(isNonCancellable bool) scop.Op {
	return &scop.UpdateSchemaChangerJob{
		JobID:           bc.scJobIDSupplier(),
		IsNonCancelable: isNonCancellable,
	}
}

func (bc buildContext) removeJobReferenceOps() (ops []scop.Op) {
	jobID := bc.scJobIDSupplier()
	screl.AllTargetDescIDs(bc.targetState).ForEach(func(descID descpb.ID) {
		ops = append(ops, &scop.RemoveJobStateFromDescriptor{
			DescriptorID: descID,
			JobID:        jobID,
		})
	})
	return ops
}

func (bc buildContext) nodes(current []scpb.Status) []*screl.Node {
	nodes := make([]*screl.Node, len(bc.targetState.Targets))
	for i, status := range current {
		t := &bc.targetState.Targets[i]
		n, ok := bc.g.GetNode(t, status)
		if !ok {
			panic(errors.AssertionFailedf("could not find node for element %s, target status %s, current status %s",
				screl.ElementString(t.Element()), t.TargetStatus, status))
		}
		nodes[i] = n
	}
	return nodes
}

func (bc buildContext) setJobStateOnDescriptorOps(initialize bool, after []scpb.Status) []scop.Op {
	descIDs, states := makeDescriptorStates(
		bc.scJobIDSupplier(), bc.rollback, bc.targetState, after,
	)
	ops := make([]scop.Op, 0, descIDs.Len())
	descIDs.ForEach(func(descID descpb.ID) {
		ops = append(ops, &scop.SetJobStateOnDescriptor{
			DescriptorID: descID,
			Initialize:   initialize,
			State:        *states[descID],
		})
	})
	return ops
}

func makeDescriptorStates(
	jobID jobspb.JobID, inRollback bool, ts scpb.TargetState, statuses []scpb.Status,
) (catalog.DescriptorIDSet, map[descpb.ID]*scpb.DescriptorState) {
	descIDs := screl.AllTargetDescIDs(ts)
	states := make(map[descpb.ID]*scpb.DescriptorState, descIDs.Len())
	descIDs.ForEach(func(id descpb.ID) {
		states[id] = &scpb.DescriptorState{
			Authorization: ts.Authorization,
			JobID:         jobID,
		}
	})
	noteRelevantStatement := func(state *scpb.DescriptorState, stmtRank uint32) {
		for i := range state.RelevantStatements {
			stmt := &state.RelevantStatements[i]
			if stmt.StatementRank != stmtRank {
				continue
			}
			return
		}
		state.RelevantStatements = append(state.RelevantStatements,
			scpb.DescriptorState_Statement{
				Statement:     ts.Statements[stmtRank],
				StatementRank: stmtRank,
			})
	}
	for i, t := range ts.Targets {
		descID := screl.GetDescID(t.Element())
		state := states[descID]
		stmtID := t.Metadata.StatementID
		noteRelevantStatement(state, stmtID)
		state.Targets = append(state.Targets, t)
		state.TargetRanks = append(state.TargetRanks, uint32(i))
		state.CurrentStatuses = append(state.CurrentStatuses, statuses[i])
		state.InRollback = inRollback
	}
	return descIDs, states
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
