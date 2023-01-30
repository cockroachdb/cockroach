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
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/rules/current"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/scgraph"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/errors"
)

// BuildStages builds the plan's stages for this and all subsequent phases.
// Note that the scJobIDSupplier function is idempotent, and must return the
// same value for all calls.
func BuildStages(
	ctx context.Context,
	init scpb.CurrentState,
	phase scop.Phase,
	g *scgraph.Graph,
	scJobIDSupplier func() jobspb.JobID,
	enforcePlannerSanityChecks bool,
) []Stage {
	c := buildContext{
		rollback:                   init.InRollback,
		g:                          g,
		isRevertibilityIgnored:     true,
		targetState:                init.TargetState,
		startingStatuses:           init.Current,
		startingPhase:              phase,
		descIDs:                    screl.AllTargetDescIDs(init.TargetState),
		enforcePlannerSanityChecks: enforcePlannerSanityChecks,
	}
	// Try building stages while ignoring revertibility constraints.
	// This is fine as long as there are no post-commit stages.
	stages := buildStages(c)
	if n := len(stages); n > 0 && stages[n-1].Phase > scop.PreCommitPhase {
		c.isRevertibilityIgnored = false
		stages = buildStages(c)
	}
	if len(stages) == 0 {
		return nil
	}
	// Decorate stages with position in plan.
	{
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
	}
	// Exit early if there is no need to create a job.
	if stages[len(stages)-1].Phase <= scop.PreCommitPhase {
		return stages
	}
	// Add job ops to the stages.
	c.scJobID = scJobIDSupplier()
	for i := range stages {
		var cur, next *Stage
		if i+1 < len(stages) {
			next = &stages[i+1]
		}
		cur = &stages[i]
		jobOps := c.computeExtraJobOps(cur, next)
		cur.ExtraOps = jobOps
	}
	return stages
}

// buildContext contains the global constants for building the stages.
// Only the BuildStages function mutates it, it's read-only everywhere else.
type buildContext struct {
	rollback                   bool
	g                          *scgraph.Graph
	scJobID                    jobspb.JobID
	isRevertibilityIgnored     bool
	targetState                scpb.TargetState
	startingStatuses           []scpb.Status
	startingPhase              scop.Phase
	descIDs                    catalog.DescriptorIDSet
	enforcePlannerSanityChecks bool
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
		// We allow mixing of revertible and non-revertible operations if there
		// are no remaining operations which can fail. That being said, we want
		// to not want to build such a stage as PostCommit but rather as
		// PostCommitNonRevertible. This condition is used to determine whether
		// the phase needs to be advanced due to the presence of non-revertible
		// operations.
		shouldBeInPostCommitNonRevertible := func() bool {
			return !bc.isRevertibilityIgnored &&
				bs.phase == scop.PostCommitPhase &&
				sb.hasAnyNonRevertibleOps()
		}
		for !sb.canMakeProgress() || shouldBeInPostCommitNonRevertible() {
			// When no further progress is possible, move to the next phase and try
			// again, until progress is possible. We haven't reached the terminal
			// state yet, so this is guaranteed (barring any horrible bugs).
			if !sb.canMakeProgress() && bs.phase == scop.PreCommitPhase {
				// This is a special case.
				// We need to move to the post-commit phase, but this will require
				// creating a schema changer job, which in turn will require this
				// otherwise-empty pre-commit stage.
				break
			}
			if bs.phase == scop.LatestPhase {
				// This should never happen, we should always be able to make forward
				// progress because we haven't reached the terminal state yet.
				var str strings.Builder
				for _, t := range sb.current {
					str.WriteString(" - ")
					str.WriteString(screl.NodeString(t.n))
					str.WriteString("\n")
				}
				panic(errors.WithDetailf(errors.AssertionFailedf("unable to make progress"), "terminal state:\n%s", str.String()))
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
		// We don't allow expensive operations in the statement transaction.
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
	numTargets := len(bc.targetState.Targets)
	sb := stageBuilder{
		bc:         bc,
		bs:         bs,
		opType:     opType,
		current:    make([]currentTargetState, numTargets),
		fulfilling: map[*screl.Node]struct{}{},
		lut:        make(map[*scpb.Target]*currentTargetState, numTargets),
		visited:    make(map[*screl.Node]uint64, numTargets),
	}
	{
		nodes := bc.nodes(bs.incumbent)

		// Determine whether there are any backfill or validation operations
		// remaining which should prevent the scheduling of any non-revertible
		// operations. This information will be used when building the current
		// set of targets below.
		for _, n := range nodes {
			if oe, ok := bc.g.GetOpEdgeFrom(n); ok && oe.CanFail() {
				sb.anyRemainingOpsCanFail = true
				break
			}
		}

		for i, n := range nodes {
			t := sb.makeCurrentTargetState(n)
			sb.current[i] = t
			sb.lut[t.n.Target] = &sb.current[i]
		}
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
			// Increment the visit epoch for the next batch of recursive calls to
			// hasUnmeetableOutboundDeps. See comments in function body for details.
			sb.visitEpoch++
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

	// anyRemainingOpsCanFail indicates whether there are any backfill or
	// validation operations remaining in the schema change. If not, then
	// revertible operations can be combined with non-revertible operations
	// in order to move the schema change into the PostCommitNonRevertiblePhase
	// earlier than it might otherwise.
	anyRemainingOpsCanFail bool

	// Helper data structures used to improve performance.

	lut        map[*scpb.Target]*currentTargetState
	visited    map[*screl.Node]uint64
	visitEpoch uint64
}

type currentTargetState struct {
	n *screl.Node
	e *scgraph.OpEdge
}

func (sb stageBuilder) makeCurrentTargetState(n *screl.Node) currentTargetState {
	e, found := sb.bc.g.GetOpEdgeFrom(n)
	if !found || !sb.isOutgoingOpEdgeAllowed(e) {
		return currentTargetState{n: n}
	}
	return currentTargetState{n: n, e: e}
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
	// We allow non-revertible ops to be included at stages preceding
	// PostCommitNonRevertible if nothing left in the schema change at this
	// point can fail. The caller is responsible for detecting whether any
	// non-revertible operations are included in a phase before
	// PostCommitNonRevertible and adjusting the phase accordingly. This is
	// critical to allow op-edges which might otherwise be revertible to be
	// grouped with non-revertible operations.
	if !sb.bc.isRevertibilityIgnored &&
		sb.bs.phase < scop.PostCommitNonRevertiblePhase &&
		!e.Revertible() &&
		// We can't act on the knowledge that nothing remaining can fail while in
		// StatementPhase because we don't know about what future targets may
		// show up which could fail.
		(sb.bs.phase < scop.PreCommitPhase || sb.anyRemainingOpsCanFail) {
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
	return sb.makeCurrentTargetState(t.e.To())
}

// hasUnmeetableOutboundDeps returns true iff the candidate node has inbound
// dependencies which aren't yet met.
//
// In plain english: we can only schedule this node in this stage if all the
// other nodes which need to be scheduled not after it have already been
// scheduled.
func (sb stageBuilder) hasUnmetInboundDeps(n *screl.Node) (ret bool) {
	_ = sb.bc.g.ForEachDepEdgeTo(n, func(de *scgraph.DepEdge) error {
		if ret = sb.isUnmetInboundDep(de); ret {
			return iterutil.StopIteration()
		}
		return nil
	})
	return ret
}

func (sb stageBuilder) isUnmetInboundDep(de *scgraph.DepEdge) bool {
	_, fromIsFulfilled := sb.bs.fulfilled[de.From()]
	_, fromIsCandidate := sb.fulfilling[de.From()]
	switch de.Kind() {

	case scgraph.PreviousTransactionPrecedence:
		return !fromIsFulfilled ||
			(sb.bs.phase <= scop.PreCommitPhase &&
				// If it has been fulfilled implicitly because it's the initial
				// status, then the current stage doesn't matter.
				de.From().CurrentStatus !=
					scpb.TargetStatus(de.From().TargetStatus).InitialStatus())
	case scgraph.PreviousStagePrecedence:
		// True iff the source node has not been fulfilled in an earlier stage.
		return !fromIsFulfilled

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
		de.String(), de.RuleNames()))
}

// hasUnmeetableOutboundDeps returns true iff the candidate node has outbound
// dependencies which cannot possibly be met.
// This is the case when, among all the nodes transitively connected to or from
// it via same-stage dependency edges, there is at least one which cannot (for
// whatever reason) be scheduled in this stage.
// This function recursively visits this set of connected nodes.
//
// In plain english: we can only schedule this node in this stage if we are able
// to also schedule all the other nodes which would be forced to be scheduled in
// the same stage as this one.
func (sb stageBuilder) hasUnmeetableOutboundDeps(n *screl.Node) (ret bool) {
	// This recursive function needs to track which nodes it has already visited
	// to avoid running around in circles: although this is a DAG that we're
	// traversing a DAG we're ignoring edge directions here.
	// We reuse the same map for each set of calls to avoid potentially wasteful
	// allocations. This requires us to maintain a _visit epoch_ counter to
	// differentiate between different traversals.
	if sb.visited[n] == sb.visitEpoch {
		// The node has already been visited in this traversal.
		// Considering that the traversal didn't end during the previous visit,
		// we can infer that this node doesn't have any unmeetable outbound
		// dependencies.
		return false
	}
	// Mark this node as having been visited in this traversal.
	sb.visited[n] = sb.visitEpoch
	// Do some sanity checks.
	if _, isFulfilled := sb.bs.fulfilled[n]; isFulfilled {
		if sb.bc.enforcePlannerSanityChecks {
			panic(errors.AssertionFailedf("%s should not yet be scheduled for this stage",
				screl.NodeString(n)))
		} else {
			return false
		}
	}
	// Look up the current target state for this node, via the lookup table.
	if t := sb.lut[n.Target]; t == nil {
		// This should never happen.
		panic(errors.AssertionFailedf("%s target not found in look-up table",
			screl.NodeString(n)))
	} else if t.e == nil || t.e.To() != n {
		// The visited node is not yet a candidate for scheduling in this stage.
		// Either we're unable to schedule it due to some unsatisfied constraint or
		// there are other nodes preceding it in the op-edge path that need to be
		// scheduled first.
		return true
	}
	// At this point, the visited node might be scheduled in this stage if it,
	// in turn, also doesn't have any unmet inbound dependencies or any unmeetable
	// outbound dependencies.
	// We check the inbound and outbound dep edges for this purpose and
	// recursively visit the neighboring nodes connected via same-stage dep edges
	// to make sure this property is verified transitively.
	_ = sb.bc.g.ForEachDepEdgeTo(n, func(de *scgraph.DepEdge) error {
		if sb.visited[de.From()] == sb.visitEpoch {
			return nil
		}
		if !sb.isUnmetInboundDep(de) {
			return nil
		}
		if de.Kind() != scgraph.SameStagePrecedence || sb.hasUnmeetableOutboundDeps(de.From()) {
			ret = true
			return iterutil.StopIteration()
		}
		return nil
	})
	if ret {
		return true
	}
	_ = sb.bc.g.ForEachDepEdgeFrom(n, func(de *scgraph.DepEdge) error {
		if sb.visited[de.To()] == sb.visitEpoch {
			return nil
		}
		if de.Kind() == scgraph.SameStagePrecedence && sb.hasUnmeetableOutboundDeps(de.To()) {
			ret = true
			return iterutil.StopIteration()
		}
		return nil
	})
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
	return s
}

// hasAnyNonRevertibleOps returns true if there are any ops in the current
// stage which are not revertible.
func (sb stageBuilder) hasAnyNonRevertibleOps() bool {
	for _, e := range sb.opEdges {
		if !e.Revertible() {
			return true
		}
	}
	return false
}

// computeExtraJobOps generates job-related operations to decorate a stage with.
//
// TODO(ajwerner): Rather than adding this above the opgen layer, it may be
// better to do it as part of graph generation. We could treat the job as
// and the job references as elements and track their relationships. The
// oddity here is that the job gets both added and removed. In practice, this
// may prove to be a somewhat common pattern in other cases: consider the
// intermediate index needed when adding and dropping columns as part of the
// same transaction.
func (bc buildContext) computeExtraJobOps(cur, next *Stage) []scop.Op {
	// Schema change job operations only affect mutation stages no sooner
	// than pre-commit.
	if cur.Phase < scop.PreCommitPhase || cur.Type() != scop.MutationType {
		return nil
	}
	initialize := cur.Phase == scop.PreCommitPhase
	ds := bc.makeDescriptorStates(cur, next)
	var descIDsPresentBefore, descIDsPresentAfter catalog.DescriptorIDSet
	bc.descIDs.ForEach(func(descID descpb.ID) {
		if s, hasStateBefore := ds[descID]; hasStateBefore {
			descIDsPresentBefore.Add(descID)
			if hasStateAfter := s != nil; hasStateAfter {
				descIDsPresentAfter.Add(descID)
			}
		}
	})
	ops := make([]scop.Op, 0, descIDsPresentBefore.Len()+1)
	addOp := func(op scop.Op) {
		ops = append(ops, op)
	}
	// Build the ops which update or remove the job state on each descriptor.
	descIDsPresentBefore.ForEach(func(descID descpb.ID) {
		if next == nil {
			// Remove job state and reference from descriptor at terminal stage.
			addOp(bc.removeJobReferenceOp(descID))
		} else if descIDsPresentAfter.Contains(descID) {
			// Update job state in descriptor in non-terminal stage, as long as the
			// descriptor is still present after the execution of the stage.
			addOp(bc.setJobStateOnDescriptorOp(initialize, descID, *ds[descID]))
		}
	})
	// Build the op which creates or updates the job.
	if initialize {
		addOp(bc.createSchemaChangeJobOp(descIDsPresentAfter, next))
	} else {
		addOp(bc.updateJobProgressOp(descIDsPresentBefore, descIDsPresentAfter, next))
	}
	return ops
}

func (bc buildContext) createSchemaChangeJobOp(
	descIDsPresentAfter catalog.DescriptorIDSet, next *Stage,
) scop.Op {
	return &scop.CreateSchemaChangerJob{
		JobID:         bc.scJobID,
		Statements:    bc.targetState.Statements,
		Authorization: bc.targetState.Authorization,
		DescriptorIDs: descIDsPresentAfter.Ordered(),
		NonCancelable: !isRevertible(next),
		RunningStatus: runningStatus(next),
	}
}

func (bc buildContext) updateJobProgressOp(
	descIDsPresentBefore, descIDsPresentAfter catalog.DescriptorIDSet, next *Stage,
) scop.Op {
	var toRemove catalog.DescriptorIDSet
	if next != nil {
		toRemove = descIDsPresentBefore.Difference(descIDsPresentAfter)
	} else {
		// If the next stage is nil, simply remove al the descriptors, we are
		// done processing
		toRemove = descIDsPresentBefore
	}
	return &scop.UpdateSchemaChangerJob{
		JobID:                 bc.scJobID,
		IsNonCancelable:       !isRevertible(next),
		RunningStatus:         runningStatus(next),
		DescriptorIDsToRemove: toRemove.Ordered(),
	}
}

func (bc buildContext) setJobStateOnDescriptorOp(
	initialize bool, descID descpb.ID, s scpb.DescriptorState,
) scop.Op {
	return &scop.SetJobStateOnDescriptor{
		DescriptorID: descID,
		Initialize:   initialize,
		State:        s,
	}
}

func (bc buildContext) removeJobReferenceOp(descID descpb.ID) scop.Op {
	return &scop.RemoveJobStateFromDescriptor{
		DescriptorID: descID,
		JobID:        bc.scJobID,
	}
}

func (bc buildContext) nodes(current []scpb.Status) []*screl.Node {
	return nodes(bc.g, bc.targetState.Targets, current)
}

func nodes(g *scgraph.Graph, targets []scpb.Target, current []scpb.Status) []*screl.Node {
	nodes := make([]*screl.Node, len(targets))
	for i, status := range current {
		t := &targets[i]
		n, ok := g.GetNode(t, status)
		if !ok {
			panic(errors.AssertionFailedf("could not find node for element %s, target status %s, current status %s",
				screl.ElementString(t.Element()), t.TargetStatus, status))
		}
		nodes[i] = n
	}
	return nodes
}

// makeDescriptorStates builds the state of the schema change job broken down
// into its affected descriptors.
func (bc buildContext) makeDescriptorStates(cur, next *Stage) map[descpb.ID]*scpb.DescriptorState {
	// Initialize the descriptor states.
	ds := make(map[descpb.ID]*scpb.DescriptorState, bc.descIDs.Len())
	bc.descIDs.ForEach(func(id descpb.ID) {
		ds[id] = &scpb.DescriptorState{
			Authorization: bc.targetState.Authorization,
			JobID:         bc.scJobID,
			InRollback:    bc.rollback,
			Revertible:    isRevertible(next),
		}
	})
	mkStmt := func(rank uint32) scpb.DescriptorState_Statement {
		return scpb.DescriptorState_Statement{
			Statement:     bc.targetState.Statements[rank],
			StatementRank: rank,
		}
	}
	// Populate the descriptor states.
	addToRelevantStatements := func(
		srs []scpb.DescriptorState_Statement, stmtRank uint32,
	) []scpb.DescriptorState_Statement {
		n := len(srs)
		if i := sort.Search(n, func(i int) bool {
			return srs[i].StatementRank >= stmtRank
		}); i == n {
			srs = append(srs, mkStmt(stmtRank))
		} else if srs[i].StatementRank != stmtRank {
			srs = append(srs, scpb.DescriptorState_Statement{})
			copy(srs[i+1:], srs[i:])
			srs[i] = mkStmt(stmtRank)
		}
		return srs
	}
	noteRelevantStatement := func(state *scpb.DescriptorState, stmtRank uint32) {
		state.RelevantStatements = addToRelevantStatements(
			state.RelevantStatements, stmtRank,
		)
	}
	for i, t := range bc.targetState.Targets {
		descID := screl.GetDescID(t.Element())
		state := ds[descID]
		stmtID := t.Metadata.StatementID
		noteRelevantStatement(state, stmtID)
		state.Targets = append(state.Targets, t)
		state.TargetRanks = append(state.TargetRanks, uint32(i))
		state.CurrentStatuses = append(state.CurrentStatuses, cur.After[i])
	}
	// Remove all mappings for elements of descriptors which no longer exist.
	// Descriptors are owned by the descriptor-elements; should these be targeting
	// ABSENT and already have reached that status, we can safely conclude that
	// the descriptor no longer exists and should therefore not be referenced by
	// the schema change job.
	//
	// Should they be reaching ABSENT as an outcome of executing this stage, we
	// replace the mapped state with a nil pointer, as a sentinel value indicating
	// that the state will be removed in all subsequent stages.
	//
	// Descriptor removal is non-revertible in nature, so we needn't do anything
	// if we haven't reached the non-revertible post-commit phase yet.
	if cur.Phase == scop.PostCommitNonRevertiblePhase {
		for i, t := range bc.targetState.Targets {
			if !current.IsDescriptor(t.Element()) || t.TargetStatus != scpb.Status_ABSENT {
				continue
			}
			descID := screl.GetDescID(t.Element())
			if cur.Before[i] == scpb.Status_ABSENT {
				delete(ds, descID)
			} else if cur.After[i] == scpb.Status_ABSENT {
				ds[descID] = nil
			}
		}
	}
	return ds
}

// isRevertible is a helper function which tells us if the schema change can be
// reverted once the current stage completes. The result is determined by the
// properties of the subsequent stage, which is why it, instead of the current
// stage, is passed as an argument.
func isRevertible(next *Stage) bool {
	return next != nil && next.Phase < scop.PostCommitNonRevertiblePhase
}

func runningStatus(next *Stage) string {
	if next == nil {
		return "all stages completed"
	}
	return fmt.Sprintf("%s pending", next)
}
