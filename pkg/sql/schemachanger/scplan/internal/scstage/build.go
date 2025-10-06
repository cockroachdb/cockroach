// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	withSanityChecks bool,
) []Stage {
	// Initialize the build context.
	bc := buildContext{
		ctx:      ctx,
		rollback: init.InRollback,
		g:        g,
		scJobID: func() func() jobspb.JobID {
			var scJobID jobspb.JobID
			return func() jobspb.JobID {
				if scJobID == 0 {
					scJobID = scJobIDSupplier()
				}
				return scJobID
			}
		}(),
		targetState: init.TargetState,
		initial:     init.Initial,
		current:     init.Current,
		targetToIdx: func() map[*scpb.Target]int {
			m := make(map[*scpb.Target]int, len(init.Targets))
			for i := range init.Targets {
				m[&init.Targets[i]] = i
			}
			return m
		}(),
		startingPhase:          phase,
		descIDs:                screl.AllTargetStateDescIDs(init.TargetState),
		withSanityChecks:       withSanityChecks,
		anyRemainingOpsCanFail: checkIfAnyRemainingOpsCanFail(init.TargetState, g),
	}
	// Build stages for all remaining phases.
	stages := buildStages(bc)
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
	// Decorate stages with extra ops.
	for i := range stages {
		var cur, next *Stage
		if i+1 < len(stages) {
			next = &stages[i+1]
		}
		cur = &stages[i]
		cur.ExtraOps = bc.computeExtraOps(cur, next)
	}
	return stages
}

// buildContext contains the global constants for building the stages.
// It's read-only everywhere after being initialized in BuildStages.
type buildContext struct {
	ctx                    context.Context
	rollback               bool
	g                      *scgraph.Graph
	scJobID                func() jobspb.JobID
	targetState            scpb.TargetState
	initial                []scpb.Status
	current                []scpb.Status
	targetToIdx            map[*scpb.Target]int
	startingPhase          scop.Phase
	descIDs                catalog.DescriptorIDSet
	anyRemainingOpsCanFail map[*screl.Node]bool
	withSanityChecks       bool
}

// checkIfAnyRemainingOpsCanFail returns a map which indicates if
// a given screl.Node can encounter a failure in the future due to
// either validation or backfill.
func checkIfAnyRemainingOpsCanFail(
	targetState scpb.TargetState, g *scgraph.Graph,
) map[*screl.Node]bool {
	// Determine which op edges can potentially fail later on due to backfill
	// failures.
	anyRemainingOpsCanFail := make(map[*screl.Node]bool)
	for i := range targetState.Targets {
		t := &targetState.Targets[i]
		currentStatus := t.TargetStatus
		anyRemainingCanFail := false
		for {
			if n, ok := g.GetNode(t, currentStatus); ok {
				if oe, ok := g.GetOpEdgeTo(n); ok {
					anyRemainingOpsCanFail[n] = anyRemainingCanFail
					// If this can potentially lead to failures, validate
					// we have ops which are non-mutation types.
					if oe.CanFail() {
						for _, op := range oe.Op() {
							anyRemainingCanFail = anyRemainingCanFail ||
								op.Type() != scop.MutationType
						}
					}
					currentStatus = oe.From().CurrentStatus
				} else {
					// Terminal status
					anyRemainingOpsCanFail[n] = anyRemainingCanFail
					break
				}
			} else {
				break
			}
		}
	}
	return anyRemainingOpsCanFail
}

// buildStages builds all stages according to the starting parameters
// in the build context.
func buildStages(bc buildContext) (stages []Stage) {
	// Build stages for all remaining phases.
	currentStatuses := func() []scpb.Status {
		if n := len(stages); n > 0 {
			return stages[n-1].After
		}
		return bc.current
	}
	currentPhase := bc.startingPhase
	switch currentPhase {
	case scop.StatementPhase:
		// Build a stage for the statement phase.
		// This stage moves the elements statuses towards their targets, and
		// applies the immediate mutation operations to materialize this in the
		// in-memory catalog, for the benefit of any potential later statement in
		// the transaction. These status transitions and their side-effects are
		// undone at pre-commit time and the whole schema change is re-planned
		// taking revertibility into account.
		//
		// This way, the side effects of a schema change become immediately visible
		// to the remainder of the transaction. For example, dropping a table makes
		// any subsequent in-txn queries on it fail, which is desirable. On the
		// other hand, persisting such statement-phase changes might be
		// undesirable. Reusing the previous example, if the drop is followed by a
		// constraint creation in the transaction, then the validation for that
		// constraint may fail and the schema change will have to be rolled back;
		// we don't want the table drop to be visible to other transactions until
		// the schema change is guaranteed to succeed.
		{
			bs := initBuildState(bc, scop.StatementPhase, currentStatuses())
			statementStage := bc.makeStageBuilder(bs).build()
			// Schedule only immediate ops in the statement phase.
			var immediateOps []scop.Op
			for _, op := range statementStage.EdgeOps {
				if _, ok := op.(scop.ImmediateMutationOp); !ok {
					continue
				}
				immediateOps = append(immediateOps, op)
			}
			statementStage.EdgeOps = immediateOps
			stages = append(stages, statementStage)
		}
		// Move to the pre-commit phase
		currentPhase = scop.PreCommitPhase
		fallthrough
	case scop.PreCommitPhase:
		// Build a stage to reset to the initial statuses for all targets
		// as a prelude to the pre-commit phase's main stage.
		{
			resetStage := Stage{
				Before: make([]scpb.Status, len(bc.initial)),
				After:  make([]scpb.Status, len(bc.initial)),
				Phase:  scop.PreCommitPhase,
			}
			copy(resetStage.Before, currentStatuses())
			copy(resetStage.After, bc.initial)
			stages = append(stages, resetStage)
			if bc.isStateTerminal(resetStage.After) {
				// Exit early if the whole schema change is a no-op. This may happen
				// when doing BEGIN; CREATE SCHEMA sc; DROP SCHEMA sc; COMMIT; for
				// example.
				break
			}
		}
		// Build the pre-commit phase's main stage.
		{
			bs := initBuildState(bc, scop.PreCommitPhase, currentStatuses())
			mainStage := bc.makeStageBuilder(bs).build()
			stages = append(stages, mainStage)
		}
		// Move to the post-commit phase.
		currentPhase = scop.PostCommitPhase
		fallthrough
	case scop.PostCommitPhase, scop.PostCommitNonRevertiblePhase:
		bs := initBuildState(bc, currentPhase, currentStatuses())
		stages = append(stages, buildPostCommitStages(bc, bs)...)
	default:
		panic(errors.AssertionFailedf("unknown phase %s", currentPhase))
	}
	return stages
}

func buildPostCommitStages(bc buildContext, bs buildState) (stages []Stage) {
	build := func(sb stageBuilder) {
		stage := sb.build()
		stages = append(stages, stage)
		// Update the build state with this stage's progress.
		for n := range sb.fulfilling {
			bs.fulfilled[n] = struct{}{}
		}
		bs.incumbent = stage.After
	}
	// Build post-commit phase stages, if applicable.
	if bs.currentPhase == scop.PostCommitPhase {
		for !bc.isStateTerminal(bs.incumbent) {
			sb := bc.makeStageBuilder(bs)
			// We allow mixing of revertible and non-revertible operations if there
			// are no remaining operations which can fail. That being said, we do
			// not want to build such a stage as PostCommit but rather as
			// PostCommitNonRevertible.
			if !sb.canMakeProgress() || sb.hasAnyNonRevertibleOps() {
				break
			}
			build(sb)
		}
		// Move to the non-revertible post-commit phase.
		bs.currentPhase = scop.PostCommitNonRevertiblePhase
	}
	// Build non-revertible post-commit stages.
	for !bc.isStateTerminal(bs.incumbent) {
		sb := bc.makeStageBuilder(bs)
		if !sb.canMakeProgress() {
			// We haven't reached the terminal state yet, however further progress
			// isn't possible despite having exhausted all phases. There must be a
			// bug somewhere in scplan/...
			var trace []string
			bs.trace = &trace
			sb = bc.makeStageBuilder(bs)
			panic(errors.WithDetailf(
				errors.AssertionFailedf("unable to make progress"),
				"terminal state:\n%s\nrule trace:\n%s", sb, strings.Join(trace, "\n")))
		}
		build(sb)
	}
	// We have reached the terminal state at this point.
	return stages
}

// buildState contains the global build state for building the stages.
// Only the buildStagesInCurrentPhase function mutates it,
// it's read-only everywhere else.
type buildState struct {
	incumbent    []scpb.Status
	fulfilled    map[*screl.Node]struct{}
	currentPhase scop.Phase
	trace        *[]string
}

// initBuildState initializes a build state for this buildContext.
func initBuildState(bc buildContext, phase scop.Phase, current []scpb.Status) buildState {
	bs := buildState{
		incumbent:    make([]scpb.Status, len(bc.current)),
		fulfilled:    make(map[*screl.Node]struct{}, bc.g.Order()),
		currentPhase: phase,
	}
	for i, n := range bc.nodes(current) {
		bs.incumbent[i] = n.CurrentStatus
		for {
			bs.fulfilled[n] = struct{}{}
			oe, ok := bc.g.GetOpEdgeTo(n)
			if !ok {
				break
			}
			n = oe.From()
		}
	}
	return bs
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
	if bs.currentPhase <= scop.PreCommitPhase {
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
		fulfilling: make(map[*screl.Node]struct{}),
		lut:        make(map[*scpb.Target]*currentTargetState, numTargets),
		visited:    make(map[*screl.Node]uint64, numTargets),
	}
	sb.debugTracef("initialized stage builder for %s", opType)
	{
		nodes := bc.nodes(bs.incumbent)

		// Determine whether there are any backfill or validation operations
		// remaining which should prevent the scheduling of any non-revertible
		// operations. This information will be used when building the current
		// set of targets below. We will do this over the set of generated operations,
		// since some of these may be no-oped for newly created tables.
		for _, n := range nodes {
			sb.anyRemainingOpsCanFail = sb.anyRemainingOpsCanFail || bc.anyRemainingOpsCanFail[n]
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
			if sb.hasDebugTrace() {
				sb.debugTracef("- %s targeting %s stuck in %s",
					screl.ElementString(t.n.Element()), t.n.TargetStatus, t.n.CurrentStatus)
			}
			if sb.hasUnmetInboundDeps(t.e.To()) {
				continue
			}
			// Increment the visit epoch for the next batch of recursive calls to
			// hasUnmeetableOutboundDeps. See comments in function body for details.
			sb.visitEpoch++
			if sb.hasDebugTrace() {
				sb.debugTracef("  progress to %s prevented by unmeetable outbound deps:",
					t.e.To().CurrentStatus)
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

func (sb stageBuilder) debugTracef(fmtStr string, args ...interface{}) {
	if !sb.hasDebugTrace() {
		return
	}
	*sb.bs.trace = append(*sb.bs.trace, fmt.Sprintf(fmtStr, args...))
}

func (sb stageBuilder) hasDebugTrace() bool {
	return sb.bs.trace != nil
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
	// At this point, consider whether this edge is allowed based on the current
	// phase, whether the edge is revertible, and other information.
	switch sb.bs.currentPhase {
	case scop.StatementPhase:
		// We ignore revertibility in the statement phase. This ensures that
		// the side effects of a schema change become immediately visible
		// to the transaction. For example, dropping a table in an explicit
		// transaction should make it impossible to query that table later
		// in the transaction.
		return true
	case scop.PreCommitPhase, scop.PostCommitPhase:
		// We allow non-revertible ops to be included in stages in these phases
		// only if none of the remaining schema change operations can fail.
		// The caller is responsible for detecting whether any non-revertible
		// operations are included in a phase before PostCommitNonRevertible
		// and for adjusting the phase accordingly. This is critical to allow
		// op-edges which might otherwise be revertible to be grouped with
		// non-revertible operations.
		return e.Revertible() || !sb.anyRemainingOpsCanFail
	case scop.PostCommitNonRevertiblePhase:
		// We allow non-revertible edges in the non-revertible post-commit phase,
		// naturally.
		return true
	}
	panic(errors.AssertionFailedf("unknown phase %s", sb.bs.currentPhase))
}

// canMakeProgress returns true if the stage built by this builder will make
// progress.
func (sb stageBuilder) canMakeProgress() bool {
	return len(sb.opEdges) > 0
}

func (sb stageBuilder) nextTargetState(t currentTargetState) currentTargetState {
	return sb.makeCurrentTargetState(t.e.To())
}

// hasUnmetInboundDeps returns true iff the candidate node has inbound
// dependencies which aren't yet met.
//
// In plain english: we can only schedule this node in this stage if all the
// other nodes which need to be scheduled before it have already been scheduled.
func (sb stageBuilder) hasUnmetInboundDeps(n *screl.Node) (ret bool) {
	_ = sb.bc.g.ForEachDepEdgeTo(n, func(de *scgraph.DepEdge) error {
		if ret = sb.isUnmetInboundDep(de); ret {
			if sb.hasDebugTrace() {
				sb.debugTracef("  progress to %s prevented by unmet inbound dep from %s",
					n.CurrentStatus, screl.ElementString(de.From().Element()))
				for _, rule := range de.Rules() {
					sb.debugTracef("  - %s: %s", rule.Kind, rule.Name)
				}
			}
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
		if !fromIsFulfilled {
			// If the source node has not already been fulfilled in an earlier
			// stage, then we can't consider scheduling the destination node
			// in the current stage.
			return true
		}
		// At this point, the source node has been fulfilled before the current
		// stage.
		if sb.bs.currentPhase > scop.PreCommitPhase {
			// Post-commit, transactions cannot have multiple stages, therefore
			// the source node was fulfilled in an earlier transaction and this
			// inbound dependency is met.
			return false
		}
		// At this point, the current stage is in the statement transaction.
		// The only way this inbound dependency can be met is if the source node
		// status is the initial status before the schema change even began,
		// in which case the node was fulfilled before the statement transaction.
		idx, ok := sb.bc.targetToIdx[de.From().Target]
		if !ok {
			panic(errors.AssertionFailedf("unknown target %q", de.From().Target))
		}
		return de.From().CurrentStatus != sb.bc.initial[idx]

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
// it via same-stage precedence (or just precedence) dependency edges, there is
// at least one which cannot (for whatever reason) be scheduled in this stage.
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
		if sb.bc.withSanityChecks {
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
		if sb.hasDebugTrace() {
			if t.e == nil {
				sb.debugTracef("  %s targeting %s does not have outbound edge yet",
					screl.ElementString(t.n.Element()), t.n.TargetStatus)
			} else {
				sb.debugTracef("  %s targeting %s hasn't reached %s yet",
					screl.ElementString(t.n.Element()), t.n.TargetStatus, t.e.To().CurrentStatus)
			}
		}
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
		// At this point, `de` is an unmet inbound dep edge.
		// The only way `n` can still be scheduled in this stage is thus to hope that
		// `de.from` can be scheduled in this stage later AND `de` is a same-stage
		// precedence (or just precedence since precedence subsumes same-stage precedence).
		// Otherwise, we can conclude `n` is not schedulable in this stage.
		if sb.hasUnmeetableOutboundDeps(de.From()) {
			// `de.from` itself has unmeetable outbound deps and hence unschedulable :(
			ret = true
			return iterutil.StopIteration()
		}
		switch de.Kind() {
		case scgraph.PreviousStagePrecedence, scgraph.PreviousTransactionPrecedence:
			// `de.from` might be schedulable but the dep edge requires `n` to be scheduled
			// at a different transaction/stage, so even if `de.from` is indeed schedulable
			// in this stage, `n` cannot be scheduled in the same stage due to this dep edge.
			if sb.hasDebugTrace() {
				sb.debugTracef("  - %s targeting %s must reach %s in a previous stage",
					screl.ElementString(de.From().Element()), de.From().TargetStatus, de.From().CurrentStatus)
			}
			ret = true
			return iterutil.StopIteration()
		case scgraph.Precedence, scgraph.SameStagePrecedence:
			// `de.from` might be schedulable and, if indeed is, the dep edge types allow
			// us to schedule `n` in the same stage as `de.from`. Hope remains!
			return nil
		default:
			panic(errors.AssertionFailedf("unknown dep edge kind %q", de.Kind()))
		}
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
	s := Stage{
		Before: make([]scpb.Status, len(sb.current)),
		After:  make([]scpb.Status, len(sb.current)),
		Phase:  sb.bs.currentPhase,
	}
	for i, t := range sb.current {
		s.Before[i] = sb.bs.incumbent[i]
		s.After[i] = t.n.CurrentStatus
	}
	for _, e := range sb.opEdges {
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

// String returns a string representation of the stageBuilder.
func (sb stageBuilder) String() string {
	var str strings.Builder
	for _, t := range sb.current {
		str.WriteString(" - ")
		str.WriteString(screl.NodeString(t.n))
		str.WriteString("\n")
	}
	return str.String()
}

// computeExtraOps generates extra operations to decorate a stage with.
// These are typically job-related.
//
// TODO(ajwerner): Rather than adding this above the opgen layer, it may be
// better to do it as part of graph generation. We could treat the job as
// and the job references as elements and track their relationships. The
// oddity here is that the job gets both added and removed. In practice, this
// may prove to be a somewhat common pattern in other cases: consider the
// intermediate index needed when adding and dropping columns as part of the
// same transaction.
func (bc buildContext) computeExtraOps(cur, next *Stage) []scop.Op {
	// Schema change extra operations only affect mutation stages no sooner
	// than pre-commit.
	if cur.Type() != scop.MutationType {
		return nil
	}
	var initializeSchemaChangeJob bool
	switch cur.Phase {
	case scop.StatementPhase:
		return nil
	case scop.PreCommitPhase:
		if cur.IsResetPreCommitStage() {
			// This is the reset stage, return the undo op.
			return []scop.Op{&scop.UndoAllInTxnImmediateMutationOpSideEffects{}}
		}
		// At this point this has to be the main stage.
		// Any subsequent stage would be post-commit.
		if next == nil {
			// There are no post-commit stages, therefore do nothing.
			return nil
		}
		// Otherwise, this is the main pre-commit stage, followed by post-commit
		// stages, add the schema change job creation ops.
		initializeSchemaChangeJob = true
	}
	// Build job-related extra ops.
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
			addOp(bc.setJobStateOnDescriptorOp(initializeSchemaChangeJob, descID, *ds[descID]))
		}
	})
	// Build the op which creates or updates the job.
	if initializeSchemaChangeJob {
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
		JobID:         bc.scJobID(),
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
		JobID:                 bc.scJobID(),
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
		JobID:        bc.scJobID(),
	}
}

func (bc buildContext) nodes(current []scpb.Status) []*screl.Node {
	ret := make([]*screl.Node, len(bc.targetState.Targets))
	for i, status := range current {
		t := &bc.targetState.Targets[i]
		n, ok := bc.g.GetNode(t, status)
		if !ok {
			panic(errors.AssertionFailedf("could not find node for element %s, target status %s, current status %s",
				screl.ElementString(t.Element()), t.TargetStatus, status))
		}
		ret[i] = n
	}
	return ret
}

// makeDescriptorStates builds the state of the schema change job broken down
// into its affected descriptors.
func (bc buildContext) makeDescriptorStates(cur, next *Stage) map[descpb.ID]*scpb.DescriptorState {
	// Initialize the descriptor states.
	ds := make(map[descpb.ID]*scpb.DescriptorState, bc.descIDs.Len())
	bc.descIDs.ForEach(func(id descpb.ID) {
		s := &scpb.DescriptorState{
			Authorization: bc.targetState.Authorization,
			JobID:         bc.scJobID(),
			InRollback:    bc.rollback,
			Revertible:    isRevertible(next),
		}
		if nm := scpb.NameMappings(bc.targetState.NameMappings).Find(id); nm != nil {
			s.NameMapping = *nm
		}
		ds[id] = s
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
