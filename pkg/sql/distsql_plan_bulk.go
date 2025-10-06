// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan/replicaoracle"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// SetupAllNodesPlanning creates a planCtx and sets up the planCtx.nodeStatuses
// map for all nodes. It returns all nodes that can be used for planning.
func (dsp *DistSQLPlanner) SetupAllNodesPlanning(
	ctx context.Context, evalCtx *extendedEvalContext, execCfg *ExecutorConfig,
) (*PlanningCtx, []base.SQLInstanceID, error) {
	return dsp.SetupAllNodesPlanningWithOracle(
		ctx, evalCtx, execCfg, physicalplan.DefaultReplicaChooser, roachpb.Locality{},
	)
}

// SetupAllNodesPlanningWithOracle creates a planCtx and sets up the
// planCtx.nodeStatuses map for all nodes. It returns all nodes that can be used
// for planning, filtered using the passed locality filter, which along with the
// passed replica oracle, is stored in the returned planning context to be used
// by subsequent physical planning such as PartitionSpans.
func (dsp *DistSQLPlanner) SetupAllNodesPlanningWithOracle(
	ctx context.Context,
	evalCtx *extendedEvalContext,
	execCfg *ExecutorConfig,
	oracle replicaoracle.Oracle,
	localityFilter roachpb.Locality,
) (*PlanningCtx, []base.SQLInstanceID, error) {
	if dsp.codec.ForSystemTenant() {
		return dsp.setupAllNodesPlanningSystem(ctx, evalCtx, execCfg, oracle, localityFilter)
	}
	return dsp.setupAllNodesPlanningTenant(ctx, evalCtx, execCfg, oracle, localityFilter)
}

// setupAllNodesPlanningSystem creates a planCtx and returns all nodes available
// in a system tenant, filtered using the passed locality filter if it is
// non-empty.
func (dsp *DistSQLPlanner) setupAllNodesPlanningSystem(
	ctx context.Context,
	evalCtx *extendedEvalContext,
	execCfg *ExecutorConfig,
	oracle replicaoracle.Oracle,
	localityFilter roachpb.Locality,
) (*PlanningCtx, []base.SQLInstanceID, error) {
	planCtx := dsp.NewPlanningCtxWithOracle(
		ctx, evalCtx, nil /* planner */, nil /* txn */, FullDistribution, oracle, localityFilter,
	)

	ss, err := execCfg.NodesStatusServer.OptionalNodesStatusServer()
	if err != nil {
		return planCtx, []base.SQLInstanceID{dsp.gatewaySQLInstanceID}, nil //nolint:returnerrcheck
	}
	resp, err := ss.ListNodesInternal(ctx, &serverpb.NodesRequest{})
	if err != nil {
		return nil, nil, err
	}
	// Because we're not going through the normal pathways, we have to set up the
	// planCtx.nodeStatuses map ourselves. checkInstanceHealthAndVersionSystem() will
	// populate it.
	for _, node := range resp.Nodes {
		if ok, _ := node.Desc.Locality.Matches(localityFilter); ok {
			_ /* NodeStatus */ = dsp.checkInstanceHealthAndVersionSystem(ctx, planCtx, base.SQLInstanceID(node.Desc.NodeID))
		}
	}
	nodes := make([]base.SQLInstanceID, 0, len(planCtx.nodeStatuses))
	for nodeID, status := range planCtx.nodeStatuses {
		if status == NodeOK {
			nodes = append(nodes, nodeID)
		}
	}
	return planCtx, nodes, nil
}

// setupAllNodesPlanningTenant creates a planCtx and returns all nodes available
// in a non-system tenant, filtered using the passed locality filter if is
// non-empty.
func (dsp *DistSQLPlanner) setupAllNodesPlanningTenant(
	ctx context.Context,
	evalCtx *extendedEvalContext,
	execCfg *ExecutorConfig,
	oracle replicaoracle.Oracle,
	localityFilter roachpb.Locality,
) (*PlanningCtx, []base.SQLInstanceID, error) {
	planCtx := dsp.NewPlanningCtxWithOracle(
		ctx, evalCtx, nil /* planner */, nil /* txn */, FullDistribution, oracle, localityFilter,
	)
	pods, err := dsp.sqlAddressResolver.GetAllInstances(ctx)
	if err != nil {
		return nil, nil, err
	}
	pods, _ = dsp.filterUnhealthyInstances(pods, planCtx.nodeStatuses)

	sqlInstanceIDs := make([]base.SQLInstanceID, 0, len(pods))
	for _, pod := range pods {
		if ok, _ := pod.Locality.Matches(localityFilter); ok {
			sqlInstanceIDs = append(sqlInstanceIDs, pod.InstanceID)
		}
	}
	return planCtx, sqlInstanceIDs, nil
}

// PhysicalPlanMaker describes a function that makes a physical plan.
type PhysicalPlanMaker func(context.Context, *DistSQLPlanner) (*PhysicalPlan, *PlanningCtx, error)

// calculatePlanGrowth returns the number of new or updated sql instances in one
// physical plan relative to an old one, as well as that number as a fraction of
// the number in the old one.
func calculatePlanGrowth(before, after *PhysicalPlan) (int, float64) {
	var changed int
	beforeSpecs, beforeCleanup := before.GenerateFlowSpecs()
	defer beforeCleanup(beforeSpecs)
	afterSpecs, afterCleanup := after.GenerateFlowSpecs()
	defer afterCleanup(afterSpecs)

	// How many nodes are in after that are not in before, or are in both but
	// have changed their spec?
	for n, afterSpec := range afterSpecs {
		if beforeSpec, ok := beforeSpecs[n]; !ok {
			changed++ // not in before at all
		} else if len(beforeSpec.Processors) != len(afterSpec.Processors) {
			changed++ // in before with different procs
		} else {
			for i := range beforeSpec.Processors {
				// TODO(dt): add equality check method gen, use it.
				if afterSpec.Processors[i].Size() != beforeSpec.Processors[i].Size() {
					changed++ // procs have differnet specs
					break
				}
			}
		}
	}

	var frac float64
	if changed > 0 {
		frac = float64(changed) / float64(len(beforeSpecs))
	}
	return changed, frac
}

// PlanChangeDecision describes a function that decides if a plan has "changed"
// within a given context, for example, if enough of its processor placements
// have changed that it should be replanned.
type PlanChangeDecision func(ctx context.Context, old, new *PhysicalPlan) bool

// ReplanOnChangedFraction returns a PlanChangeDecision that returns true when a
// new plan has a number of instances that would be assigned new or changed
// processors exceeding the passed fraction of the old plan's total instances.
// For example, if the old plan had three instances, A, B, and C, and the new
// plan has B, C' and D, where D is new and C' differs from C, then it would
// compare 2/3 = 0.6 to the threshold's value.
func ReplanOnChangedFraction(thresholdFn func() float64) PlanChangeDecision {
	return func(ctx context.Context, oldPlan, newPlan *PhysicalPlan) bool {
		changed, growth := calculatePlanGrowth(oldPlan, newPlan)
		threshold := thresholdFn()
		replan := threshold != 0.0 && growth > threshold
		if replan || growth > 0.1 || log.V(1) {
			log.Infof(ctx, "Re-planning would add or alter flows on %d nodes / %.2f, threshold %.2f, replan %v",
				changed, growth, threshold, replan)
		}
		return replan
	}
}

// countNodesFn counts the source and dest nodes in a Physical Plan
type countNodesFn func(plan *PhysicalPlan) (src, dst map[string]struct{}, nodeCount int)

// ReplanOnCustomFunc returns a PlanChangeDecision that returns true when a new
// plan is sufficiently different than the previous plan. This occurs if the
// measureChangeFn returns a scalar higher than the thresholdFn.
//
// If the thresholdFn returns 0.0, a new plan is never chosen.
func ReplanOnCustomFunc(getNodes countNodesFn, thresholdFn func() float64) PlanChangeDecision {
	return func(ctx context.Context, oldPlan, newPlan *PhysicalPlan) bool {
		threshold := thresholdFn()
		if threshold == 0.0 {
			return false
		}
		change := MeasurePlanChange(oldPlan, newPlan, getNodes)
		replan := change > threshold
		log.VEventf(ctx, 1, "Replanning change: %.2f; threshold: %.2f; choosing new plan %v", change,
			threshold, replan)
		return replan
	}
}

// measurePlanChange computes the number of node changes (addition or removal)
// in the source and destination clusters as a fraction of the total number of
// nodes in both clusters in the previous plan.
func MeasurePlanChange(
	before, after *PhysicalPlan,
	getNodes func(plan *PhysicalPlan) (src, dst map[string]struct{}, nodeCount int),
) float64 {
	countMissingElements := func(set1, set2 map[string]struct{}) int {
		diff := 0
		for id := range set1 {
			if _, ok := set2[id]; !ok {
				diff++
			}
		}
		return diff
	}

	oldSrc, oldDst, oldCount := getNodes(before)
	newSrc, newDst, _ := getNodes(after)
	diff := 0
	// To check for both introduced nodes and removed nodes, swap input order.
	diff += countMissingElements(oldSrc, newSrc)
	diff += countMissingElements(newSrc, oldSrc)
	diff += countMissingElements(oldDst, newDst)
	diff += countMissingElements(newDst, oldDst)
	return float64(diff) / float64(oldCount)
}

// ErrPlanChanged is a sentinel marker error for use to signal a plan changed.
var ErrPlanChanged = errors.New("physical plan has changed")

// PhysicalPlanChangeChecker returns a function which will periodically call the
// passed function at the requested interval until the returned channel is
// closed and compare the plan it returns to the passed initial plan, returning
// an error if it has changed (as defined by CalculatePlanGrowth) by more than
// the passed threshold. A threshold of 0 disables it.
func PhysicalPlanChangeChecker(
	ctx context.Context,
	initial *PhysicalPlan,
	fn PhysicalPlanMaker,
	execCtx interface{ DistSQLPlanner() *DistSQLPlanner },
	decider PlanChangeDecision,
	freq func() time.Duration,
) (func(context.Context) error, func()) {
	stop := make(chan struct{})

	return func(ctx context.Context) error {
		tick := time.NewTicker(freq())
		defer tick.Stop()
		done := ctx.Done()
		for {
			select {
			case <-stop:
				return nil
			case <-done:
				return ctx.Err()
			case <-tick.C:
				dsp := execCtx.DistSQLPlanner()
				p, _, err := fn(ctx, dsp)
				if err != nil {
					log.Warningf(ctx, "job replanning check failed to generate plan: %v", err)
					continue
				}
				if decider(ctx, initial, p) {
					return ErrPlanChanged
				}
			}
		}
	}, func() { close(stop) }
}
