// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
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
	return dsp.SetupAllNodesPlanningWithOracle(ctx, evalCtx, execCfg, physicalplan.DefaultReplicaChooser)
}

// SetupAllNodesPlanning creates a planCtx and sets up the planCtx.nodeStatuses
// map for all nodes. It returns all nodes that can be used for planning.
func (dsp *DistSQLPlanner) SetupAllNodesPlanningWithOracle(
	ctx context.Context,
	evalCtx *extendedEvalContext,
	execCfg *ExecutorConfig,
	oracle replicaoracle.Oracle,
) (*PlanningCtx, []base.SQLInstanceID, error) {
	if dsp.codec.ForSystemTenant() {
		return dsp.setupAllNodesPlanningSystem(ctx, evalCtx, execCfg, oracle)
	}
	return dsp.setupAllNodesPlanningTenant(ctx, evalCtx, execCfg, oracle)
}

// setupAllNodesPlanningSystem creates a planCtx and returns all nodes available
// in a system tenant.
func (dsp *DistSQLPlanner) setupAllNodesPlanningSystem(
	ctx context.Context,
	evalCtx *extendedEvalContext,
	execCfg *ExecutorConfig,
	oracle replicaoracle.Oracle,
) (*PlanningCtx, []base.SQLInstanceID, error) {
	planCtx := dsp.NewPlanningCtxWithOracle(ctx, evalCtx, nil /* planner */, nil, /* txn */
		DistributionTypeAlways, oracle)

	ss, err := execCfg.NodesStatusServer.OptionalNodesStatusServer(47900)
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
		_ /* NodeStatus */ = dsp.checkInstanceHealthAndVersionSystem(ctx, planCtx, base.SQLInstanceID(node.Desc.NodeID))
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
// in a non-system tenant.
func (dsp *DistSQLPlanner) setupAllNodesPlanningTenant(
	ctx context.Context,
	evalCtx *extendedEvalContext,
	execCfg *ExecutorConfig,
	oracle replicaoracle.Oracle,
) (*PlanningCtx, []base.SQLInstanceID, error) {
	planCtx := dsp.NewPlanningCtxWithOracle(ctx, evalCtx, nil /* planner */, nil, /* txn */
		DistributionTypeAlways, oracle)
	pods, err := dsp.sqlAddressResolver.GetAllInstances(ctx)
	if err != nil {
		return nil, nil, err
	}
	sqlInstanceIDs := make([]base.SQLInstanceID, len(pods))
	for i, pod := range pods {
		sqlInstanceIDs[i] = pod.InstanceID
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
	beforeSpecs := before.GenerateFlowSpecs()
	afterSpecs := after.GenerateFlowSpecs()

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

// PlanChangeDecision descrubes a function that decides if a plan has "changed"
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

// ErrPlanChanged is a sentinel marker error for use to signal a plan changed.
var ErrPlanChanged = errors.New("physical plan has changed")

// PhysicalPlanChangeChecker returns a function which will periodically call the
// passed function at the requested interval until the returned channel is
// closed and compare the plan it returns to the passed initial plan, returning
// and error if it has changed (as defined by CalculatePlanGrowth) by more than
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
