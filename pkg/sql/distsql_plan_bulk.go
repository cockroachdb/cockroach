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
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// SetupAllNodesPlanning creates a planCtx and sets up the planCtx.NodeStatuses
// map for all nodes. It returns all nodes that can be used for planning.
func (dsp *DistSQLPlanner) SetupAllNodesPlanning(
	ctx context.Context, evalCtx *extendedEvalContext, execCfg *ExecutorConfig,
) (*PlanningCtx, []base.SQLInstanceID, error) {
	if dsp.codec.ForSystemTenant() {
		return dsp.setupAllNodesPlanningSystem(ctx, evalCtx, execCfg)
	}
	return dsp.setupAllNodesPlanningTenant(ctx, evalCtx, execCfg)
}

// setupAllNodesPlanningSystem creates a planCtx and returns all nodes available
// in a system tenant.
func (dsp *DistSQLPlanner) setupAllNodesPlanningSystem(
	ctx context.Context, evalCtx *extendedEvalContext, execCfg *ExecutorConfig,
) (*PlanningCtx, []base.SQLInstanceID, error) {
	planCtx := dsp.NewPlanningCtx(ctx, evalCtx, nil /* planner */, nil, /* txn */
		DistributionTypeAlways)

	ss, err := execCfg.NodesStatusServer.OptionalNodesStatusServer(47900)
	if err != nil {
		return planCtx, []base.SQLInstanceID{dsp.gatewaySQLInstanceID}, nil //nolint:returnerrcheck
	}
	resp, err := ss.ListNodesInternal(ctx, &serverpb.NodesRequest{})
	if err != nil {
		return nil, nil, err
	}
	// Because we're not going through the normal pathways, we have to set up the
	// planCtx.NodeStatuses map ourselves. CheckInstanceHealthAndVersion() will
	// populate it.
	for _, node := range resp.Nodes {
		_ /* NodeStatus */ = dsp.CheckInstanceHealthAndVersion(planCtx, base.SQLInstanceID(node.Desc.NodeID))
	}
	nodes := make([]base.SQLInstanceID, 0, len(planCtx.NodeStatuses))
	for nodeID, status := range planCtx.NodeStatuses {
		if status == NodeOK {
			nodes = append(nodes, nodeID)
		}
	}
	// Shuffle node order so that multiple IMPORTs done in parallel will not
	// identically schedule CSV reading. For example, if there are 3 nodes and 4
	// files, the first node will get 2 files while the other nodes will each get 1
	// file. Shuffling will make that first node random instead of always the same.
	rand.Shuffle(len(nodes), func(i, j int) {
		nodes[i], nodes[j] = nodes[j], nodes[i]
	})
	return planCtx, nodes, nil
}

// setupAllNodesPlanningTenant creates a planCtx and returns all nodes available
// in a non-system tenant.
func (dsp *DistSQLPlanner) setupAllNodesPlanningTenant(
	ctx context.Context, evalCtx *extendedEvalContext, execCfg *ExecutorConfig,
) (*PlanningCtx, []base.SQLInstanceID, error) {
	if dsp.sqlInstanceProvider == nil {
		return nil, nil, errors.New("sql instance provider not available in multi-tenant environment")
	}
	planCtx := dsp.NewPlanningCtx(ctx, evalCtx, nil /* planner */, nil, /* txn */
		DistributionTypeAlways)
	pods, err := dsp.sqlInstanceProvider.GetAllInstances(ctx)
	if err != nil {
		return nil, nil, err
	}
	sqlInstanceIDs := make([]base.SQLInstanceID, len(pods))
	for i, pod := range pods {
		sqlInstanceIDs[i] = pod.InstanceID
	}
	// Shuffle node order so that multiple IMPORTs done in parallel will not
	// identically schedule CSV reading. For example, if there are 3 nodes and 4
	// files, the first node will get 2 files while the other nodes will each get 1
	// file. Shuffling will make that first node random instead of always the same.
	rand.Shuffle(len(sqlInstanceIDs), func(i, j int) {
		sqlInstanceIDs[i], sqlInstanceIDs[j] = sqlInstanceIDs[j], sqlInstanceIDs[i]
	})
	return planCtx, sqlInstanceIDs, nil
}

// PhysicalPlanMaker describes a function that makes a physical plan.
type PhysicalPlanMaker func(context.Context, *DistSQLPlanner) (*PhysicalPlan, *PlanningCtx, error)

// CalculatePlanGrowth returns the number of new or updated sql instances in one
// physical plan relative to an old one, as well as that number as a fraction of
// the number in the old one.
func CalculatePlanGrowth(before, after *PhysicalPlan) (int, float64) {
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

	return changed, float64(changed) / float64(len(beforeSpecs))
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
	name string,
	initial *PhysicalPlan,
	fn PhysicalPlanMaker,
	execCtx JobExecContext,
	freq time.Duration,
	thresholdFn func() float64,
) (func(context.Context) error, chan struct{}) {
	stop := make(chan struct{})

	return func(ctx context.Context) error {
		tick := time.NewTicker(freq)
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
				changed, growth := CalculatePlanGrowth(initial, p)
				threshold := thresholdFn()
				replan := threshold != 0.0 && growth > threshold
				if growth > 0.1 || log.V(1) {
					log.Infof(ctx, "Re-planned %s would add/alter flows on %d nodes / %.2f, threshold %.2f, replan %v",
						changed, growth, threshold, replan)
				}
				if replan {
					return ErrPlanChanged
				}
			}
		}
	}, stop
}
