// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"math"

	"github.com/cockroachdb/cockroach/pkg/sql/colflow"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
)

// explainVecNode is a planNode that wraps a plan and returns
// information related to running that plan with the vectorized engine.
type explainVecNode struct {
	optColumnsSlot

	options *tree.ExplainOptions
	plan    planComponents

	run struct {
		lines []string
		// The current row returned by the node.
		values tree.Datums
	}
}

func (n *explainVecNode) startExec(params runParams) error {
	n.run.values = make(tree.Datums, 1)
	distSQLPlanner := params.extendedEvalCtx.DistSQLPlanner
	distribution, _ := getPlanDistribution(
		params.ctx, params.p.Descriptors().HasUncommittedTypes(),
		params.extendedEvalCtx.SessionData(), n.plan.main, &params.p.distSQLVisitor,
	)
	outerSubqueries := params.p.curPlan.subqueryPlans
	planCtx := newPlanningCtxForExplainPurposes(distSQLPlanner, params, n.plan.subqueryPlans, distribution)
	defer func() {
		planCtx.planner.curPlan.subqueryPlans = outerSubqueries
	}()
	physPlan, cleanup, err := newPhysPlanForExplainPurposes(params.ctx, planCtx, distSQLPlanner, n.plan.main)
	defer cleanup()
	if err != nil {
		if len(n.plan.subqueryPlans) > 0 {
			return errors.New("running EXPLAIN (VEC) on this query is " +
				"unsupported because of the presence of subqueries")
		}
		return err
	}

	finalizePlanWithRowCount(params.ctx, planCtx, physPlan, n.plan.mainRowCount)
	flows, flowsCleanup := physPlan.GenerateFlowSpecs()
	defer flowsCleanup(flows)
	flowCtx := newFlowCtxForExplainPurposes(params.ctx, params.p)

	// We want to get the vectorized plan which would be executed with the
	// current 'vectorize' option. If 'vectorize' is set to 'off', then the
	// vectorized engine is disabled, and we will return an error in such case.
	// With all other options, we don't change the setting to the
	// most-inclusive option as we used to because the plan can be different
	// based on 'vectorize' setting.
	if flowCtx.EvalCtx.SessionData().VectorizeMode == sessiondatapb.VectorizeOff {
		return errors.New("vectorize is set to 'off'")
	}
	verbose := n.options.Flags[tree.ExplainFlagVerbose]
	flowCtx.Local = !physPlan.Distribution.WillDistribute()
	// When running EXPLAIN (VEC) we choose the option of "not recording stats"
	// since we don't know whether the next invocation of the explained
	// statement would result in the collection of execution stats or not.
	const recordingStats = false
	n.run.lines, err = colflow.ExplainVec(
		params.ctx, flowCtx, flows, physPlan.LocalProcessors, nil, /* opChains */
		distSQLPlanner.gatewaySQLInstanceID, verbose, recordingStats,
	)
	if err != nil {
		return err
	}
	return nil
}

func newFlowCtxForExplainPurposes(ctx context.Context, p *planner) *execinfra.FlowCtx {
	monitor := mon.NewMonitor(mon.Options{
		Name:     mon.MakeMonitorName("explain"),
		Settings: p.execCfg.Settings,
	})
	// Note that we do not use planner's monitor here in order to not link any
	// monitors created later to the planner's monitor (since we might not close
	// the components that use them). This also allows us to not close this
	// monitor because eventually the whole monitor tree rooted in this monitor
	// will be garbage collected.
	monitor.Start(ctx, nil /* pool */, mon.NewStandaloneBudget(math.MaxInt64))
	return &execinfra.FlowCtx{
		NodeID:  p.EvalContext().NodeID,
		EvalCtx: p.EvalContext(),
		Mon:     monitor,
		Txn:     p.txn,
		Cfg: &execinfra.ServerConfig{
			Settings:          p.execCfg.Settings,
			LogicalClusterID:  p.DistSQLPlanner().distSQLSrv.ServerConfig.LogicalClusterID,
			VecFDSemaphore:    p.execCfg.DistSQLSrv.VecFDSemaphore,
			SQLInstanceDialer: p.DistSQLPlanner().sqlInstanceDialer,
		},
		Descriptors: p.Descriptors(),
		DiskMonitor: &mon.BytesMonitor{},
	}
}

func newPlanningCtxForExplainPurposes(
	distSQLPlanner *DistSQLPlanner,
	params runParams,
	subqueryPlans []subquery,
	distribution physicalplan.PlanDistribution,
) *PlanningCtx {
	distribute := DistributionType(LocalDistribution)
	if distribution.WillDistribute() {
		distribute = FullDistribution
	}
	planCtx := distSQLPlanner.NewPlanningCtx(params.ctx, params.extendedEvalCtx,
		params.p, params.p.txn, distribute)
	planCtx.planner.curPlan.subqueryPlans = subqueryPlans
	for i := range planCtx.planner.curPlan.subqueryPlans {
		p := &planCtx.planner.curPlan.subqueryPlans[i]
		// Fake subquery results - they're not important for our explain output.
		p.started = true
		p.result = tree.DNull
	}
	return planCtx
}

func (n *explainVecNode) Next(runParams) (bool, error) {
	if len(n.run.lines) == 0 {
		return false, nil
	}
	n.run.values[0] = tree.NewDString(n.run.lines[0])
	n.run.lines = n.run.lines[1:]
	return true, nil
}

func (n *explainVecNode) Values() tree.Datums { return n.run.values }
func (n *explainVecNode) Close(ctx context.Context) {
	n.plan.close(ctx)
}

func (n *explainVecNode) InputCount() int {
	// We check whether planNode is nil because the input might be represented
	// physically, which we can't traverse into currently.
	// TODO(yuzefovich/mgartner): Figure out a way to traverse into physical
	// plans, if necessary.
	if n.plan.main.planNode != nil {
		return 1
	}
	return 0
}

func (n *explainVecNode) Input(i int) (planNode, error) {
	if i == 0 && n.plan.main.planNode != nil {
		return n.plan.main.planNode, nil
	}
	return nil, errors.AssertionFailedf("input index %d is out of range", i)
}
