// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
)

type explainDDLNode struct {
	optColumnsSlot
	options *tree.ExplainOptions
	plan    planComponents
	next    int
	values  []tree.Datums
}

func (n *explainDDLNode) Next(params runParams) (bool, error) {
	if n.next >= len(n.values) {
		return false, nil
	}
	n.next++
	return true, nil
}

func (n *explainDDLNode) Values() tree.Datums {
	return n.values[n.next-1]
}

func (n *explainDDLNode) Close(ctx context.Context) {
	n.next = len(n.values)
}

func (n *explainDDLNode) InputCount() int {
	// We check whether planNode is nil because the input might be represented
	// physically, which we can't traverse into currently.
	// TODO(yuzefovich/mgartner): Figure out a way to traverse into physical
	// plans, if necessary.
	if n.plan.main.planNode != nil {
		return 1
	}
	return 0
}

func (n *explainDDLNode) Input(i int) (planNode, error) {
	if i == 0 && n.plan.main.planNode != nil {
		return n.plan.main.planNode, nil
	}
	return nil, errors.AssertionFailedf("input index %d is out of range", i)
}

var _ planNode = (*explainDDLNode)(nil)

var explainNotPossibleError = pgerror.New(pgcode.FeatureNotSupported,
	"cannot explain a statement which is not supported by the declarative schema changer")

func (n *explainDDLNode) startExec(params runParams) error {
	// TODO(postamar): better error messages for each error case
	scNode, ok := n.plan.main.planNode.(*schemaChangePlanNode)
	if !ok {
		if n.plan.main.physPlan == nil {
			return explainNotPossibleError
		} else if len(n.plan.main.physPlan.planNodesToClose) > 0 {
			scNode, ok = n.plan.main.physPlan.planNodesToClose[0].(*schemaChangePlanNode)
			if !ok {
				return explainNotPossibleError
			}
		} else {
			return explainNotPossibleError
		}
	}
	return n.setExplainValues(params.ctx, params.ExecCfg().Settings,
		scNode.plannedState, &params.p.ExtendedEvalContext().SchemaChangerState.memAcc)
}

func (n *explainDDLNode) setExplainValues(
	ctx context.Context,
	settings *cluster.Settings,
	scState scpb.CurrentState,
	memAcc *mon.BoundAccount,
) (err error) {
	defer func() {
		err = errors.WithAssertionFailure(err)
	}()
	var p scplan.Plan
	p, err = scplan.MakePlan(ctx, scState, scplan.Params{
		Ctx:                        ctx,
		ActiveVersion:              settings.Version.ActiveVersion(ctx),
		ExecutionPhase:             scop.StatementPhase,
		SchemaChangerJobIDSupplier: func() jobspb.JobID { return 1 },
		MemAcc:                     memAcc,
	})
	if err != nil {
		return err
	}
	if n.options.Flags[tree.ExplainFlagViz] {
		stagesURL, depsURL, err := p.ExplainViz()
		n.values = []tree.Datums{
			{tree.NewDString(stagesURL)},
			{tree.NewDString(depsURL)},
		}
		return err
	}

	var info string
	if n.options.Flags[tree.ExplainFlagVerbose] {
		info, err = p.ExplainVerbose()
	} else if n.options.Flags[tree.ExplainFlagShape] {
		info, err = p.ExplainShape()
	} else {
		info, err = p.ExplainCompact()
	}
	n.values = []tree.Datums{
		{tree.NewDString(info)},
	}
	return err
}
