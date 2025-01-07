// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

func (p *planner) CheckExternalConnection(
	_ context.Context, n *tree.CheckExternalConnection,
) (planNode, error) {
	return &checkExternalConnectionNode{node: n, columns: CloudCheckHeader}, nil
}

type checkExternalConnectionNode struct {
	zeroInputPlanNode
	node    *tree.CheckExternalConnection
	loc     string
	params  CloudCheckParams
	rows    chan tree.Datums
	row     tree.Datums
	columns colinfo.ResultColumns
}

var _ planNode = &checkExternalConnectionNode{}

func (n *checkExternalConnectionNode) startExec(params runParams) error {
	if err := n.parseParams(params); err != nil {
		return err
	}
	if err := CheckDestinationPrivileges(
		params.ctx,
		params.p,
		[]string{n.loc},
	); err != nil {
		return err
	}

	ctx, span := tracing.ChildSpan(params.ctx, "CheckExternalConnection")
	defer span.Finish()

	store, err := params.ExecCfg().DistSQLSrv.ExternalStorageFromURI(ctx, n.loc, params.p.User())
	if err != nil {
		return errors.Wrap(err, "connect to external storage")
	}
	defer store.Close()

	dsp := params.p.DistSQLPlanner()
	evalCtx := params.extendedEvalCtx
	planCtx, sqlInstanceIDs, err := dsp.SetupAllNodesPlanning(ctx, evalCtx, params.ExecCfg())
	if err != nil {
		return err
	}
	plan := planCtx.NewPhysicalPlan()
	corePlacement := make([]physicalplan.ProcessorCorePlacement, len(sqlInstanceIDs))
	spec := &execinfrapb.CloudStorageTestSpec{Location: n.loc, Params: n.params}
	for i := range sqlInstanceIDs {
		corePlacement[i].SQLInstanceID = sqlInstanceIDs[i]
		corePlacement[i].Core.CloudStorageTest = spec
	}
	plan.AddNoInputStage(
		corePlacement,
		execinfrapb.PostProcessSpec{},
		cloudCheckFlowTypes,
		execinfrapb.Ordering{},
	)
	plan.PlanToStreamColMap = make([]int, len(cloudCheckFlowTypes))
	for i := range plan.PlanToStreamColMap {
		plan.PlanToStreamColMap[i] = i
	}
	FinalizePlan(ctx, planCtx, plan)

	rateFromDatums := func(bytes tree.Datum, nanos tree.Datum) string {
		return string(humanizeutil.DataRate(
			int64(tree.MustBeDInt(bytes)),
			time.Duration(tree.MustBeDInt(nanos)),
		))
	}
	n.rows = make(chan tree.Datums, int64(len(sqlInstanceIDs))*n.params.Concurrency)
	rowWriter := NewCallbackResultWriter(func(ctx context.Context, row tree.Datums) error {
		// collapse the two pairs of bytes+time to a single string rate each.
		res := make(tree.Datums, len(row)-1)
		copy(res[:4], row[:4])
		res[4] = tree.NewDString(string(humanizeutil.IBytes(int64(tree.MustBeDInt(row[6])))))
		res[5] = tree.NewDString(rateFromDatums(row[4], row[5]))
		res[6] = tree.NewDString(rateFromDatums(row[6], row[7]))
		res[7] = row[8]
		n.rows <- res
		return nil
	})

	go func() {
		recv := MakeDistSQLReceiver(
			ctx,
			rowWriter,
			tree.Rows,
			nil, /* rangeCache */
			nil, /* txn - the flow does not read or write the database */
			nil, /* clockUpdater */
			evalCtx.Tracing,
		)
		defer recv.Release()
		defer close(n.rows)

		evalCtxCopy := *evalCtx
		dsp.Run(ctx, planCtx, nil, plan, recv, &evalCtxCopy, nil /* finishedSetupFn */)
	}()
	return nil
}

func (n *checkExternalConnectionNode) Next(params runParams) (bool, error) {
	if n.rows == nil {
		return false, nil
	}
	select {
	case <-params.ctx.Done():
		return false, params.ctx.Err()
	case row, more := <-n.rows:
		if !more {
			n.rows = nil
			return false, nil
		}
		n.row = row
		return true, nil
	}
}

func (n *checkExternalConnectionNode) Values() tree.Datums {
	return n.row
}

func (n *checkExternalConnectionNode) Close(_ context.Context) {
	if n.rows != nil {
		close(n.rows)
		n.rows = nil
	}
}

func (n *checkExternalConnectionNode) parseParams(params runParams) error {
	exprEval := params.p.ExprEvaluator("CHECK EXTERNAL CONNECTION")
	loc, err := exprEval.String(params.ctx, n.node.URI)
	if err != nil {
		return err
	}
	n.loc = loc
	if n.node.Options.TransferSize != nil {
		transferSizeStr, err := exprEval.String(params.ctx, n.node.Options.TransferSize)
		if err != nil {
			return err
		}
		parsed, err := humanizeutil.ParseBytes(transferSizeStr)
		if err != nil {
			return err
		}
		n.params.TransferSize = parsed
	}
	if n.node.Options.Duration != nil {
		durationStr, err := exprEval.String(params.ctx, n.node.Options.Duration)
		if err != nil {
			return err
		}
		parsed, err := time.ParseDuration(durationStr)
		if err != nil {
			return err
		}
		n.params.MinDuration = parsed
	}
	if n.node.Options.Concurrency != nil {
		concurrency, err := exprEval.Int(params.ctx, n.node.Options.Concurrency)
		if err != nil {
			return err
		}
		n.params.Concurrency = concurrency
	}
	return nil
}
