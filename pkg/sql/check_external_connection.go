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
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
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
	execGrp ctxgroup.Group
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

	ctx, span := tracing.ChildSpan(params.ctx, "CheckExternalConnection-planning")
	defer span.Finish()

	store, err := params.ExecCfg().DistSQLSrv.ExternalStorageFromURI(ctx, n.loc, params.p.User())
	if err != nil {
		return errors.Wrap(err, "connect to external storage")
	}
	defer store.Close()

	dsp := params.p.DistSQLPlanner()
	planCtx, sqlInstanceIDs, err := dsp.SetupAllNodesPlanning(ctx, params.extendedEvalCtx, params.ExecCfg())
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
		nil, /* finalizeLastStageCb */
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
	n.rows = make(chan tree.Datums, len(sqlInstanceIDs)*getCloudCheckConcurrency(n.params))
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

	workerStarted := make(chan struct{})
	n.execGrp = ctxgroup.WithContext(params.ctx)
	n.execGrp.GoCtx(func(ctx context.Context) error {
		// Derive a separate tracing span since the planning one will be
		// finished when the main goroutine exits from startExec.
		ctx, span := tracing.ChildSpan(ctx, "CheckExternalConnection-execution")
		defer span.Finish()
		// Unblock the main goroutine after having created the tracing span.
		close(workerStarted)

		recv := MakeDistSQLReceiver(
			ctx,
			rowWriter,
			tree.Rows,
			nil, /* rangeCache */
			nil, /* txn - the flow does not read or write the database */
			nil, /* clockUpdater */
			params.extendedEvalCtx.Tracing,
		)
		defer recv.Release()
		defer close(n.rows)

		// Copy the eval.Context, as dsp.Run() might change it.
		evalCtxCopy := params.extendedEvalCtx.Context.Copy()
		dsp.Run(ctx, planCtx, nil, plan, recv, evalCtxCopy, nil /* finishedSetupFn */)
		return nil
	})

	// Block until the worker goroutine has started. This allows us to guarantee
	// that params.ctx contains a tracing span that hasn't been finished.
	// TODO(yuzefovich): this is a bit hacky. The issue is that
	// planNodeToRowSource has already created a new tracing span for this
	// checkExternalConnectionNode and has updated params.ctx accordingly; then,
	// if the query is canceled before the worker goroutine starts, the tracing
	// span is finished, yet it will have already been captured by the ctxgroup.
	<-workerStarted
	return nil
}

func (n *checkExternalConnectionNode) Next(params runParams) (bool, error) {
	select {
	case <-params.ctx.Done():
		return false, params.ctx.Err()
	case row, more := <-n.rows:
		if !more {
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
	_ = n.execGrp.Wait()
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
