// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cloudcheck

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cloud/cloudprivilege"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

var Header = colinfo.ResultColumns{
	{Name: "node", Typ: types.Int},
	{Name: "locality", Typ: types.String},
	{Name: "ok", Typ: types.Bool},
	{Name: "error", Typ: types.String},
	{Name: "transferred", Typ: types.String},
	{Name: "read_speed", Typ: types.String},
	{Name: "write_speed", Typ: types.String},
	{Name: "can_delete", Typ: types.Bool},
}

// Params configure the behavior of a cloud storage check.
type Params = execinfrapb.CloudStorageTestSpec_Params

// ShowCloudStorageTestPlanHook is currently called by showBackup hook but
// should be extended to be a standalone plan instead.
func ShowCloudStorageTestPlanHook(
	ctx context.Context, p sql.PlanHookState, location string, params Params,
) (sql.PlanHookRowFn, colinfo.ResultColumns, []sql.PlanNode, bool, error) {

	if err := cloudprivilege.CheckDestinationPrivileges(ctx, p, []string{location}); err != nil {
		return nil, nil, nil, false, err
	}

	fn := func(ctx context.Context, _ []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		ctx, span := tracing.ChildSpan(ctx, "ShowCloudStorageTestPlanHook")
		defer span.Finish()

		store, err := p.ExecCfg().DistSQLSrv.ExternalStorageFromURI(ctx, location, p.User())
		if err != nil {
			return errors.Wrapf(err, "connect to external storage")
		}
		defer store.Close()

		dsp := p.DistSQLPlanner()
		evalCtx := p.ExtendedEvalContext()
		planCtx, sqlInstanceIDs, err := dsp.SetupAllNodesPlanning(ctx, evalCtx, p.ExecCfg())
		if err != nil {
			return err
		}
		plan := planCtx.NewPhysicalPlan()
		corePlacement := make([]physicalplan.ProcessorCorePlacement, len(sqlInstanceIDs))
		spec := &execinfrapb.CloudStorageTestSpec{Location: location, Params: params}
		for i := range sqlInstanceIDs {
			corePlacement[i].SQLInstanceID = sqlInstanceIDs[i]
			corePlacement[i].Core.CloudStorageTest = spec
		}
		plan.AddNoInputStage(corePlacement, execinfrapb.PostProcessSpec{}, flowTypes, execinfrapb.Ordering{})

		plan.PlanToStreamColMap = make([]int, len(flowTypes))
		for i := range plan.PlanToStreamColMap {
			plan.PlanToStreamColMap[i] = i
		}
		sql.FinalizePlan(ctx, planCtx, plan)

		rateFromDatums := func(bytes tree.Datum, nanos tree.Datum) string {
			return string(humanizeutil.DataRate(int64(tree.MustBeDInt(bytes)), time.Duration(tree.MustBeDInt(nanos))))
		}
		rowResultWriter := sql.NewCallbackResultWriter(func(ctx context.Context, row tree.Datums) error {
			// collapse the two pairs of bytes+time to a single string rate each.
			res := make(tree.Datums, len(row)-1)
			copy(res[:4], row[:4])
			res[4] = tree.NewDString(string(humanizeutil.IBytes(int64(tree.MustBeDInt(row[6])))))
			res[5] = tree.NewDString(rateFromDatums(row[4], row[5]))
			res[6] = tree.NewDString(rateFromDatums(row[6], row[7]))
			res[7] = row[8]
			resultsCh <- res
			return nil
		})

		recv := sql.MakeDistSQLReceiver(
			ctx,
			rowResultWriter,
			tree.Rows,
			nil, /* rangeCache */
			nil, /* txn - the flow does not read or write the database */
			nil, /* clockUpdater */
			evalCtx.Tracing,
		)
		defer recv.Release()

		evalCtxCopy := *evalCtx
		dsp.Run(ctx, planCtx, nil, plan, recv, &evalCtxCopy, nil /* finishedSetupFn */)
		return rowResultWriter.Err()
	}
	return fn, Header, nil, false, nil
}
