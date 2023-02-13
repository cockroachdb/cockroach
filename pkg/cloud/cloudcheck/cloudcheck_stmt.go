// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cloudcheck

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/cloud/cloudprivilege"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/pkg/errors"
)

var Header = colinfo.ResultColumns{
	{Name: "node", Typ: types.Int},
	{Name: "locality", Typ: types.String},
	{Name: "ok", Typ: types.Bool},
	{Name: "error", Typ: types.String},
	{Name: "read_speed", Typ: types.String},
	{Name: "write_speed", Typ: types.String},
	{Name: "can_delete", Typ: types.Bool},
}

// ShowCloudStorageTestPlanHook is currently called by showBackup hook but
// should be extended to be a standalone plan instead.
func ShowCloudStorageTestPlanHook(
	ctx context.Context, p sql.PlanHookState, location string,
) (sql.PlanHookRowFn, colinfo.ResultColumns, []sql.PlanNode, bool, error) {

	if err := cloudprivilege.CheckDestinationPrivileges(ctx, p, []string{location}); err != nil {
		return nil, nil, nil, false, err
	}

	fn := func(ctx context.Context, _ []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		ctx, span := tracing.ChildSpan(ctx, "cloudcheck")
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
		spec := &execinfrapb.CloudStorageTestSpec{Location: location}
		for i := range sqlInstanceIDs {
			corePlacement[i].SQLInstanceID = sqlInstanceIDs[i]
			corePlacement[i].Core.CloudStorageTest = spec
		}
		plan.AddNoInputStage(corePlacement, execinfrapb.PostProcessSpec{}, flowTypes, execinfrapb.Ordering{})

		plan.PlanToStreamColMap = []int{0, 1, 2, 3, 4, 5, 6}

		sql.FinalizePlan(ctx, planCtx, plan)

		rowResultWriter := sql.NewCallbackResultWriter(func(ctx context.Context, row tree.Datums) error {
			row[4] = tree.NewDString(fmt.Sprintf("%0.2fMiB/s", float64(tree.MustBeDInt(row[4]))/float64(1<<20)))
			row[5] = tree.NewDString(fmt.Sprintf("%0.2fMiB/s", float64(tree.MustBeDInt(row[5]))/float64(1<<20)))
			resultsCh <- row
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
