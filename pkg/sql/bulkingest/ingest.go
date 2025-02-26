package bulkingest

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func IngestFiles(
	ctx context.Context,
	execCtx sql.JobExecContext,
	spans []roachpb.Span,
	ssts []execinfrapb.BulkMergeSpec_SST,
) error {
	splits, err := pickSplits(spans, ssts)
	if err != nil {
		return err
	}

	db := execCtx.ExecCfg().InternalDB.KV()
	if err := splitAndScatterSpans(ctx, db, splits); err != nil {
		return err
	}

	plan, planCtx, err := planIngest(ctx, execCtx, ssts)
	if err != nil {
		return err
	}

	res := sql.NewMetadataOnlyMetadataCallbackWriter()
	recv := sql.MakeDistSQLReceiver(
		ctx,
		res,
		tree.Ack,
		execCtx.ExecCfg().RangeDescriptorCache, /* rangeCache */
		nil,                                    /* txn - the flow does not read or write the database */
		nil,                                    /* clockUpdater */
		execCtx.ExtendedEvalContext().Tracing,
	)
	defer recv.Release()

	execCtx.DistSQLPlanner().Run(ctx, planCtx, nil, plan, recv, execCtx.ExtendedEvalContext(), nil /* finishedSetupFn */)

	return res.Err()
}
