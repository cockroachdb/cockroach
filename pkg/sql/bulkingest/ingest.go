// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulkingest

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// IngestFiles runs the distributed ingest phase for a set of pre-built SSTs.
// It:
//   - picks range splits that align with SST boundaries,
//   - pre-splits and scatters those ranges, and
//   - executes an ingest DistSQL flow that AddSSTables each file.
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
	if err := SplitAndScatterSpans(ctx, db, splits); err != nil {
		return err
	}

	plan, planCtx, err := planIngest(ctx, execCtx, ssts)
	if err != nil {
		return err
	}

	res := sql.NewMetadataOnlyMetadataCallbackWriter(func(context.Context, *execinfrapb.ProducerMetadata) error { return nil })
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

	evalCtxCopy := execCtx.ExtendedEvalContext().Context.Copy()
	execCtx.DistSQLPlanner().Run(
		ctx, planCtx, nil, plan, recv, evalCtxCopy, nil, /* finishedSetupFn */
	)

	return res.Err()
}

func init() {
	sql.RegisterBulkIngest(IngestFiles)
}
