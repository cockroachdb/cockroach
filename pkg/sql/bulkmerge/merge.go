// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulkmerge

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

// Merge creates and waits on a DistSQL flow that merges the provided SSTs into
// into the ranges defined by the input splits.
func Merge(
	ctx context.Context,
	execCtx sql.JobExecContext,
	ssts []execinfrapb.BulkMergeSpec_SST,
	splits []roachpb.Key,
	outputURI func(sqlInstance base.SQLInstanceID) string,
) ([]execinfrapb.BulkMergeSpec_SST, error) {
	execCfg := execCtx.ExecCfg()

	plan, planCtx, err := newBulkMergePlan(ctx, execCtx, ssts, splits, outputURI)
	if err != nil {
		return nil, err
	}

	var result execinfrapb.BulkMergeSpec_Output
	rowWriter := sql.NewCallbackResultWriter(func(ctx context.Context, row tree.Datums) error {
		return protoutil.Unmarshal([]byte(*row[0].(*tree.DBytes)), &result)
	})

	sqlReciever := sql.MakeDistSQLReceiver(
		ctx,
		rowWriter,
		tree.Rows,
		execCfg.RangeDescriptorCache,
		nil,
		nil,
		execCtx.ExtendedEvalContext().Tracing)
	defer sqlReciever.Release()

	execCtx.DistSQLPlanner().Run(
		ctx,
		planCtx,
		nil,
		plan,
		sqlReciever,
		execCtx.ExtendedEvalContext(),
		nil,
	)

	if err := rowWriter.Err(); err != nil {
		return nil, err
	}

	return result.Ssts, nil
}
