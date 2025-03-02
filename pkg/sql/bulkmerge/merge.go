// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulkmerge

import (
	"bytes"
	"context"
	"slices"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// Merge creates and waits on a DistSQL flow that merges the provided SSTs into
// into the ranges defined by the input splits.
func Merge(
	ctx context.Context,
	execCtx sql.JobExecContext,
	ssts []execinfrapb.BulkMergeSpec_SST,
	spans []roachpb.Span,
	outputURI func(sqlInstance base.SQLInstanceID) string,
) ([]execinfrapb.BulkMergeSpec_SST, error) {
	// TODO(jeffswenson): validate the splits are in order
	log.Infof(ctx, "bulk merge ssts:%d spans:%d", len(ssts), len(spans))

	execCfg := execCtx.ExecCfg()

	for _, sst := range ssts {
		if len(sst.Uri) == 0 {
			return nil, errors.New("invalid merge input: sst uri is empty")
		}
	}

	plan, planCtx, err := newBulkMergePlan(ctx, execCtx, ssts, spans, outputURI)
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

	// Sort the SSTs by their range start key. Ingest requires that SSTs are
	// sorted an non-overlapping. The output of merge is not sorted because SSTs
	// are emitted as their task is completed.
	slices.SortFunc(result.Ssts, func(i, j execinfrapb.BulkMergeSpec_SST) int {
		return bytes.Compare(i.StartKey, j.StartKey)
	})

	return result.Ssts, nil
}
