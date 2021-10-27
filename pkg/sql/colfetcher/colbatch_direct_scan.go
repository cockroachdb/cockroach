// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colfetcher

import (
	"context"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/colserde"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecargs"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	protoTypes "github.com/gogo/protobuf/types"
)

type ColBatchDirectScan struct {
	*ColBatchScan
	fetcher row.KVBatchFetcher
	spec    execinfrapb.TableReaderSpec
	post    execinfrapb.PostProcessSpec
	batch   coldata.Batch
	data    []*array.Data

	converter       *colserde.ArrowBatchConverter
	deser           *colserde.RecordBatchSerializer
	flowCtx         *execinfra.FlowCtx
	kvFetcherMemAcc *mon.BoundAccount
}

// NewColBatchScan creates a new ColBatchScan operator.
func NewColDirectBatchScan(
	ctx context.Context,
	allocator *colmem.Allocator,
	kvFetcherMemAcc *mon.BoundAccount,
	flowCtx *execinfra.FlowCtx,
	evalCtx *tree.EvalContext,
	helper *colexecargs.ExprHelper,
	spec *execinfrapb.TableReaderSpec,
	post *execinfrapb.PostProcessSpec,
	estimatedRowCount uint64,
) (*ColBatchDirectScan, error) {
	scan, err := NewColBatchScan(ctx, allocator, kvFetcherMemAcc, flowCtx,
		evalCtx, helper, spec, post, estimatedRowCount)
	if err != nil {
		return nil, err
	}

	return &ColBatchDirectScan{
		ColBatchScan:    scan,
		data:            make([]*array.Data, len(scan.ResultTypes)),
		spec:            *spec,
		post:            *post,
		kvFetcherMemAcc: kvFetcherMemAcc,
		flowCtx:         flowCtx,
	}, nil
}

func (c *ColBatchDirectScan) Init(ctx context.Context) {
	c.ColBatchScan.Init(ctx)
	spec, err := protoTypes.MarshalAny(&c.spec)
	if err != nil {
		colexecerror.InternalError(err)
	}
	c.deser, err = colserde.NewRecordBatchSerializer(c.ResultTypes)
	if err != nil {
		colexecerror.InternalError(err)
	}
	c.converter, err = colserde.NewArrowBatchConverter(c.ResultTypes)
	if err != nil {
		colexecerror.InternalError(err)
	}
	c.batch = coldata.NewMemBatch(c.ResultTypes, coldata.StandardColumnFactory)
	// If we have a limit hint, we limit the first batch size. Subsequent
	// batches get larger to avoid making things too slow (e.g. in case we have
	// a very restrictive filter and actually have to retrieve a lot of rows).
	firstBatchLimit := rowinfra.KeyLimit(c.limitHint)
	if firstBatchLimit != 0 {
		// Keep track of the maximum keys per row to accommodate a
		// limitHint when StartScan is invoked.
		table := c.spec.BuildTableDescriptor()
		index := table.ActiveIndexes()[c.spec.IndexIdx]
		keysPerRow, err := table.KeysPerRow(index.GetID())
		if err != nil {
			colexecerror.InternalError(err)
		}
		// The limitHint is a row limit, but each row could be made up
		// of more than one key. We take the maximum possible keys
		// per row out of all the table rows we could potentially
		// scan over.
		firstBatchLimit = rowinfra.KeyLimit(int(c.limitHint) * keysPerRow)
		// We need an extra key to make sure we form the last row.
		firstBatchLimit++
	}
	c.fetcher, err = row.MakeKVBatchFetcher(
		ctx,
		row.MakeKVBatchFetcherDefaultSendFunc(c.flowCtx.Txn),
		c.Spans,
		c.spec.Reverse,
		c.batchBytesLimit,
		firstBatchLimit,
		roachpb.COL_BATCH_RESPONSE,
		row.ColFormatArgs{
			Spec:          spec,
			TenantID:      c.flowCtx.Codec().TenantID(),
			IsProjection:  c.post.Projection,
			OutputColumns: c.post.OutputColumns,
		},
		c.spec.LockingStrength,
		c.spec.LockingWaitPolicy,
		c.flowCtx.EvalCtx.SessionData().LockTimeout,
		c.kvFetcherMemAcc,
		c.flowCtx.EvalCtx.TestingKnobs.ForceProductionBatchSizes,
		c.flowCtx.Txn.AdmissionHeader(),
		c.flowCtx.Txn.DB().SQLKVResponseAdmissionQ,
	)
	if err != nil {
		colexecerror.InternalError(err)
	}
}

func (c *ColBatchDirectScan) Next() coldata.Batch {
	ok, _, response, err := c.fetcher.NextBatch(c.Ctx)
	if err != nil {
		colexecerror.InternalError(err)
	}
	if !ok || len(response) == 0 {
		return coldata.ZeroBatch
	}
	c.data = c.data[:0]
	batchLength, err := c.deser.Deserialize(&c.data, response)
	if err != nil {
		colexecerror.InternalError(err)
	}
	if err := c.converter.ArrowToBatch(c.data, batchLength, c.batch); err != nil {
		colexecerror.InternalError(err)
	}
	return c.batch
}
