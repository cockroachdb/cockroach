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
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
	protoTypes "github.com/gogo/protobuf/types"
)

type ColBatchDirectScan struct {
	colexecbase.ZeroInputNode
	row.KVBatchFetcher
	converter       *colserde.ArrowBatchConverter
	deser           *colserde.RecordBatchSerializer
	spans           roachpb.Spans
	flowCtx         *execinfra.FlowCtx
	limitHint       int64
	ResultTypes     []*types.T
	data            []*array.Data
	batch           coldata.Batch
	firstBatchLimit int64
	spec            execinfrapb.TableReaderSpec
	post            execinfrapb.PostProcessSpec
	parallelize     bool
}

func (c ColBatchDirectScan) DrainMeta(ctx context.Context) []execinfrapb.ProducerMetadata {
	return nil
}

var _ colexecbase.Operator = &ColBatchScan{}

// NewColBatchScan creates a new ColBatchScan operator.
func NewColDirectBatchScan(
	allocator *colmem.Allocator,
	flowCtx *execinfra.FlowCtx,
	spec *execinfrapb.TableReaderSpec,
	post *execinfrapb.PostProcessSpec,
) (*ColBatchDirectScan, error) {
	// NB: we hit this with a zero NodeID (but !ok) with multi-tenancy.
	if nodeID, ok := flowCtx.NodeID.OptionalNodeID(); nodeID == 0 && ok {
		return nil, errors.Errorf("attempting to create a ColBatchScan with uninitialized NodeID")
	}

	limitHint := execinfra.LimitHint(spec.LimitHint, post)

	returnMutations := spec.Visibility == execinfra.ScanVisibilityPublicAndNotPublic
	// TODO(ajwerner): The need to construct an ImmutableTableDescriptor here
	// indicates that we're probably doing this wrong. Instead we should be
	// just seting the ID and Version in the spec or something like that and
	// retrieving the hydrated ImmutableTableDescriptor from cache.
	table := sqlbase.NewImmutableTableDescriptor(spec.Table)
	typs := table.ColumnTypesWithMutations(returnMutations)
	columnIdxMap := table.ColumnIdxMapWithMutations(returnMutations)
	// Add all requested system columns to the output.
	sysColTypes, sysColDescs, err := sqlbase.GetSystemColumnTypesAndDescriptors(&spec.Table, spec.SystemColumns)
	if err != nil {
		return nil, err
	}
	typs = append(typs, sysColTypes...)
	for i := range sysColDescs {
		columnIdxMap[sysColDescs[i].ID] = len(columnIdxMap)
	}

	nSpans := len(spec.Spans)
	spans := make(roachpb.Spans, nSpans)
	for i := range spans {
		spans[i] = spec.Spans[i].Span
	}
	index, _, err := table.FindIndexByIndexIdx(int(spec.IndexIdx))
	if err != nil {
		return nil, err
	}
	// Keep track of the maximum keys per row to accommodate a
	// limitHint when StartScan is invoked.
	keysPerRow, err := table.KeysPerRow(index.ID)
	if err != nil {
		return nil, err
	}
	// If we have a limit hint, we limit the first batch size. Subsequent
	// batches get larger to avoid making things too slow (e.g. in case we have
	// a very restrictive filter and actually have to retrieve a lot of rows).
	firstBatchLimit := limitHint
	if firstBatchLimit != 0 {
		// The limitHint is a row limit, but each row could be made up
		// of more than one key. We take the maximum possible keys
		// per row out of all the table rows we could potentially
		// scan over.
		firstBatchLimit = limitHint * int64(keysPerRow)
		// We need an extra key to make sure we form the last row.
		firstBatchLimit++
	}

	return &ColBatchDirectScan{
		spans:     spans,
		data:      make([]*array.Data, len(typs)),
		flowCtx:   flowCtx,
		limitHint: limitHint,
		spec:      *spec,
		post:      *post,
		// Parallelize shouldn't be set when there's a limit hint, but double-check
		// just in case.
		parallelize:     spec.Parallelize && limitHint == 0,
		firstBatchLimit: firstBatchLimit,
		ResultTypes:     typs,
	}, nil
}

func (c *ColBatchDirectScan) Init() {
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
	c.KVBatchFetcher, err = row.MakeKVBatchFetcher(
		c.flowCtx.Txn, c.spans, c.spec.Reverse,
		!c.parallelize,
		c.firstBatchLimit,
		roachpb.COL_BATCH_RESPONSE,
		row.ColFormatArgs{
			Spec:          spec,
			TenantID:      c.flowCtx.Codec().TenantID(),
			IsProjection:  c.post.Projection,
			OutputColumns: c.post.OutputColumns,
		},
		c.spec.LockingStrength,
	)
	if err != nil {
		colexecerror.InternalError(err)
	}
}

func (c *ColBatchDirectScan) Next(ctx context.Context) coldata.Batch {
	ok, _, response, _, err := c.NextBatch(ctx)
	if err != nil {
		colexecerror.InternalError(err)
	}
	if !ok || len(response) == 0 {
		return coldata.ZeroBatch
	}
	c.data = c.data[:0]
	if err := c.deser.Deserialize(&c.data, response); err != nil {
		colexecerror.InternalError(err)
	}
	if err := c.converter.ArrowToBatch(c.data, c.batch); err != nil {
		colexecerror.InternalError(err)
	}
	return c.batch
}
