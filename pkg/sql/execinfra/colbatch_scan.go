// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execinfra

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
)

// ColBatchScan is the exec.Operator implementation of TableReader. It reads a table
// from kv, presenting it as coldata.Batches via the exec.Operator interface.
type ColBatchScan struct {
	ZeroInputNode
	spans     roachpb.Spans
	flowCtx   *FlowCtx
	rf        *cFetcher
	limitHint int64
	ctx       context.Context
	// maxResults is non-zero if there is a limit on the total number of rows
	// that the ColBatchScan will read.
	maxResults uint64
	// init is true after Init() has been called.
	init bool
}

var _ Operator = &ColBatchScan{}

func (s *ColBatchScan) Init() {
	s.ctx = context.Background()
	s.init = true

	limitBatches := ScanShouldLimitBatches(s.maxResults, s.limitHint, s.flowCtx)

	if err := s.rf.StartScan(
		s.ctx, s.flowCtx.Txn, s.spans,
		limitBatches, s.limitHint, s.flowCtx.TraceKV,
	); err != nil {
		colexecerror.InternalError(err)
	}
}

func (s *ColBatchScan) Next(ctx context.Context) coldata.Batch {
	bat, err := s.rf.NextBatch(ctx)
	if err != nil {
		colexecerror.InternalError(err)
	}
	if bat.Selection() != nil {
		colexecerror.InternalError("unexpectedly a selection vector is set on the batch coming from CFetcher")
	}
	return bat
}

// DrainMeta is part of the MetadataSource interface.
func (s *ColBatchScan) DrainMeta(ctx context.Context) []execinfrapb.ProducerMetadata {
	if !s.init {
		// In some pathological queries like `SELECT 1 FROM t HAVING true`, Init()
		// and Next() may never get called. Return early to avoid using an
		// uninitialized fetcher.
		return nil
	}
	var trailingMeta []execinfrapb.ProducerMetadata
	if !s.flowCtx.Local {
		nodeID, ok := s.flowCtx.NodeID.OptionalNodeID()
		if ok {
			ranges := MisplannedRanges(ctx, s.rf.GetRangesInfo(), nodeID)
			if ranges != nil {
				trailingMeta = append(trailingMeta, execinfrapb.ProducerMetadata{Ranges: ranges})
			}
		}
	}
	if tfs := GetLeafTxnFinalState(ctx, s.flowCtx.Txn); tfs != nil {
		trailingMeta = append(trailingMeta, execinfrapb.ProducerMetadata{LeafTxnFinalState: tfs})
	}
	return trailingMeta
}

// NewColBatchScan creates a new ColBatchScan operator.
func NewColBatchScan(
	allocator *colmem.Allocator,
	flowCtx *FlowCtx,
	spec *execinfrapb.TableReaderSpec,
	post *execinfrapb.PostProcessSpec,
) (*ColBatchScan, error) {
	// NB: we hit this with a zero NodeID (but !ok) with multi-tenancy.
	if nodeID, ok := flowCtx.NodeID.OptionalNodeID(); nodeID == 0 && ok {
		return nil, errors.Errorf("attempting to create a ColBatchScan with uninitialized NodeID")
	}

	limitHint := LimitHint(spec.LimitHint, post)

	returnMutations := spec.Visibility == execinfrapb.ScanVisibility_PUBLIC_AND_NOT_PUBLIC
	typs := spec.Table.ColumnTypesWithMutations(returnMutations)
	helper := ProcOutputHelper{}
	if err := helper.Init(
		post,
		typs,
		flowCtx.NewEvalCtx(),
		nil,
	); err != nil {
		return nil, err
	}

	neededColumns := helper.NeededColumns()

	columnIdxMap := spec.Table.ColumnIdxMapWithMutations(returnMutations)
	fetcher := cFetcher{}
	if _, _, err := initCRowFetcher(
		flowCtx.Codec(), allocator, &fetcher, &spec.Table, int(spec.IndexIdx), columnIdxMap,
		spec.Reverse, neededColumns, spec.IsCheck, spec.Visibility, spec.LockingStrength,
	); err != nil {
		return nil, err
	}

	nSpans := len(spec.Spans)
	spans := make(roachpb.Spans, nSpans)
	for i := range spans {
		spans[i] = spec.Spans[i].Span
	}
	return &ColBatchScan{
		spans:      spans,
		flowCtx:    flowCtx,
		rf:         &fetcher,
		limitHint:  limitHint,
		maxResults: spec.MaxResults,
	}, nil
}

// initCRowFetcher initializes a row.cFetcher. See initRowFetcher.
func initCRowFetcher(
	codec keys.SQLCodec,
	allocator *colmem.Allocator,
	fetcher *cFetcher,
	desc *sqlbase.TableDescriptor,
	indexIdx int,
	colIdxMap map[sqlbase.ColumnID]int,
	reverseScan bool,
	valNeededForCol util.FastIntSet,
	isCheck bool,
	scanVisibility execinfrapb.ScanVisibility,
	lockStr sqlbase.ScanLockingStrength,
) (index *sqlbase.IndexDescriptor, isSecondaryIndex bool, err error) {
	immutDesc := sqlbase.NewImmutableTableDescriptor(*desc)
	index, isSecondaryIndex, err = immutDesc.FindIndexByIndexIdx(indexIdx)
	if err != nil {
		return nil, false, err
	}

	cols := immutDesc.Columns
	if scanVisibility == execinfrapb.ScanVisibility_PUBLIC_AND_NOT_PUBLIC {
		cols = immutDesc.ReadableColumns
	}
	tableArgs := row.FetcherTableArgs{
		Desc:             immutDesc,
		Index:            index,
		ColIdxMap:        colIdxMap,
		IsSecondaryIndex: isSecondaryIndex,
		Cols:             cols,
		ValNeededForCol:  valNeededForCol,
	}
	if err := fetcher.Init(
		codec, allocator, reverseScan, lockStr, true /* returnRangeInfo */, isCheck, tableArgs,
	); err != nil {
		return nil, false, err
	}

	return index, isSecondaryIndex, nil
}

type accCloser struct {
	acc *mon.BoundAccount
}

var _ IdempotentCloser = &accCloser{}

func (c *accCloser) IdempotentClose(ctx context.Context) error {
	if c.acc != nil {
		c.acc.Close(ctx)
		c.acc = nil
	}
	return nil
}

func NewTableReader(
	flowCtx *FlowCtx,
	processorID int32,
	spec *execinfrapb.TableReaderSpec,
	post *execinfrapb.PostProcessSpec,
	output RowReceiver,
) (Processor, error) {
	acc := flowCtx.EvalCtx.Mon.MakeBoundAccount()
	scanOp, err := NewColBatchScan(colmem.NewAllocator(context.TODO(), &acc), flowCtx, spec, post)
	if err != nil {
		acc.Close(context.TODO())
		return nil, err
	}

	returnMutations := spec.Visibility == execinfrapb.ScanVisibility_PUBLIC_AND_NOT_PUBLIC
	types := spec.Table.ColumnTypesWithMutations(returnMutations)
	m, err := NewMaterializerWithPost(
		flowCtx,
		processorID,
		scanOp,
		types,
		post,
		output,
		[]execinfrapb.MetadataSource{scanOp},
		[]IdempotentCloser{&accCloser{acc: &acc}},
		nil, /* outputStatsToTrace */
		nil, /* cancelFlow */
	)
	if err != nil {
		acc.Close(context.TODO())
	}
	return m, err
}
