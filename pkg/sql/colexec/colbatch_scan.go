// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/pkg/errors"
)

// TODO(yuzefovich): reading the data through a pair of colBatchScan and
// materializer turns out to be more efficient than through a table reader (at
// the moment, the exception is the case of reading very small number of rows
// because we still pre-allocate batches of 1024 size). Once we can control the
// initial size of pre-allocated batches (probably via a batch allocator), we
// should get rid off table readers entirely. We will have to be careful about
// propagating the metadata though.

// colBatchScan is the exec.Operator implementation of TableReader. It reads a table
// from kv, presenting it as coldata.Batches via the exec.Operator interface.
type colBatchScan struct {
	ZeroInputNode
	spans     roachpb.Spans
	flowCtx   *execinfra.FlowCtx
	rf        *cFetcher
	limitHint int64
	ctx       context.Context
	// maxResults is non-zero if there is a limit on the total number of rows
	// that the colBatchScan will read.
	maxResults uint64
	// init is true after Init() has been called.
	init bool
}

var _ StaticMemoryOperator = &colBatchScan{}

func (s *colBatchScan) EstimateStaticMemoryUsage() int {
	return s.rf.EstimateStaticMemoryUsage()
}

func (s *colBatchScan) Init() {
	s.ctx = context.Background()
	s.init = true

	limitBatches := execinfra.ScanShouldLimitBatches(s.maxResults, s.limitHint, s.flowCtx)

	if err := s.rf.StartScan(
		s.ctx, s.flowCtx.Txn, s.spans,
		limitBatches, s.limitHint, s.flowCtx.TraceKV,
	); err != nil {
		execerror.VectorizedInternalPanic(err)
	}
}

func (s *colBatchScan) Next(ctx context.Context) coldata.Batch {
	bat, err := s.rf.NextBatch(ctx)
	if err != nil {
		execerror.VectorizedInternalPanic(err)
	}
	bat.SetSelection(false)
	return bat
}

// DrainMeta is part of the MetadataSource interface.
func (s *colBatchScan) DrainMeta(ctx context.Context) []execinfrapb.ProducerMetadata {
	if !s.init {
		// In some pathological queries like `SELECT 1 FROM t HAVING true`, Init()
		// and Next() may never get called. Return early to avoid using an
		// uninitialized fetcher.
		return nil
	}
	var trailingMeta []execinfrapb.ProducerMetadata
	if !s.flowCtx.Local {
		ranges := execinfra.MisplannedRanges(ctx, s.rf.GetRangesInfo(), s.flowCtx.NodeID)
		if ranges != nil {
			trailingMeta = append(trailingMeta, execinfrapb.ProducerMetadata{Ranges: ranges})
		}
	}
	if meta := execinfra.GetTxnCoordMeta(ctx, s.flowCtx.Txn); meta != nil {
		trailingMeta = append(trailingMeta, execinfrapb.ProducerMetadata{TxnCoordMeta: meta})
	}
	return trailingMeta
}

// newColBatchScan creates a new colBatchScan operator.
func newColBatchScan(
	flowCtx *execinfra.FlowCtx, spec *execinfrapb.TableReaderSpec, post *execinfrapb.PostProcessSpec,
) (*colBatchScan, error) {
	if flowCtx.NodeID == 0 {
		return nil, errors.Errorf("attempting to create a colBatchScan with uninitialized NodeID")
	}

	limitHint := execinfra.LimitHint(spec.LimitHint, post)

	returnMutations := spec.Visibility == execinfrapb.ScanVisibility_PUBLIC_AND_NOT_PUBLIC
	typs := spec.Table.ColumnTypesWithMutations(returnMutations)
	helper := execinfra.ProcOutputHelper{}
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
		&fetcher, &spec.Table, int(spec.IndexIdx), columnIdxMap, spec.Reverse,
		neededColumns, spec.IsCheck, spec.Visibility,
	); err != nil {
		return nil, err
	}

	nSpans := len(spec.Spans)
	spans := make(roachpb.Spans, nSpans)
	for i := range spans {
		spans[i] = spec.Spans[i].Span
	}
	return &colBatchScan{
		spans:      spans,
		flowCtx:    flowCtx,
		rf:         &fetcher,
		limitHint:  limitHint,
		maxResults: spec.MaxResults,
	}, nil
}

// initCRowFetcher initializes a row.cFetcher. See initRowFetcher.
func initCRowFetcher(
	fetcher *cFetcher,
	desc *sqlbase.TableDescriptor,
	indexIdx int,
	colIdxMap map[sqlbase.ColumnID]int,
	reverseScan bool,
	valNeededForCol util.FastIntSet,
	isCheck bool,
	scanVisibility execinfrapb.ScanVisibility,
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
		reverseScan, true /* returnRangeInfo */, isCheck, tableArgs,
	); err != nil {
		return nil, false, err
	}

	return index, isSecondaryIndex, nil
}
