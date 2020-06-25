// Copyright 2016 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
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
	colexecbase.ZeroInputNode
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

var _ colexecbase.Operator = &colBatchScan{}

func (s *colBatchScan) Init() {
	s.ctx = context.Background()
	s.init = true

	limitBatches := execinfra.ScanShouldLimitBatches(s.maxResults, s.limitHint, s.flowCtx)

	if err := s.rf.StartScan(
		s.ctx, s.flowCtx.Txn, s.spans,
		limitBatches, s.limitHint, s.flowCtx.TraceKV,
	); err != nil {
		colexecerror.InternalError(err)
	}
}

func (s *colBatchScan) Next(ctx context.Context) coldata.Batch {
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
func (s *colBatchScan) DrainMeta(ctx context.Context) []execinfrapb.ProducerMetadata {
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
			ranges := execinfra.MisplannedRanges(ctx, s.rf.GetRangesInfo(), nodeID)
			if ranges != nil {
				trailingMeta = append(trailingMeta, execinfrapb.ProducerMetadata{Ranges: ranges})
			}
		}
	}
	if tfs := execinfra.GetLeafTxnFinalState(ctx, s.flowCtx.Txn); tfs != nil {
		trailingMeta = append(trailingMeta, execinfrapb.ProducerMetadata{LeafTxnFinalState: tfs})
	}
	return trailingMeta
}

// NewColBatchScan creates a new colBatchScan operator.
func NewColBatchScan(
	allocator *colmem.Allocator,
	flowCtx *execinfra.FlowCtx,
	spec *execinfrapb.TableReaderSpec,
	post *execinfrapb.PostProcessSpec,
) (colexecbase.DrainableOperator, error) {
	// NB: we hit this with a zero NodeID (but !ok) with multi-tenancy.
	if nodeID, ok := flowCtx.NodeID.OptionalNodeID(); nodeID == 0 && ok {
		return nil, errors.Errorf("attempting to create a colBatchScan with uninitialized NodeID")
	}

	limitHint := execinfra.LimitHint(spec.LimitHint, post)

	returnMutations := spec.Visibility == execinfra.ScanVisibilityPublicAndNotPublic
	typs := spec.Table.ColumnTypesWithMutations(returnMutations)
	evalCtx := flowCtx.NewEvalCtx()
	// Before we can safely use types from the table descriptor, we need to
	// make sure they are hydrated. In row execution engine it is done during
	// the processor initialization, but neither colBatchScan nor cFetcher are
	// processors, so we need to do the hydration ourselves.
	if err := execinfrapb.HydrateTypeSlice(evalCtx, typs); err != nil {
		return nil, err
	}
	helper := execinfra.ProcOutputHelper{}
	if err := helper.Init(
		post,
		typs,
		evalCtx,
		nil, /* output */
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
	if scanVisibility == execinfra.ScanVisibilityPublicAndNotPublic {
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
