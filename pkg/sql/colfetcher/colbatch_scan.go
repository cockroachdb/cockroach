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
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

// TODO(yuzefovich): reading the data through a pair of ColBatchScan and
// materializer turns out to be more efficient than through a table reader (at
// the moment, the exception is the case of reading very small number of rows
// because we still pre-allocate batches of 1024 size). Once we can control the
// initial size of pre-allocated batches (probably via a batch allocator), we
// should get rid off table readers entirely. We will have to be careful about
// propagating the metadata though.

// ColBatchScan is the exec.Operator implementation of TableReader. It reads a table
// from kv, presenting it as coldata.Batches via the exec.Operator interface.
type ColBatchScan struct {
	colexecop.ZeroInputNode
	colexecop.InitHelper

	spans       roachpb.Spans
	flowCtx     *execinfra.FlowCtx
	rf          *cFetcher
	limitHint   int64
	parallelize bool
	// tracingSpan is created when the stats should be collected for the query
	// execution, and it will be finished when closing the operator.
	tracingSpan *tracing.Span
	mu          struct {
		syncutil.Mutex
		// rowsRead contains the number of total rows this ColBatchScan has
		// returned so far.
		rowsRead int64
	}
	// ResultTypes is the slice of resulting column types from this operator.
	// It should be used rather than the slice of column types from the scanned
	// table because the scan might synthesize additional implicit system columns.
	ResultTypes []*types.T
}

var _ colexecop.KVReader = &ColBatchScan{}
var _ execinfra.Releasable = &ColBatchScan{}
var _ colexecop.Closer = &ColBatchScan{}
var _ colexecop.Operator = &ColBatchScan{}

// Init initializes a ColBatchScan.
func (s *ColBatchScan) Init(ctx context.Context) {
	if !s.InitHelper.Init(ctx) {
		return
	}
	// If tracing is enabled, we need to start a child span so that the only
	// contention events present in the recording would be because of this
	// cFetcher. Note that ProcessorSpan method itself will check whether
	// tracing is enabled.
	s.Ctx, s.tracingSpan = execinfra.ProcessorSpan(s.Ctx, "colbatchscan")
	limitBatches := !s.parallelize
	if err := s.rf.StartScan(
		s.flowCtx.Txn, s.spans, limitBatches, s.limitHint, s.flowCtx.TraceKV,
		s.flowCtx.EvalCtx.TestingKnobs.ForceProductionBatchSizes,
	); err != nil {
		colexecerror.InternalError(err)
	}
}

// Next is part of the Operator interface.
func (s *ColBatchScan) Next() coldata.Batch {
	bat, err := s.rf.NextBatch(s.Ctx)
	if err != nil {
		colexecerror.InternalError(err)
	}
	if bat.Selection() != nil {
		colexecerror.InternalError(errors.AssertionFailedf("unexpectedly a selection vector is set on the batch coming from CFetcher"))
	}
	s.mu.Lock()
	s.mu.rowsRead += int64(bat.Length())
	s.mu.Unlock()
	return bat
}

// DrainMeta is part of the colexecop.MetadataSource interface.
func (s *ColBatchScan) DrainMeta() []execinfrapb.ProducerMetadata {
	var trailingMeta []execinfrapb.ProducerMetadata
	if !s.flowCtx.Local {
		nodeID, ok := s.flowCtx.NodeID.OptionalNodeID()
		if ok {
			ranges := execinfra.MisplannedRanges(s.Ctx, s.spans, nodeID, s.flowCtx.Cfg.RangeCache)
			if ranges != nil {
				trailingMeta = append(trailingMeta, execinfrapb.ProducerMetadata{Ranges: ranges})
			}
		}
	}
	if tfs := execinfra.GetLeafTxnFinalState(s.Ctx, s.flowCtx.Txn); tfs != nil {
		trailingMeta = append(trailingMeta, execinfrapb.ProducerMetadata{LeafTxnFinalState: tfs})
	}
	meta := execinfrapb.GetProducerMeta()
	meta.Metrics = execinfrapb.GetMetricsMeta()
	meta.Metrics.BytesRead = s.GetBytesRead()
	meta.Metrics.RowsRead = s.GetRowsRead()
	trailingMeta = append(trailingMeta, *meta)
	if trace := execinfra.GetTraceData(s.Ctx); trace != nil {
		trailingMeta = append(trailingMeta, execinfrapb.ProducerMetadata{TraceData: trace})
	}
	return trailingMeta
}

// GetBytesRead is part of the colexecop.KVReader interface.
func (s *ColBatchScan) GetBytesRead() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Note that if Init() was never called, s.rf.fetcher will remain nil, and
	// GetBytesRead() will return 0. We are also holding the mutex, so a
	// concurrent call to Init() will have to wait, and the fetcher will remain
	// uninitialized until we return.
	return s.rf.fetcher.GetBytesRead()
}

// GetRowsRead is part of the colexecop.KVReader interface.
func (s *ColBatchScan) GetRowsRead() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mu.rowsRead
}

// GetCumulativeContentionTime is part of the colexecop.KVReader interface.
func (s *ColBatchScan) GetCumulativeContentionTime() time.Duration {
	return execinfra.GetCumulativeContentionTime(s.Ctx)
}

var colBatchScanPool = sync.Pool{
	New: func() interface{} {
		return &ColBatchScan{}
	},
}

// NewColBatchScan creates a new ColBatchScan operator.
func NewColBatchScan(
	ctx context.Context,
	allocator *colmem.Allocator,
	flowCtx *execinfra.FlowCtx,
	evalCtx *tree.EvalContext,
	spec *execinfrapb.TableReaderSpec,
	post *execinfrapb.PostProcessSpec,
	estimatedRowCount uint64,
) (*ColBatchScan, error) {
	// NB: we hit this with a zero NodeID (but !ok) with multi-tenancy.
	if nodeID, ok := flowCtx.NodeID.OptionalNodeID(); nodeID == 0 && ok {
		return nil, errors.Errorf("attempting to create a ColBatchScan with uninitialized NodeID")
	}
	if spec.IsCheck {
		// cFetchers don't support these checks.
		return nil, errors.AssertionFailedf("attempting to create a cFetcher with the IsCheck flag set")
	}

	limitHint := execinfra.LimitHint(spec.LimitHint, post)
	// TODO(ajwerner): The need to construct an immutable here
	// indicates that we're probably doing this wrong. Instead we should be
	// just setting the ID and Version in the spec or something like that and
	// retrieving the hydrated immutable from cache.
	table := spec.BuildTableDescriptor()
	virtualColumn := tabledesc.FindVirtualColumn(table, spec.VirtualColumn)
	cols := table.PublicColumns()
	if spec.Visibility == execinfra.ScanVisibilityPublicAndNotPublic {
		cols = table.DeletableColumns()
	}
	columnIdxMap := catalog.ColumnIDToOrdinalMap(cols)
	typs := catalog.ColumnTypesWithVirtualCol(cols, virtualColumn)

	// Add all requested system columns to the output.
	if spec.HasSystemColumns {
		for _, sysCol := range table.SystemColumns() {
			typs = append(typs, sysCol.GetType())
			columnIdxMap.Set(sysCol.GetID(), columnIdxMap.Len())
		}
	}

	// Before we can safely use types from the table descriptor, we need to
	// make sure they are hydrated. In row execution engine it is done during
	// the processor initialization, but neither ColBatchScan nor cFetcher are
	// processors, so we need to do the hydration ourselves.
	resolver := flowCtx.TypeResolverFactory.NewTypeResolver(evalCtx.Txn)
	if err := resolver.HydrateTypeSlice(ctx, typs); err != nil {
		return nil, err
	}

	var neededColumns util.FastIntSet
	for _, neededColumn := range spec.NeededColumns {
		neededColumns.Add(int(neededColumn))
	}

	fetcher := cFetcherPool.Get().(*cFetcher)
	fetcher.estimatedRowCount = estimatedRowCount
	if _, _, err := initCRowFetcher(
		flowCtx.Codec(), allocator, execinfra.GetWorkMemLimit(flowCtx),
		fetcher, table, columnIdxMap, neededColumns, spec, spec.HasSystemColumns,
	); err != nil {
		return nil, err
	}

	s := colBatchScanPool.Get().(*ColBatchScan)
	spans := s.spans[:0]
	specSpans := spec.Spans
	for i := range specSpans {
		//gcassert:bce
		spans = append(spans, specSpans[i].Span)
	}
	*s = ColBatchScan{
		spans:     spans,
		flowCtx:   flowCtx,
		rf:        fetcher,
		limitHint: limitHint,
		// Parallelize shouldn't be set when there's a limit hint, but double-check
		// just in case.
		parallelize: spec.Parallelize && limitHint == 0,
		ResultTypes: typs,
	}
	return s, nil
}

// initCRowFetcher initializes a row.cFetcher. See initRowFetcher.
func initCRowFetcher(
	codec keys.SQLCodec,
	allocator *colmem.Allocator,
	memoryLimit int64,
	fetcher *cFetcher,
	desc catalog.TableDescriptor,
	colIdxMap catalog.TableColMap,
	valNeededForCol util.FastIntSet,
	spec *execinfrapb.TableReaderSpec,
	withSystemColumns bool,
) (index catalog.Index, isSecondaryIndex bool, err error) {
	indexIdx := int(spec.IndexIdx)
	if indexIdx >= len(desc.ActiveIndexes()) {
		return nil, false, errors.Errorf("invalid indexIdx %d", indexIdx)
	}
	index = desc.ActiveIndexes()[indexIdx]
	isSecondaryIndex = !index.Primary()

	tableArgs := row.FetcherTableArgs{
		Desc:             desc,
		Index:            index,
		ColIdxMap:        colIdxMap,
		IsSecondaryIndex: isSecondaryIndex,
		ValNeededForCol:  valNeededForCol,
	}

	virtualColumn := tabledesc.FindVirtualColumn(desc, spec.VirtualColumn)
	tableArgs.InitCols(desc, spec.Visibility, withSystemColumns, virtualColumn)

	if err := fetcher.Init(
		codec, allocator, memoryLimit, spec.Reverse, spec.LockingStrength, spec.LockingWaitPolicy, tableArgs,
	); err != nil {
		return nil, false, err
	}

	return index, isSecondaryIndex, nil
}

// Release implements the execinfra.Releasable interface.
func (s *ColBatchScan) Release() {
	s.rf.Release()
	// Deeply reset the spans so that we don't hold onto the keys of the spans.
	for i := range s.spans {
		s.spans[i] = roachpb.Span{}
	}
	*s = ColBatchScan{
		spans: s.spans[:0],
	}
	colBatchScanPool.Put(s)
}

// Close implements the colexecop.Closer interface.
func (s *ColBatchScan) Close() error {
	if s.tracingSpan != nil {
		s.tracingSpan.Finish()
		s.tracingSpan = nil
	}
	return nil
}
