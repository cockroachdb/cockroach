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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
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
		ctx context.Context
		// init is true after Init() has been called.
		init bool
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
func (s *ColBatchScan) Init() {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Note that we're intentionally holding the lock until the cFetcher is
	// properly initialized.
	s.mu.init = true
	limitBatches := !s.parallelize
	if err := s.rf.StartScan(
		s.flowCtx.Txn, s.spans, limitBatches, s.limitHint, s.flowCtx.TraceKV,
		s.flowCtx.EvalCtx.TestingKnobs.ForceProductionBatchSizes,
	); err != nil {
		colexecerror.InternalError(err)
	}
}

// Next is part of the Operator interface.
func (s *ColBatchScan) Next(ctx context.Context) coldata.Batch {
	s.mu.Lock()
	if s.mu.ctx == nil {
		// This is the first call to Next(), so we will capture the context and
		// possibly replace it with a child below.
		s.mu.ctx = ctx
		if execinfra.ShouldCollectStats(s.mu.ctx, s.flowCtx) {
			// We need to start a child span so that the only contention events
			// present in the recording would be because of this cFetcher.
			s.mu.ctx, s.tracingSpan = execinfra.ProcessorSpan(s.mu.ctx, "colbatchscan")
		}
	}
	ctx = s.mu.ctx
	s.mu.Unlock()
	bat, err := s.rf.NextBatch(ctx)
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
func (s *ColBatchScan) DrainMeta(ctx context.Context) []execinfrapb.ProducerMetadata {
	s.mu.Lock()
	initialized := s.mu.init
	s.mu.Unlock()
	if !initialized {
		// In some pathological queries like `SELECT 1 FROM t HAVING true`, Init()
		// and Next() may never get called. Return early to avoid using an
		// uninitialized fetcher.
		// TODO(yuzefovich): we probably don't need this anymore given that we
		// call Init() on fixedNumTuplesNoInputOp and we have the invariants
		// checker ensuring that Init() is called before DrainMeta().
		return nil
	}
	var trailingMeta []execinfrapb.ProducerMetadata
	if !s.flowCtx.Local {
		nodeID, ok := s.flowCtx.NodeID.OptionalNodeID()
		if ok {
			ranges := execinfra.MisplannedRanges(ctx, s.spans, nodeID, s.flowCtx.Cfg.RangeCache)
			if ranges != nil {
				trailingMeta = append(trailingMeta, execinfrapb.ProducerMetadata{Ranges: ranges})
			}
		}
	}
	if tfs := execinfra.GetLeafTxnFinalState(ctx, s.flowCtx.Txn); tfs != nil {
		trailingMeta = append(trailingMeta, execinfrapb.ProducerMetadata{LeafTxnFinalState: tfs})
	}
	meta := execinfrapb.GetProducerMeta()
	meta.Metrics = execinfrapb.GetMetricsMeta()
	meta.Metrics.BytesRead = s.GetBytesRead()
	meta.Metrics.RowsRead = s.GetRowsRead()
	trailingMeta = append(trailingMeta, *meta)
	if s.tracingSpan != nil {
		// If tracingSpan is non-nil, then we have derived a new context in
		// Next() and we have to collect the trace data.
		//
		// If tracingSpan is nil, then we used the same context that was passed
		// in Next() and it is the responsibility of the caller-component
		// (either materializer, or wrapped processor, or an outbox) to collect
		// the trace data. If we were to do it here too, we would see duplicate
		// spans.
		// TODO(yuzefovich): this is temporary hack that will be fixed by adding
		// context.Context argument to Init() and removing it from Next() and
		// DrainMeta().
		s.mu.Lock()
		traceCtx := s.mu.ctx
		s.mu.Unlock()
		if trace := execinfra.GetTraceData(traceCtx); trace != nil {
			trailingMeta = append(trailingMeta, execinfrapb.ProducerMetadata{TraceData: trace})
		}
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
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mu.ctx == nil {
		// Next was never called, so there was no contention events.
		return 0
	}
	return execinfra.GetCumulativeContentionTime(s.mu.ctx)
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
		flowCtx.Codec(), allocator, execinfra.GetWorkMemLimit(flowCtx.Cfg),
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
) (index *descpb.IndexDescriptor, isSecondaryIndex bool, err error) {
	indexIdx := int(spec.IndexIdx)
	if indexIdx >= len(desc.ActiveIndexes()) {
		return nil, false, errors.Errorf("invalid indexIdx %d", indexIdx)
	}
	indexI := desc.ActiveIndexes()[indexIdx]
	index = indexI.IndexDesc()
	isSecondaryIndex = !indexI.Primary()

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
	*s = ColBatchScan{
		spans: s.spans[:0],
	}
	colBatchScanPool.Put(s)
}

// Close implements the colexecop.Closer interface.
func (s *ColBatchScan) Close(context.Context) error {
	if s.tracingSpan != nil {
		s.tracingSpan.Finish()
		s.tracingSpan = nil
	}
	return nil
}
