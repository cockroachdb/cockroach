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
	"bytes"
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/colserde"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/fetchpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra/execreleasable"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/execstats"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
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

// ColBatchScan is the exec.Operator implementation of TableReader. It reads a
// table from kv, presenting it as coldata.Batches via the exec.Operator
// interface.
type ColBatchScan struct {
	colexecop.ZeroInputNode
	colexecop.InitHelper
	execinfra.SpansWithCopy

	flowCtx         *execinfra.FlowCtx
	bsHeader        *roachpb.BoundedStalenessHeader
	cf              *cFetcher
	limitHint       rowinfra.RowLimit
	batchBytesLimit rowinfra.BytesLimit
	parallelize     bool
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

// ScanOperator combines common interfaces between operators that perform KV
// scans, such as ColBatchScan and ColIndexJoin.
type ScanOperator interface {
	colexecop.KVReader
	execreleasable.Releasable
	colexecop.ClosableOperator
}

var _ ScanOperator = &ColBatchScan{}

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
	if err := s.cf.StartScan(
		s.Ctx,
		s.Spans,
		limitBatches,
		s.batchBytesLimit,
		s.limitHint,
	); err != nil {
		colexecerror.InternalError(err)
	}
}

// Next is part of the Operator interface.
func (s *ColBatchScan) Next() coldata.Batch {
	bat, err := s.cf.NextBatch(s.Ctx)
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
			ranges := execinfra.MisplannedRanges(s.Ctx, s.SpansCopy, nodeID, s.flowCtx.Cfg.RangeCache)
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
	if trace := tracing.SpanFromContext(s.Ctx).GetConfiguredRecording(); trace != nil {
		trailingMeta = append(trailingMeta, execinfrapb.ProducerMetadata{TraceData: trace})
	}
	return trailingMeta
}

// GetBytesRead is part of the colexecop.KVReader interface.
func (s *ColBatchScan) GetBytesRead() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cf.getBytesRead()
}

// GetRowsRead is part of the colexecop.KVReader interface.
func (s *ColBatchScan) GetRowsRead() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mu.rowsRead
}

// GetBatchRequestsIssued is part of the colexecop.KVReader interface.
func (s *ColBatchScan) GetBatchRequestsIssued() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cf.getBatchRequestsIssued()
}

// GetContentionInfo is part of the colexecop.KVReader interface.
func (s *ColBatchScan) GetContentionInfo() (time.Duration, []roachpb.ContentionEvent) {
	return execstats.GetCumulativeContentionTime(s.Ctx, nil /* recording */)
}

// GetScanStats is part of the colexecop.KVReader interface.
func (s *ColBatchScan) GetScanStats() execstats.ScanStats {
	return execstats.GetScanStats(s.Ctx, nil /* recording */)
}

type cFetcherWrapper struct {
	fetcher    *cFetcher
	converter  *colserde.ArrowBatchConverter
	serializer *colserde.RecordBatchSerializer
	buf        bytes.Buffer
}

var _ storage.CFetcherWrapper = &cFetcherWrapper{}

func init() {
	storage.GetCFetcherWrapper = newCFetcherWrapper
}

func (c *cFetcherWrapper) NextBatch(
	ctx context.Context, serialize bool,
) ([]byte, coldata.Batch, error) {
	batch, err := c.fetcher.NextBatch(ctx)
	if err != nil {
		return nil, nil, err
	}
	if batch.Length() == 0 {
		return nil, nil, nil
	}
	if !serialize {
		return nil, batch, nil
	}
	data, err := c.converter.BatchToArrow(batch)
	if err != nil {
		return nil, nil, err
	}
	c.buf.Reset()
	_, _, err = c.serializer.Serialize(&c.buf, data, batch.Length())
	if err != nil {
		return nil, nil, err
	}
	return c.buf.Bytes(), nil, nil
}

func (c *cFetcherWrapper) Close(ctx context.Context) {
	if c.fetcher != nil {
		c.fetcher.Close(ctx)
		c.fetcher.Release()
		c.fetcher = nil
	}
}

func newCFetcherWrapper(
	ctx context.Context,
	acc *mon.BoundAccount,
	fetchSpec *fetchpb.IndexFetchSpec,
	nextKVer storage.NextKVer,
) (_ storage.CFetcherWrapper, retErr error) {
	// TODO: typeResolver.
	tableArgs, err := populateTableArgs(ctx, fetchSpec, nil /* typeResolver */)
	if err != nil {
		return nil, err
	}

	fetcher := cFetcherPool.Get().(*cFetcher)
	defer func() {
		if retErr != nil {
			fetcher.Release()
		}
	}()
	// TODO: args.
	fetcher.cFetcherArgs = cFetcherArgs{
		execinfra.DefaultMemoryLimit,
		0,     /* estimatedRowCount */
		false, /* traceKV */
		true,  /* singleUse */
	}

	// TODO: column factory.
	allocator := colmem.NewAllocator(ctx, acc, coldata.StandardColumnFactory)
	if err = fetcher.Init(allocator, nextKVer, tableArgs); err != nil {
		return nil, err
	}
	wrapper := cFetcherWrapper{}
	wrapper.fetcher = fetcher
	wrapper.converter, err = colserde.NewArrowBatchConverter(tableArgs.typs)
	if err != nil {
		return nil, err
	}
	wrapper.serializer, err = colserde.NewRecordBatchSerializer(tableArgs.typs)
	if err != nil {
		return nil, err
	}
	return &wrapper, nil
}

var colBatchScanPool = sync.Pool{
	New: func() interface{} {
		return &ColBatchScan{}
	},
}

// NewColBatchScan creates a new ColBatchScan operator.
func NewColBatchScan(
	ctx context.Context,
	fetcherAllocator *colmem.Allocator,
	kvFetcherMemAcc *mon.BoundAccount,
	flowCtx *execinfra.FlowCtx,
	spec *execinfrapb.TableReaderSpec,
	post *execinfrapb.PostProcessSpec,
	estimatedRowCount uint64,
	typeResolver *descs.DistSQLTypeResolver,
) (*ColBatchScan, error) {
	// NB: we hit this with a zero NodeID (but !ok) with multi-tenancy.
	if nodeID, ok := flowCtx.NodeID.OptionalNodeID(); nodeID == 0 && ok {
		return nil, errors.Errorf("attempting to create a ColBatchScan with uninitialized NodeID")
	}
	var bsHeader *roachpb.BoundedStalenessHeader
	if aost := flowCtx.EvalCtx.AsOfSystemTime; aost != nil && aost.BoundedStaleness {
		ts := aost.Timestamp
		// If the descriptor's modification time is after the bounded staleness min bound,
		// we have to increase the min bound.
		// Otherwise, we would have table data which would not correspond to the correct
		// schema.
		if aost.Timestamp.Less(spec.TableDescriptorModificationTime) {
			ts = spec.TableDescriptorModificationTime
		}
		bsHeader = &roachpb.BoundedStalenessHeader{
			MinTimestampBound:       ts,
			MinTimestampBoundStrict: aost.NearestOnly,
			MaxTimestampBound:       flowCtx.EvalCtx.AsOfSystemTime.MaxTimestampBound, // may be empty
		}
	}

	limitHint := rowinfra.RowLimit(execinfra.LimitHint(spec.LimitHint, post))
	tableArgs, err := populateTableArgs(ctx, &spec.FetchSpec, typeResolver)
	if err != nil {
		return nil, err
	}

	kvFetcher := row.NewKVFetcher(
		flowCtx.Txn,
		bsHeader,
		spec.Reverse,
		spec.LockingStrength,
		spec.LockingWaitPolicy,
		flowCtx.EvalCtx.SessionData().LockTimeout,
		kvFetcherMemAcc,
		flowCtx.EvalCtx.TestingKnobs.ForceProductionValues,
	)

	fetcher := cFetcherPool.Get().(*cFetcher)
	fetcher.cFetcherArgs = cFetcherArgs{
		execinfra.GetWorkMemLimit(flowCtx),
		estimatedRowCount,
		flowCtx.TraceKV,
		true, /* singleUse */
	}

	if err = fetcher.Init(
		fetcherAllocator, kvFetcher, tableArgs,
	); err != nil {
		fetcher.Release()
		return nil, err
	}

	s := colBatchScanPool.Get().(*ColBatchScan)
	s.Spans = spec.Spans
	if !flowCtx.Local {
		// Make a copy of the spans so that we could get the misplanned ranges
		// info.
		//
		// Note that we cannot use fetcherAllocator to track this memory usage
		// (because the cFetcher requires that its allocator is not shared with
		// any other component), but we can use the memory account of the KV
		// fetcher.
		if err = kvFetcherMemAcc.Grow(ctx, s.Spans.MemUsage()); err != nil {
			return nil, err
		}
		s.MakeSpansCopy()
	}

	if spec.LimitHint > 0 || spec.BatchBytesLimit > 0 {
		// Parallelize shouldn't be set when there's a limit hint, but double-check
		// just in case.
		spec.Parallelize = false
	}
	var batchBytesLimit rowinfra.BytesLimit
	if !spec.Parallelize {
		batchBytesLimit = rowinfra.BytesLimit(spec.BatchBytesLimit)
		if batchBytesLimit == 0 {
			batchBytesLimit = rowinfra.GetDefaultBatchBytesLimit(flowCtx.EvalCtx.TestingKnobs.ForceProductionValues)
		}
	}

	*s = ColBatchScan{
		SpansWithCopy:   s.SpansWithCopy,
		flowCtx:         flowCtx,
		bsHeader:        bsHeader,
		cf:              fetcher,
		limitHint:       limitHint,
		batchBytesLimit: batchBytesLimit,
		parallelize:     spec.Parallelize,
		ResultTypes:     tableArgs.typs,
	}
	return s, nil
}

// Release implements the execinfra.Releasable interface.
func (s *ColBatchScan) Release() {
	s.cf.Release()
	// Deeply reset the spans so that we don't hold onto the keys of the spans.
	s.SpansWithCopy.Reset()
	*s = ColBatchScan{
		SpansWithCopy: s.SpansWithCopy,
	}
	colBatchScanPool.Put(s)
}

// Close implements the colexecop.Closer interface.
func (s *ColBatchScan) Close(context.Context) error {
	// Note that we're using the context of the ColBatchScan rather than the
	// argument of Close() because the ColBatchScan derives its own tracing
	// span.
	ctx := s.EnsureCtx()
	s.cf.Close(ctx)
	if s.tracingSpan != nil {
		s.tracingSpan.Finish()
		s.tracingSpan = nil
	}
	return nil
}
