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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
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
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

// colBatchScanBase is the common base for ColBatchScan and ColBatchDirectScan
// operators.
type colBatchScanBase struct {
	colexecop.ZeroInputNode
	colexecop.InitHelper
	execinfra.SpansWithCopy

	flowCtx         *execinfra.FlowCtx
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
}

func (s *colBatchScanBase) drainMeta() []execinfrapb.ProducerMetadata {
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
	if trace := tracing.SpanFromContext(s.Ctx).GetConfiguredRecording(); trace != nil {
		trailingMeta = append(trailingMeta, execinfrapb.ProducerMetadata{TraceData: trace})
	}
	return trailingMeta
}

// GetRowsRead is part of the colexecop.KVReader interface.
func (s *colBatchScanBase) GetRowsRead() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mu.rowsRead
}

// GetContentionInfo is part of the colexecop.KVReader interface.
func (s *colBatchScanBase) GetContentionInfo() (time.Duration, []roachpb.ContentionEvent) {
	return execstats.GetCumulativeContentionTime(s.Ctx, nil /* recording */)
}

// GetScanStats is part of the colexecop.KVReader interface.
func (s *colBatchScanBase) GetScanStats() execstats.ScanStats {
	return execstats.GetScanStats(s.Ctx, nil /* recording */)
}

// Release implements the execreleasable.Releasable interface.
func (s *colBatchScanBase) Release() {
	// Deeply reset the spans so that we don't hold onto the keys of the spans.
	s.SpansWithCopy.Reset()
	*s = colBatchScanBase{
		SpansWithCopy: s.SpansWithCopy,
	}
	colBatchScanBasePool.Put(s)
}

func (s *colBatchScanBase) close() error {
	if s.tracingSpan != nil {
		s.tracingSpan.Finish()
		s.tracingSpan = nil
	}
	return nil
}

var colBatchScanBasePool = sync.Pool{
	New: func() interface{} {
		return &colBatchScanBase{}
	},
}

// newColBatchScanBase creates a new colBatchScanBase.
func newColBatchScanBase(
	ctx context.Context,
	kvFetcherMemAcc *mon.BoundAccount,
	flowCtx *execinfra.FlowCtx,
	spec *execinfrapb.TableReaderSpec,
	post *execinfrapb.PostProcessSpec,
	typeResolver *descs.DistSQLTypeResolver,
) (*colBatchScanBase, *roachpb.BoundedStalenessHeader, *cFetcherTableArgs, error) {
	// NB: we hit this with a zero NodeID (but !ok) with multi-tenancy.
	if nodeID, ok := flowCtx.NodeID.OptionalNodeID(); nodeID == 0 && ok {
		return nil, nil, nil, errors.Errorf("attempting to create a ColBatchScan with uninitialized NodeID")
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
	tableArgs, err := populateTableArgs(ctx, &spec.FetchSpec, typeResolver, false /* allowUnhydratedEnums */)
	if err != nil {
		return nil, nil, nil, err
	}

	s := colBatchScanBasePool.Get().(*colBatchScanBase)
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
			return nil, nil, nil, err
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

	*s = colBatchScanBase{
		SpansWithCopy:   s.SpansWithCopy,
		flowCtx:         flowCtx,
		limitHint:       limitHint,
		batchBytesLimit: batchBytesLimit,
		parallelize:     spec.Parallelize,
	}
	return s, bsHeader, tableArgs, nil
}

// ColBatchScan is the colexecop.Operator implementation of TableReader. It
// reads a table from the KV layer, presenting it as coldata.Batches via the
// colexecop.Operator interface.
type ColBatchScan struct {
	*colBatchScanBase
	cf *cFetcher
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

// Next is part of the colexecop.Operator interface.
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
	trailingMeta := s.colBatchScanBase.drainMeta()
	meta := execinfrapb.GetProducerMeta()
	meta.Metrics = execinfrapb.GetMetricsMeta()
	meta.Metrics.BytesRead = s.GetBytesRead()
	meta.Metrics.RowsRead = s.GetRowsRead()
	trailingMeta = append(trailingMeta, *meta)
	return trailingMeta
}

// GetBytesRead is part of the colexecop.KVReader interface.
func (s *ColBatchScan) GetBytesRead() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cf.getBytesRead()
}

// GetBatchRequestsIssued is part of the colexecop.KVReader interface.
func (s *ColBatchScan) GetBatchRequestsIssued() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cf.getBatchRequestsIssued()
}

// GetKVCPUTime is part of the colexecop.KVReader interface.
func (s *ColBatchScan) GetKVCPUTime() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cf.getKVCPUTime()
}

// Release implements the execreleasable.Releasable interface.
func (s *ColBatchScan) Release() {
	s.colBatchScanBase.Release()
	s.cf.Release()
	*s = ColBatchScan{}
}

// Close implements the colexecop.Closer interface.
func (s *ColBatchScan) Close(context.Context) error {
	// Note that we're using the context of the ColBatchScan rather than the
	// argument of Close() because the ColBatchScan derives its own tracing
	// span.
	ctx := s.EnsureCtx()
	s.cf.Close(ctx)
	return s.colBatchScanBase.close()
}

// NewColBatchScan creates a new ColBatchScan operator.
//
// It also returns a slice of resulting column types from this operator. It
// should be used rather than the slice of column types from the scanned table
// because the scan might synthesize additional implicit system columns.
func NewColBatchScan(
	ctx context.Context,
	fetcherAllocator *colmem.Allocator,
	kvFetcherMemAcc *mon.BoundAccount,
	flowCtx *execinfra.FlowCtx,
	spec *execinfrapb.TableReaderSpec,
	post *execinfrapb.PostProcessSpec,
	estimatedRowCount uint64,
	typeResolver *descs.DistSQLTypeResolver,
) (*ColBatchScan, []*types.T, error) {
	base, bsHeader, tableArgs, err := newColBatchScanBase(
		ctx, kvFetcherMemAcc, flowCtx, spec, post, typeResolver,
	)
	if err != nil {
		return nil, nil, err
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
		execstats.ShouldCollectStats(ctx, flowCtx.CollectStats),
	}
	if err = fetcher.Init(fetcherAllocator, kvFetcher, tableArgs); err != nil {
		fetcher.Release()
		return nil, nil, err
	}
	return &ColBatchScan{
		colBatchScanBase: base,
		cf:               fetcher,
	}, tableArgs.typs, nil
}
