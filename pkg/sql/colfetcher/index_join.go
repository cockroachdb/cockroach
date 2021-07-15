// Copyright 2021 The Cockroach Authors.
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
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecspan"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

// ColIndexJoin operators are used to execute index joins (lookup joins that
// scan the primary index and discard input rows).
type ColIndexJoin struct {
	colexecop.InitHelper
	colexecop.OneInputNode

	state indexJoinState

	// spanAssembler is used to construct the lookup spans for each input batch.
	spanAssembler colexecspan.ColSpanAssembler

	flowCtx *execinfra.FlowCtx
	rf      *cFetcher

	// tracingSpan is created when the stats should be collected for the query
	// execution, and it will be finished when closing the operator.
	tracingSpan *tracing.Span
	mu          struct {
		syncutil.Mutex
		// rowsRead contains the number of total rows this ColIndexJoin has
		// returned so far.
		rowsRead int64
	}
	// ResultTypes is the slice of resulting column types from this operator.
	// It should be used rather than the slice of column types from the scanned
	// table because the scan might synthesize additional implicit system columns.
	ResultTypes []*types.T

	// maintainOrdering is true when the index join is required to maintain its
	// input ordering, in which case the ordering of the spans cannot be changed.
	maintainOrdering bool
}

var _ colexecop.KVReader = &ColIndexJoin{}
var _ execinfra.Releasable = &ColIndexJoin{}
var _ colexecop.ClosableOperator = &ColIndexJoin{}

// Init initializes a ColIndexJoin.
func (s *ColIndexJoin) Init(ctx context.Context) {
	if !s.InitHelper.Init(ctx) {
		return
	}
	// If tracing is enabled, we need to start a child span so that the only
	// contention events present in the recording would be because of this
	// cFetcher. Note that ProcessorSpan method itself will check whether
	// tracing is enabled.
	s.Ctx, s.tracingSpan = execinfra.ProcessorSpan(s.Ctx, "colindexjoin")
	s.Input.Init(ctx)
}

type indexJoinState uint8

const (
	indexJoinConstructingSpans indexJoinState = iota
	indexJoinScanning
	indexJoinDone
)

// Next is part of the Operator interface.
func (s *ColIndexJoin) Next() coldata.Batch {
	for {
		switch s.state {
		case indexJoinConstructingSpans:
			var spans roachpb.Spans
			var rowCount int
			for batch := s.Input.Next(); ; batch = s.Input.Next() {
				// Because index joins discard input rows, we do not have to maintain a
				// reference to input tuples after span generation.
				rowCount += batch.Length()
				if batch.Length() == 0 || s.spanAssembler.ConsumeBatch(batch) {
					// Reached the memory limit or the end of the input.
					spans = s.spanAssembler.GetSpans()
					break
				}
			}
			if len(spans) == 0 {
				// No lookups left to perform.
				s.state = indexJoinDone
				continue
			}

			if !s.maintainOrdering {
				// Sort the spans when !maintainOrdering. This allows lower layers to
				// optimize iteration over the data. Note that the looked up rows are
				// output unchanged, in the retrieval order, so it is not safe to do
				// this when maintainOrdering is true (the ordering to be maintained
				// may be different than the ordering in the index).
				sort.Sort(spans)
			}

			// Index joins will always return exactly one output row per input row.
			s.rf.setEstimatedRowCount(uint64(rowCount))
			if err := s.rf.StartScan(
				s.flowCtx.Txn, spans, false /* limitBatches */, 0 /* limitHint */, s.flowCtx.TraceKV,
				s.flowCtx.EvalCtx.TestingKnobs.ForceProductionBatchSizes,
			); err != nil {
				colexecerror.InternalError(err)
			}
			s.state = indexJoinScanning
		case indexJoinScanning:
			batch, err := s.rf.NextBatch(s.Ctx)
			if err != nil {
				colexecerror.InternalError(err)
			}
			if batch.Selection() != nil {
				colexecerror.InternalError(
					errors.AssertionFailedf("unexpected selection vector on the batch coming from CFetcher"))
			}
			n := batch.Length()
			if n == 0 {
				s.state = indexJoinConstructingSpans
				continue
			}
			s.mu.Lock()
			s.mu.rowsRead += int64(n)
			s.mu.Unlock()
			return batch
		case indexJoinDone:
			return coldata.ZeroBatch
		}
	}
}

// DrainMeta is part of the colexecop.MetadataSource interface.
func (s *ColIndexJoin) DrainMeta() []execinfrapb.ProducerMetadata {
	var trailingMeta []execinfrapb.ProducerMetadata
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
func (s *ColIndexJoin) GetBytesRead() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Note that if Init() was never called, s.rf.fetcher will remain nil, and
	// GetBytesRead() will return 0. We are also holding the mutex, so a
	// concurrent call to Init() will have to wait, and the fetcher will remain
	// uninitialized until we return.
	return s.rf.fetcher.GetBytesRead()
}

// GetRowsRead is part of the colexecop.KVReader interface.
func (s *ColIndexJoin) GetRowsRead() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mu.rowsRead
}

// GetCumulativeContentionTime is part of the colexecop.KVReader interface.
func (s *ColIndexJoin) GetCumulativeContentionTime() time.Duration {
	return execinfra.GetCumulativeContentionTime(s.Ctx)
}

// NewColIndexJoin creates a new ColIndexJoin operator.
func NewColIndexJoin(
	ctx context.Context,
	allocator *colmem.Allocator,
	flowCtx *execinfra.FlowCtx,
	evalCtx *tree.EvalContext,
	semaCtx *tree.SemaContext,
	input colexecop.Operator,
	spec *execinfrapb.JoinReaderSpec,
	post *execinfrapb.PostProcessSpec,
	inputTypes []*types.T,
) (*ColIndexJoin, error) {
	// NB: we hit this with a zero NodeID (but !ok) with multi-tenancy.
	if nodeID, ok := flowCtx.NodeID.OptionalNodeID(); nodeID == 0 && ok {
		return nil, errors.Errorf("attempting to create a ColIndexJoin with uninitialized NodeID")
	}
	if !spec.LookupExpr.Empty() {
		return nil, errors.AssertionFailedf("non-empty lookup expressions are not supported for index joins")
	}
	if !spec.RemoteLookupExpr.Empty() {
		return nil, errors.AssertionFailedf("non-empty remote lookup expressions are not supported for index joins")
	}
	if !spec.OnExpr.Empty() {
		return nil, errors.AssertionFailedf("non-empty ON expressions are not supported for index joins")
	}

	// TODO(ajwerner): The need to construct an immutable here
	// indicates that we're probably doing this wrong. Instead we should be
	// just setting the ID and Version in the spec or something like that and
	// retrieving the hydrated immutable from cache.
	table := spec.BuildTableDescriptor()

	cols := table.PublicColumns()
	if spec.Visibility == execinfra.ScanVisibilityPublicAndNotPublic {
		cols = table.DeletableColumns()
	}
	columnIdxMap := catalog.ColumnIDToOrdinalMap(cols)
	typs := catalog.ColumnTypesWithVirtualCol(cols, nil /* virtualCol */)

	// Add all requested system columns to the output.
	if spec.HasSystemColumns {
		for _, sysCol := range table.SystemColumns() {
			typs = append(typs, sysCol.GetType())
			columnIdxMap.Set(sysCol.GetID(), columnIdxMap.Len())
		}
	}

	// Before we can safely use types from the table descriptor, we need to
	// make sure they are hydrated. In row execution engine it is done during
	// the processor initialization, but neither ColIndexJoin nor cFetcher are
	// processors, so we need to do the hydration ourselves.
	resolver := flowCtx.TypeResolverFactory.NewTypeResolver(evalCtx.Txn)
	if err := resolver.HydrateTypeSlice(ctx, typs); err != nil {
		return nil, err
	}

	indexIdx := int(spec.IndexIdx)
	if indexIdx >= len(table.ActiveIndexes()) {
		return nil, errors.Errorf("invalid indexIdx %d", indexIdx)
	}
	index := table.ActiveIndexes()[indexIdx]

	// Retrieve the set of columns that the index join needs to fetch.
	var neededColumns util.FastIntSet
	if post.OutputColumns != nil {
		for _, neededColumn := range post.OutputColumns {
			neededColumns.Add(int(neededColumn))
		}
	} else {
		proc := &execinfra.ProcOutputHelper{}
		if err := proc.Init(post, typs, semaCtx, evalCtx); err != nil {
			colexecerror.InternalError(err)
		}
		neededColumns = proc.NeededColumns()
	}

	fetcher, err := initCFetcher(
		flowCtx, allocator, table, index, neededColumns, columnIdxMap, nil, /* virtualColumn */
		cFetcherArgs{
			visibility:        spec.Visibility,
			lockingStrength:   spec.LockingStrength,
			lockingWaitPolicy: spec.LockingWaitPolicy,
			hasSystemColumns:  spec.HasSystemColumns,
			memoryLimit:       execinfra.GetWorkMemLimit(flowCtx),
		},
	)
	if err != nil {
		return nil, err
	}

	// Allow 1/16 of the operator's working memory for the span key bytes.
	batchSizeLimit := int(float64(execinfra.GetWorkMemLimit(flowCtx)) * defaultBatchSizeLimitRatio)
	spanAssembler := colexecspan.NewColSpanAssembler(
		flowCtx.Codec(), allocator, table, index, inputTypes, neededColumns, batchSizeLimit)

	return &ColIndexJoin{
		OneInputNode:     colexecop.NewOneInputNode(input),
		flowCtx:          flowCtx,
		rf:               fetcher,
		spanAssembler:    spanAssembler,
		ResultTypes:      typs,
		maintainOrdering: spec.MaintainOrdering,
	}, nil
}

// defaultBatchSizeLimitRatio is the fraction of the operator working memory
// limit that is devoted to the underlying bytes for the span keys.
var defaultBatchSizeLimitRatio = 1.0 / 16.0

// Release implements the execinfra.Releasable interface.
func (s *ColIndexJoin) Release() {
	s.rf.Release()
	s.spanAssembler.Release()
}

// Close implements the colexecop.Closer interface.
func (s *ColIndexJoin) Close() error {
	if s.tracingSpan != nil {
		s.tracingSpan.Finish()
		s.tracingSpan = nil
	}
	s.spanAssembler.Close()
	return nil
}
