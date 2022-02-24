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
	"math"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvstreamer"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecspan"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/memsize"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
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

	// batch keeps track of the input batch currently being processed; if we only
	// generate spans for a portion of the batch on one iteration, we need to keep
	// a reference to it for the next iteration.
	batch coldata.Batch

	// startIdx keeps track of the index into the current input batch from which
	// the next set of spans should start to be generated. This is necessary
	// because the size of input rows from which spans are generated is limited,
	// and may not correspond to batch boundaries.
	startIdx int

	mem struct {
		// inputBatchSize tracks the size of the rows that have been used to
		// generate spans so far. This is used to prevent memory usage from growing
		// too large.
		inputBatchSize int64

		// inputBatchSizeLimit is a batch size limit for the number of input
		// rows that will be used to form lookup spans for each scan. It is
		// usually equal to the inputBatchSizeLimit metamorphic variable, but it
		// might be lower than that value when low distsql_workmem limit is
		// used.
		inputBatchSizeLimit int64

		// currentBatchSize tracks the size of the current input batch. This
		// provides a shortcut when the entire batch fits in the memory limit.
		currentBatchSize int64

		// constRowSize tracks the portion of the size of each row that remains
		// constant between rows - for example, an int64 column will add 8 bytes to
		// this field.
		constRowSize int64

		// Fields that deal with variable-size types.
		hasVarSizeCols bool
		varSizeVecIdxs util.FastIntSet
		byteLikeCols   []*coldata.Bytes
		decimalCols    []coldata.Decimals
		datumCols      []coldata.DatumVec
	}

	flowCtx *execinfra.FlowCtx
	cf      *cFetcher

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

	// usesStreamer indicates whether the ColIndexJoin is using the Streamer
	// API.
	usesStreamer bool
	streamerInfo struct {
		*kvstreamer.Streamer
		budgetAcc   *mon.BoundAccount
		budgetLimit int64
	}
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
	s.Input.Init(s.Ctx)
	if s.usesStreamer {
		s.streamerInfo.Streamer = kvstreamer.NewStreamer(
			s.flowCtx.Cfg.DistSender,
			s.flowCtx.Stopper(),
			s.flowCtx.Txn,
			s.flowCtx.EvalCtx.Settings,
			row.GetWaitPolicy(s.cf.lockWaitPolicy),
			s.streamerInfo.budgetLimit,
			s.streamerInfo.budgetAcc,
		)
		s.streamerInfo.Streamer.Init(
			kvstreamer.OutOfOrder,
			kvstreamer.Hints{UniqueRequests: true},
			int(s.cf.table.spec.MaxKeysPerRow),
		)
	}
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
			var rowCount int
			var spans roachpb.Spans
			s.mem.inputBatchSize = 0
			for s.next() {
				// Because index joins discard input rows, we do not have to maintain a
				// reference to input tuples after span generation. So, we can discard
				// the input batch reference on each iteration.
				endIdx := s.findEndIndex(rowCount > 0)
				rowCount += endIdx - s.startIdx
				s.spanAssembler.ConsumeBatch(s.batch, s.startIdx, endIdx)
				s.startIdx = endIdx
				if endIdx < s.batch.Length() {
					// Reached the memory limit.
					break
				}
			}
			spans = s.spanAssembler.GetSpans()
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
			s.cf.setEstimatedRowCount(uint64(rowCount))
			// Note that the fetcher takes ownership of the spans slice - it
			// will modify it and perform the memory accounting. We don't care
			// about the modification here, but we want to be conscious about
			// the memory accounting - we don't double count for any memory of
			// spans because the spanAssembler released all of the relevant
			// memory from its account in GetSpans().
			var err error
			if s.usesStreamer {
				err = s.cf.StartScanStreaming(
					s.Ctx,
					s.streamerInfo.Streamer,
					spans,
					rowinfra.NoRowLimit,
				)
			} else {
				err = s.cf.StartScan(
					s.Ctx,
					s.flowCtx.Txn,
					spans,
					nil,   /* bsHeader */
					false, /* limitBatches */
					rowinfra.NoBytesLimit,
					rowinfra.NoRowLimit,
					s.flowCtx.EvalCtx.TestingKnobs.ForceProductionBatchSizes,
				)
			}
			if err != nil {
				colexecerror.InternalError(err)
			}
			s.state = indexJoinScanning
		case indexJoinScanning:
			batch, err := s.cf.NextBatch(s.Ctx)
			if err != nil {
				colexecerror.InternalError(err)
			}
			if batch.Selection() != nil {
				colexecerror.InternalError(
					errors.AssertionFailedf("unexpected selection vector on the batch coming from CFetcher"))
			}
			n := batch.Length()
			if n == 0 {
				// NB: the fetcher has just been closed automatically, so it
				// released all of the resources. We now have to tell the
				// ColSpanAssembler to account for the spans slice since it
				// still has the references to it.
				s.spanAssembler.AccountForSpans()
				s.state = indexJoinConstructingSpans
				continue
			}
			s.mu.Lock()
			s.mu.rowsRead += int64(n)
			s.mu.Unlock()
			return batch
		case indexJoinDone:
			// Eagerly close the index joiner. Note that closeInternal() is
			// idempotent, so it's ok if it'll be closed again.
			s.closeInternal()
			return coldata.ZeroBatch
		}
	}
}

// findEndIndex returns an index endIdx into s.batch such that generating spans
// for rows in the interval [s.startIdx, endIdx) will get as close to the memory
// limit as possible without exceeding it, subject to the length of the batch.
// If no spans have been generated so far, the interval will include at least
// one row to ensure that progress is made. If no more spans should be generated
// for the current iteration, endIdx == s.startIdx.
func (s *ColIndexJoin) findEndIndex(hasSpans bool) (endIdx int) {
	n := s.batch.Length()
	if n == 0 || s.startIdx >= n || s.mem.inputBatchSize >= s.mem.inputBatchSizeLimit {
		// No more spans should be generated.
		return s.startIdx
	}
	if s.mem.inputBatchSize+s.mem.currentBatchSize <= s.mem.inputBatchSizeLimit {
		// The entire batch fits within the memory limit.
		s.mem.inputBatchSize += s.mem.currentBatchSize
		return n
	}
	for endIdx = s.startIdx; endIdx < n; endIdx++ {
		s.mem.inputBatchSize += s.getRowSize(endIdx)
		if s.mem.inputBatchSize > s.mem.inputBatchSizeLimit {
			// The current row (but not the previous) brings us to or over the memory
			// limit, so use it as the exclusive end index.
			break
		}
		if s.mem.inputBatchSize == s.mem.inputBatchSizeLimit {
			// The current row exactly meets the memory limit. Increment idx in order
			// to make it exclusive.
			endIdx++
			break
		}
	}
	if !hasSpans && endIdx == s.startIdx {
		// We must generate spans for at least one row in order to make progress.
		return s.startIdx + 1
	}
	return endIdx
}

// getRowSize calculates the size of the row stored at index i in the current
// batch. Note that it accounts only for the size of the data itself, and
// ignores extra overhead such as selection vectors or byte offsets.
func (s *ColIndexJoin) getRowSize(idx int) int64 {
	rowSize := s.mem.constRowSize
	if s.mem.hasVarSizeCols {
		for i := range s.mem.byteLikeCols {
			rowSize += adjustMemEstimate(s.mem.byteLikeCols[i].ElemSize(idx))
		}
		for i := range s.mem.decimalCols {
			rowSize += adjustMemEstimate(int64(s.mem.decimalCols[i][idx].Size()))
		}
		for i := range s.mem.datumCols {
			memEstimate := int64(s.mem.datumCols[i].Get(idx).(tree.Datum).Size()) + memsize.DatumOverhead
			rowSize += adjustMemEstimate(memEstimate)
		}
	}
	return rowSize
}

// getBatchSize calculates the size of the entire current batch. Note that it
// accounts only for the size of the data itself, and ignores extra overhead
// such as selection vectors or byte offsets. getBatchSize is not exactly
// equivalent to calling getRowSize for every row, but it is not necessary for
// the accounting to be exact, anyway.
func (s *ColIndexJoin) getBatchSize() int64 {
	n := s.batch.Length()
	batchSize := colmem.GetBatchMemSize(s.batch)
	batchSize += int64(n*s.batch.Width()) * memEstimateAdditive
	batchSize += int64(n) * int64(rowenc.EncDatumRowOverhead)
	return batchSize
}

// next pulls the next input batch (if the current one is entirely finished)
// and performs initial processing of the batch. This includes performing
// interface conversions up front and retrieving the overall memory footprint of
// the data. next returns false once the input is finished, and otherwise true.
func (s *ColIndexJoin) next() bool {
	if s.batch == nil || s.startIdx >= s.batch.Length() {
		// The current batch is finished.
		s.startIdx = 0
		s.batch = s.Input.Next()
		if s.batch.Length() == 0 {
			return false
		}
		s.mem.currentBatchSize = s.getBatchSize()
	}
	if !s.mem.hasVarSizeCols {
		return true
	}
	s.mem.byteLikeCols = s.mem.byteLikeCols[:0]
	s.mem.decimalCols = s.mem.decimalCols[:0]
	s.mem.datumCols = s.mem.datumCols[:0]
	for i, ok := s.mem.varSizeVecIdxs.Next(0); ok; i, ok = s.mem.varSizeVecIdxs.Next(i + 1) {
		vec := s.batch.ColVec(i)
		switch vec.CanonicalTypeFamily() {
		case types.BytesFamily:
			s.mem.byteLikeCols = append(s.mem.byteLikeCols, vec.Bytes())
		case types.JsonFamily:
			s.mem.byteLikeCols = append(s.mem.byteLikeCols, &vec.JSON().Bytes)
		case types.DecimalFamily:
			s.mem.decimalCols = append(s.mem.decimalCols, vec.Decimal())
		case typeconv.DatumVecCanonicalTypeFamily:
			s.mem.datumCols = append(s.mem.datumCols, vec.Datum())
		}
	}
	return true
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
	return s.cf.getBytesRead()
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

// inputBatchSizeLimit is a batch size limit for the number of input rows that
// will be used to form lookup spans for each scan. This is used as a proxy for
// result batch size in order to prevent OOMs, because index joins do not limit
// result batches. TODO(drewk): once the Streamer work is finished, the fetcher
// logic will be able to control result size without sacrificing parallelism, so
// we can remove this limit.
var inputBatchSizeLimit = int64(util.ConstantWithMetamorphicTestRange(
	"ColIndexJoin-batch-size",
	4<<20, /* 4 MB */
	1,     /* min */
	4<<20, /* max */
))

// NewColIndexJoin creates a new ColIndexJoin operator.
func NewColIndexJoin(
	ctx context.Context,
	allocator *colmem.Allocator,
	fetcherAllocator *colmem.Allocator,
	kvFetcherMemAcc *mon.BoundAccount,
	streamerBudgetAcc *mon.BoundAccount,
	flowCtx *execinfra.FlowCtx,
	input colexecop.Operator,
	spec *execinfrapb.JoinReaderSpec,
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

	tableArgs, err := populateTableArgs(ctx, flowCtx, &spec.FetchSpec)
	if err != nil {
		return nil, err
	}

	memoryLimit := execinfra.GetWorkMemLimit(flowCtx)

	useStreamer := flowCtx.Txn != nil && flowCtx.Txn.Type() == kv.LeafTxn &&
		row.CanUseStreamer(ctx, flowCtx.EvalCtx.Settings) &&
		!spec.MaintainOrdering
	if useStreamer {
		if streamerBudgetAcc == nil {
			return nil, errors.AssertionFailedf("streamer budget account is nil when the Streamer API is desired")
		}
		// Keep the quarter of the memory limit for the output batch of the
		// cFetcher, and we'll give the remaining three quarters to the streamer
		// budget below.
		memoryLimit = int64(math.Ceil(float64(memoryLimit) / 4.0))
	}

	fetcher := cFetcherPool.Get().(*cFetcher)
	fetcher.cFetcherArgs = cFetcherArgs{
		spec.LockingStrength,
		spec.LockingWaitPolicy,
		flowCtx.EvalCtx.SessionData().LockTimeout,
		memoryLimit,
		// Note that the correct estimated row count will be set by the index
		// joiner for each set of spans to read.
		0,     /* estimatedRowCount */
		false, /* reverse */
		flowCtx.TraceKV,
	}
	if err = fetcher.Init(
		fetcherAllocator, kvFetcherMemAcc, tableArgs,
	); err != nil {
		fetcher.Release()
		return nil, err
	}

	spanAssembler := colexecspan.NewColSpanAssembler(
		flowCtx.Codec(), allocator, &spec.FetchSpec, spec.SplitFamilyIDs, inputTypes,
	)

	op := &ColIndexJoin{
		OneInputNode:     colexecop.NewOneInputNode(input),
		flowCtx:          flowCtx,
		cf:               fetcher,
		spanAssembler:    spanAssembler,
		ResultTypes:      tableArgs.typs,
		maintainOrdering: spec.MaintainOrdering,
		usesStreamer:     useStreamer,
	}
	op.mem.inputBatchSizeLimit = inputBatchSizeLimit
	op.prepareMemLimit(inputTypes)
	if useStreamer {
		op.streamerInfo.budgetLimit = 3 * memoryLimit
		op.streamerInfo.budgetAcc = streamerBudgetAcc
		if memoryLimit < inputBatchSizeLimit {
			// If we have a low workmem limit, then we want to reduce the input
			// batch size limit.
			//
			// The Streamer gets three quarters of workmem as its budget which
			// accounts for two usages - for the footprint of the spans
			// themselves in the enqueued requests as well as the footprint of
			// the responses received by the Streamer. If we don't reduce the
			// input batch size limit here, then 4MiB value will be used, and
			// the constructed spans (i.e. the enqueued requests) alone might
			// exceed the budget leading to the Streamer erroring out in
			// Enqueue().
			op.mem.inputBatchSizeLimit = memoryLimit
		}
	}

	return op, nil
}

// prepareMemLimit sets up the fields used to limit lookup batch size.
func (s *ColIndexJoin) prepareMemLimit(inputTypes []*types.T) {
	// Add the EncDatum overhead to ensure parity with row engine size limits.
	s.mem.constRowSize = int64(rowenc.EncDatumRowOverhead)
	for i, t := range inputTypes {
		switch typeconv.TypeFamilyToCanonicalTypeFamily(t.Family()) {
		case
			types.BoolFamily,
			types.IntFamily,
			types.FloatFamily,
			types.TimestampTZFamily,
			types.IntervalFamily:
			s.mem.constRowSize += adjustMemEstimate(colmem.GetFixedSizeTypeSize(t))
		case
			types.DecimalFamily,
			types.BytesFamily,
			types.JsonFamily,
			typeconv.DatumVecCanonicalTypeFamily:
			s.mem.varSizeVecIdxs.Add(i)
			s.mem.hasVarSizeCols = true
		default:
			colexecerror.InternalError(errors.AssertionFailedf("unhandled type %s", t))
		}
	}
	s.mem.hasVarSizeCols = !s.mem.varSizeVecIdxs.Empty()
}

var (
	// memEstimateAdditive is an additive correction that simulates the overhead
	// of the EncDatum struct the row engine uses to store values.
	memEstimateAdditive = int64(rowenc.EncDatumOverhead)

	// memEstimateMultiplier is a multiplicative correction that simulates the
	// overhead of the encoded bytes field of EncDatum objects. It is somewhat
	// arbitrary, but the size of the 'encoded' field should not greatly exceed
	// the size of the decoded value, so the result should not be too far off.
	memEstimateMultiplier = int64(2)
)

// adjustMemEstimate attempts to adjust the given estimate for the size of a
// single data value to reflect what the size would be in the row engine. This
// is necessary in order to achieve similar batch sizes to the row-wise index
// joiner. Until the Streamer work is finished, increasing batch size could
// increase cluster instability.
func adjustMemEstimate(estimate int64) int64 {
	return estimate*memEstimateMultiplier + memEstimateAdditive
}

// GetScanStats is part of the colexecop.KVReader interface.
func (s *ColIndexJoin) GetScanStats() execinfra.ScanStats {
	return execinfra.GetScanStats(s.Ctx)
}

// Release implements the execinfra.Releasable interface.
func (s *ColIndexJoin) Release() {
	s.cf.Release()
	s.spanAssembler.Release()
	*s = ColIndexJoin{}
}

// Close implements the colexecop.Closer interface.
func (s *ColIndexJoin) Close(context.Context) error {
	s.closeInternal()
	if s.tracingSpan != nil {
		s.tracingSpan.Finish()
		s.tracingSpan = nil
	}
	return nil
}

// closeInternal is a subset of Close() which doesn't finish the operator's
// span.
func (s *ColIndexJoin) closeInternal() {
	// Note that we're using the context of the ColIndexJoin rather than the
	// argument of Close() because the ColIndexJoin derives its own tracing
	// span.
	ctx := s.EnsureCtx()
	s.cf.Close(ctx)
	if s.spanAssembler != nil {
		// spanAssembler can be nil if Release() has already been called.
		s.spanAssembler.Close()
	}
	if s.streamerInfo.Streamer != nil {
		s.streamerInfo.Streamer.Close()
	}
	s.batch = nil
}
