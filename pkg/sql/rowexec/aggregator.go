// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rowexec

import (
	"context"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/cancelchecker"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/optional"
	"github.com/cockroachdb/cockroach/pkg/util/stringarena"
	"github.com/cockroachdb/errors"
)

type aggregateFuncs []tree.AggregateFunc

func (af aggregateFuncs) close(ctx context.Context) {
	for _, f := range af {
		f.Close(ctx)
	}
}

// aggregatorBase is the foundation of the processor core type that does
// "aggregation" in the SQL sense. It groups rows and computes an aggregate for
// each group. The group is configured using the group key and the aggregator
// can be configured with one or more aggregation functions, as defined in the
// AggregatorSpec_Func enum.
//
// aggregatorBase's output schema is comprised of what is specified by the
// accompanying SELECT expressions.
type aggregatorBase struct {
	execinfra.ProcessorBase

	// runningState represents the state of the aggregator. This is in addition to
	// ProcessorBase.State - the runningState is only relevant when
	// ProcessorBase.State == StateRunning.
	runningState aggregatorState
	input        execinfra.RowSource
	inputDone    bool
	inputTypes   []*types.T
	funcs        []*aggregateFuncHolder
	outputTypes  []*types.T
	datumAlloc   rowenc.DatumAlloc
	rowAlloc     rowenc.EncDatumRowAlloc

	bucketsAcc  mon.BoundAccount
	aggFuncsAcc mon.BoundAccount

	// isScalar can only be set if there are no groupCols, and it means that we
	// will generate a result row even if there are no input rows. Used for
	// queries like SELECT MAX(n) FROM t.
	isScalar         bool
	groupCols        []uint32
	orderedGroupCols []uint32
	aggregations     []execinfrapb.AggregatorSpec_Aggregation

	lastOrdGroupCols rowenc.EncDatumRow
	arena            stringarena.Arena
	row              rowenc.EncDatumRow
	scratch          []byte

	cancelChecker *cancelchecker.CancelChecker
}

// init initializes the aggregatorBase.
//
// trailingMetaCallback is passed as part of ProcStateOpts; the inputs to drain
// are in aggregatorBase.
func (ag *aggregatorBase) init(
	self execinfra.RowSource,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.AggregatorSpec,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
	trailingMetaCallback func() []execinfrapb.ProducerMetadata,
) error {
	ctx := flowCtx.EvalCtx.Ctx()
	memMonitor := execinfra.NewMonitor(ctx, flowCtx.EvalCtx.Mon, "aggregator-mem")
	if execinfra.ShouldCollectStats(ctx, flowCtx) {
		input = newInputStatCollector(input)
		ag.ExecStatsForTrace = ag.execStatsForTrace
	}
	ag.input = input
	ag.isScalar = spec.IsScalar()
	ag.groupCols = spec.GroupCols
	ag.orderedGroupCols = spec.OrderedGroupCols
	ag.aggregations = spec.Aggregations
	ag.funcs = make([]*aggregateFuncHolder, len(spec.Aggregations))
	ag.outputTypes = make([]*types.T, len(spec.Aggregations))
	ag.row = make(rowenc.EncDatumRow, len(spec.Aggregations))
	ag.bucketsAcc = memMonitor.MakeBoundAccount()
	ag.arena = stringarena.Make(&ag.bucketsAcc)
	ag.aggFuncsAcc = memMonitor.MakeBoundAccount()

	// Loop over the select expressions and extract any aggregate functions --
	// non-aggregation functions are replaced with parser.NewIdentAggregate,
	// (which just returns the last value added to them for a bucket) to provide
	// grouped-by values for each bucket.  ag.funcs is updated to contain all
	// the functions which need to be fed values.
	ag.inputTypes = input.OutputTypes()
	semaCtx := flowCtx.TypeResolverFactory.NewSemaContext(flowCtx.EvalCtx.Txn)
	for i, aggInfo := range spec.Aggregations {
		if aggInfo.FilterColIdx != nil {
			col := *aggInfo.FilterColIdx
			if col >= uint32(len(ag.inputTypes)) {
				return errors.Errorf("FilterColIdx out of range (%d)", col)
			}
			t := ag.inputTypes[col].Family()
			if t != types.BoolFamily && t != types.UnknownFamily {
				return errors.Errorf(
					"filter column %d must be of boolean type, not %s", *aggInfo.FilterColIdx, t,
				)
			}
		}
		constructor, arguments, outputType, err := execinfrapb.GetAggregateConstructor(
			flowCtx.EvalCtx, semaCtx, &aggInfo, ag.inputTypes,
		)
		if err != nil {
			return err
		}
		ag.funcs[i] = ag.newAggregateFuncHolder(constructor, arguments)
		if aggInfo.Distinct {
			ag.funcs[i].seen = make(map[string]struct{})
		}
		ag.outputTypes[i] = outputType
	}

	return ag.ProcessorBase.Init(
		self, post, ag.outputTypes, flowCtx, processorID, output, memMonitor,
		execinfra.ProcStateOpts{
			InputsToDrain:        []execinfra.RowSource{ag.input},
			TrailingMetaCallback: trailingMetaCallback,
		},
	)
}

// execStatsForTrace implements ProcessorBase.ExecStatsForTrace.
func (ag *aggregatorBase) execStatsForTrace() *execinfrapb.ComponentStats {
	is, ok := getInputStats(ag.input)
	if !ok {
		return nil
	}
	return &execinfrapb.ComponentStats{
		Inputs: []execinfrapb.InputStats{is},
		Exec: execinfrapb.ExecStats{
			MaxAllocatedMem: optional.MakeUint(uint64(ag.MemMonitor.MaximumBytes())),
		},
		Output: ag.OutputHelper.Stats(),
	}
}

// ChildCount is part of the execinfra.OpNode interface.
func (ag *aggregatorBase) ChildCount(verbose bool) int {
	if _, ok := ag.input.(execinfra.OpNode); ok {
		return 1
	}
	return 0
}

// Child is part of the execinfra.OpNode interface.
func (ag *aggregatorBase) Child(nth int, verbose bool) execinfra.OpNode {
	if nth == 0 {
		if n, ok := ag.input.(execinfra.OpNode); ok {
			return n
		}
		panic("input to aggregatorBase is not an execinfra.OpNode")
	}
	panic(errors.AssertionFailedf("invalid index %d", nth))
}

const (
	// hashAggregatorBucketsInitialLen is a guess on how many "items" the
	// 'buckets' map of hashAggregator has the capacity for initially.
	hashAggregatorBucketsInitialLen = 8
	// hashAggregatorSizeOfBucketsItem is a guess on how much space (in bytes)
	// each item added to 'buckets' map of hashAggregator takes up in the map
	// (i.e. it is memory internal to the map, orthogonal to "key-value" pair
	// that we're adding to the map).
	hashAggregatorSizeOfBucketsItem = 64
)

// hashAggregator is a specialization of aggregatorBase that must keep track of
// multiple grouping buckets at a time.
type hashAggregator struct {
	aggregatorBase

	// buckets is used during the accumulation phase to track the bucket keys
	// that have been seen. After accumulation, the keys are extracted into
	// bucketsIter for iteration.
	buckets     map[string]aggregateFuncs
	bucketsIter []string
	// bucketsLenGrowThreshold is the threshold which, when reached by the
	// number of items in 'buckets', will trigger the update to memory
	// accounting. It will start out at hashAggregatorBucketsInitialLen and
	// then will be doubling in size.
	bucketsLenGrowThreshold int
	// alreadyAccountedFor tracks the number of items in 'buckets' memory for
	// which we have already accounted for.
	alreadyAccountedFor int
}

// orderedAggregator is a specialization of aggregatorBase that only needs to
// keep track of a single grouping bucket at a time.
type orderedAggregator struct {
	aggregatorBase

	// bucket is used during the accumulation phase to aggregate results.
	bucket aggregateFuncs
}

var _ execinfra.Processor = &hashAggregator{}
var _ execinfra.RowSource = &hashAggregator{}
var _ execinfra.OpNode = &hashAggregator{}

const hashAggregatorProcName = "hash aggregator"

var _ execinfra.Processor = &orderedAggregator{}
var _ execinfra.RowSource = &orderedAggregator{}
var _ execinfra.OpNode = &orderedAggregator{}

const orderedAggregatorProcName = "ordered aggregator"

// aggregatorState represents the state of the processor.
type aggregatorState int

const (
	aggStateUnknown aggregatorState = iota
	// aggAccumulating means that rows are being read from the input and used to
	// compute intermediary aggregation results.
	aggAccumulating
	// aggEmittingRows means that accumulation has finished and rows are being
	// sent to the output.
	aggEmittingRows
)

func newAggregator(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.AggregatorSpec,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (execinfra.Processor, error) {
	if spec.IsRowCount() {
		return newCountAggregator(flowCtx, processorID, input, post, output)
	}
	if len(spec.OrderedGroupCols) == len(spec.GroupCols) {
		return newOrderedAggregator(flowCtx, processorID, spec, input, post, output)
	}

	ag := &hashAggregator{
		buckets:                 make(map[string]aggregateFuncs),
		bucketsLenGrowThreshold: hashAggregatorBucketsInitialLen,
	}

	if err := ag.init(
		ag,
		flowCtx,
		processorID,
		spec,
		input,
		post,
		output,
		func() []execinfrapb.ProducerMetadata {
			ag.close()
			return nil
		},
	); err != nil {
		return nil, err
	}

	// A new tree.EvalCtx was created during initializing aggregatorBase above
	// and will be used only by this aggregator, so it is ok to update EvalCtx
	// directly.
	ag.EvalCtx.SingleDatumAggMemAccount = &ag.aggFuncsAcc
	return ag, nil
}

func newOrderedAggregator(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.AggregatorSpec,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (*orderedAggregator, error) {
	ag := &orderedAggregator{}

	if err := ag.init(
		ag,
		flowCtx,
		processorID,
		spec,
		input,
		post,
		output,
		func() []execinfrapb.ProducerMetadata {
			ag.close()
			return nil
		},
	); err != nil {
		return nil, err
	}

	// A new tree.EvalCtx was created during initializing aggregatorBase above
	// and will be used only by this aggregator, so it is ok to update EvalCtx
	// directly.
	ag.EvalCtx.SingleDatumAggMemAccount = &ag.aggFuncsAcc
	return ag, nil
}

// Start is part of the RowSource interface.
func (ag *hashAggregator) Start(ctx context.Context) {
	ag.start(ctx, hashAggregatorProcName)
}

// Start is part of the RowSource interface.
func (ag *orderedAggregator) Start(ctx context.Context) {
	ag.start(ctx, orderedAggregatorProcName)
}

func (ag *aggregatorBase) start(ctx context.Context, procName string) {
	ctx = ag.StartInternal(ctx, procName)
	ag.input.Start(ctx)
	ag.cancelChecker = cancelchecker.NewCancelChecker(ctx)
	ag.runningState = aggAccumulating
}

func (ag *hashAggregator) close() {
	if ag.InternalClose() {
		log.VEventf(ag.Ctx, 2, "exiting aggregator")
		// If we have started emitting rows, bucketsIter will represent which
		// buckets are still open, since buckets are closed once their results are
		// emitted.
		if ag.bucketsIter == nil {
			for _, bucket := range ag.buckets {
				bucket.close(ag.Ctx)
			}
		} else {
			for _, bucket := range ag.bucketsIter {
				ag.buckets[bucket].close(ag.Ctx)
			}
		}
		// Make sure to release any remaining memory under 'buckets'.
		ag.buckets = nil
		// Note that we should be closing accounts only after closing all the
		// buckets since the latter might be releasing some precisely tracked
		// memory, and if we were to close the accounts first, there would be
		// no memory to release for the buckets.
		ag.bucketsAcc.Close(ag.Ctx)
		ag.aggFuncsAcc.Close(ag.Ctx)
		ag.MemMonitor.Stop(ag.Ctx)
	}
}

func (ag *orderedAggregator) close() {
	if ag.InternalClose() {
		log.VEventf(ag.Ctx, 2, "exiting aggregator")
		if ag.bucket != nil {
			ag.bucket.close(ag.Ctx)
		}
		// Note that we should be closing accounts only after closing the
		// bucket since the latter might be releasing some precisely tracked
		// memory, and if we were to close the accounts first, there would be
		// no memory to release for the bucket.
		ag.bucketsAcc.Close(ag.Ctx)
		ag.aggFuncsAcc.Close(ag.Ctx)
		ag.MemMonitor.Stop(ag.Ctx)
	}
}

// matchLastOrdGroupCols takes a row and matches it with the row stored by
// lastOrdGroupCols. It returns true if the two rows are equal on the grouping
// columns, and false otherwise.
func (ag *aggregatorBase) matchLastOrdGroupCols(row rowenc.EncDatumRow) (bool, error) {
	for _, colIdx := range ag.orderedGroupCols {
		res, err := ag.lastOrdGroupCols[colIdx].Compare(
			ag.inputTypes[colIdx], &ag.datumAlloc, ag.EvalCtx, &row[colIdx],
		)
		if res != 0 || err != nil {
			return false, err
		}
	}
	return true, nil
}

// accumulateRows continually reads rows from the input and accumulates them
// into intermediary aggregate results. If it encounters metadata, the metadata
// is immediately returned. Subsequent calls of this function will resume row
// accumulation.
func (ag *hashAggregator) accumulateRows() (
	aggregatorState,
	rowenc.EncDatumRow,
	*execinfrapb.ProducerMetadata,
) {
	for {
		row, meta := ag.input.Next()
		if meta != nil {
			if meta.Err != nil {
				ag.MoveToDraining(nil /* err */)
				return aggStateUnknown, nil, meta
			}
			return aggAccumulating, nil, meta
		}
		if row == nil {
			log.VEvent(ag.Ctx, 1, "accumulation complete")
			ag.inputDone = true
			break
		}

		if ag.lastOrdGroupCols == nil {
			ag.lastOrdGroupCols = ag.rowAlloc.CopyRow(row)
		} else {
			matched, err := ag.matchLastOrdGroupCols(row)
			if err != nil {
				ag.MoveToDraining(err)
				return aggStateUnknown, nil, nil
			}
			if !matched {
				copy(ag.lastOrdGroupCols, row)
				break
			}
		}
		if err := ag.accumulateRow(row); err != nil {
			ag.MoveToDraining(err)
			return aggStateUnknown, nil, nil
		}
	}

	// Queries like `SELECT MAX(n) FROM t` expect a row of NULLs if nothing was
	// aggregated.
	if len(ag.buckets) < 1 && len(ag.groupCols) == 0 {
		bucket, err := ag.createAggregateFuncs()
		if err != nil {
			ag.MoveToDraining(err)
			return aggStateUnknown, nil, nil
		}
		ag.buckets[""] = bucket
	}

	// Note that, for simplicity, we're ignoring the overhead of the slice of
	// strings.
	if err := ag.bucketsAcc.Grow(ag.Ctx, int64(len(ag.buckets))*sizeOfString); err != nil {
		ag.MoveToDraining(err)
		return aggStateUnknown, nil, nil
	}
	ag.bucketsIter = make([]string, 0, len(ag.buckets))
	for bucket := range ag.buckets {
		ag.bucketsIter = append(ag.bucketsIter, bucket)
	}

	// Transition to aggEmittingRows, and let it generate the next row/meta.
	return aggEmittingRows, nil, nil
}

// accumulateRows continually reads rows from the input and accumulates them
// into intermediary aggregate results. If it encounters metadata, the metadata
// is immediately returned. Subsequent calls of this function will resume row
// accumulation.
func (ag *orderedAggregator) accumulateRows() (
	aggregatorState,
	rowenc.EncDatumRow,
	*execinfrapb.ProducerMetadata,
) {
	for {
		row, meta := ag.input.Next()
		if meta != nil {
			if meta.Err != nil {
				ag.MoveToDraining(nil /* err */)
				return aggStateUnknown, nil, meta
			}
			return aggAccumulating, nil, meta
		}
		if row == nil {
			log.VEvent(ag.Ctx, 1, "accumulation complete")
			ag.inputDone = true
			break
		}

		if ag.lastOrdGroupCols == nil {
			ag.lastOrdGroupCols = ag.rowAlloc.CopyRow(row)
		} else {
			matched, err := ag.matchLastOrdGroupCols(row)
			if err != nil {
				ag.MoveToDraining(err)
				return aggStateUnknown, nil, nil
			}
			if !matched {
				copy(ag.lastOrdGroupCols, row)
				break
			}
		}
		if err := ag.accumulateRow(row); err != nil {
			ag.MoveToDraining(err)
			return aggStateUnknown, nil, nil
		}
	}

	// Queries like `SELECT MAX(n) FROM t` expect a row of NULLs if nothing was
	// aggregated.
	if ag.bucket == nil && ag.isScalar {
		var err error
		ag.bucket, err = ag.createAggregateFuncs()
		if err != nil {
			ag.MoveToDraining(err)
			return aggStateUnknown, nil, nil
		}
	}

	// Transition to aggEmittingRows, and let it generate the next row/meta.
	return aggEmittingRows, nil, nil
}

// getAggResults returns the new aggregatorState and the results from the
// bucket. The bucket is closed.
func (ag *aggregatorBase) getAggResults(
	bucket aggregateFuncs,
) (aggregatorState, rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	defer bucket.close(ag.Ctx)

	for i, b := range bucket {
		result, err := b.Result()
		if err != nil {
			ag.MoveToDraining(err)
			return aggStateUnknown, nil, nil
		}
		if result == nil {
			// We can't encode nil into an EncDatum, so we represent it with DNull.
			result = tree.DNull
		}
		ag.row[i] = rowenc.DatumToEncDatum(ag.outputTypes[i], result)
	}

	if outRow := ag.ProcessRowHelper(ag.row); outRow != nil {
		return aggEmittingRows, outRow, nil
	}
	// We might have switched to draining, we might not have. In case we
	// haven't, aggEmittingRows is accurate. If we have, it will be ignored by
	// the caller.
	return aggEmittingRows, nil, nil
}

// emitRow constructs an output row from an accumulated bucket and returns it.
//
// emitRow() might move to stateDraining. It might also not return a row if the
// ProcOutputHelper filtered the current row out.
func (ag *hashAggregator) emitRow() (
	aggregatorState,
	rowenc.EncDatumRow,
	*execinfrapb.ProducerMetadata,
) {
	if len(ag.bucketsIter) == 0 {
		// We've exhausted all of the aggregation buckets.
		if ag.inputDone {
			// The input has been fully consumed. Transition to draining so that we
			// emit any metadata that we've produced.
			ag.MoveToDraining(nil /* err */)
			return aggStateUnknown, nil, nil
		}

		// We've only consumed part of the input where the rows are equal over
		// the columns specified by ag.orderedGroupCols, so we need to continue
		// accumulating the remaining rows.

		if err := ag.arena.UnsafeReset(ag.Ctx); err != nil {
			ag.MoveToDraining(err)
			return aggStateUnknown, nil, nil
		}
		// Before we create a new 'buckets' map below, we need to "release" the
		// already accounted for memory of the current map.
		ag.bucketsAcc.Shrink(ag.Ctx, int64(ag.alreadyAccountedFor)*hashAggregatorSizeOfBucketsItem)
		// Note that, for simplicity, we're ignoring the overhead of the slice of
		// strings.
		ag.bucketsAcc.Shrink(ag.Ctx, int64(len(ag.buckets))*sizeOfString)
		ag.bucketsIter = nil
		ag.buckets = make(map[string]aggregateFuncs)
		ag.bucketsLenGrowThreshold = hashAggregatorBucketsInitialLen
		ag.alreadyAccountedFor = 0
		for _, f := range ag.funcs {
			if f.seen != nil {
				// It turns out that it is faster to delete entries from the
				// old map rather than allocating a new one.
				for s := range f.seen {
					delete(f.seen, s)
				}
			}
		}

		if err := ag.accumulateRow(ag.lastOrdGroupCols); err != nil {
			ag.MoveToDraining(err)
			return aggStateUnknown, nil, nil
		}

		return aggAccumulating, nil, nil
	}

	bucket := ag.bucketsIter[0]
	ag.bucketsIter = ag.bucketsIter[1:]

	// Once we get the results from the bucket, we can delete it from the map.
	// This will allow us to return the memory to the system before the hash
	// aggregator is fully done (which matters when we have many buckets).
	// NOTE: accounting for the memory under aggregate builtins in the bucket
	// is updated in getAggResults (the bucket will be closed), however, we
	// choose to not reduce our estimate of the map's internal footprint
	// because it is error-prone to estimate the new footprint (we don't know
	// whether and when Go runtime will release some of the underlying memory).
	// This behavior is ok, though, since actual usage of buckets will be lower
	// than what we accounted for - in the worst case, the query might hit a
	// memory budget limit and error out when it might actually be within the
	// limit. However, we might be under accounting memory usage in other
	// places, so having some over accounting here might be actually beneficial
	// as a defensive mechanism against OOM crashes.
	state, row, meta := ag.getAggResults(ag.buckets[bucket])
	delete(ag.buckets, bucket)
	return state, row, meta
}

// emitRow constructs an output row from an accumulated bucket and returns it.
//
// emitRow() might move to stateDraining. It might also not return a row if the
// ProcOutputHelper filtered a the current row out.
func (ag *orderedAggregator) emitRow() (
	aggregatorState,
	rowenc.EncDatumRow,
	*execinfrapb.ProducerMetadata,
) {
	if ag.bucket == nil {
		// We've exhausted all of the aggregation buckets.
		if ag.inputDone {
			// The input has been fully consumed. Transition to draining so that we
			// emit any metadata that we've produced.
			ag.MoveToDraining(nil /* err */)
			return aggStateUnknown, nil, nil
		}

		// We've only consumed part of the input where the rows are equal over
		// the columns specified by ag.orderedGroupCols, so we need to continue
		// accumulating the remaining rows.

		if err := ag.arena.UnsafeReset(ag.Ctx); err != nil {
			ag.MoveToDraining(err)
			return aggStateUnknown, nil, nil
		}
		for _, f := range ag.funcs {
			if f.seen != nil {
				// It turns out that it is faster to delete entries from the
				// old map rather than allocating a new one.
				for s := range f.seen {
					delete(f.seen, s)
				}
			}
		}

		if err := ag.accumulateRow(ag.lastOrdGroupCols); err != nil {
			ag.MoveToDraining(err)
			return aggStateUnknown, nil, nil
		}

		return aggAccumulating, nil, nil
	}

	bucket := ag.bucket
	ag.bucket = nil
	return ag.getAggResults(bucket)
}

// Next is part of the RowSource interface.
func (ag *hashAggregator) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for ag.State == execinfra.StateRunning {
		var row rowenc.EncDatumRow
		var meta *execinfrapb.ProducerMetadata
		switch ag.runningState {
		case aggAccumulating:
			ag.runningState, row, meta = ag.accumulateRows()
		case aggEmittingRows:
			ag.runningState, row, meta = ag.emitRow()
		default:
			log.Fatalf(ag.Ctx, "unsupported state: %d", ag.runningState)
		}

		if row == nil && meta == nil {
			continue
		}
		return row, meta
	}
	return nil, ag.DrainHelper()
}

// Next is part of the RowSource interface.
func (ag *orderedAggregator) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for ag.State == execinfra.StateRunning {
		var row rowenc.EncDatumRow
		var meta *execinfrapb.ProducerMetadata
		switch ag.runningState {
		case aggAccumulating:
			ag.runningState, row, meta = ag.accumulateRows()
		case aggEmittingRows:
			ag.runningState, row, meta = ag.emitRow()
		default:
			log.Fatalf(ag.Ctx, "unsupported state: %d", ag.runningState)
		}

		if row == nil && meta == nil {
			continue
		}
		return row, meta
	}
	return nil, ag.DrainHelper()
}

// ConsumerClosed is part of the RowSource interface.
func (ag *hashAggregator) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	ag.close()
}

// ConsumerClosed is part of the RowSource interface.
func (ag *orderedAggregator) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	ag.close()
}

func (ag *aggregatorBase) accumulateRowIntoBucket(
	row rowenc.EncDatumRow, groupKey []byte, bucket aggregateFuncs,
) error {
	var err error
	// Feed the func holders for this bucket the non-grouping datums.
	for i, a := range ag.aggregations {
		if a.FilterColIdx != nil {
			col := *a.FilterColIdx
			if err := row[col].EnsureDecoded(ag.inputTypes[col], &ag.datumAlloc); err != nil {
				return err
			}
			if row[*a.FilterColIdx].Datum != tree.DBoolTrue {
				// This row doesn't contribute to this aggregation.
				continue
			}
		}
		// Extract the corresponding arguments from the row to feed into the
		// aggregate function.
		// Most functions require at most one argument thus we separate
		// the first argument and allocation of (if applicable) a variadic
		// collection of arguments thereafter.
		var firstArg tree.Datum
		var otherArgs tree.Datums
		if len(a.ColIdx) > 1 {
			otherArgs = make(tree.Datums, len(a.ColIdx)-1)
		}
		isFirstArg := true
		for j, c := range a.ColIdx {
			if err := row[c].EnsureDecoded(ag.inputTypes[c], &ag.datumAlloc); err != nil {
				return err
			}
			if isFirstArg {
				firstArg = row[c].Datum
				isFirstArg = false
				continue
			}
			otherArgs[j-1] = row[c].Datum
		}

		canAdd := true
		if a.Distinct {
			canAdd, err = ag.funcs[i].isDistinct(
				ag.Ctx,
				&ag.datumAlloc,
				groupKey,
				firstArg,
				otherArgs,
			)
			if err != nil {
				return err
			}
		}
		if !canAdd {
			continue
		}
		if err := bucket[i].Add(ag.Ctx, firstArg, otherArgs...); err != nil {
			return err
		}
	}
	return nil
}

// encode returns the encoding for the grouping columns, this is then used as
// our group key to determine which bucket to add to.
func (ag *hashAggregator) encode(
	appendTo []byte, row rowenc.EncDatumRow,
) (encoding []byte, err error) {
	for _, colIdx := range ag.groupCols {
		// We might allocate tree.Datums when hashing the row, so we'll ask the
		// fingerprint to account for them. Note that if the datums are later
		// used by the aggregate functions (and accounted for accordingly),
		// this can lead to over-accounting which is acceptable.
		appendTo, err = row[colIdx].Fingerprint(
			ag.Ctx, ag.inputTypes[colIdx], &ag.datumAlloc, appendTo, &ag.bucketsAcc,
		)
		if err != nil {
			return appendTo, err
		}
	}
	return appendTo, nil
}

// accumulateRow accumulates a single row, returning an error if accumulation
// failed for any reason.
func (ag *hashAggregator) accumulateRow(row rowenc.EncDatumRow) error {
	if err := ag.cancelChecker.Check(); err != nil {
		return err
	}

	// The encoding computed here determines which bucket the non-grouping
	// datums are accumulated to.
	encoded, err := ag.encode(ag.scratch, row)
	if err != nil {
		return err
	}
	ag.scratch = encoded[:0]

	bucket, ok := ag.buckets[string(encoded)]
	if !ok {
		s, err := ag.arena.AllocBytes(ag.Ctx, encoded)
		if err != nil {
			return err
		}
		bucket, err = ag.createAggregateFuncs()
		if err != nil {
			return err
		}
		ag.buckets[s] = bucket
		if len(ag.buckets) == ag.bucketsLenGrowThreshold {
			toAccountFor := ag.bucketsLenGrowThreshold - ag.alreadyAccountedFor
			if err := ag.bucketsAcc.Grow(ag.Ctx, int64(toAccountFor)*hashAggregatorSizeOfBucketsItem); err != nil {
				return err
			}
			ag.alreadyAccountedFor = ag.bucketsLenGrowThreshold
			ag.bucketsLenGrowThreshold *= 2
		}
	}

	return ag.accumulateRowIntoBucket(row, encoded, bucket)
}

// accumulateRow accumulates a single row, returning an error if accumulation
// failed for any reason.
func (ag *orderedAggregator) accumulateRow(row rowenc.EncDatumRow) error {
	if err := ag.cancelChecker.Check(); err != nil {
		return err
	}

	if ag.bucket == nil {
		var err error
		ag.bucket, err = ag.createAggregateFuncs()
		if err != nil {
			return err
		}
	}

	return ag.accumulateRowIntoBucket(row, nil /* groupKey */, ag.bucket)
}

type aggregateFuncHolder struct {
	create func(*tree.EvalContext, tree.Datums) tree.AggregateFunc

	// arguments is the list of constant (non-aggregated) arguments to the
	// aggregate, for instance, the separator in string_agg.
	arguments tree.Datums

	seen  map[string]struct{}
	arena *stringarena.Arena
}

const (
	sizeOfString         = int64(unsafe.Sizeof(""))
	sizeOfAggregateFuncs = int64(unsafe.Sizeof(aggregateFuncs{}))
	sizeOfAggregateFunc  = int64(unsafe.Sizeof(tree.AggregateFunc(nil)))
)

func (ag *aggregatorBase) newAggregateFuncHolder(
	create func(*tree.EvalContext, tree.Datums) tree.AggregateFunc, arguments tree.Datums,
) *aggregateFuncHolder {
	return &aggregateFuncHolder{
		create:    create,
		arena:     &ag.arena,
		arguments: arguments,
	}
}

// isDistinct returns whether this aggregateFuncHolder has not already seen the
// encoding of grouping columns and argument columns. It should be used *only*
// when we have DISTINCT aggregation so that we can aggregate only the "first"
// row in the group.
func (a *aggregateFuncHolder) isDistinct(
	ctx context.Context,
	alloc *rowenc.DatumAlloc,
	prefix []byte,
	firstArg tree.Datum,
	otherArgs tree.Datums,
) (bool, error) {
	// Allocate one EncDatum that will be reused when encoding every argument.
	ed := rowenc.EncDatum{Datum: firstArg}
	// We know that we have tree.Datum, so there will definitely be no need to
	// decode ed for fingerprinting, so we pass in nil memory account.
	encoded, err := ed.Fingerprint(ctx, firstArg.ResolvedType(), alloc, prefix, nil /* acc */)
	if err != nil {
		return false, err
	}
	for _, arg := range otherArgs {
		// Note that we don't need to explicitly unset ed because encoded
		// field is never set during fingerprinting - we'll compute the
		// encoding and return it without updating the EncDatum; therefore,
		// simply setting Datum field to the argument is sufficient.
		ed.Datum = arg
		encoded, err = ed.Fingerprint(ctx, arg.ResolvedType(), alloc, encoded, nil /* acc */)
		if err != nil {
			return false, err
		}
	}

	if _, ok := a.seen[string(encoded)]; ok {
		// We have already seen a row with such combination of grouping and
		// argument columns.
		return false, nil
	}
	s, err := a.arena.AllocBytes(ctx, encoded)
	if err != nil {
		return false, err
	}
	a.seen[s] = struct{}{}
	return true, nil
}

func (ag *aggregatorBase) createAggregateFuncs() (aggregateFuncs, error) {
	if err := ag.bucketsAcc.Grow(ag.Ctx, sizeOfAggregateFuncs+sizeOfAggregateFunc*int64(len(ag.funcs))); err != nil {
		return nil, err
	}
	bucket := make(aggregateFuncs, len(ag.funcs))
	for i, f := range ag.funcs {
		agg := f.create(ag.EvalCtx, f.arguments)
		if err := ag.bucketsAcc.Grow(ag.Ctx, agg.Size()); err != nil {
			return nil, err
		}
		bucket[i] = agg
	}
	return bucket, nil
}
