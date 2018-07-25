// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package distsqlrun

import (
	"context"
	"strings"
	"unsafe"

	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/stringarena"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
)

// GetAggregateInfo returns the aggregate constructor and the return type for
// the given aggregate function when applied on the given type.
func GetAggregateInfo(
	fn AggregatorSpec_Func, inputTypes ...sqlbase.ColumnType,
) (
	aggregateConstructor func(*tree.EvalContext) tree.AggregateFunc,
	returnType sqlbase.ColumnType,
	err error,
) {
	if fn == AggregatorSpec_ANY_NOT_NULL {
		// The ANY_NOT_NULL builtin does not have a fixed return type;
		// handle it separately.
		if len(inputTypes) != 1 {
			return nil, sqlbase.ColumnType{}, errors.Errorf("any_not_null aggregate needs 1 input")
		}
		return builtins.NewAnyNotNullAggregate, inputTypes[0], nil
	}
	datumTypes := make([]types.T, len(inputTypes))
	for i := range inputTypes {
		datumTypes[i] = inputTypes[i].ToDatumType()
	}

	_, builtins := builtins.GetBuiltinProperties(strings.ToLower(fn.String()))
	for _, b := range builtins {
		types := b.Types.Types()
		if len(types) != len(inputTypes) {
			continue
		}
		match := true
		for i, t := range types {
			if !datumTypes[i].Equivalent(t) {
				match = false
				break
			}
		}
		if match {
			// Found!
			constructAgg := func(evalCtx *tree.EvalContext) tree.AggregateFunc {
				return b.AggregateFunc(datumTypes, evalCtx)
			}

			colTyp, err := sqlbase.DatumTypeToColumnType(b.FixedReturnType())
			if err != nil {
				return nil, sqlbase.ColumnType{}, err
			}
			return constructAgg, colTyp, nil
		}
	}
	return nil, sqlbase.ColumnType{}, errors.Errorf(
		"no builtin aggregate for %s on %v", fn, inputTypes,
	)
}

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
	processorBase

	// runningState represents the state of the aggregator. This is in addition to
	// processorBase.state - the runningState is only relevant when
	// processorBase.state == stateRunning.
	runningState aggregatorState
	input        RowSource
	inputDone    bool
	inputTypes   []sqlbase.ColumnType
	funcs        []*aggregateFuncHolder
	outputTypes  []sqlbase.ColumnType
	datumAlloc   sqlbase.DatumAlloc
	rowAlloc     sqlbase.EncDatumRowAlloc

	bucketsAcc mon.BoundAccount

	// isScalar can only be set if there are no groupCols, and it means that we
	// will generate a result row even if there are no input rows. Used for
	// queries like SELECT MAX(n) FROM t.
	isScalar         bool
	groupCols        columns
	orderedGroupCols columns
	aggregations     []AggregatorSpec_Aggregation

	lastOrdGroupCols sqlbase.EncDatumRow
	arena            stringarena.Arena
	row              sqlbase.EncDatumRow
	scratch          []byte

	cancelChecker *sqlbase.CancelChecker
}

// init initializes the aggregatorBase.
//
// trailingMetaCallback is passed as part of procStateOpts; the inputs to drain
// are in aggregatorBase.
func (ag *aggregatorBase) init(
	self RowSource,
	flowCtx *FlowCtx,
	processorID int32,
	spec *AggregatorSpec,
	input RowSource,
	post *PostProcessSpec,
	output RowReceiver,
	trailingMetaCallback func() []ProducerMetadata,
) error {
	ctx := flowCtx.EvalCtx.Ctx()
	memMonitor := newMonitor(ctx, flowCtx.EvalCtx.Mon, "aggregator-mem")
	if sp := opentracing.SpanFromContext(ctx); sp != nil && tracing.IsRecording(sp) {
		input = NewInputStatCollector(input)
		ag.finishTrace = ag.outputStatsToTrace
	}
	ag.input = input
	switch spec.Type {
	case AggregatorSpec_SCALAR:
		ag.isScalar = true
	case AggregatorSpec_NON_SCALAR:
		ag.isScalar = false
	default:
		// This case exists for backward compatibility.
		ag.isScalar = (len(spec.GroupCols) == 0)
	}
	ag.groupCols = spec.GroupCols
	ag.orderedGroupCols = spec.OrderedGroupCols
	ag.aggregations = spec.Aggregations
	ag.funcs = make([]*aggregateFuncHolder, len(spec.Aggregations))
	ag.outputTypes = make([]sqlbase.ColumnType, len(spec.Aggregations))
	ag.row = make(sqlbase.EncDatumRow, len(spec.Aggregations))
	ag.bucketsAcc = memMonitor.MakeBoundAccount()
	ag.arena = stringarena.Make(&ag.bucketsAcc)

	// Loop over the select expressions and extract any aggregate functions --
	// non-aggregation functions are replaced with parser.NewIdentAggregate,
	// (which just returns the last value added to them for a bucket) to provide
	// grouped-by values for each bucket.  ag.funcs is updated to contain all
	// the functions which need to be fed values.
	ag.inputTypes = input.OutputTypes()
	for i, aggInfo := range spec.Aggregations {
		if aggInfo.FilterColIdx != nil {
			col := *aggInfo.FilterColIdx
			if col >= uint32(len(ag.inputTypes)) {
				return errors.Errorf("FilterColIdx out of range (%d)", col)
			}
			t := ag.inputTypes[col].SemanticType
			if t != sqlbase.ColumnType_BOOL && t != sqlbase.ColumnType_NULL {
				return errors.Errorf(
					"filter column %d must be of boolean type, not %s", *aggInfo.FilterColIdx, t,
				)
			}
		}
		argTypes := make([]sqlbase.ColumnType, len(aggInfo.ColIdx))
		for i, c := range aggInfo.ColIdx {
			if c >= uint32(len(ag.inputTypes)) {
				return errors.Errorf("ColIdx out of range (%d)", aggInfo.ColIdx)
			}
			argTypes[i] = ag.inputTypes[c]
		}
		aggConstructor, retType, err := GetAggregateInfo(aggInfo.Func, argTypes...)
		if err != nil {
			return err
		}

		ag.funcs[i] = ag.newAggregateFuncHolder(aggConstructor)
		if aggInfo.Distinct {
			ag.funcs[i].seen = make(map[string]struct{})
		}

		ag.outputTypes[i] = retType
	}

	return ag.processorBase.init(
		self, post, ag.outputTypes, flowCtx, processorID, output, memMonitor,
		procStateOpts{
			inputsToDrain:        []RowSource{ag.input},
			trailingMetaCallback: trailingMetaCallback,
		},
	)
}

var _ DistSQLSpanStats = &AggregatorStats{}

const aggregatorTagPrefix = "aggregator."

// Stats implements the SpanStats interface.
func (as *AggregatorStats) Stats() map[string]string {
	inputStatsMap := as.InputStats.Stats(aggregatorTagPrefix)
	inputStatsMap[aggregatorTagPrefix+maxMemoryTagSuffix] = humanizeutil.IBytes(as.MaxAllocatedMem)
	return inputStatsMap
}

// StatsForQueryPlan implements the DistSQLSpanStats interface.
func (as *AggregatorStats) StatsForQueryPlan() []string {
	return append(
		as.InputStats.StatsForQueryPlan("" /* prefix */),
		fmt.Sprintf("%s: %s", maxMemoryQueryPlanSuffix, humanizeutil.IBytes(as.MaxAllocatedMem)),
	)
}

func (ag *aggregatorBase) outputStatsToTrace() {
	is, ok := getInputStats(ag.flowCtx, ag.input)
	if !ok {
		return
	}
	if sp := opentracing.SpanFromContext(ag.ctx); sp != nil {
		tracing.SetSpanStats(
			sp,
			&AggregatorStats{
				InputStats:      is,
				MaxAllocatedMem: ag.memMonitor.MaximumBytes(),
			},
		)
	}
}

// hashAggregator is a specialization of aggregatorBase that must keep track of
// multiple grouping buckets at a time.
type hashAggregator struct {
	aggregatorBase

	// buckets is used during the accumulation phase to track the bucket keys
	// that have been seen. After accumulation, the keys are extracted into
	// bucketsIter for iteration.
	buckets     map[string]aggregateFuncs
	bucketsIter []string
}

// orderedAggregator is a specialization of aggregatorBase that only needs to
// keep track of a single grouping bucket at a time.
type orderedAggregator struct {
	aggregatorBase

	// bucket is used during the accumulation phase to aggregate results.
	bucket aggregateFuncs
}

var _ Processor = &hashAggregator{}
var _ RowSource = &hashAggregator{}

const hashAggregatorProcName = "hash aggregator"

var _ Processor = &orderedAggregator{}
var _ RowSource = &orderedAggregator{}

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
	flowCtx *FlowCtx,
	processorID int32,
	spec *AggregatorSpec,
	input RowSource,
	post *PostProcessSpec,
	output RowReceiver,
) (Processor, error) {
	if len(spec.OrderedGroupCols) == len(spec.GroupCols) {
		return newOrderedAggregator(flowCtx, processorID, spec, input, post, output)
	}

	ag := &hashAggregator{buckets: make(map[string]aggregateFuncs)}

	if err := ag.init(
		ag,
		flowCtx,
		processorID,
		spec,
		input,
		post,
		output,
		func() []ProducerMetadata {
			ag.close()
			return nil
		},
	); err != nil {
		return nil, err
	}

	return ag, nil
}

func newOrderedAggregator(
	flowCtx *FlowCtx,
	processorID int32,
	spec *AggregatorSpec,
	input RowSource,
	post *PostProcessSpec,
	output RowReceiver,
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
		func() []ProducerMetadata {
			ag.close()
			return nil
		},
	); err != nil {
		return nil, err
	}

	return ag, nil
}

// Start is part of the RowSource interface.
func (ag *hashAggregator) Start(ctx context.Context) context.Context {
	return ag.start(ctx, hashAggregatorProcName)
}

// Start is part of the RowSource interface.
func (ag *orderedAggregator) Start(ctx context.Context) context.Context {
	return ag.start(ctx, orderedAggregatorProcName)
}

func (ag *aggregatorBase) start(ctx context.Context, procName string) context.Context {
	ag.input.Start(ctx)
	ctx = ag.startInternal(ctx, procName)
	ag.cancelChecker = sqlbase.NewCancelChecker(ctx)
	ag.runningState = aggAccumulating
	return ctx
}

func (ag *hashAggregator) close() {
	if ag.internalClose() {
		log.VEventf(ag.ctx, 2, "exiting aggregator")
		ag.bucketsAcc.Close(ag.ctx)
		// If we have started emitting rows, bucketsIter will represent which
		// buckets are still open, since buckets are closed once their results are
		// emitted.
		if ag.bucketsIter == nil {
			for _, bucket := range ag.buckets {
				bucket.close(ag.ctx)
			}
		} else {
			for _, bucket := range ag.bucketsIter {
				ag.buckets[bucket].close(ag.ctx)
			}
		}
		ag.memMonitor.Stop(ag.ctx)
	}
}

func (ag *orderedAggregator) close() {
	if ag.internalClose() {
		log.VEventf(ag.ctx, 2, "exiting aggregator")
		ag.bucketsAcc.Close(ag.ctx)
		if ag.bucket != nil {
			ag.bucket.close(ag.ctx)
		}
		ag.memMonitor.Stop(ag.ctx)
	}
}

// matchLastOrdGroupCols takes a row and matches it with the row stored by
// lastOrdGroupCols. It returns true if the two rows are equal on the grouping
// columns, and false otherwise.
func (ag *aggregatorBase) matchLastOrdGroupCols(row sqlbase.EncDatumRow) (bool, error) {
	for _, colIdx := range ag.orderedGroupCols {
		cmp, err := ag.lastOrdGroupCols[colIdx].Distinct(
			&ag.inputTypes[colIdx], &ag.datumAlloc, &ag.flowCtx.EvalCtx, &row[colIdx],
		)
		if cmp || err != nil {
			return false, err
		}
	}
	return true, nil
}

// accumulateRows continually reads rows from the input and accumulates them
// into intermediary aggregate results. If it encounters metadata, the metadata
// is immediately returned. Subsequent calls of this function will resume row
// accumulation.
func (ag *hashAggregator) accumulateRows() (aggregatorState, sqlbase.EncDatumRow, *ProducerMetadata) {
	for {
		row, meta := ag.input.Next()
		if meta != nil {
			if meta.Err != nil {
				ag.moveToDraining(nil /* err */)
				return aggStateUnknown, nil, meta
			}
			return aggAccumulating, nil, meta
		}
		if row == nil {
			log.VEvent(ag.ctx, 1, "accumulation complete")
			ag.inputDone = true
			break
		}

		if ag.lastOrdGroupCols == nil {
			ag.lastOrdGroupCols = ag.rowAlloc.CopyRow(row)
		} else {
			matched, err := ag.matchLastOrdGroupCols(row)
			if err != nil {
				ag.moveToDraining(err)
				return aggStateUnknown, nil, nil
			}
			if !matched {
				ag.lastOrdGroupCols = ag.rowAlloc.CopyRow(row)
				break
			}
		}
		if err := ag.accumulateRow(row); err != nil {
			ag.moveToDraining(err)
			return aggStateUnknown, nil, nil
		}
	}

	// Queries like `SELECT MAX(n) FROM t` expect a row of NULLs if nothing was
	// aggregated.
	if len(ag.buckets) < 1 && len(ag.groupCols) == 0 {
		bucket, err := ag.createAggregateFuncs()
		if err != nil {
			ag.moveToDraining(err)
			return aggStateUnknown, nil, nil
		}
		ag.buckets[""] = bucket
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
func (ag *orderedAggregator) accumulateRows() (aggregatorState, sqlbase.EncDatumRow, *ProducerMetadata) {
	for {
		row, meta := ag.input.Next()
		if meta != nil {
			if meta.Err != nil {
				ag.moveToDraining(nil /* err */)
				return aggStateUnknown, nil, meta
			}
			return aggAccumulating, nil, meta
		}
		if row == nil {
			log.VEvent(ag.ctx, 1, "accumulation complete")
			ag.inputDone = true
			break
		}

		if ag.lastOrdGroupCols == nil {
			ag.lastOrdGroupCols = ag.rowAlloc.CopyRow(row)
		} else {
			matched, err := ag.matchLastOrdGroupCols(row)
			if err != nil {
				ag.moveToDraining(err)
				return aggStateUnknown, nil, nil
			}
			if !matched {
				ag.lastOrdGroupCols = ag.rowAlloc.CopyRow(row)
				break
			}
		}
		if err := ag.accumulateRow(row); err != nil {
			ag.moveToDraining(err)
			return aggStateUnknown, nil, nil
		}
	}

	// Queries like `SELECT MAX(n) FROM t` expect a row of NULLs if nothing was
	// aggregated.
	if ag.bucket == nil && ag.isScalar {
		var err error
		ag.bucket, err = ag.createAggregateFuncs()
		if err != nil {
			ag.moveToDraining(err)
			return aggStateUnknown, nil, nil
		}
	}

	// Transition to aggEmittingRows, and let it generate the next row/meta.
	return aggEmittingRows, nil, nil
}

func (ag *aggregatorBase) getAggResults(
	bucket aggregateFuncs,
) (aggregatorState, sqlbase.EncDatumRow, *ProducerMetadata) {
	for i, b := range bucket {
		result, err := b.Result()
		if err != nil {
			ag.moveToDraining(err)
			return aggStateUnknown, nil, nil
		}
		if result == nil {
			// We can't encode nil into an EncDatum, so we represent it with DNull.
			result = tree.DNull
		}
		ag.row[i] = sqlbase.DatumToEncDatum(ag.outputTypes[i], result)
	}
	bucket.close(ag.ctx)

	if outRow := ag.processRowHelper(ag.row); outRow != nil {
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
func (ag *hashAggregator) emitRow() (aggregatorState, sqlbase.EncDatumRow, *ProducerMetadata) {
	if len(ag.bucketsIter) == 0 {
		// We've exhausted all of the aggregation buckets.
		if ag.inputDone {
			// The input has been fully consumed. Transition to draining so that we
			// emit any metadata that we've produced.
			ag.moveToDraining(nil /* err */)
			return aggStateUnknown, nil, nil
		}

		// We've only consumed part of the input where the rows are equal over
		// the columns specified by ag.orderedGroupCols, so we need to continue
		// accumulating the remaining rows.

		if err := ag.arena.UnsafeReset(ag.ctx); err != nil {
			ag.moveToDraining(err)
			return aggStateUnknown, nil, nil
		}
		ag.bucketsIter = nil
		ag.buckets = make(map[string]aggregateFuncs)
		for _, f := range ag.funcs {
			if f.seen != nil {
				f.seen = make(map[string]struct{})
			}
		}

		if err := ag.accumulateRow(ag.lastOrdGroupCols); err != nil {
			ag.moveToDraining(err)
			return aggStateUnknown, nil, nil
		}

		return aggAccumulating, nil, nil
	}

	bucket := ag.bucketsIter[0]
	ag.bucketsIter = ag.bucketsIter[1:]

	return ag.getAggResults(ag.buckets[bucket])
}

// emitRow constructs an output row from an accumulated bucket and returns it.
//
// emitRow() might move to stateDraining. It might also not return a row if the
// ProcOutputHelper filtered a the current row out.
func (ag *orderedAggregator) emitRow() (aggregatorState, sqlbase.EncDatumRow, *ProducerMetadata) {
	if ag.bucket == nil {
		// We've exhausted all of the aggregation buckets.
		if ag.inputDone {
			// The input has been fully consumed. Transition to draining so that we
			// emit any metadata that we've produced.
			ag.moveToDraining(nil /* err */)
			return aggStateUnknown, nil, nil
		}

		// We've only consumed part of the input where the rows are equal over
		// the columns specified by ag.orderedGroupCols, so we need to continue
		// accumulating the remaining rows.

		if err := ag.arena.UnsafeReset(ag.ctx); err != nil {
			ag.moveToDraining(err)
			return aggStateUnknown, nil, nil
		}
		for _, f := range ag.funcs {
			if f.seen != nil {
				f.seen = make(map[string]struct{})
			}
		}

		if err := ag.accumulateRow(ag.lastOrdGroupCols); err != nil {
			ag.moveToDraining(err)
			return aggStateUnknown, nil, nil
		}

		return aggAccumulating, nil, nil
	}

	bucket := ag.bucket
	ag.bucket = nil
	return ag.getAggResults(bucket)
}

// Next is part of the RowSource interface.
func (ag *hashAggregator) Next() (sqlbase.EncDatumRow, *ProducerMetadata) {
	for ag.state == stateRunning {
		var row sqlbase.EncDatumRow
		var meta *ProducerMetadata
		switch ag.runningState {
		case aggAccumulating:
			ag.runningState, row, meta = ag.accumulateRows()
		case aggEmittingRows:
			ag.runningState, row, meta = ag.emitRow()
		default:
			log.Fatalf(ag.ctx, "unsupported state: %d", ag.runningState)
		}

		if row == nil && meta == nil {
			continue
		}
		return row, meta
	}
	return nil, ag.drainHelper()
}

// Next is part of the RowSource interface.
func (ag *orderedAggregator) Next() (sqlbase.EncDatumRow, *ProducerMetadata) {
	for ag.state == stateRunning {
		var row sqlbase.EncDatumRow
		var meta *ProducerMetadata
		switch ag.runningState {
		case aggAccumulating:
			ag.runningState, row, meta = ag.accumulateRows()
		case aggEmittingRows:
			ag.runningState, row, meta = ag.emitRow()
		default:
			log.Fatalf(ag.ctx, "unsupported state: %d", ag.runningState)
		}

		if row == nil && meta == nil {
			continue
		}
		return row, meta
	}
	return nil, ag.drainHelper()
}

// ConsumerDone is part of the RowSource interface.
func (ag *aggregatorBase) ConsumerDone() {
	ag.moveToDraining(nil /* err */)
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
	row sqlbase.EncDatumRow, groupKey []byte, bucket aggregateFuncs,
) error {
	// Feed the func holders for this bucket the non-grouping datums.
	for i, a := range ag.aggregations {
		if a.FilterColIdx != nil {
			col := *a.FilterColIdx
			if err := row[col].EnsureDecoded(&ag.inputTypes[col], &ag.datumAlloc); err != nil {
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
			if err := row[c].EnsureDecoded(&ag.inputTypes[c], &ag.datumAlloc); err != nil {
				return err
			}
			if isFirstArg {
				firstArg = row[c].Datum
				isFirstArg = false
				continue
			}
			otherArgs[j-1] = row[c].Datum
		}

		canAdd, err := ag.funcs[i].canAdd(ag.ctx, groupKey, firstArg, otherArgs)
		if err != nil {
			return err
		}
		if !canAdd {
			continue
		}
		if err := bucket[i].Add(ag.ctx, firstArg, otherArgs...); err != nil {
			return err
		}
	}
	return nil
}

// accumulateRow accumulates a single row, returning an error if accumulation
// failed for any reason.
func (ag *hashAggregator) accumulateRow(row sqlbase.EncDatumRow) error {
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
		s, err := ag.arena.AllocBytes(ag.ctx, encoded)
		if err != nil {
			return err
		}
		bucket, err = ag.createAggregateFuncs()
		if err != nil {
			return err
		}
		ag.buckets[s] = bucket
	}

	return ag.accumulateRowIntoBucket(row, encoded, bucket)
}

// accumulateRow accumulates a single row, returning an error if accumulation
// failed for any reason.
func (ag *orderedAggregator) accumulateRow(row sqlbase.EncDatumRow) error {
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
	create func(*tree.EvalContext) tree.AggregateFunc
	group  *aggregatorBase
	seen   map[string]struct{}
	arena  *stringarena.Arena
}

const sizeOfAggregateFunc = int64(unsafe.Sizeof(tree.AggregateFunc(nil)))

func (ag *aggregatorBase) newAggregateFuncHolder(
	create func(*tree.EvalContext) tree.AggregateFunc,
) *aggregateFuncHolder {
	return &aggregateFuncHolder{
		create: create,
		group:  ag,
		arena:  &ag.arena,
	}
}

func (a *aggregateFuncHolder) canAdd(
	ctx context.Context, encodingPrefix []byte, firstArg tree.Datum, otherArgs tree.Datums,
) (bool, error) {
	if a.seen != nil {
		encoded, err := sqlbase.EncodeDatum(encodingPrefix, firstArg)
		if err != nil {
			return false, err
		}
		// Encode additional arguments if necessary.
		if otherArgs != nil {
			encoded, err = sqlbase.EncodeDatums(encoded, otherArgs)
			if err != nil {
				return false, err
			}
		}

		if _, ok := a.seen[string(encoded)]; ok {
			// skip
			return false, nil
		}
		s, err := a.arena.AllocBytes(ctx, encoded)
		if err != nil {
			return false, err
		}
		a.seen[s] = struct{}{}
	}

	return true, nil
}

// encode returns the encoding for the grouping columns, this is then used as
// our group key to determine which bucket to add to.
func (ag *aggregatorBase) encode(
	appendTo []byte, row sqlbase.EncDatumRow,
) (encoding []byte, err error) {
	for _, colIdx := range ag.groupCols {
		appendTo, err = row[colIdx].Encode(
			&ag.inputTypes[colIdx], &ag.datumAlloc, sqlbase.DatumEncoding_ASCENDING_KEY, appendTo)
		if err != nil {
			return appendTo, err
		}
	}
	return appendTo, nil
}

func (ag *aggregatorBase) createAggregateFuncs() (aggregateFuncs, error) {
	bucket := make(aggregateFuncs, len(ag.funcs))
	for i, f := range ag.funcs {
		// TODO(radu): we should account for the size of impl (this needs to be done
		// in each aggregate constructor).
		bucket[i] = f.create(&ag.flowCtx.EvalCtx)
	}
	if err := ag.bucketsAcc.Grow(ag.ctx, sizeOfAggregateFunc*int64(len(ag.funcs))); err != nil {
		return nil, err
	}
	return bucket, nil
}
