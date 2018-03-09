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
	"sync"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/stringarena"
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
	if fn == AggregatorSpec_IDENT {
		if len(inputTypes) != 1 {
			return nil, sqlbase.ColumnType{}, errors.Errorf("ident aggregate needs 1 input")
		}
		return builtins.NewIdentAggregate, inputTypes[0], nil
	}

	datumTypes := make([]types.T, len(inputTypes))
	for i := range inputTypes {
		datumTypes[i] = inputTypes[i].ToDatumType()
	}

	builtins := builtins.Aggregates[strings.ToLower(fn.String())]
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

// aggregator is the processor core type that does "aggregation" in the SQL
// sense. It groups rows and computes an aggregate for each group. The group is
// configured using the group key and the aggregator can be configured with one
// or more aggregation functions, as defined in the AggregatorSpec_Func enum.
//
// aggregator's output schema is comprised of what is specified by the
// accompanying SELECT expressions.
type aggregator struct {
	processorBase
	rowSourceBase

	accumulating bool
	draining     bool
	input        RowSource
	inputTypes   []sqlbase.ColumnType
	funcs        []*aggregateFuncHolder
	outputTypes  []sqlbase.ColumnType
	datumAlloc   sqlbase.DatumAlloc

	bucketsAcc mon.BoundAccount

	groupCols    columns
	aggregations []AggregatorSpec_Aggregation

	// buckets is used during the accumulation phase to track the bucket keys
	// that have been seen. After accumulation, the keys are extracted into
	// bucketsIter for iteration.
	buckets     map[string]struct{}
	bucketsIter []string
	arena       stringarena.Arena
	row         sqlbase.EncDatumRow
	scratch     []byte

	cancelChecker *sqlbase.CancelChecker
}

var _ Processor = &aggregator{}
var _ RowSource = &aggregator{}

func newAggregator(
	flowCtx *FlowCtx,
	spec *AggregatorSpec,
	input RowSource,
	post *PostProcessSpec,
	output RowReceiver,
) (*aggregator, error) {
	ag := &aggregator{
		input:        input,
		groupCols:    spec.GroupCols,
		aggregations: spec.Aggregations,
		buckets:      make(map[string]struct{}),
		funcs:        make([]*aggregateFuncHolder, len(spec.Aggregations)),
		outputTypes:  make([]sqlbase.ColumnType, len(spec.Aggregations)),
		bucketsAcc:   flowCtx.EvalCtx.Mon.MakeBoundAccount(),
	}
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
				return nil, errors.Errorf("FilterColIdx out of range (%d)", col)
			}
			t := ag.inputTypes[col].SemanticType
			if t != sqlbase.ColumnType_BOOL && t != sqlbase.ColumnType_NULL {
				return nil, errors.Errorf(
					"filter column %d must be of boolean type, not %s", *aggInfo.FilterColIdx, t,
				)
			}
		}
		argTypes := make([]sqlbase.ColumnType, len(aggInfo.ColIdx))
		for i, c := range aggInfo.ColIdx {
			if c >= uint32(len(ag.inputTypes)) {
				return nil, errors.Errorf("ColIdx out of range (%d)", aggInfo.ColIdx)
			}
			argTypes[i] = ag.inputTypes[c]
		}
		aggConstructor, retType, err := GetAggregateInfo(aggInfo.Func, argTypes...)
		if err != nil {
			return nil, err
		}

		ag.funcs[i] = ag.newAggregateFuncHolder(aggConstructor)
		if aggInfo.Distinct {
			ag.funcs[i].seen = make(map[string]struct{})
		}

		ag.outputTypes[i] = retType
	}
	if err := ag.init(post, ag.outputTypes, flowCtx, nil /* evalCtx */, output); err != nil {
		return nil, err
	}

	return ag, nil
}

// Run is part of the processor interface.
func (ag *aggregator) Run(wg *sync.WaitGroup) {
	if ag.out.output == nil {
		panic("aggregator output not initialized for emitting rows")
	}
	Run(ag.flowCtx.Ctx, ag, ag.out.output)
	if wg != nil {
		wg.Done()
	}
}

func (ag *aggregator) close() {
	if !ag.closed {
		log.VEventf(ag.ctx, 2, "exiting aggregator")
		ag.bucketsAcc.Close(ag.ctx)
		for _, f := range ag.funcs {
			for _, aggFunc := range f.buckets {
				aggFunc.Close(ag.ctx)
			}
		}
		ag.accumulating = false
		ag.draining = true
	}
	ag.internalClose()
}

// producerMeta constructs the ProducerMetadata after consumption of rows has
// terminated, either due to being indicated by the consumer, or because the
// processor ran out of rows or encountered an error. It is ok for err to be
// nil indicating that we're done producing rows even though no error occurred.
func (ag *aggregator) producerMeta(err error) *ProducerMetadata {
	var meta *ProducerMetadata
	if !ag.closed {
		if err != nil {
			meta = &ProducerMetadata{Err: err}
		} else if trace := getTraceData(ag.ctx); trace != nil {
			meta = &ProducerMetadata{TraceData: trace}
		}
		// We need to close as soon as we send producer metadata as we're done
		// sending rows. The consumer is allowed to not call ConsumerDone().
		ag.close()
	}
	return meta
}

// Next is part of the RowSource interface.
func (ag *aggregator) Next() (sqlbase.EncDatumRow, *ProducerMetadata) {
	if ag.maybeStart("aggregator", "Agg") {
		log.VEventf(ag.ctx, 2, "starting aggregation process")
		ag.cancelChecker = sqlbase.NewCancelChecker(ag.ctx)
		ag.accumulating = true
	}

	if ag.accumulating {
		for {
			row, meta := ag.input.Next()
			if meta != nil {
				if meta.Err != nil {
					return nil, ag.producerMeta(meta.Err)
				}
				return nil, meta
			}
			if row == nil {
				break
			}
			if ag.closed || ag.consumerStatus != NeedMoreRows {
				continue
			}
			if err := ag.accumulateRow(row); err != nil {
				return nil, ag.producerMeta(err)
			}
		}

		// Queries like `SELECT MAX(n) FROM t` expect a row of NULLs if nothing was
		// aggregated.
		if len(ag.buckets) < 1 && len(ag.groupCols) == 0 {
			ag.buckets[""] = struct{}{}
		}

		ag.bucketsIter = make([]string, 0, len(ag.buckets))
		for bucket := range ag.buckets {
			ag.bucketsIter = append(ag.bucketsIter, bucket)
		}
		ag.buckets = nil

		ag.accumulating = false
		log.VEvent(ag.ctx, 1, "accumulation complete")

		ag.row = make(sqlbase.EncDatumRow, len(ag.funcs))
	}

	if ag.draining {
		for {
			row, meta := ag.input.Next()
			if row != nil {
				continue
			}
			if meta != nil {
				return nil, meta
			}
			break
		}
		ag.draining = false
	}

	if ag.closed || ag.consumerStatus != NeedMoreRows {
		return nil, ag.producerMeta(nil /* err */)
	}

	for {
		if len(ag.bucketsIter) == 0 {
			return nil, ag.producerMeta(nil /* err */)
		}

		bucket := ag.bucketsIter[0]
		ag.bucketsIter = ag.bucketsIter[1:]

		for i, f := range ag.funcs {
			result, err := f.get(bucket)
			if err != nil {
				return nil, ag.producerMeta(err)
			}
			if result == nil {
				// Special case useful when this is a local stage of a distributed
				// aggregation.
				result = tree.DNull
			}
			ag.row[i] = sqlbase.DatumToEncDatum(ag.outputTypes[i], result)
		}

		outRow, status, err := ag.out.ProcessRow(ag.ctx, ag.row)
		if outRow != nil {
			return outRow, nil
		}
		if outRow == nil && err == nil && status == NeedMoreRows {
			continue
		}
		return nil, ag.producerMeta(err)
	}
}

// ConsumerDone is part of the RowSource interface.
func (ag *aggregator) ConsumerDone() {
	ag.consumerDone()
	ag.input.ConsumerDone()
}

// ConsumerClosed is part of the RowSource interface.
func (ag *aggregator) ConsumerClosed() {
	ag.consumerClosed("aggregator")
	// The consumer is done, Next() will not be called again.
	ag.close()
}

// accumulateRow accumulates a single row, returning an error if accumulation
// failed for any reason.
func (ag *aggregator) accumulateRow(row sqlbase.EncDatumRow) error {
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

	if _, ok := ag.buckets[string(encoded)]; !ok {
		s, err := ag.arena.AllocBytes(ag.ctx, encoded)
		if err != nil {
			return err
		}
		ag.buckets[s] = struct{}{}
	}

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

		if err := ag.funcs[i].add(ag.ctx, encoded, firstArg, otherArgs); err != nil {
			return err
		}
	}
	return nil
}

type aggregateFuncHolder struct {
	create        func(*tree.EvalContext) tree.AggregateFunc
	group         *aggregator
	buckets       map[string]tree.AggregateFunc
	seen          map[string]struct{}
	arena         *stringarena.Arena
	bucketsMemAcc *mon.BoundAccount
}

const sizeOfAggregateFunc = int64(unsafe.Sizeof(tree.AggregateFunc(nil)))

func (ag *aggregator) newAggregateFuncHolder(
	create func(*tree.EvalContext) tree.AggregateFunc,
) *aggregateFuncHolder {
	return &aggregateFuncHolder{
		create:        create,
		group:         ag,
		buckets:       make(map[string]tree.AggregateFunc),
		bucketsMemAcc: &ag.bucketsAcc,
		arena:         &ag.arena,
	}
}

func (a *aggregateFuncHolder) add(
	ctx context.Context, bucket []byte, firstArg tree.Datum, otherArgs tree.Datums,
) error {
	if a.seen != nil {
		encoded, err := sqlbase.EncodeDatum(bucket, firstArg)
		if err != nil {
			return err
		}
		// Encode additional arguments if necessary.
		if otherArgs != nil {
			encoded, err = sqlbase.EncodeDatums(bucket, otherArgs)
			if err != nil {
				return err
			}
		}

		if _, ok := a.seen[string(encoded)]; ok {
			// skip
			return nil
		}
		s, err := a.arena.AllocBytes(ctx, encoded)
		if err != nil {
			return err
		}
		a.seen[s] = struct{}{}
	}

	impl, ok := a.buckets[string(bucket)]
	if !ok {
		// TODO(radu): we should account for the size of impl (this needs to be done
		// in each aggregate constructor).
		impl = a.create(&a.group.flowCtx.EvalCtx)
		usage := int64(len(bucket))
		usage += sizeOfAggregateFunc
		// TODO(radu): this model of each func having a map of buckets (one per
		// group) for each func plus a global map is very wasteful. We should have a
		// single map that stores all the AggregateFuncs.
		if err := a.bucketsMemAcc.Grow(ctx, usage); err != nil {
			return err
		}
		a.buckets[string(bucket)] = impl
	}

	return impl.Add(ctx, firstArg, otherArgs...)
}

func (a *aggregateFuncHolder) get(bucket string) (tree.Datum, error) {
	found, ok := a.buckets[bucket]
	if !ok {
		found = a.create(&a.group.flowCtx.EvalCtx)
	}

	return found.Result()
}

// encode returns the encoding for the grouping columns, this is then used as
// our group key to determine which bucket to add to.
func (ag *aggregator) encode(
	appendTo []byte, row sqlbase.EncDatumRow,
) (encoding []byte, err error) {
	for _, colIdx := range ag.groupCols {
		appendTo, err = row[colIdx].Encode(&ag.inputTypes[colIdx], &ag.datumAlloc, sqlbase.DatumEncoding_ASCENDING_KEY, appendTo)
		if err != nil {
			return appendTo, err
		}
	}
	return appendTo, nil
}
