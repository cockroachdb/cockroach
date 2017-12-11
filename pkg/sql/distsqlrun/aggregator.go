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
	"strings"
	"sync"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
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
	Batching bool
	processorBase

	flowCtx     *FlowCtx
	input       RowSource
	inputTypes  []sqlbase.ColumnType
	funcs       []*aggregateFuncHolder
	outputTypes []sqlbase.ColumnType
	datumAlloc  sqlbase.DatumAlloc

	bucketsAcc mon.BoundAccount

	groupCols    columns
	aggregations []AggregatorSpec_Aggregation

	buckets map[string]struct{} // The set of bucket keys.
}

var _ Processor = &aggregator{}

func newAggregator(
	flowCtx *FlowCtx,
	spec *AggregatorSpec,
	input RowSource,
	post *PostProcessSpec,
	output RowReceiver,
) (*aggregator, error) {
	ag := &aggregator{
		flowCtx:      flowCtx,
		input:        input,
		groupCols:    spec.GroupCols,
		aggregations: spec.Aggregations,
		buckets:      make(map[string]struct{}),
		funcs:        make([]*aggregateFuncHolder, len(spec.Aggregations)),
		outputTypes:  make([]sqlbase.ColumnType, len(spec.Aggregations)),
		bucketsAcc:   flowCtx.EvalCtx.Mon.MakeBoundAccount(),
	}

	// Loop over the select expressions and extract any aggregate functions --
	// non-aggregation functions are replaced with parser.NewIdentAggregate,
	// (which just returns the last value added to them for a bucket) to provide
	// grouped-by values for each bucket.  ag.funcs is updated to contain all
	// the functions which need to be fed values.
	ag.inputTypes = input.Types()
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
	if err := ag.init(post, ag.outputTypes, flowCtx, output); err != nil {
		return nil, err
	}

	return ag, nil
}

func (ag *aggregator) Close(ctx context.Context) {
	for _, f := range ag.funcs {
		for _, aggFunc := range f.buckets {
			aggFunc.Close(ctx)
		}
	}
	ag.bucketsAcc.Close(ctx)
}

// Run is part of the processor interface.
func (ag *aggregator) Run(ctx context.Context, wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}
	defer ag.Close(ctx)

	ctx = log.WithLogTag(ctx, "Agg", nil)
	ctx, span := processorSpan(ctx, "aggregator")
	defer tracing.FinishSpan(span)

	if log.V(2) {
		log.Infof(ctx, "starting aggregation process")
		defer log.Infof(ctx, "exiting aggregator")
	}

	if err := ag.accumulateRows(ctx); err != nil {
		// We swallow the error here, it has already been forwarded to the output.
		return
	}

	log.VEvent(ctx, 1, "accumulation complete")
	ag.RenderResults(ctx)

}

func (ag *aggregator) RenderResults(ctx context.Context) {
	// Queries like `SELECT MAX(n) FROM t` expect a row of NULLs if nothing was
	// aggregated.
	if len(ag.buckets) < 1 && len(ag.groupCols) == 0 {
		ag.buckets[""] = struct{}{}
	}

	// Render the results.
	var consumerDone bool
	row := make(sqlbase.EncDatumRow, len(ag.funcs))
	for bucket := range ag.buckets {
		for i, f := range ag.funcs {
			result, err := f.get(bucket)
			if err != nil {
				DrainAndClose(ctx, ag.out.output, err, ag.input)
				return
			}
			if result == nil {
				// Special case useful when this is a local stage of a distributed
				// aggregation.
				result = tree.DNull
			}
			row[i] = sqlbase.DatumToEncDatum(ag.outputTypes[i], result)
		}

		consumerDone = !emitHelper(ctx, &ag.out, row, ProducerMetadata{})
		if consumerDone {
			break
		}
	}
	// If the consumer has been found to be done, emitHelper() already closed the
	// output.
	if !consumerDone {
		sendTraceData(ctx, ag.out.output)
		ag.out.Close()
	}
}

// accumulateRows reads and accumulates all input rows.
// If no error is return, it means that all the rows from the input have been
// consumed.
// If an error is returned, both the input and the output have been properly
// closed, and the error has also been forwarded to the output.
func (ag *aggregator) accumulateRows(ctx context.Context) (err error) {
	var scratch []byte
	if ag.Batching {
		rowChan := ag.input.(*RowChannel)
		for {
			batch, meta := rowChan.NextBatch()
			for _, row := range batch {
				if cont, err := ag.NextRow(ctx, scratch, row, meta); err != nil || !cont {
					return err
				}
			}
			if batch == nil {
				return nil
			}
		}
		return nil
	}
	for {
		row, meta := ag.input.Next()
		if cont, err := ag.NextRow(ctx, scratch, row, meta); err != nil || !cont {
			return err
		}
	}
}

func (ag *aggregator) NextRow(ctx context.Context, scratch []byte, row sqlbase.EncDatumRow, meta ProducerMetadata) (cont bool, err error) {
	cleanupRequired := true
	defer func() {
		if err != nil {
			log.Infof(ctx, "accumulate error %s", err)
			if cleanupRequired {
				DrainAndClose(ctx, ag.out.output, err, ag.input)
			}
		}
	}()
	if !meta.Empty() {
		if meta.Err != nil {
			return false, meta.Err
		}
		if !emitHelper(ctx, &ag.out, nil /* row */, meta, ag.input) {
			// TODO(andrei): here, because we're passing metadata through, we have
			// an opportunity to find out that the consumer doesn't need the data
			// any more. If the producer doesn't push any metadata, then there's no
			// opportunity to find this out until the accumulation phase is done. We
			// should have a way to periodically peek at the state of the
			// RowReceiver that's hiding behind the ProcOutputHelper.
			cleanupRequired = false
			return false, errors.Errorf("consumer stopped before it received rows")
		}
		return true, nil
	}
	if row == nil {
		return false, nil
	}

	// The encoding computed here determines which bucket the non-grouping
	// datums are accumulated to.
	encoded, err := ag.encode(scratch, row)
	if err != nil {
		return false, err
	}

	if _, ok := ag.buckets[string(encoded)]; !ok {
		if err := ag.bucketsAcc.Grow(ctx, int64(len(encoded))); err != nil {
			return false, err
		}
		ag.buckets[string(encoded)] = struct{}{}
	}

	// Feed the func holders for this bucket the non-grouping datums.
	for i, a := range ag.aggregations {
		if a.FilterColIdx != nil {
			col := *a.FilterColIdx
			if err := row[col].EnsureDecoded(&ag.inputTypes[col], &ag.datumAlloc); err != nil {
				return false, err
			}
			if row[*a.FilterColIdx].Datum != tree.DBoolTrue {
				// This row doesn't contribute to this aggregation.
				return true, nil
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
				return false, err
			}
			if isFirstArg {
				firstArg = row[c].Datum
				isFirstArg = false
				return true, nil
			}
			otherArgs[j-1] = row[c].Datum
		}

		if err := ag.funcs[i].add(ctx, encoded, firstArg, otherArgs); err != nil {
			return false, err
		}
	}

	scratch = encoded[:0]
	return true, nil
}

type aggregateFuncHolder struct {
	create        func(*tree.EvalContext) tree.AggregateFunc
	group         *aggregator
	buckets       map[string]tree.AggregateFunc
	seen          map[string]struct{}
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
		if err := a.bucketsMemAcc.Grow(ctx, int64(len(encoded))); err != nil {
			return err
		}
		a.seen[string(encoded)] = struct{}{}
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
