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
//
// Author: Irfan Sharif (irfansharif@cockroachlabs.com)

package distsqlrun

import (
	"strings"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/mon"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

// GetAggregateInfo returns the aggregate constructor and the return type for
// the given aggregate function when applied on the given type.
func GetAggregateInfo(
	fn AggregatorSpec_Func, inputType sqlbase.ColumnType,
) (
	aggregateConstructor func(*parser.EvalContext) parser.AggregateFunc,
	returnType sqlbase.ColumnType,
	err error,
) {
	if fn == AggregatorSpec_IDENT {
		return parser.NewIdentAggregate, inputType, nil
	}

	inputDatumType := inputType.ToDatumType()
	builtins := parser.Aggregates[strings.ToLower(fn.String())]
	for _, b := range builtins {
		for _, t := range b.Types.Types() {
			if inputDatumType.Equivalent(t) {
				// Found!
				constructAgg := func(evalCtx *parser.EvalContext) parser.AggregateFunc {
					return b.AggregateFunc([]parser.Type{inputDatumType}, evalCtx)
				}
				return constructAgg, sqlbase.DatumTypeToColumnType(b.FixedReturnType()), nil
			}
		}
	}
	return nil, sqlbase.ColumnType{}, errors.Errorf(
		"no builtin aggregate for %s on %s", fn, inputType.Kind,
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
	flowCtx     *FlowCtx
	input       RowSource
	funcs       []*aggregateFuncHolder
	outputTypes []sqlbase.ColumnType
	datumAlloc  sqlbase.DatumAlloc
	acc         *mon.MemoryAccount

	groupCols columns
	inputCols columns
	buckets   map[string]struct{} // The set of bucket keys.

	out procOutputHelper
}

var _ processor = &aggregator{}

func newAggregator(
	flowCtx *FlowCtx,
	spec *AggregatorSpec,
	input RowSource,
	post *PostProcessSpec,
	output RowReceiver,
) (*aggregator, error) {
	ag := &aggregator{
		flowCtx:     flowCtx,
		input:       input,
		buckets:     make(map[string]struct{}),
		inputCols:   make(columns, len(spec.Aggregations)),
		funcs:       make([]*aggregateFuncHolder, len(spec.Aggregations)),
		outputTypes: make([]sqlbase.ColumnType, len(spec.Aggregations)),
		groupCols:   make(columns, len(spec.GroupCols)),
		acc:         &mon.MemoryAccount{},
	}
	flowCtx.evalCtx.Mon.OpenAccount(ag.acc)

	for i, aggInfo := range spec.Aggregations {
		ag.inputCols[i] = aggInfo.ColIdx
	}
	copy(ag.groupCols, spec.GroupCols)

	// Loop over the select expressions and extract any aggregate functions --
	// non-aggregation functions are replaced with parser.NewIdentAggregate,
	// (which just returns the last value added to them for a bucket) to provide
	// grouped-by values for each bucket.  ag.funcs is updated to contain all
	// the functions which need to be fed values.
	inputTypes := input.Types()
	for i, aggInfo := range spec.Aggregations {
		aggConstructor, retType, err := GetAggregateInfo(aggInfo.Func, inputTypes[aggInfo.ColIdx])
		if err != nil {
			return nil, err
		}

		ag.funcs[i] = ag.newAggregateFuncHolder(aggConstructor)
		if aggInfo.Distinct {
			ag.funcs[i].seen = make(map[string]struct{})
		}

		ag.outputTypes[i] = retType
	}
	if err := ag.out.init(post, ag.outputTypes, &flowCtx.evalCtx, output); err != nil {
		return nil, err
	}

	return ag, nil
}

// Run is part of the processor interface.
func (ag *aggregator) Run(ctx context.Context, wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}

	ctx = log.WithLogTag(ctx, "Agg", nil)
	ctx, span := tracing.ChildSpan(ctx, "aggregator")
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
			result := f.get(bucket)
			if result == nil {
				// Special case useful when this is a local stage of a distributed
				// aggregation.
				result = parser.DNull
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
		ag.out.close()
	}
}

// accumulateRows reads and accumulates all input rows.
// If no error is return, it means that all the rows from the input have been
// consumed.
// If an error is returned, both the input and the output have been properly
// closed, and the error has also been forwarded to the output.
func (ag *aggregator) accumulateRows(ctx context.Context) (err error) {
	cleanupRequired := true
	defer func() {
		if err != nil && cleanupRequired {
			DrainAndClose(ctx, ag.out.output, err, ag.input)
		}
	}()

	var scratch []byte
	for {
		row, meta := ag.input.Next()
		if !meta.Empty() {
			if meta.Err != nil {
				return meta.Err
			}
			if !emitHelper(ctx, &ag.out, nil /* row */, meta, ag.input) {
				// TODO(andrei): here, because we're passing metadata through, we have
				// an opportunity to find out that the consumer doesn't need the data
				// any more. If the producer doesn't push any metadata, then there's no
				// opportunity to find this out until the accumulation phase is done. We
				// should have a way to periodically peek at the state of the
				// RowReceiver that's hiding behind the procOutputHelper.
				cleanupRequired = false
				return errors.Errorf("consumer stopped before it received rows")
			}
			continue
		}
		if row == nil {
			return nil
		}

		// The encoding computed here determines which bucket the non-grouping
		// datums are accumulated to.
		encoded, err := ag.encode(scratch, row)
		if err != nil {
			return err
		}

		ag.buckets[string(encoded)] = struct{}{}
		// Feed the func holders for this bucket the non-grouping datums.
		for i, colIdx := range ag.inputCols {
			if err := row[colIdx].EnsureDecoded(&ag.datumAlloc); err != nil {
				return err
			}
			if err := ag.funcs[i].add(ctx, encoded, row[colIdx].Datum); err != nil {
				return err
			}
		}
		scratch = encoded[:0]
	}
}

type aggregateFuncHolder struct {
	create  func(*parser.EvalContext) parser.AggregateFunc
	group   *aggregator
	buckets map[string]parser.AggregateFunc
	seen    map[string]struct{}
}

func (ag *aggregator) newAggregateFuncHolder(
	create func(*parser.EvalContext) parser.AggregateFunc,
) *aggregateFuncHolder {
	return &aggregateFuncHolder{
		create:  create,
		group:   ag,
		buckets: make(map[string]parser.AggregateFunc),
	}
}

func (a *aggregateFuncHolder) add(ctx context.Context, bucket []byte, d parser.Datum) error {
	if a.seen != nil {
		encoded, err := sqlbase.EncodeDatum(bucket, d)
		if err != nil {
			return err
		}
		if _, ok := a.seen[string(encoded)]; ok {
			// skip
			return nil
		}
		a.seen[string(encoded)] = struct{}{}
	}

	impl, ok := a.buckets[string(bucket)]
	if !ok {
		impl = a.create(&a.group.flowCtx.evalCtx)
		a.buckets[string(bucket)] = impl
	}

	return impl.Add(ctx, d)
}

func (a *aggregateFuncHolder) get(bucket string) parser.Datum {
	found, ok := a.buckets[bucket]
	if !ok {
		found = a.create(&a.group.flowCtx.evalCtx)
	}

	return found.Result()
}

// encode returns the encoding for the grouping columns, this is then used as
// our group key to determine which bucket to add to.
func (ag *aggregator) encode(
	appendTo []byte, row sqlbase.EncDatumRow,
) (encoding []byte, err error) {
	for _, colIdx := range ag.groupCols {
		appendTo, err = row[colIdx].Encode(&ag.datumAlloc, sqlbase.DatumEncoding_VALUE, appendTo)
		if err != nil {
			return appendTo, err
		}
	}
	return appendTo, nil
}
