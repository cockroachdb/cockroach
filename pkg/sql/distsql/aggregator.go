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

package distsql

import (
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

type columns []uint32

// aggregator is the processor core type that does "aggregation" in the SQL
// sense. It groups rows and computes an aggregate for each group. The group is
// configured using the group key and the aggregator can be configured with one
// or more of the following aggregation functions:
//
//      SUM
//      COUNT
//      MIN
//      MAX
//      AVG
//      DISTINCT
//      COUNT DISTINCT
//
// aggregator's output schema is comprised of what is specified by the
// accompanying SELECT expressions.
type aggregator struct {
	input      RowSource
	output     RowReceiver
	ctx        context.Context
	rows       *RowBuffer
	funcs      []*aggregateFuncHolder
	datumAlloc sqlbase.DatumAlloc
	rowAlloc   sqlbase.EncDatumRowAlloc
	tuple      parser.DTuple

	groupCols columns
	inputCols columns
	buckets   map[string]struct{} // The set of bucket keys.
}

func newAggregator(
	ctx *FlowCtx, spec *AggregatorSpec, input RowSource, output RowReceiver,
) (*aggregator, error) {
	ag := &aggregator{
		input:     input,
		output:    output,
		ctx:       log.WithLogTag(ctx.Context, "Agg", nil),
		rows:      &RowBuffer{},
		tuple:     make(parser.DTuple, len(spec.Exprs)+len(spec.GroupCols)),
		buckets:   make(map[string]struct{}),
		inputCols: make(columns, len(spec.Exprs)),
		groupCols: make(columns, len(spec.GroupCols)),
	}

	for i, expr := range spec.Exprs {
		ag.inputCols[i] = expr.ColIdx
	}
	copy(ag.groupCols, spec.GroupCols)

	// Loop over the select expressions and extract any aggregate functions --
	// non-aggregation functions are replaced with parser.NewIdentAggregate,
	// (which just returns the last value added to them for a bucket) to provide
	// grouped-by values for each bucket.  ag.funcs is updated to contain all
	// the functions which need to be fed values.
	eh := &exprHelper{types: spec.Types}
	eh.vars = parser.MakeIndexedVarHelper(eh, len(eh.types))
	for _, expr := range spec.Exprs {
		fn, err := ag.extractFunc(expr, eh)
		if err != nil {
			return nil, err
		}
		ag.funcs = append(ag.funcs, fn)
	}

	return ag, nil
}

// Run is part of the processor interface.
func (ag *aggregator) Run(wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}

	if log.V(2) {
		log.Infof(ag.ctx, "starting aggregation process")
		defer log.Infof(ag.ctx, "exiting aggregator")
	}

	if err := ag.accumulateRows(); err != nil {
		ag.output.Close(err)
		return
	}
	if err := ag.computeAggregates(); err != nil {
		ag.output.Close(err)
		return
	}

	for {
		row, err := ag.rows.NextRow()
		if err != nil || row == nil {
			ag.output.Close(err)
			return
		}
		if log.V(3) {
			log.Infof(ag.ctx, "pushing %s\n", row)
		}
		// Push the row to the output RowReceiver; stop if they don't need more
		// rows.
		if !ag.output.PushRow(row) {
			if log.V(2) {
				log.Infof(ag.ctx, "no more rows required")
			}
			ag.output.Close(nil)
			return
		}
	}
}

func (ag *aggregator) accumulateRows() error {
	var scratch []byte
	for {
		row, err := ag.input.NextRow()
		if err != nil {
			return err
		}
		if row == nil {
			return nil
		}

		// By retrieving the values corresponding to the group key we
		// determine which bucket the values accumulated by evaluating the
		// SELECT expressions are added to.
		aggregatedValues, groupedValues, err := ag.extract(row)
		if err != nil {
			return err
		}

		// TODO(irfansharif): EncDatum types have an Encode method we should be
		// using instead of decoding, extracting tuples then decoding.
		encoded, err := sqlbase.EncodeDTuple(scratch, groupedValues)
		if err != nil {
			return err
		}

		ag.buckets[string(encoded)] = struct{}{}
		// Feed the aggregateFuncHolders for this bucket the non-grouped
		// values.
		for i, value := range aggregatedValues {
			if err := ag.funcs[i].add(encoded, value); err != nil {
				return err
			}
		}
		scratch = encoded[:0]
	}
}

func (ag *aggregator) computeAggregates() error {
	// Render the results.
	tuple := make(parser.DTuple, 0, len(ag.funcs))
	for bucket := range ag.buckets {
		for _, f := range ag.funcs {
			datum := f.get(bucket)
			tuple = append(tuple, datum)
		}

		row := ag.rowAlloc.AllocRow(len(tuple))
		err := sqlbase.DTupleToEncDatumRow(row, tuple)
		if err != nil {
			return err
		}

		if ok := ag.rows.PushRow(row); !ok {
			return errors.Errorf("unable to add row %s", row)
		}
		tuple = tuple[:0]
	}
	return nil
}

type aggregateFuncHolder struct {
	create  func() parser.AggregateFunc
	group   *aggregator
	buckets map[string]parser.AggregateFunc
	seen    map[string]struct{}
}

func (ag *aggregator) newAggregateFuncHolder(
	create func() parser.AggregateFunc,
) *aggregateFuncHolder {
	return &aggregateFuncHolder{
		create:  create,
		group:   ag,
		buckets: make(map[string]parser.AggregateFunc),
	}
}

func (a *aggregateFuncHolder) add(bucket []byte, d parser.Datum) error {
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
		impl = a.create()
		a.buckets[string(bucket)] = impl
	}

	impl.Add(d)
	return nil
}

func (a *aggregateFuncHolder) get(bucket string) parser.Datum {
	found, ok := a.buckets[bucket]
	if !ok {
		found = a.create()
	}

	return found.Result()
}

// extract returns a tuple comprised of the indexes specified by inputCols and
// groupKey.
func (ag *aggregator) extract(row sqlbase.EncDatumRow) (parser.DTuple, parser.DTuple, error) {
	for i, colIdx := range ag.inputCols {
		if err := row[colIdx].Decode(&ag.datumAlloc); err != nil {
			return nil, nil, err
		}
		ag.tuple[i] = row[colIdx].Datum
	}

	for i, colIdx := range ag.groupCols {
		if err := row[colIdx].Decode(&ag.datumAlloc); err != nil {
			return nil, nil, err
		}
		ag.tuple[i+len(ag.inputCols)] = row[colIdx].Datum
	}

	return ag.tuple[:len(ag.inputCols)], ag.tuple[len(ag.inputCols):], nil
}

// extractFunc returns an aggregateFuncHolder for a given SelectExpr specifying
// an aggregation function.
func (ag *aggregator) extractFunc(
	expr AggregatorSpec_Expr, eh *exprHelper,
) (*aggregateFuncHolder, error) {
	if expr.Func == AggregatorSpec_IDENT {
		fn := ag.newAggregateFuncHolder(parser.NewIdentAggregate)
		return fn, nil
	}

	// In order to reuse the aggregate functions as defined in the parser
	// package we are relying on the fact the each name defined in the Func enum
	// within the AggregatorSpec matches a SQL function name known to the parser.
	// See pkg/sql/parser/aggregate_builtins.go for the aggregate builtins we
	// are repurposing.
	p := &parser.FuncExpr{
		Name: parser.NormalizableFunctionName{
			FunctionName: &parser.QualifiedFunctionName{
				FunctionName: parser.Name(expr.Func.String()),
			},
		},
		Exprs: []parser.Expr{eh.indexToExpr(int(expr.ColIdx))},
	}

	_, err := p.TypeCheck(nil, parser.NoTypePreference)
	if err != nil {
		return nil, err
	}
	if agg := p.GetAggregateConstructor(); agg != nil {
		fn := ag.newAggregateFuncHolder(agg)
		if expr.Distinct {
			fn.seen = make(map[string]struct{})
		}
		return fn, nil
	}
	return nil, errors.Errorf("unable to get aggregate constructor for %s",
		AggregatorSpec_Func_name[int32(expr.Func)])
}
