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

	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

type column uint32
type columns []column

// aggregator is the processor core type that does "aggregation" in the SQL sense. It groups rows
// and computes an aggregate for each group. The group is configured using the group key and the
// aggregator can be configured with one or more of the following aggregation functions:
//
//      SUM
//      COUNT
//      MIN
//      MAX
//      AVG
//      DISTINCT
//      COUNT DISTINCT
//
// aggregator's output schema is comprised of what is specified by the accompanying SELECT
// expressions.
type aggregator struct {
	inputs   []RowSource
	output   RowReceiver
	ctx      context.Context
	agValues *values
	funcs    []*aggregateFuncHolder

	groupKey  columns
	render    columns
	buckets   map[string]struct{} // The set of bucket keys.
	populated bool
}

func newAggregator(
	ctx *FlowCtx, spec *AggregatorSpec, inputs []RowSource, output RowReceiver,
) (*aggregator, error) {
	ag := &aggregator{
		inputs:   inputs,
		output:   output,
		ctx:      log.WithLogTag(ctx.Context, "Aggregator", nil),
		agValues: &values{},
	}

	ag.render = make(columns, len(spec.SelectExprs))
	for i, expr := range spec.SelectExprs {
		ag.render[i] = column(expr.ColIdx)
	}

	ag.groupKey = make(columns, len(spec.GroupKey))
	for i, colIdx := range spec.GroupKey {
		ag.groupKey[i] = column(colIdx)
	}

	// Loop over the select expressions and extract any aggregate functions -- non-aggregation
	// functions are replaced with parser.NewIdentAggregate, (which just returns the last value
	// added to them for a bucket) to provide grouped-by values for each bucket.
	// ag.funcs is updated to contain all the functions which need to be fed values.
	eh := &exprHelper{types: spec.Types}
	eh.vars = parser.MakeIndexedVarHelper(eh, len(eh.types))
	for _, expr := range spec.SelectExprs {
		fn, err := ag.extractFunc(expr, eh)
		if err != nil {
			return nil, err
		}
		ag.funcs = append(ag.funcs, fn)
	}

	ag.buckets = make(map[string]struct{})
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

	for {
		row, err := ag.next()
		if err != nil || row == nil {
			ag.output.Close(err)
			return
		}
		if log.V(3) {
			log.Infof(ag.ctx, "pushing %s\n", row)
		}
		// Push the row to the output RowReceiver; stop if they don't need more rows.
		if !ag.output.PushRow(row) {
			if log.V(2) {
				log.Infof(ag.ctx, "no more rows required")
			}
			ag.output.Close(nil)
			return
		}
	}
}

func (ag *aggregator) next() (sqlbase.EncDatumRow, error) {
	if ag.populated {
		return ag.agValues.Next()
	}

	var scratch []byte
	for _, input := range ag.inputs {
		for {
			row, err := input.NextRow()
			if err != nil {
				return nil, err
			}
			if row == nil {
				// No more rows from this input source.
				break
			}

			// By retrieving the values corresponding to the group key we determine which bucket
			// the values accumulated by evaluating the SELECT expressions are added to.
			aggregatedValues, groupedValues := ag.extract(row)
			encoded, err := sqlbase.EncodeDTuple(scratch, groupedValues)
			if err != nil {
				return nil, err
			}

			ag.buckets[string(encoded)] = struct{}{}
			// Feed the aggregateFuncHolders for this bucket the non-grouped values.
			for i, value := range aggregatedValues {
				if err := ag.funcs[i].add(encoded, value); err != nil {
					return nil, err
				}
			}
			scratch = encoded[:0]
		}
	}

	ag.populated = true
	if err := ag.computeAggregates(); err != nil {
		return nil, err
	}
	return ag.agValues.Next()
}

func (ag *aggregator) computeAggregates() error {
	// Render the results.
	for bucket := range ag.buckets {
		tuple := make(parser.DTuple, 0, len(ag.render))
		for _, f := range ag.funcs {
			datum := f.get(bucket)
			tuple = append(tuple, datum)
		}

		row, err := sqlbase.DTupleToEncDatumRow(tuple)
		if err != nil {
			return err
		}
		if err := ag.agValues.Add(row); err != nil {
			return err
		}
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

// extract returns a tuple comprised of the indexes specified by render and the group key.
func (ag *aggregator) extract(row sqlbase.EncDatumRow) (parser.DTuple, parser.DTuple) {
	tuple := make(parser.DTuple, len(ag.render)+len(ag.groupKey))
	for i, colIdx := range ag.render {
		tuple[i] = row[colIdx].Datum
	}

	for i, colIdx := range ag.groupKey {
		tuple[i+len(ag.render)] = row[colIdx].Datum
	}

	return tuple[:len(ag.render)], tuple[len(ag.render):]
}

// extractFunc returns an aggregateFuncHolder for a given SelectExpr specifying an aggregation
// function.
func (ag *aggregator) extractFunc(
	expr AggregatorSpec_SelectExpr, eh *exprHelper,
) (*aggregateFuncHolder, error) {
	if expr.Func == AggregatorSpec_IDENT {
		fn := ag.newAggregateFuncHolder(parser.NewIdentAggregate)
		return fn, nil
	}

	p := &parser.FuncExpr{
		Name: parser.NormalizableFunctionName{
			FunctionName: &parser.QualifiedFunctionName{
				FunctionName: parser.Name(AggregatorSpec_Func_name[int32(expr.Func)]),
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
