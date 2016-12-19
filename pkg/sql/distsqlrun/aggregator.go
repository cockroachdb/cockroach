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
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

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
	input       RowSource
	output      RowReceiver
	ctx         context.Context
	rows        *RowBuffer
	funcs       []*aggregateFuncHolder
	outputTypes []*sqlbase.ColumnType
	datumAlloc  sqlbase.DatumAlloc
	rowAlloc    sqlbase.EncDatumRowAlloc

	groupCols columns
	inputCols columns
	buckets   map[string]struct{} // The set of bucket keys.
}

func newAggregator(
	ctx *FlowCtx, spec *AggregatorSpec, input RowSource, output RowReceiver,
) (*aggregator, error) {
	ag := &aggregator{
		input:       input,
		output:      output,
		ctx:         log.WithLogTag(ctx.Context, "Agg", nil),
		rows:        &RowBuffer{},
		buckets:     make(map[string]struct{}),
		inputCols:   make(columns, len(spec.Exprs)),
		outputTypes: make([]*sqlbase.ColumnType, len(spec.Exprs)),
		groupCols:   make(columns, len(spec.GroupCols)),
	}

	inputTypes := make([]*sqlbase.ColumnType, len(spec.Exprs))
	for i, expr := range spec.Exprs {
		ag.inputCols[i] = expr.ColIdx
		inputTypes[i] = spec.Types[expr.ColIdx]
	}
	copy(ag.groupCols, spec.GroupCols)

	// Loop over the select expressions and extract any aggregate functions --
	// non-aggregation functions are replaced with parser.NewIdentAggregate,
	// (which just returns the last value added to them for a bucket) to provide
	// grouped-by values for each bucket.  ag.funcs is updated to contain all
	// the functions which need to be fed values.
	eh := &exprHelper{types: inputTypes}
	eh.vars = parser.MakeIndexedVarHelper(eh, len(eh.types))
	for i, expr := range spec.Exprs {
		fn, retType, err := ag.extractFunc(expr, eh)
		if err != nil {
			return nil, err
		}
		ag.funcs = append(ag.funcs, fn)

		// The aggregate function extracted is an identity function, the return
		// type of this therefore being the i-th input type.
		if retType == nil {
			ag.outputTypes[i] = inputTypes[i]
		} else {
			typ := sqlbase.DatumTypeToColumnType(retType)
			ag.outputTypes[i] = &typ
		}
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
			if err := ag.funcs[i].add(encoded, row[colIdx].Datum); err != nil {
				return err
			}
		}
		scratch = encoded[:0]
	}
}

func (ag *aggregator) computeAggregates() error {
	// Queries like `SELECT MAX(n) FROM t` expect a row of NULLs if nothing was
	// aggregated.
	if len(ag.buckets) < 1 && len(ag.groupCols) == 0 {
		ag.buckets[""] = struct{}{}
	}

	// Render the results.
	tuple := make(parser.DTuple, 0, len(ag.funcs))
	for bucket := range ag.buckets {
		for _, f := range ag.funcs {
			datum := f.get(bucket)
			tuple = append(tuple, datum)
		}

		row := ag.rowAlloc.AllocRow(len(tuple))
		sqlbase.DTupleToEncDatumRow(row, ag.outputTypes, tuple)
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

// encode returns the encoding for the grouping columns, this is then used as
// our group key to determine which bucket to add to.
func (ag *aggregator) encode(appendTo []byte, row sqlbase.EncDatumRow) (encoding []byte, err error) {
	for _, colIdx := range ag.groupCols {
		appendTo, err = row[colIdx].Encode(&ag.datumAlloc, sqlbase.DatumEncoding_VALUE, appendTo)
		if err != nil {
			return appendTo, err
		}
	}
	return appendTo, nil
}

// extractFunc returns an aggregateFuncHolder for a given SelectExpr specifying
// an aggregation function, along with the return type of the function. If the
// return type is nil (provided error != nil) we are returning an identity
// aggregation function, the return type of which is whatever is fed into the
// function itself.
func (ag *aggregator) extractFunc(
	expr AggregatorSpec_Expr, eh *exprHelper,
) (*aggregateFuncHolder, parser.Type, error) {
	if expr.Func == AggregatorSpec_IDENT {
		fn := ag.newAggregateFuncHolder(parser.NewIdentAggregate)
		return fn, nil, nil
	}

	// In order to reuse the aggregate functions as defined in the parser
	// package we are relying on the fact the each name defined in the Func enum
	// within the AggregatorSpec matches a SQL function name known to the parser.
	// See pkg/sql/parser/aggregate_builtins.go for the aggregate builtins we
	// are repurposing.
	p := &parser.FuncExpr{
		Func: parser.ResolvableFunctionReference{
			FunctionReference: parser.UnresolvedName{parser.Name(expr.Func.String())},
		},
		Exprs: []parser.Expr{eh.vars.IndexedVar(int(expr.ColIdx))},
	}

	t, err := p.TypeCheck(nil, parser.TypeAny)
	if err != nil {
		return nil, nil, err
	}
	if agg := p.GetAggregateConstructor(); agg != nil {
		fn := ag.newAggregateFuncHolder(agg)
		if expr.Distinct {
			fn.seen = make(map[string]struct{})
		}
		return fn, t.ResolvedType(), nil
	}
	return nil, nil, errors.Errorf("unable to get aggregate constructor for %s",
		AggregatorSpec_Func_name[int32(expr.Func)])
}
