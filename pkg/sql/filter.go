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
// Author: Raphael 'kena' Poss (knz@cockroachlabs.com)

package sql

import (
	"bytes"
	"fmt"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// filterNode implements a filtering stage. It is intended to be used
// during plan optimizations in order to avoid instantiating a fully
// blown selectTopNode/renderNode pair.
type filterNode struct {
	p          *planner
	source     planDataSource
	filter     parser.TypedExpr
	ivarHelper parser.IndexedVarHelper
	// support attributes for EXPLAIN(DEBUG)
	explain   explainMode
	debugVals debugValues
}

// IndexedVarEval implements the parser.IndexedVarContainer interface.
func (f *filterNode) IndexedVarEval(idx int, ctx *parser.EvalContext) (parser.Datum, error) {
	return f.source.plan.Values()[idx].Eval(ctx)
}

// IndexedVarResolvedType implements the parser.IndexedVarContainer interface.
func (f *filterNode) IndexedVarResolvedType(idx int) parser.Type {
	return f.source.info.sourceColumns[idx].Typ
}

// IndexedVarFormat implements the parser.IndexedVarContainer interface.
func (f *filterNode) IndexedVarFormat(buf *bytes.Buffer, fl parser.FmtFlags, idx int) {
	f.source.info.FormatVar(buf, fl, idx)
}

// Start implements the planNode interface.
func (f *filterNode) Start(ctx context.Context) error {
	return f.source.plan.Start(ctx)
}

// Next implements the planNode interface.
func (f *filterNode) Next(ctx context.Context) (bool, error) {
	for {
		if next, err := f.source.plan.Next(ctx); !next {
			return false, err
		}

		if f.explain == explainDebug {
			f.debugVals = f.source.plan.DebugValues()

			if f.debugVals.output != debugValueRow {
				// Let the debug values pass through.
				return true, nil
			}
		}

		passesFilter, err := sqlbase.RunFilter(f.filter, &f.p.evalCtx)
		if err != nil {
			return false, err
		}

		if passesFilter {
			return true, nil
		} else if f.explain == explainDebug {
			// Mark the row as filtered out.
			f.debugVals.output = debugValueFiltered
			return true, nil
		}
		// Row was filtered out; grab the next row.
	}
}

func (f *filterNode) MarkDebug(mode explainMode) {
	if mode != explainDebug {
		panic(fmt.Sprintf("unknown debug mode %d", mode))
	}
	f.explain = mode
	f.source.plan.MarkDebug(mode)
}

func (f *filterNode) DebugValues() debugValues {
	if f.explain != explainDebug {
		panic(fmt.Sprintf("node not in debug mode (mode %d)", f.explain))
	}
	return f.debugVals
}

func (f *filterNode) Close(ctx context.Context) {
	f.source.plan.Close(ctx)
}
func (f *filterNode) Values() parser.Datums  { return f.source.plan.Values() }
func (f *filterNode) Columns() ResultColumns { return f.source.plan.Columns() }
func (f *filterNode) Ordering() orderingInfo { return f.source.plan.Ordering() }

func (f *filterNode) Spans(ctx context.Context) (_, _ roachpb.Spans, _ error) {
	return f.source.plan.Spans(ctx)
}
