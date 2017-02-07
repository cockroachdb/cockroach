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
// Author: Matt Jibson (mjibson@cockroachlabs.com)

package sql

import (
	"bytes"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// returningHelper implements the logic used for statements with RETURNING clauses. It accumulates
// result rows, one for each call to append().
type returningHelper struct {
	p *planner
	// Expected columns.
	columns ResultColumns
	// Processed copies of expressions from ReturningExprs.
	exprs        []parser.TypedExpr
	rowCount     int
	source       *dataSourceInfo
	curSourceRow parser.Datums

	// This struct must be allocated on the heap and its location stay
	// stable after construction because it implements
	// IndexedVarContainer and the IndexedVar objects in sub-expressions
	// will link to it by reference after checkRenderStar / analyzeExpr.
	// Enforce this using NoCopy.
	noCopy util.NoCopy
}

// newReturningHelper creates a new returningHelper for use by an
// insert/update node.
func (p *planner) newReturningHelper(
	r parser.ReturningExprs,
	desiredTypes []parser.Type,
	alias string,
	tablecols []sqlbase.ColumnDescriptor,
) (*returningHelper, error) {
	rh := &returningHelper{
		p: p,
	}
	if len(r) == 0 {
		return rh, nil
	}

	for _, e := range r {
		if err := p.parser.AssertNoAggregationOrWindowing(
			e.Expr, "RETURNING", p.session.SearchPath,
		); err != nil {
			return nil, err
		}
	}

	rh.columns = make(ResultColumns, 0, len(r))
	aliasTableName := parser.TableName{TableName: parser.Name(alias)}
	rh.source = newSourceInfoForSingleTable(aliasTableName, makeResultColumns(tablecols))
	rh.exprs = make([]parser.TypedExpr, 0, len(r))
	ivarHelper := parser.MakeIndexedVarHelper(rh, len(tablecols))
	for _, target := range r {
		cols, typedExprs, _, err := p.computeRender(target, parser.TypeAny, rh.source, ivarHelper, true)
		if err != nil {
			return nil, err
		}
		rh.columns = append(rh.columns, cols...)
		rh.exprs = append(rh.exprs, typedExprs...)
	}
	return rh, nil
}

// cookResultRow prepares a row according to the ReturningExprs, with input values
// from rowVals.
func (rh *returningHelper) cookResultRow(rowVals parser.Datums) (parser.Datums, error) {
	if rh.exprs == nil {
		rh.rowCount++
		return rowVals, nil
	}
	rh.curSourceRow = rowVals
	resRow := make(parser.Datums, len(rh.exprs))
	for i, e := range rh.exprs {
		d, err := e.Eval(&rh.p.evalCtx)
		if err != nil {
			return nil, err
		}
		resRow[i] = d
	}
	return resRow, nil
}

// IndexedVarEval implements the parser.IndexedVarContainer interface.
func (rh *returningHelper) IndexedVarEval(idx int, ctx *parser.EvalContext) (parser.Datum, error) {
	return rh.curSourceRow[idx].Eval(ctx)
}

// IndexedVarResolvedType implements the parser.IndexedVarContainer interface.
func (rh *returningHelper) IndexedVarResolvedType(idx int) parser.Type {
	return rh.source.sourceColumns[idx].Typ
}

// IndexedVarFormat implements the parser.IndexedVarContainer interface.
func (rh *returningHelper) IndexedVarFormat(buf *bytes.Buffer, f parser.FmtFlags, idx int) {
	rh.source.FormatVar(buf, f, idx)
}
