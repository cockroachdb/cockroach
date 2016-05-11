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
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
)

// returningHelper implements the logic used for statements with RETURNING clauses. It accumulates
// result rows, one for each call to append().
type returningHelper struct {
	p *planner
	// Expected columns.
	columns []ResultColumn
	// Processed copies of expressions from ReturningExprs.
	untypedExprs parser.Exprs
	exprs        []parser.TypedExpr
	qvals        qvalMap
	rowCount     int
	desiredTypes []parser.Datum
	table        *tableInfo
}

func (p *planner) makeReturningHelper(
	r parser.ReturningExprs,
	desiredTypes []parser.Datum,
	alias string,
	tablecols []sqlbase.ColumnDescriptor,
) (returningHelper, error) {
	rh := returningHelper{
		p:            p,
		desiredTypes: desiredTypes,
	}
	if len(r) == 0 {
		return rh, nil
	}

	rh.columns = make([]ResultColumn, 0, len(r))
	rh.table = &tableInfo{
		columns: makeResultColumns(tablecols),
		alias:   alias,
	}
	rh.qvals = make(qvalMap)
	rh.untypedExprs = make([]parser.Expr, 0, len(r))
	for _, target := range r {
		if isStar, cols, exprs, err := checkRenderStar(target, rh.table, rh.qvals); err != nil {
			return returningHelper{}, err
		} else if isStar {
			for _, expr := range exprs {
				rh.untypedExprs = append(rh.untypedExprs, expr)
			}
			rh.columns = append(rh.columns, cols...)
			continue
		}

		// When generating an output column name it should exactly match the original
		// expression, so determine the output column name before we perform any
		// manipulations to the expression.
		outputName := getRenderColName(target)

		expr, err := resolveQNames(target.Expr, []*tableInfo{rh.table}, rh.qvals, &p.qnameVisitor)
		if err != nil {
			return returningHelper{}, err
		}
		rh.untypedExprs = append(rh.untypedExprs, expr)
		rh.columns = append(rh.columns, ResultColumn{Name: outputName})
	}
	rh.exprs = make([]parser.TypedExpr, len(rh.untypedExprs))
	return rh, nil
}

// cookResultRow prepares a row according to the ReturningExprs, with input values
// from rowVals.
func (rh *returningHelper) cookResultRow(rowVals parser.DTuple) (parser.DTuple, error) {
	if rh.exprs == nil {
		rh.rowCount++
		return rowVals, nil
	}
	rh.qvals.populateQVals(rh.table, rowVals)
	resRow := make(parser.DTuple, len(rh.exprs))
	for i, e := range rh.exprs {
		d, err := e.Eval(rh.p.evalCtx)
		if err != nil {
			return nil, err
		}
		resRow[i] = d
	}
	return resRow, nil
}

// TypeCheck ensures that the expressions mentioned in the
// returningHelper have the right type.
// TODO(knz): this both annotates the type of placeholders
// (a task for prepare) and controls that provided values
// for placeholders match their context (a task for exec). This
// ought to be split into two phases.
func (rh *returningHelper) TypeCheck() error {
	for i, expr := range rh.untypedExprs {
		desired := parser.NoTypePreference
		if len(rh.desiredTypes) > i {
			desired = rh.desiredTypes[i]
		}
		typedExpr, err := parser.TypeCheck(expr, rh.p.evalCtx.Args, desired)
		if err != nil {
			return err
		}
		typedExpr, err = rh.p.parser.NormalizeExpr(rh.p.evalCtx, typedExpr)
		if err != nil {
			return err
		}
		rh.exprs[i] = typedExpr
		rh.columns[i].Typ = typedExpr.ReturnType()
	}
	return nil
}
