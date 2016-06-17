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
	"fmt"

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
	exprs    []parser.TypedExpr
	qvals    qvalMap
	rowCount int
	source   *dataSourceInfo
}

func (p *planner) makeReturningHelper(
	r parser.ReturningExprs,
	desiredTypes []parser.Datum,
	alias string,
	tablecols []sqlbase.ColumnDescriptor,
) (returningHelper, error) {
	rh := returningHelper{
		p: p,
	}
	if len(r) == 0 {
		return rh, nil
	}

	for _, e := range r {
		if p.parser.AggregateInExpr(e.Expr) {
			return rh, fmt.Errorf("aggregate functions are not allowed in RETURNING")
		}
	}

	rh.columns = make([]ResultColumn, 0, len(r))
	rh.source = newSourceInfoForSingleTable(alias, makeResultColumns(tablecols))
	rh.qvals = make(qvalMap)
	rh.exprs = make([]parser.TypedExpr, 0, len(r))
	for i, target := range r {
		if isStar, cols, typedExprs, err := checkRenderStar(target, rh.source, rh.qvals); err != nil {
			return returningHelper{}, err
		} else if isStar {
			rh.exprs = append(rh.exprs, typedExprs...)
			rh.columns = append(rh.columns, cols...)
			continue
		}

		// When generating an output column name it should exactly match the original
		// expression, so determine the output column name before we perform any
		// manipulations to the expression.
		outputName := getRenderColName(target)

		desired := parser.NoTypePreference
		if len(desiredTypes) > i {
			desired = desiredTypes[i]
		}

		typedExpr, err := rh.p.analyzeExpr(target.Expr, multiSourceInfo{rh.source}, rh.qvals, desired, false, "")
		if err != nil {
			return returningHelper{}, err
		}
		rh.exprs = append(rh.exprs, typedExpr)
		rh.columns = append(rh.columns, ResultColumn{Name: outputName, Typ: typedExpr.ReturnType()})
	}
	return rh, nil
}

// cookResultRow prepares a row according to the ReturningExprs, with input values
// from rowVals.
func (rh *returningHelper) cookResultRow(rowVals parser.DTuple) (parser.DTuple, error) {
	if rh.exprs == nil {
		rh.rowCount++
		return rowVals, nil
	}
	rh.qvals.populateQVals(rh.source, rowVals)
	resRow := make(parser.DTuple, len(rh.exprs))
	for i, e := range rh.exprs {
		d, err := e.Eval(&rh.p.evalCtx)
		if err != nil {
			return nil, err
		}
		resRow[i] = d
	}
	return resRow, nil
}

func (rh *returningHelper) expandPlans() error {
	for _, expr := range rh.exprs {
		if err := rh.p.expandSubqueryPlans(expr); err != nil {
			return err
		}
	}
	return nil
}

func (rh *returningHelper) startPlans() error {
	for _, expr := range rh.exprs {
		if err := rh.p.startSubqueryPlans(expr); err != nil {
			return err
		}
	}
	return nil
}
