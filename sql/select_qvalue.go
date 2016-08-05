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
// Author: Radu Berinde (radu@cockroachlabs.com)
//
// This file implements the select code that deals with column references
// and resolving column names in expressions.

package sql

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/sql/parser"
)

// columnRef is a reference to a resultColumn of a FROM node
type columnRef struct {
	source *dataSourceInfo

	// Index of column (in from.columns).
	colIdx int
}

const invalidColIdx = -1

// get dereferences the columnRef to the resultColumn.
func (cr columnRef) get() ResultColumn {
	return cr.source.sourceColumns[cr.colIdx]
}

type qvalResolver struct {
	sources multiSourceInfo
	qvals   qvalMap
}

// qvalue implements the parser.VariableExpr interface and is used as a
// replacement node for VarNames in expressions that can change their
// values for each row. Since it is a reference, expression walking can
// discover the qvalues and the columns they refer to.
type qvalue struct {
	datum  parser.Datum
	colRef columnRef
}

type qvalMap map[columnRef]*qvalue

var _ parser.TypedExpr = &qvalue{}
var _ parser.VariableExpr = &qvalue{}

// Variable implements the VariableExpr interface.
func (*qvalue) Variable() {}

func (q *qvalue) Format(buf *bytes.Buffer, f parser.FmtFlags) {
	if f == parser.FmtQualify {
		tableAlias := q.colRef.source.findTableAlias(q.colRef.colIdx)
		if tableAlias.TableName != "" {
			if tableAlias.DatabaseName != "" {
				parser.FormatNode(buf, f, tableAlias.DatabaseName)
				buf.WriteByte('.')
			}
			parser.FormatNode(buf, f, tableAlias.TableName)
			buf.WriteByte('.')
		}
	}
	buf.WriteString(q.colRef.get().Name)
}
func (q *qvalue) String() string { return parser.AsString(q) }

// Walk implements the Expr interface.
func (q *qvalue) Walk(v parser.Visitor) parser.Expr {
	if e, changed := parser.WalkExpr(v, q.datum); changed {
		return &qvalue{datum: e.(parser.Datum), colRef: q.colRef}
	}
	return q
}

// TypeCheck implements the Expr interface.
func (q *qvalue) TypeCheck(_ *parser.SemaContext, desired parser.Datum) (parser.TypedExpr, error) {
	return q, nil
}

// Eval implements the TypedExpr interface.
func (q *qvalue) Eval(ctx *parser.EvalContext) (parser.Datum, error) {
	return q.datum.Eval(ctx)
}

// ReturnType implements the TypedExpr interface.
func (q *qvalue) ReturnType() parser.Datum {
	return q.datum.ReturnType()
}

// getQVal creates a qvalue for a column reference. Created qvalues are
// stored in the qvals map. If a qvalue was previously created for the same
// reference, the existing qvalue is returned.
func (q qvalMap) getQVal(colRef columnRef) *qvalue {
	qval := q[colRef]
	if qval == nil {
		col := colRef.get()
		// We initialize the qvalue expression to a datum of the type matching the
		// column. This allows type analysis to be performed on the expression
		// before we start retrieving rows.
		//
		// TODO(pmattis): Nullable columns can have NULL values. The type analysis
		// needs to take that into consideration, but how to surface that info?
		qval = &qvalue{colRef: colRef, datum: col.Typ}
		q[colRef] = qval
	}
	return qval
}

// nameResolutionVisitor is a parser.Visitor implementation used to
// resolve the column names in an expression.
type nameResolutionVisitor struct {
	qt  qvalResolver
	err error
}

var _ parser.Visitor = &nameResolutionVisitor{}

func (v *nameResolutionVisitor) VisitPre(expr parser.Expr) (recurse bool, newNode parser.Expr) {
	if v.err != nil {
		return false, expr
	}

	switch t := expr.(type) {
	case *qvalue:
		// We allow resolving qvalues on expressions that have already been resolved by this
		// resolver. This is used in some cases when adding render targets for grouping or sorting.
		if v.qt.qvals[t.colRef] != t {
			panic(fmt.Sprintf("qvalue already resolved with different resolver (name: %s)", t))
		}
		return true, expr

	case parser.UnresolvedName:
		vn, err := t.NormalizeVarName()
		if err != nil {
			v.err = err
			return false, expr
		}
		return v.VisitPre(vn)

	case *parser.ColumnItem:
		var colRef columnRef

		colRef.source, colRef.colIdx, v.err = v.qt.sources.findColumn(t)
		if v.err != nil {
			return false, expr
		}
		return true, v.qt.qvals.getQVal(colRef)

	case *parser.FuncExpr:
		// Check for invalid use of *, which, if it is an argument, is the only argument.
		if len(t.Exprs) != 1 {
			break
		}
		vn, ok := t.Exprs[0].(parser.VarName)
		if !ok {
			break
		}
		vn, v.err = vn.NormalizeVarName()
		if v.err != nil {
			return false, expr
		}
		// Save back to avoid re-doing the work later.
		t.Exprs[0] = vn

		fn, err := t.Name.Normalize()
		if err != nil {
			v.err = err
			return false, expr
		}

		if strings.EqualFold(fn.Function(), "count") {
			// Special case handling for COUNT(*). This is a special construct to
			// count the number of rows; in this case * does NOT refer to a set of
			// columns. A * is invalid elsewhere (and will be caught by TypeCheck()).
			// Replace the function argument with a special non-NULL VariableExpr.
			switch arg := vn.(type) {
			case parser.UnqualifiedStar:
				// Replace, see below.
			case *parser.AllColumnsSelector:
				// Replace, see below.
				// However we must first properly reject SELECT COUNT(foo.*) FROM bar.
				if _, err := v.qt.sources.checkDatabaseName(arg.TableName); err != nil {
					v.err = err
					return false, expr
				}
			default:
				// Nothing to do, recurse.
				return true, expr
			}

			t = t.CopyNode()
			t.Exprs[0] = starDatumInstance
			return false, t
		}
		return true, t

	case *parser.Subquery:
		// Do not recurse into subqueries.
		return false, expr
	}

	return true, expr
}

func (*nameResolutionVisitor) VisitPost(expr parser.Expr) parser.Expr { return expr }

func (s *selectNode) resolveNames(expr parser.Expr) (parser.Expr, error) {
	var v *nameResolutionVisitor
	if s.planner != nil {
		v = &s.planner.nameResolutionVisitor
	}
	return resolveNames(expr, s.sourceInfo, s.qvals, v)
}

// resolveNames walks the provided expression and resolves all names
// using the tableInfo and qvalMap. The function takes an optional
// nameResolutionVisitor to provide the caller the option of avoiding an
// allocation.
func resolveNames(
	expr parser.Expr, sources multiSourceInfo, qvals qvalMap, v *nameResolutionVisitor,
) (parser.Expr, error) {
	if expr == nil {
		return expr, nil
	}
	if v == nil {
		v = new(nameResolutionVisitor)
	}
	*v = nameResolutionVisitor{
		qt: qvalResolver{
			sources: sources,
			qvals:   qvals,
		},
	}
	expr, _ = parser.WalkExpr(v, expr)
	return expr, v.err
}

// Populates one table's datum fields in the qval map given a row of values.
func (q qvalMap) populateQVals(source *dataSourceInfo, row parser.DTuple) {
	for ref, qval := range q {
		if ref.source == source {
			qval.datum = row[ref.colIdx]
			if qval.datum == nil {
				panic(fmt.Sprintf("Unpopulated value for column %d", ref.colIdx))
			}
		}
	}
}

// starDatum is a VariableExpr implementation used as a dummy argument for the
// special case COUNT(*).  This ends up being processed correctly by the count
// aggregator since it is not parser.DNull.
//
// We need to implement enough functionality to satisfy the type checker and to
// allow the intermediate rendering of the value (before the group
// aggregation).
type starDatum struct{}

var starDatumInstance = &starDatum{}
var _ parser.TypedExpr = starDatumInstance
var _ parser.VariableExpr = starDatumInstance

// Variable implements the VariableExpr interface.
func (*starDatum) Variable() {}

func (*starDatum) Format(buf *bytes.Buffer, f parser.FmtFlags) {
	buf.WriteByte('*')
}
func (s *starDatum) String() string { return parser.AsString(s) }

// Walk implements the Expr interface.
func (s *starDatum) Walk(v parser.Visitor) parser.Expr { return s }

// TypeCheck implements the Expr interface.
func (s *starDatum) TypeCheck(_ *parser.SemaContext, desired parser.Datum) (parser.TypedExpr, error) {
	return s, nil
}

// Eval implements the TypedExpr interface.
func (*starDatum) Eval(ctx *parser.EvalContext) (parser.Datum, error) {
	return parser.TypeInt.Eval(ctx)
}

// ReturnType implements the TypedExpr interface.
func (*starDatum) ReturnType() parser.Datum {
	return parser.TypeInt.ReturnType()
}
