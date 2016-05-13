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
	"github.com/cockroachdb/cockroach/sql/sqlbase"
)

// columnRef is a reference to a resultColumn of a FROM node
type columnRef struct {
	table *tableInfo

	// Index of column (in from.columns).
	colIdx int
}

const invalidColIdx = -1

// get dereferences the columnRef to the resultColumn.
func (cr columnRef) get() ResultColumn {
	return cr.table.columns[cr.colIdx]
}

type qvalResolver struct {
	tables []*tableInfo
	qvals  qvalMap
}

// findColumn looks up the column described by a QualifiedName. The qname will be normalized.
func (qt qvalResolver) findColumn(qname *parser.QualifiedName) (columnRef, error) {

	ref := columnRef{colIdx: invalidColIdx}

	if err := qname.NormalizeColumnName(); err != nil {
		return ref, err
	}

	// We can't resolve stars to a single column.
	if qname.IsStar() {
		err := fmt.Errorf("qualified name \"%s\" not found", qname)
		return ref, err
	}

	colName := sqlbase.NormalizeName(qname.Column())
	for _, table := range qt.tables {
		if qname.Base == "" || sqlbase.EqualName(table.alias, string(qname.Base)) {
			for idx, col := range table.columns {
				if sqlbase.NormalizeName(col.Name) == colName {
					if ref.colIdx != invalidColIdx {
						return ref, fmt.Errorf("column reference \"%s\" is ambiguous", qname)
					}
					ref.table = table
					ref.colIdx = idx
				}
			}
		}
	}

	if ref.colIdx == invalidColIdx {
		return ref, fmt.Errorf("qualified name \"%s\" not found", qname)
	}
	return ref, nil
}

// qvalue implements the parser.VariableExpr interface and is used as a
// replacement node for QualifiedNames in expressions that can change their
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
func (q *qvalue) TypeCheck(args parser.MapArgs, desired parser.Datum) (parser.TypedExpr, error) {
	return q, nil
}

// Eval implements the TypedExpr interface.
func (q *qvalue) Eval(ctx parser.EvalContext) (parser.Datum, error) {
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

// qnameVisitor is a parser.Visitor implementation used to resolve the
// column names in an expression.
type qnameVisitor struct {
	qt  qvalResolver
	err error
}

var _ parser.Visitor = &qnameVisitor{}

func (v *qnameVisitor) VisitPre(expr parser.Expr) (recurse bool, newNode parser.Expr) {
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

	case *parser.QualifiedName:
		var colRef columnRef

		colRef, v.err = v.qt.findColumn(t)
		if v.err != nil {
			return false, expr
		}
		return true, v.qt.qvals.getQVal(colRef)

	case *parser.FuncExpr:
		// Special case handling for COUNT(*). This is a special construct to
		// count the number of rows; in this case * does NOT refer to a set of
		// columns.
		if len(t.Name.Indirect) > 0 || !strings.EqualFold(string(t.Name.Base), "count") {
			break
		}
		// The COUNT function takes a single argument. Exit out if this isn't true
		// as this will be detected during expression evaluation.
		if len(t.Exprs) != 1 {
			break
		}
		qname, ok := t.Exprs[0].(*parser.QualifiedName)
		if !ok {
			break
		}
		v.err = qname.NormalizeColumnName()
		if v.err != nil {
			return false, expr
		}
		if !qname.IsStar() {
			// This will cause us to recurse into the arguments of the function which
			// will perform normal qualified name resolution.
			break
		}
		// Replace the function argument with a special non-NULL VariableExpr.
		t = t.CopyNode()
		t.Exprs[0] = starDatumInstance
		return true, t

	case *parser.Subquery:
		// Do not recurse into subqueries.
		return false, expr
	}

	return true, expr
}

func (*qnameVisitor) VisitPost(expr parser.Expr) parser.Expr { return expr }

func (s *selectNode) resolveQNames(expr parser.Expr) (parser.Expr, error) {
	var v *qnameVisitor
	if s.planner != nil {
		v = &s.planner.qnameVisitor
	}
	return resolveQNames(expr, []*tableInfo{&s.table}, s.qvals, v)
}

// resolveQNames walks the provided expression and resolves all qualified
// names using the tableInfo and qvalMap. The function takes an optional
// qnameVisitor to provide the caller the option of avoiding an allocation.
func resolveQNames(
	expr parser.Expr, tables []*tableInfo, qvals qvalMap, v *qnameVisitor,
) (parser.Expr, error) {
	if expr == nil {
		return expr, nil
	}
	if v == nil {
		v = new(qnameVisitor)
	}
	*v = qnameVisitor{
		qt: qvalResolver{
			tables: tables,
			qvals:  qvals,
		},
	}
	expr, _ = parser.WalkExpr(v, expr)
	return expr, v.err
}

// Populates one table's datum fields in the qval map given a row of values.
func (q qvalMap) populateQVals(table *tableInfo, row parser.DTuple) {
	for ref, qval := range q {
		if ref.table == table {
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
// allow the the intermediate rendering of the value (before the group
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
func (s *starDatum) TypeCheck(args parser.MapArgs, desired parser.Datum) (parser.TypedExpr, error) {
	return s, nil
}

// Eval implements the TypedExpr interface.
func (*starDatum) Eval(ctx parser.EvalContext) (parser.Datum, error) {
	return parser.TypeInt.Eval(ctx)
}

// ReturnType implements the TypedExpr interface.
func (*starDatum) ReturnType() parser.Datum {
	return parser.TypeInt.ReturnType()
}
