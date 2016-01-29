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
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
)

// columnRef is a reference to a resultColumn of a FROM node
type columnRef struct {
	from *fromInfo

	// Index of column (in from.columns).
	colIdx int
}

const invalidColIdx = -1

// get dereferences the columnRef to the resultColumn.
func (cr columnRef) get() ResultColumn {
	return cr.from.columns[cr.colIdx]
}

// findColumn looks up the column described by a QualifiedName. The qname will be normalized.
func (s *selectNode) findColumn(qname *parser.QualifiedName) (columnRef, *roachpb.Error) {

	ref := columnRef{colIdx: invalidColIdx}

	if err := roachpb.NewError(qname.NormalizeColumnName()); err != nil {
		return ref, err
	}

	// We can't resolve stars to a single column.
	if qname.IsStar() {
		err := roachpb.NewUErrorf("qualified name \"%s\" not found", qname)
		return ref, err
	}

	// TODO(radu): when we support multiple FROMs, we will find the node with the correct alias; if
	// no alias is given, we will search for the column in all FROMs and make sure there is only
	// one.  For now we just check that the name matches (if given).
	if qname.Base == "" {
		qname.Base = parser.Name(s.from.alias)
	}
	if equalName(s.from.alias, string(qname.Base)) {
		colName := qname.Column()
		for idx, col := range s.from.columns {
			if equalName(col.Name, colName) {
				ref.from = &s.from
				ref.colIdx = idx
				return ref, nil
			}
		}
	}

	err := roachpb.NewUErrorf("qualified name \"%s\" not found", qname)
	return ref, err
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

var _ parser.VariableExpr = &qvalue{}

func (*qvalue) Variable() {}

func (q *qvalue) String() string {
	return q.colRef.get().Name
}

func (q *qvalue) Walk(v parser.Visitor) {
	q.datum = parser.WalkExpr(v, q.datum).(parser.Datum)
}

func (q *qvalue) TypeCheck(args parser.MapArgs) (parser.Datum, error) {
	return q.datum.TypeCheck(args)
}

func (q *qvalue) Eval(ctx parser.EvalContext) (parser.Datum, error) {
	return q.datum.Eval(ctx)
}

// getQVal creates a qvalue for a column reference. Created qvalues are
// stored in the qvals map. If a qvalue was previously created for the same
// reference, the existing qvalue is returned.
func (s *selectNode) getQVal(colRef columnRef) *qvalue {
	qval := s.qvals[colRef]
	if qval == nil {
		col := colRef.get()
		// We initialize the qvalue expression to a datum of the type matching the
		// column. This allows type analysis to be performed on the expression
		// before we start retrieving rows.
		//
		// TODO(pmattis): Nullable columns can have NULL values. The type analysis
		// needs to take that into consideration, but how to surface that info?
		qval = &qvalue{colRef: colRef, datum: col.Typ}
		s.qvals[colRef] = qval
	}
	return qval
}

// qnameVisitor is a parser.Visitor implementation used to resolve the
// column names in an expression.
type qnameVisitor struct {
	selNode *selectNode
	pErr    *roachpb.Error
}

var _ parser.Visitor = &qnameVisitor{}

// Visit is invoked for each Expr node. It can return a new expression for the
// node, or it can stop processing by returning a nil Visitor.
func (v *qnameVisitor) Visit(expr parser.Expr, pre bool) (parser.Visitor, parser.Expr) {
	if !pre || v.pErr != nil {
		return nil, expr
	}

	switch t := expr.(type) {
	case *qvalue:
		// We will encounter a qvalue in the expression during retry of an
		// auto-transaction. When that happens, we've already gone through
		// qualified name normalization and lookup, we just need to hook the qvalue
		// up to the scanNode.
		//
		// TODO(pmattis): Should we be more careful about ensuring that the various
		// statement implementations do not modify the AST nodes they are passed?
		colRef := t.colRef
		// TODO(radu): this is pretty hacky and won't work with multiple FROMs..
		colRef.from = &v.selNode.from
		return v, v.selNode.getQVal(colRef)

	case *parser.QualifiedName:
		var colRef columnRef

		colRef, v.pErr = v.selNode.findColumn(t)
		if v.pErr != nil {
			return nil, expr
		}
		return v, v.selNode.getQVal(colRef)

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
		v.pErr = roachpb.NewError(qname.NormalizeColumnName())
		if v.pErr != nil {
			return nil, expr
		}
		if !qname.IsStar() {
			// This will cause us to recurse into the arguments of the function which
			// will perform normal qualified name resolution.
			break
		}
		// Replace the function argument with a special non-NULL VariableExpr.
		t.Exprs[0] = starDatumInstance
		return v, expr

	case *parser.Subquery:
		// Do not recurse into subqueries.
		return nil, expr
	}

	return v, expr
}

func (s *selectNode) resolveQNames(expr parser.Expr) (parser.Expr, *roachpb.Error) {
	if expr == nil {
		return expr, nil
	}
	v := qnameVisitor{selNode: s}
	expr = parser.WalkExpr(&v, expr)
	return expr, v.pErr
}

// Populates the datum fields of the qvalues in the qval map (given a row
// of values retrieved from the fromNode).
func (s *selectNode) populateQVals(row parser.DTuple) {
	for ref, qval := range s.qvals {
		qval.datum = row[ref.colIdx]
		if qval.datum == nil {
			panic(fmt.Sprintf("Unpopulated value for column %d", ref.colIdx))
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
var _ parser.VariableExpr = starDatumInstance

func (*starDatum) Variable() {}

func (*starDatum) String() string {
	return "*"
}

func (*starDatum) Walk(v parser.Visitor) {}

func (*starDatum) TypeCheck(args parser.MapArgs) (parser.Datum, error) {
	return parser.DummyInt.TypeCheck(args)
}

func (*starDatum) Eval(ctx parser.EvalContext) (parser.Datum, error) {
	return parser.DummyInt.Eval(ctx)
}
