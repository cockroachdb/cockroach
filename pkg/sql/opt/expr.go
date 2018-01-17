// Copyright 2017 The Cockroach Authors.
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

package opt

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
)

// Expr implements the node of a unified expressions tree for both relational
// and scalar expressions in a query.
//
// Expressions have optional inputs. Expressions also maintain properties; the
// types of properties depend on the expression type (or equivalently, operator
// type). For scalar expressions, the properties are stored in scalarProps. An
// example of a scalar property is the type (types.T) of the scalar expression.
// For relational expressions, the properties are stored in relProps. An example
// of a relational property is the set of columns referenced in the expression.
//
// Every unique column and every projection (that is more than just a pass
// through of a column) is given a column index within the query. Additionally,
// every reference to a table in the query gets a new set of output column
// indexes. Consider the query:
//
//   SELECT * FROM a AS l JOIN a AS r ON (l.x = r.y)
//
// In this query, `l.x` is not equivalent to `r.x` and `l.y` is not
// equivalent to `r.y`. In order to achieve this, we need to give these
// columns different indexes.
//
// In all cases, the column indexes are global to the query. For example,
// consider the query:
//
//   SELECT x FROM a WHERE y > 0
//
// There are 2 columns in the above query: x and y. During name resolution, the
// above query becomes:
//
//   SELECT [0] FROM a WHERE [1] > 0
//   -- [0] -> x
//   -- [1] -> y
//
// Relational expressions are composed of inputs, and optional auxiliary
// expressions (not yet implemented). The output columns are derived by the
// operator from the inputs and stored in relProps.columns.
//
//   +---------+---------+-------+--------+
//   |  out 0  |  out 1  |  ...  |  out N |
//   +---------+---------+-------+--------+
//   |                operator            |
//   +---------+---------+-------+--------+
//   |  in 0   |  in 1   |  ...  |  in N  |
//   +---------+---------+-------+--------+
//
// A query is composed of a tree of relational expressions. For example, a
// simple join might look like:
//
//   +-----------+
//   | join a, b |
//   +-----------+
//      |     |
//      |     |   +--------+
//      |     +---| scan b |
//      |         +--------+
//      |
//      |    +--------+
//      +----| scan a |
//           +--------+
type Expr struct {
	op operator

	// subOperator is used for array operators (SOME, ALL). For
	// example, for "a < SOME(SELECT * FROM t)" the operator is
	// someOp and the subOperator is ltOp.
	subOperator operator

	// Child expressions. The interpretation of the children is operator
	// dependent. For example, for a eqOp, there are two child expressions (the
	// left-hand side and the right-hand side); for an andOp, there are at least
	// two child expressions (each one being a conjunct).
	children []*Expr

	// Relational properties. Nil for scalar expressions.
	relProps *relationalProps

	// Scalar properties (properties that pertain only to scalar operators).
	// Nil for relational expressions.
	scalarProps *scalarProps

	// Operator-dependent data used by this expression. For example, constOp
	// stores a pointer to the constant value.
	private interface{}
}

// exprLayout describes the layout of auxiliary children expressions. The layout
// is operator specific and accessed via the operatorTab table.
type exprLayout struct {
	numAux       int8 // used internally, no need to specify manually
	aggregations int8
	filters      int8
	groupings    int8
	projections  int8
}

func (e *Expr) opClass() operatorClass {
	return operatorTab[e.op].class
}

// Applies normalization rules to an expression.
func normalizeExpr(e *Expr) {
	for _, input := range e.children {
		normalizeExpr(input)
	}
	normalizeExprNode(e)
}

// Applies normalization rules to an expression node. This is like
// normalizeExpr, except that it does not recursively normalize children.
func normalizeExprNode(e *Expr) {
	if normalizeFn := operatorTab[e.op].normalizeFn; normalizeFn != nil {
		normalizeFn(e)
	}
}

// formatRelational adds a node for a relational operator and returns a
// reference to the new treeprinter.Node (for adding more children).
func formatRelational(e *Expr, tp treeprinter.Node) treeprinter.Node {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%v", e.op)
	if !e.relProps.outputCols.Empty() {
		fmt.Fprintf(&buf, " [out=%s]", e.relProps.outputCols)
	}
	n := tp.Child(buf.String())
	e.relProps.format(n)
	return n
}

// formatExprs formats the given expressions as children of the same
// node. Optionally creates a new parent node (if title is not "", and we have
// expressions).
func formatExprs(tp treeprinter.Node, title string, exprs []*Expr) {
	if len(exprs) > 0 {
		if title != "" {
			tp = tp.Child(title)
		}
		for _, e := range exprs {
			e.format(tp)
		}
	}
}

// format is part of the operatorClass interface.
func (e *Expr) format(tp treeprinter.Node) {
	if e == nil {
		panic("format on nil Expr")
	}
	e.opClass().format(e, tp)
}

func (e *Expr) String() string {
	tp := treeprinter.New()
	e.format(tp)
	return tp.String()
}

// inputs returns the children expressions, excluding auxiliary expressions.
func (e *Expr) inputs() []*Expr {
	return e.children[:len(e.children)-int(e.layout().numAux)]
}

// aux returns the auxiliary expression(s) at location i in the children of
// Expr e.
func (e *Expr) aux(i int8, op operator) []*Expr {
	t := e.children[i : i+1]
	if t[0] == nil {
		return nil
	}
	if t[0].op == op {
		return t[0].children
	}
	return t
}

// addAux1 adds a single auxiliary expression with operator op at location
// i in the children of Expr e.
func (e *Expr) addAux1(i int8, op operator, aux *Expr) {
	if t := e.children[i]; t == nil {
		e.children[i] = aux
	} else if t.op != op {
		e.children[i] = &Expr{
			op:       op,
			children: []*Expr{t, aux},
		}
	} else {
		t.children = append(t.children, aux)
	}
}

func (e *Expr) filters() []*Expr {
	l := e.layout()
	if l.filters < 0 {
		return nil
	}
	return e.aux(l.filters, andOp)
}

func (e *Expr) addFilter(f *Expr) {
	// Recursively flatten AND expressions when adding them as a filter. The
	// filters for an expression are implicitly AND'ed together (i.e. they are in
	// conjunctive normal form).
	if f.op == andOp {
		for _, input := range f.inputs() {
			e.addFilter(input)
		}
		return
	}

	e.addAux1(e.layout().filters, andOp, f)
}

func (e *Expr) layout() exprLayout {
	return operatorTab[e.op].layout
}

func (e *Expr) initProps() {
	if e.relProps != nil {
		e.relProps.init()
	}
}

func (e *Expr) shallowCopy() *Expr {
	// TODO(radu): use something like buildContext to allocate in bulk.
	r := &Expr{
		op:          e.op,
		scalarProps: &scalarProps{},
		private:     e.private,
	}
	*r.scalarProps = *e.scalarProps
	if len(e.children) > 0 {
		r.children = append([]*Expr(nil), e.children...)
	}
	return r
}

func (e *Expr) deepCopy() *Expr {
	// TODO(radu): use something like buildContext to allocate in bulk.
	e = e.shallowCopy()
	for i, c := range e.children {
		e.children[i] = c.deepCopy()
	}
	return e
}
