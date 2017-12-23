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

import "github.com/cockroachdb/cockroach/pkg/util/treeprinter"

// expr implements the node of a unified expressions tree for both relational
// and scalar expressions in a query.
//
// Expressions have optional inputs. Expressions also maintain properties; the
// types of properties depend on the expression type (or equivalently, operator
// type). For scalar expressions, the properties are stored in scalarProps. An
// example of a scalar property is the type (types.T) of the scalar expression.
//
// Currently, expr only supports scalar expressions and operators. More
// information pertaining to relational operators will be added when they are
// supported.
//
// TODO(radu): support relational operators and extend this description.
type expr struct {
	op operator
	// Child expressions. The interpretation of the children is operator
	// dependent. For example, for a eqOp, there are two child expressions (the
	// left-hand side and the right-hand side); for an andOp, there are at least
	// two child expressions (each one being a conjunct).
	children []*expr
	// Scalar properties (properties that pertain only to scalar operators).
	scalarProps *scalarProps
	// Operator-dependent data used by this expression. For example, constOp
	// stores a pointer to the constant value.
	private interface{}
}

func (e *expr) opClass() operatorClass {
	return operatorTab[e.op].class
}

// Applies normalization rules to an expression.
func normalizeExpr(e *expr) {
	for _, input := range e.children {
		normalizeExpr(input)
	}
	normalizeExprNode(e)
}

// Applies normalization rules to an expression node. This is like
// normalizeExpr, except that it does not recursively normalize children.
func normalizeExprNode(e *expr) {
	if normalizeFn := operatorTab[e.op].normalizeFn; normalizeFn != nil {
		normalizeFn(e)
	}
}

// formatExprs formats the given expressions as children of the same
// node. Optionally creates a new parent node (if title is not "", and we have
// expressions).
func formatExprs(tp treeprinter.Node, title string, exprs []*expr) {
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
func (e *expr) format(tp treeprinter.Node) {
	if e == nil {
		panic("format on nil expr")
	}
	e.opClass().format(e, tp)
}

func (e *expr) String() string {
	tp := treeprinter.New()
	e.format(tp)
	return tp.String()
}

func (e *expr) shallowCopy() *expr {
	// TODO(radu): use something like buildContext to allocate in bulk.
	r := &expr{
		op:          e.op,
		scalarProps: &scalarProps{},
		private:     e.private,
	}
	*r.scalarProps = *e.scalarProps
	if len(e.children) > 0 {
		r.children = append([]*expr(nil), e.children...)
	}
	return r
}

func (e *expr) deepCopy() *expr {
	// TODO(radu): use something like buildContext to allocate in bulk.
	e = e.shallowCopy()
	for i, c := range e.children {
		e.children[i] = c.deepCopy()
	}
	return e
}
