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

func (e *expr) inputs() []*expr {
	return e.children
}

func formatExprs(tp treeprinter.Node, title string, exprs []*expr) {
	if len(exprs) > 0 {
		n := tp.Child(title)
		for _, e := range exprs {
			if e != nil {
				e.format(n)
			}
		}
	}
}

// format is part of the operatorClass interface.
func (e *expr) format(tp treeprinter.Node) {
	e.opClass().format(e, tp)
}

func (e *expr) String() string {
	tp := treeprinter.New()
	e.format(tp)
	return tp.String()
}
