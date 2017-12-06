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

// expr is a unified interface for both relational and scalar expressions in a
// query. TODO(radu): currently, it only supports scalar expressions.
type expr struct {
	op operator
	// Child expressions. The interpretation of the children is operator
	// dependent.
	children []*expr
	// Scalar properties.
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

func formatExprs(tp *treePrinter, title string, exprs []*expr) {
	if len(exprs) > 0 {
		tp.Add(title)
		tp.Enter()
		for _, e := range exprs {
			if e != nil {
				e.format(tp)
			}
		}
		tp.Exit()
	}
}

// format is part of the operatorInfo interface.
func (e *expr) format(tp *treePrinter) {
	e.opClass().format(e, tp)
}

func (e *expr) String() string {
	tp := makeTreePrinter()
	e.format(&tp)
	return tp.String()
}
