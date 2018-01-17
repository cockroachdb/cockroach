// Copyright 2018 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
)

func init() {
	registerOperator(selectOp, operatorInfo{name: "select", class: selectClass{}})
}

func initSelectExpr(e *Expr, input *Expr) {
	e.op = selectOp
	e.children = []*Expr{input, nil /* filter */}

	// Assumes that e.relProps has already been initialized using
	// buildContext.newRelationalExpr().
	e.relProps.columns = make([]columnProps, len(input.relProps.columns))
	copy(e.relProps.columns, input.relProps.columns)
	e.relProps.notNullCols.UnionWith(input.relProps.notNullCols)
}

type selectClass struct{}

var _ operatorClass = selectClass{}

func (selectClass) layout() exprLayout {
	return exprLayout{
		filters: 1,
	}
}

func (selectClass) format(e *Expr, tp treeprinter.Node) {
	n := formatRelational(e, tp)
	formatExprs(n, "filters", e.filters())
	formatExprs(n, "inputs", e.inputs())
}
