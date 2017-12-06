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

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

func init() {
	registerOperator(variableOp, "variable", scalarClass{})
	registerOperator(constOp, "const", scalarClass{})
	registerOperator(listOp, "list", scalarClass{})
	registerOperator(orderedListOp, "ordered-list", scalarClass{})
	registerOperator(existsOp, "exists", scalarClass{})
	registerOperator(andOp, "AND", scalarClass{})
	registerOperator(orOp, "OR", scalarClass{})
	registerOperator(notOp, "NOT", scalarClass{})
	registerOperator(eqOp, "eq", scalarClass{})
	registerOperator(ltOp, "lt", scalarClass{})
	registerOperator(gtOp, "gt", scalarClass{})
	registerOperator(leOp, "le", scalarClass{})
	registerOperator(geOp, "ge", scalarClass{})
	registerOperator(neOp, "ne", scalarClass{})
	registerOperator(inOp, "IN", scalarClass{})
	registerOperator(notInOp, "NOT-IN", scalarClass{})
	registerOperator(likeOp, "LIKE", scalarClass{})
	registerOperator(notLikeOp, "NOT-LIKE", scalarClass{})
	registerOperator(iLikeOp, "ILIKE", scalarClass{})
	registerOperator(notILikeOp, "NOT-ILIKE", scalarClass{})
	registerOperator(similarToOp, "SIMILAR-TO", scalarClass{})
	registerOperator(notSimilarToOp, "NOT-SIMILAR-TO", scalarClass{})
	registerOperator(regMatchOp, "regmatch", scalarClass{})
	registerOperator(notRegMatchOp, "not-regmatch", scalarClass{})
	registerOperator(regIMatchOp, "regimatch", scalarClass{})
	registerOperator(notRegIMatchOp, "not-regimatch", scalarClass{})
	registerOperator(isDistinctFromOp, "IS-DISTINCT-FROM", scalarClass{})
	registerOperator(isNotDistinctFromOp, "IS-NOT-DISTINCT-FROM", scalarClass{})
	registerOperator(isOp, "IS", scalarClass{})
	registerOperator(isNotOp, "IS-NOT", scalarClass{})
	registerOperator(anyOp, "ANY", scalarClass{})
	registerOperator(someOp, "SOME", scalarClass{})
	registerOperator(allOp, "ALL", scalarClass{})
	registerOperator(bitandOp, "bitand", scalarClass{})
	registerOperator(bitorOp, "bitor", scalarClass{})
	registerOperator(bitxorOp, "bitxor", scalarClass{})
	registerOperator(plusOp, "plus", scalarClass{})
	registerOperator(minusOp, "minus", scalarClass{})
	registerOperator(multOp, "mult", scalarClass{})
	registerOperator(divOp, "div", scalarClass{})
	registerOperator(floorDivOp, "floor-div", scalarClass{})
	registerOperator(modOp, "mod", scalarClass{})
	registerOperator(powOp, "pow", scalarClass{})
	registerOperator(concatOp, "concat", scalarClass{})
	registerOperator(lShiftOp, "lshift", scalarClass{})
	registerOperator(rShiftOp, "rshift", scalarClass{})
	registerOperator(unaryPlusOp, "unary-plus", scalarClass{})
	registerOperator(unaryMinusOp, "unary-minus", scalarClass{})
	registerOperator(unaryComplementOp, "complement", scalarClass{})
	registerOperator(functionOp, "func", scalarClass{})
}

type scalarProps struct {
	typ types.T
}

type scalarClass struct{}

var _ operatorClass = scalarClass{}

func (scalarClass) format(e *expr, tp *treePrinter) {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%v", e.op)
	if e.private != nil {
		fmt.Fprintf(&buf, " (%v)", e.private)
	}
	fmt.Fprintf(&buf, " (type: %s)", e.scalarProps.typ)
	tp.Add(buf.String())
	tp.Enter()
	formatExprs(tp, "inputs", e.inputs())
	tp.Exit()
}

func initConstExpr(e *expr, datum tree.Datum) {
	e.op = constOp
	e.private = datum
}

func initFunctionExpr(e *expr, def *tree.FunctionDefinition, children []*expr) {
	e.op = functionOp
	e.children = children
	e.private = def
}

func initUnaryExpr(e *expr, op operator, input *expr) {
	e.op = op
	e.children = []*expr{input}
}

func initBinaryExpr(e *expr, op operator, input1 *expr, input2 *expr) {
	e.op = op
	e.children = []*expr{input1, input2}
}

func initVariableExpr(e *expr, index int) {
	e.op = variableOp
	e.private = index
}
