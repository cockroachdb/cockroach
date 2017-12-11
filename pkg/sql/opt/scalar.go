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
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
)

func init() {
	scalarOpNames := map[operator]string{
		variableOp:          "variable",
		constOp:             "const",
		listOp:              "list",
		orderedListOp:       "ordered-list",
		andOp:               "and",
		orOp:                "or",
		notOp:               "not",
		eqOp:                "eq",
		ltOp:                "lt",
		gtOp:                "gt",
		leOp:                "le",
		geOp:                "ge",
		neOp:                "ne",
		inOp:                "in",
		notInOp:             "not-in",
		likeOp:              "like",
		notLikeOp:           "not-like",
		iLikeOp:             "ilike",
		notILikeOp:          "not-ilike",
		similarToOp:         "similar-to",
		notSimilarToOp:      "not-similar-to",
		regMatchOp:          "regmatch",
		notRegMatchOp:       "not-regmatch",
		regIMatchOp:         "regimatch",
		notRegIMatchOp:      "not-regimatch",
		isDistinctFromOp:    "is-distinct-from",
		isNotDistinctFromOp: "is-not-distinct-from",
		isOp:                "is",
		isNotOp:             "is-not",
		anyOp:               "any",
		someOp:              "some",
		allOp:               "all",
		bitandOp:            "bitand",
		bitorOp:             "bitor",
		bitxorOp:            "bitxor",
		plusOp:              "plus",
		minusOp:             "minus",
		multOp:              "mult",
		divOp:               "div",
		floorDivOp:          "floor-div",
		modOp:               "mod",
		powOp:               "pow",
		concatOp:            "concat",
		lShiftOp:            "lshift",
		rShiftOp:            "rshift",
		unaryPlusOp:         "unary-plus",
		unaryMinusOp:        "unary-minus",
		unaryComplementOp:   "complement",
		functionCallOp:      "func",
	}

	for op, name := range scalarOpNames {
		registerOperator(op, name, scalarClass{})
	}
}

// scalarProps are properties specific to scalar expressions. An instance of
// scalarProps is associated with an expr node with a scalar operator.
type scalarProps struct {
	// typ is the semantic type of the scalar expression.
	typ types.T
}

type scalarClass struct{}

var _ operatorClass = scalarClass{}

func (scalarClass) format(e *expr, tp treeprinter.Node) {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%v", e.op)
	if e.private != nil {
		fmt.Fprintf(&buf, " (%v)", e.private)
	}
	fmt.Fprintf(&buf, " (type: %s)", e.scalarProps.typ)
	n := tp.Child(buf.String())
	formatExprs(n, "inputs", e.inputs())
}

// The following initializers are called on an already allocated expression
// node. The scalarProps must be initialized separately.

// initConstExpr initializes a constOp expression node.
func initConstExpr(e *expr, datum tree.Datum) {
	e.op = constOp
	e.private = datum
}

// initFunctionCallExpr initializes a functionCallOp expression node.
func initFunctionCallExpr(e *expr, def *tree.FunctionDefinition, children []*expr) {
	e.op = functionCallOp
	e.children = children
	e.private = def
}

// initUnaryExpr initializes expression nodes for operators with a single input.
func initUnaryExpr(e *expr, op operator, input *expr) {
	e.op = op
	e.children = []*expr{input}
}

// initBinaryExpr initializes expression nodes for operators with two inputs.
func initBinaryExpr(e *expr, op operator, input1 *expr, input2 *expr) {
	e.op = op
	e.children = []*expr{input1, input2}
}

// initVariableExpr initializes a variableOp expression node.
// The index refers to a column index (currently derived from an IndexedVar).
func initVariableExpr(e *expr, index int) {
	e.op = variableOp
	e.private = index
}
