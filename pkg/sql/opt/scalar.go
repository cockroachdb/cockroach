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
	// A note on normalization functions for scalar expressions, for now we expect
	// to build exprs from TypedExprs which have gone through a normalization
	// process; we implement only additional rules.
	scalarOpInfos := map[operator]operatorInfo{
		variableOp:          {name: "variable"},
		constOp:             {name: "const"},
		listOp:              {name: "list"},
		orderedListOp:       {name: "ordered-list"},
		andOp:               {name: "and", normalizeFn: normalizeAndOrOp},
		orOp:                {name: "or", normalizeFn: normalizeAndOrOp},
		notOp:               {name: "not"},
		eqOp:                {name: "eq", normalizeFn: normalizeEqOp},
		ltOp:                {name: "lt"},
		gtOp:                {name: "gt"},
		leOp:                {name: "le"},
		geOp:                {name: "ge"},
		neOp:                {name: "ne"},
		inOp:                {name: "in"},
		notInOp:             {name: "not-in"},
		likeOp:              {name: "like"},
		notLikeOp:           {name: "not-like"},
		iLikeOp:             {name: "ilike"},
		notILikeOp:          {name: "not-ilike"},
		similarToOp:         {name: "similar-to"},
		notSimilarToOp:      {name: "not-similar-to"},
		regMatchOp:          {name: "regmatch"},
		notRegMatchOp:       {name: "not-regmatch"},
		regIMatchOp:         {name: "regimatch"},
		notRegIMatchOp:      {name: "not-regimatch"},
		isDistinctFromOp:    {name: "is-distinct-from"},
		isNotDistinctFromOp: {name: "is-not-distinct-from"},
		isOp:                {name: "is"},
		isNotOp:             {name: "is-not"},
		anyOp:               {name: "any"},
		someOp:              {name: "some"},
		allOp:               {name: "all"},
		bitandOp:            {name: "bitand"},
		bitorOp:             {name: "bitor"},
		bitxorOp:            {name: "bitxor"},
		plusOp:              {name: "plus"},
		minusOp:             {name: "minus"},
		multOp:              {name: "mult"},
		divOp:               {name: "div"},
		floorDivOp:          {name: "floor-div"},
		modOp:               {name: "mod"},
		powOp:               {name: "pow"},
		concatOp:            {name: "concat"},
		lShiftOp:            {name: "lshift"},
		rShiftOp:            {name: "rshift"},
		unaryPlusOp:         {name: "unary-plus"},
		unaryMinusOp:        {name: "unary-minus"},
		unaryComplementOp:   {name: "complement"},
		functionCallOp:      {name: "func"},
	}

	for op, info := range scalarOpInfos {
		info.class = scalarClass{}
		registerOperator(op, info)
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
	formatExprs(n, "", e.children)
}

// The following initializers are called on an already allocated expression
// node. The scalarProps must be initialized separately.

// initConstExpr initializes a constOp expression node.
func initConstExpr(e *expr, datum tree.Datum) {
	e.op = constOp
	e.private = datum
}

// isConstBool checks whether e is a constOp with a boolean value, in
// which case it returns the boolean value.
func isConstBool(e *expr) (ok bool, val bool) {
	if e.op == constOp {
		switch e.private {
		case tree.DBoolTrue:
			return true, true
		case tree.DBoolFalse:
			return true, false
		}
	}
	return false, false
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

// isIndexedVar checks if e is a variableOp that represents an
// indexed variable with the given index.
func isIndexedVar(e *expr, index int) bool {
	return e.op == variableOp && e.private.(int) == index
}

func initTupleExpr(e *expr, children []*expr) {
	// In general, the order matters in a tuple so we use an "ordered list"
	// operator. In some cases (IN) the order doesn't matter; we could convert
	// those to listOp during normalization, but there doesn't seem to be a
	// benefit at this time.
	e.op = orderedListOp
	e.children = children
}

func isTupleOfConstants(e *expr) bool {
	if e.op != orderedListOp {
		return false
	}
	for _, c := range e.children {
		if c.op != constOp {
			return false
		}
	}
	return true
}

// Normalization rules for andOp and orOp: merge in any children that have the
// same operator.
// Example: a and (b and c)  ->  a and b and c
func normalizeAndOrOp(e *expr) {
	if e.op != andOp && e.op != orOp {
		panic(fmt.Sprintf("invalid call on %s", e))
	}
	var found bool
	newNumChildren := len(e.children)
	for _, child := range e.children {
		if child.op == e.op {
			found = true
			// We will add the grandchildren as direct children of this node (and
			// remove the child). The child has been normalized already, so we don't
			// need to look deeper.
			newNumChildren += len(child.children) - 1
		}
	}
	if !found {
		return
	}
	saved := e.children
	e.children = make([]*expr, 0, newNumChildren)

	for _, child := range saved {
		if child.op == e.op {
			e.children = append(e.children, child.children...)
		} else {
			e.children = append(e.children, child)
		}
	}
}

func normalizeEqOp(e *expr) {
	if e.op != eqOp {
		panic(fmt.Sprintf("invalid call on %s", e))
	}
	lhs, rhs := e.children[0], e.children[1]
	if lhs.op == orderedListOp && rhs.op == orderedListOp {
		// Break up expressions like
		//   (a, b, c) = (x, y, z)
		// into
		//   (a = x) AND (b = y) AND (c = z)
		// This transformation helps reduce the complexity of the index
		// constraints code which would otherwise have to deal with this case
		// separately.
		e.op = andOp
		if len(lhs.children) != len(rhs.children) {
			panic(fmt.Sprintf("tuple length mismatch in eqOp: %s", e))
		}
		e.children = make([]*expr, len(lhs.children))
		for i := range lhs.children {
			e.children[i] = &expr{
				op:          andOp,
				scalarProps: &scalarProps{typ: types.Bool},
			}
			initBinaryExpr(e.children[i], eqOp, lhs.children[i], rhs.children[i])
			// Normalize the new child node. This is for cases like:
			// ((a, b), (c, d)) = ((x, y), (z, u))
			normalizeExprNode(e.children[i])
		}
		// Normalize the new expression (some of the other rules, like coalescing
		// AND operations might apply now).
		normalizeExprNode(e)
	} else if e.children[0].op != variableOp && e.children[1].op == variableOp {
		// Normalize (1 = @1) to (@1 = 1).
		// Note: this transformation is already performed by the TypedExpr
		// NormalizeExpr, but we may be creating new such expressions above.
		e.children[0], e.children[1] = rhs, lhs
	}
}
