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

// This file implements conversion of scalar expressions to tree.TypedExpr

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

var typedExprConvMap [numOperators]func(c *typedExprConvCtx, e *Expr) tree.TypedExpr

func init() {
	// This code is not inline to avoid an initialization loop error (some of the
	// functions depend on scalarToTypedExpr which depends on typedExprConvMap).
	typedExprConvMap = [numOperators]func(c *typedExprConvCtx, e *Expr) tree.TypedExpr{
		constOp:    constOpToTypedExpr,
		variableOp: variableOpToTypedExpr,

		andOp: boolOpToTypedExpr,
		orOp:  boolOpToTypedExpr,
		notOp: boolOpToTypedExpr,

		unaryPlusOp:       unaryOpToTypedExpr,
		unaryMinusOp:      unaryOpToTypedExpr,
		unaryComplementOp: unaryOpToTypedExpr,

		eqOp:           comparisonOpToTypedExpr,
		ltOp:           comparisonOpToTypedExpr,
		gtOp:           comparisonOpToTypedExpr,
		leOp:           comparisonOpToTypedExpr,
		geOp:           comparisonOpToTypedExpr,
		neOp:           comparisonOpToTypedExpr,
		inOp:           comparisonOpToTypedExpr,
		notInOp:        comparisonOpToTypedExpr,
		likeOp:         comparisonOpToTypedExpr,
		notLikeOp:      comparisonOpToTypedExpr,
		iLikeOp:        comparisonOpToTypedExpr,
		notILikeOp:     comparisonOpToTypedExpr,
		similarToOp:    comparisonOpToTypedExpr,
		notSimilarToOp: comparisonOpToTypedExpr,
		regMatchOp:     comparisonOpToTypedExpr,
		notRegMatchOp:  comparisonOpToTypedExpr,
		regIMatchOp:    comparisonOpToTypedExpr,
		notRegIMatchOp: comparisonOpToTypedExpr,
		isOp:           comparisonOpToTypedExpr,
		isNotOp:        comparisonOpToTypedExpr,
		containsOp:     comparisonOpToTypedExpr,
		containedByOp:  comparisonOpToTypedExpr,
		anyOp:          comparisonOpToTypedExpr,
		someOp:         comparisonOpToTypedExpr,
		allOp:          comparisonOpToTypedExpr,

		bitandOp:   binaryOpToTypedExpr,
		bitorOp:    binaryOpToTypedExpr,
		bitxorOp:   binaryOpToTypedExpr,
		plusOp:     binaryOpToTypedExpr,
		minusOp:    binaryOpToTypedExpr,
		multOp:     binaryOpToTypedExpr,
		divOp:      binaryOpToTypedExpr,
		floorDivOp: binaryOpToTypedExpr,
		modOp:      binaryOpToTypedExpr,
		powOp:      binaryOpToTypedExpr,
		concatOp:   binaryOpToTypedExpr,
		lShiftOp:   binaryOpToTypedExpr,
		rShiftOp:   binaryOpToTypedExpr,

		tupleOp: tupleOpToTypedExpr,

		unsupportedScalarOp: unsupportedScalarOpToTypedExpr,
	}
}

type typedExprConvCtx struct {
	ivh *tree.IndexedVarHelper

	// varToIndexedVar is a map used when converting a variableOp into an
	// IndexedVar. It is optional: if it is nil, a 1-to-1 mapping is assumed.
	varToIndexedVar columnMap
}

func constOpToTypedExpr(c *typedExprConvCtx, e *Expr) tree.TypedExpr {
	return e.private.(tree.Datum)
}

func variableOpToTypedExpr(c *typedExprConvCtx, e *Expr) tree.TypedExpr {
	col := e.private.(*columnProps)
	var idx int
	if c.varToIndexedVar.Empty() {
		idx = col.index
	} else {
		var ok bool
		idx, ok = c.varToIndexedVar.Get(col.index)
		if !ok {
			panic(fmt.Sprintf("missing variable-IndexedVar mapping for %d", col.index))
		}
	}
	return c.ivh.IndexedVar(idx)
}

func boolOpToTypedExpr(c *typedExprConvCtx, e *Expr) tree.TypedExpr {
	switch e.op {
	case andOp, orOp:
		n := scalarToTypedExpr(c, e.children[0])
		for _, child := range e.children[1:] {
			m := scalarToTypedExpr(c, child)
			if e.op == andOp {
				n = tree.NewTypedAndExpr(n, m)
			} else {
				n = tree.NewTypedOrExpr(n, m)
			}
		}
		return n

	case notOp:
		return tree.NewTypedNotExpr(scalarToTypedExpr(c, e.children[0]))
	default:
		panic(fmt.Sprintf("invalid op %s", e.op))
	}
}

func tupleOpToTypedExpr(c *typedExprConvCtx, e *Expr) tree.TypedExpr {
	if isTupleOfConstants(e) {
		datums := make(tree.Datums, len(e.children))
		for i, child := range e.children {
			datums[i] = constOpToTypedExpr(c, child).(tree.Datum)
		}
		return tree.NewDTuple(datums...)
	}
	children := make([]tree.TypedExpr, len(e.children))
	for i, child := range e.children {
		children[i] = scalarToTypedExpr(c, child)
	}
	return tree.NewTypedTuple(children)
}

func unaryOpToTypedExpr(c *typedExprConvCtx, e *Expr) tree.TypedExpr {
	return tree.NewTypedUnaryExpr(
		unaryOpReverseMap[e.op],
		scalarToTypedExpr(c, e.children[0]),
		e.scalarProps.typ,
	)
}

func comparisonOpToTypedExpr(c *typedExprConvCtx, e *Expr) tree.TypedExpr {
	return tree.NewTypedComparisonExprWithSubOp(
		comparisonOpReverseMap[e.op],
		comparisonOpReverseMap[e.subOperator],
		scalarToTypedExpr(c, e.children[0]),
		scalarToTypedExpr(c, e.children[1]),
	)
}

func binaryOpToTypedExpr(c *typedExprConvCtx, e *Expr) tree.TypedExpr {
	return tree.NewTypedBinaryExpr(
		binaryOpReverseMap[e.op],
		scalarToTypedExpr(c, e.children[0]),
		scalarToTypedExpr(c, e.children[1]),
		e.scalarProps.typ,
	)
}

func unsupportedScalarOpToTypedExpr(c *typedExprConvCtx, e *Expr) tree.TypedExpr {
	return e.private.(tree.TypedExpr)
}

func scalarToTypedExpr(c *typedExprConvCtx, e *Expr) tree.TypedExpr {
	if fn := typedExprConvMap[e.op]; fn != nil {
		return fn(c, e)
	}
	panic(fmt.Sprintf("unsupported op %s", e.op))
}
