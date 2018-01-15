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

var typedExprConvMap [numOperators]func(e *Expr, ivh *tree.IndexedVarHelper) tree.TypedExpr

func init() {
	// This code is not inline to avoid an initialization loop error (some of the
	// functions depend on scalarToTypedExpr which depends on typedExprConvMap).
	typedExprConvMap = [numOperators]func(e *Expr, ivh *tree.IndexedVarHelper) tree.TypedExpr{
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

func constOpToTypedExpr(e *Expr, ivh *tree.IndexedVarHelper) tree.TypedExpr {
	return e.private.(tree.Datum)
}

func variableOpToTypedExpr(e *Expr, ivh *tree.IndexedVarHelper) tree.TypedExpr {
	return ivh.IndexedVar(e.private.(*columnProps).index)
}

func boolOpToTypedExpr(e *Expr, ivh *tree.IndexedVarHelper) tree.TypedExpr {
	switch e.op {
	case andOp, orOp:
		n := scalarToTypedExpr(e.children[0], ivh)
		for _, child := range e.children[1:] {
			m := scalarToTypedExpr(child, ivh)
			if e.op == andOp {
				n = tree.NewTypedAndExpr(n, m)
			} else {
				n = tree.NewTypedOrExpr(n, m)
			}
		}
		return n

	case notOp:
		return tree.NewTypedNotExpr(scalarToTypedExpr(e.children[0], ivh))
	default:
		panic(fmt.Sprintf("invalid op %s", e.op))
	}
}

func tupleOpToTypedExpr(e *Expr, ivh *tree.IndexedVarHelper) tree.TypedExpr {
	if isTupleOfConstants(e) {
		datums := make(tree.Datums, len(e.children))
		for i, child := range e.children {
			datums[i] = constOpToTypedExpr(child, ivh).(tree.Datum)
		}
		return tree.NewDTuple(datums...)
	}
	children := make([]tree.TypedExpr, len(e.children))
	for i, child := range e.children {
		children[i] = scalarToTypedExpr(child, ivh)
	}
	return tree.NewTypedTuple(children)
}

func unaryOpToTypedExpr(e *Expr, ivh *tree.IndexedVarHelper) tree.TypedExpr {
	return tree.NewTypedUnaryExpr(
		unaryOpReverseMap[e.op],
		scalarToTypedExpr(e.children[0], ivh),
		e.scalarProps.typ,
	)
}

func comparisonOpToTypedExpr(e *Expr, ivh *tree.IndexedVarHelper) tree.TypedExpr {
	return tree.NewTypedComparisonExprWithSubOp(
		comparisonOpReverseMap[e.op],
		comparisonOpReverseMap[e.subOperator],
		scalarToTypedExpr(e.children[0], ivh),
		scalarToTypedExpr(e.children[1], ivh),
	)
}

func binaryOpToTypedExpr(e *Expr, ivh *tree.IndexedVarHelper) tree.TypedExpr {
	return tree.NewTypedBinaryExpr(
		binaryOpReverseMap[e.op],
		scalarToTypedExpr(e.children[0], ivh),
		scalarToTypedExpr(e.children[1], ivh),
		e.scalarProps.typ,
	)
}

func unsupportedScalarOpToTypedExpr(e *Expr, ivh *tree.IndexedVarHelper) tree.TypedExpr {
	return e.private.(tree.TypedExpr)
}

func scalarToTypedExpr(e *Expr, ivh *tree.IndexedVarHelper) tree.TypedExpr {
	if fn := typedExprConvMap[e.op]; fn != nil {
		return fn(e, ivh)
	}
	panic(fmt.Sprintf("unsupported op %s", e.op))
}
