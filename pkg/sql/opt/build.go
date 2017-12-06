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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// Map from tree.ComparisonOperator to operator.
var comparisonOpMap = [...]operator{
	tree.EQ:                eqOp,
	tree.LT:                ltOp,
	tree.GT:                gtOp,
	tree.LE:                leOp,
	tree.GE:                geOp,
	tree.NE:                neOp,
	tree.In:                inOp,
	tree.NotIn:             notInOp,
	tree.Like:              likeOp,
	tree.NotLike:           notLikeOp,
	tree.ILike:             iLikeOp,
	tree.NotILike:          notILikeOp,
	tree.SimilarTo:         similarToOp,
	tree.NotSimilarTo:      notSimilarToOp,
	tree.RegMatch:          regMatchOp,
	tree.NotRegMatch:       notRegMatchOp,
	tree.RegIMatch:         regIMatchOp,
	tree.NotRegIMatch:      notRegIMatchOp,
	tree.IsDistinctFrom:    isDistinctFromOp,
	tree.IsNotDistinctFrom: isNotDistinctFromOp,
	tree.Is:                isOp,
	tree.IsNot:             isNotOp,
	tree.Any:               anyOp,
	tree.Some:              someOp,
	tree.All:               allOp,
}

// Map from tree.BinaryOperator to operator.
var binaryOpMap = [...]operator{
	tree.Bitand:   bitandOp,
	tree.Bitor:    bitorOp,
	tree.Bitxor:   bitxorOp,
	tree.Plus:     plusOp,
	tree.Minus:    minusOp,
	tree.Mult:     multOp,
	tree.Div:      divOp,
	tree.FloorDiv: floorDivOp,
	tree.Mod:      modOp,
	tree.Pow:      powOp,
	tree.Concat:   concatOp,
	tree.LShift:   lShiftOp,
	tree.RShift:   rShiftOp,
}

// Map from tree.UnaryOperator to operator.
var unaryOpMap = [...]operator{
	tree.UnaryPlus:       unaryPlusOp,
	tree.UnaryMinus:      unaryMinusOp,
	tree.UnaryComplement: unaryComplementOp,
}

type buildContext struct {
	// We allocate *scalarProps and *expr in chunks.
	preallocScalarProps []scalarProps
	preallocExprs       []expr
}

const exprAllocChunk = 16
const scalarPropsAllocChunk = 16

func (bc *buildContext) newScalarProps() *scalarProps {
	if len(bc.preallocScalarProps) == 0 {
		bc.preallocScalarProps = make([]scalarProps, scalarPropsAllocChunk)
	}
	p := &bc.preallocScalarProps[0]
	bc.preallocScalarProps = bc.preallocScalarProps[1:]
	return p
}

// newExpr returns a new *expr with a new, blank scalarProps.
func (bc *buildContext) newExpr() *expr {
	if len(bc.preallocExprs) == 0 {
		bc.preallocExprs = make([]expr, exprAllocChunk)
	}
	e := &bc.preallocExprs[0]
	bc.preallocExprs = bc.preallocExprs[1:]
	e.scalarProps = bc.newScalarProps()
	return e
}

// buildScalar converts a tree.TypedExpr to an expr tree.
func buildScalar(buildCtx *buildContext, pexpr tree.TypedExpr) *expr {
	switch t := pexpr.(type) {
	case *tree.ParenExpr:
		return buildScalar(buildCtx, t.TypedInnerExpr())
	}

	e := buildCtx.newExpr()
	e.scalarProps.typ = pexpr.ResolvedType()

	switch t := pexpr.(type) {
	case *tree.AndExpr:
		initBinaryExpr(
			e, andOp,
			buildScalar(buildCtx, t.TypedLeft()),
			buildScalar(buildCtx, t.TypedRight()),
		)
	case *tree.OrExpr:
		initBinaryExpr(
			e, orOp,
			buildScalar(buildCtx, t.TypedLeft()),
			buildScalar(buildCtx, t.TypedRight()),
		)
	case *tree.NotExpr:
		initUnaryExpr(e, notOp, buildScalar(buildCtx, t.TypedInnerExpr()))

	case *tree.BinaryExpr:
		initBinaryExpr(
			e, binaryOpMap[t.Operator],
			buildScalar(buildCtx, t.TypedLeft()),
			buildScalar(buildCtx, t.TypedRight()),
		)
	case *tree.ComparisonExpr:
		initBinaryExpr(
			e, comparisonOpMap[t.Operator],
			buildScalar(buildCtx, t.TypedLeft()),
			buildScalar(buildCtx, t.TypedRight()),
		)
	case *tree.UnaryExpr:
		initUnaryExpr(e, unaryOpMap[t.Operator], buildScalar(buildCtx, t.TypedInnerExpr()))

	case *tree.FuncExpr:
		def, err := t.Func.Resolve(tree.SearchPath{})
		if err != nil {
			panic(err.Error())
		}
		children := make([]*expr, len(t.Exprs))
		for i, pexpr := range t.Exprs {
			children[i] = buildScalar(buildCtx, pexpr.(tree.TypedExpr))
		}
		initFunctionExpr(e, def, children)

	case *tree.IndexedVar:
		initVariableExpr(e, t.Idx)

	case tree.Datum:
		initConstExpr(e, t)

	default:
		panicf("node %T not supported", t)
	}
	return e
}
