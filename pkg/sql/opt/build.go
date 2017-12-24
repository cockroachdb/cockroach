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
	"fmt"

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
	tree.IsNotDistinctFrom: isOp,
	tree.IsDistinctFrom:    isNotOp,
	tree.Any:               anyOp,
	tree.Some:              someOp,
	tree.All:               allOp,
}

var comparisonOpReverseMap = map[operator]tree.ComparisonOperator{
	eqOp:           tree.EQ,
	ltOp:           tree.LT,
	gtOp:           tree.GT,
	leOp:           tree.LE,
	geOp:           tree.GE,
	neOp:           tree.NE,
	inOp:           tree.In,
	notInOp:        tree.NotIn,
	likeOp:         tree.Like,
	notLikeOp:      tree.NotLike,
	iLikeOp:        tree.ILike,
	notILikeOp:     tree.NotILike,
	similarToOp:    tree.SimilarTo,
	notSimilarToOp: tree.NotSimilarTo,
	regMatchOp:     tree.RegMatch,
	notRegMatchOp:  tree.NotRegMatch,
	regIMatchOp:    tree.RegIMatch,
	notRegIMatchOp: tree.NotRegIMatch,
	isOp:           tree.IsNotDistinctFrom,
	isNotOp:        tree.IsDistinctFrom,
	anyOp:          tree.Any,
	someOp:         tree.Some,
	allOp:          tree.All,
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

var binaryOpReverseMap = map[operator]tree.BinaryOperator{
	bitandOp:   tree.Bitand,
	bitorOp:    tree.Bitor,
	bitxorOp:   tree.Bitxor,
	plusOp:     tree.Plus,
	minusOp:    tree.Minus,
	multOp:     tree.Mult,
	divOp:      tree.Div,
	floorDivOp: tree.FloorDiv,
	modOp:      tree.Mod,
	powOp:      tree.Pow,
	concatOp:   tree.Concat,
	lShiftOp:   tree.LShift,
	rShiftOp:   tree.RShift,
}

// Map from tree.UnaryOperator to operator.
var unaryOpMap = [...]operator{
	tree.UnaryPlus:       unaryPlusOp,
	tree.UnaryMinus:      unaryMinusOp,
	tree.UnaryComplement: unaryComplementOp,
}

// Map from tree.UnaryOperator to operator.
var unaryOpReverseMap = map[operator]tree.UnaryOperator{
	unaryPlusOp:       tree.UnaryPlus,
	unaryMinusOp:      tree.UnaryMinus,
	unaryComplementOp: tree.UnaryComplement,
}

// We allocate *scalarProps and *expr in chunks of these sizes.
const exprAllocChunk = 16
const scalarPropsAllocChunk = 16

type buildContext struct {
	preallocScalarProps []scalarProps
	preallocExprs       []expr
}

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
		children := make([]*expr, len(t.Exprs))
		for i, pexpr := range t.Exprs {
			children[i] = buildScalar(buildCtx, pexpr.(tree.TypedExpr))
		}
		initFunctionCallExpr(e, t.ResolvedFunc(), children)

	case *tree.IndexedVar:
		initVariableExpr(e, t.Idx)

	case *tree.Tuple:
		children := make([]*expr, len(t.Exprs))
		for i, e := range t.Exprs {
			children[i] = buildScalar(buildCtx, e.(tree.TypedExpr))
		}
		initTupleExpr(e, children)

	case *tree.DTuple:
		children := make([]*expr, len(t.D))
		for i, d := range t.D {
			children[i] = buildScalar(buildCtx, d)
		}
		initTupleExpr(e, children)

	case tree.Datum:
		initConstExpr(e, t)

	default:
		panic(fmt.Sprintf("node %T not supported", t))
	}
	return e
}

func scalarToTypedExpr(e *expr, ivh *tree.IndexedVarHelper) tree.TypedExpr {
	children := make([]tree.TypedExpr, len(e.children))
	for i, c := range e.children {
		children[i] = scalarToTypedExpr(c, ivh)
	}
	switch e.op {
	case constOp:
		return e.private.(tree.Datum)

	case variableOp:
		return ivh.IndexedVar(e.private.(int))

	case andOp:
		n := children[0]
		for _, m := range children[1:] {
			n = tree.NewTypedAndExpr(n, m)
		}
		return n

	case notOp:
		return tree.NewTypedNotExpr(children[0])

	case orOp:
		n := children[0]
		for _, m := range children[1:] {
			n = tree.NewTypedOrExpr(n, m)
		}
		return n

	case orderedListOp:
		if isTupleOfConstants(e) {
			datums := make(tree.Datums, len(children))
			for i, c := range children {
				datums[i] = c.(tree.Datum)
			}
			return tree.NewDTuple(datums...)
		}
		return tree.NewTypedTuple(children)
	}

	switch len(children) {
	case 1:
		if unaryOp, ok := unaryOpReverseMap[e.op]; ok {
			return tree.NewTypedUnaryExpr(unaryOp, children[0], e.scalarProps.typ)
		}
	case 2:
		if cmpOp, ok := comparisonOpReverseMap[e.op]; ok {
			return tree.NewTypedComparisonExpr(cmpOp, children[0], children[1])
		}
		if binOp, ok := binaryOpReverseMap[e.op]; ok {
			return tree.NewTypedBinaryExpr(binOp, children[0], children[1], e.scalarProps.typ)
		}
	}
	panic(fmt.Sprintf("unhandled op %s", e.op))
}
