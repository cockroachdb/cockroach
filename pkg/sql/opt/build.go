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

var comparisonOpReverseMap = [...]tree.ComparisonOperator{
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

var binaryOpReverseMap = [...]tree.BinaryOperator{
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
var unaryOpReverseMap = [...]tree.UnaryOperator{
	unaryPlusOp:       tree.UnaryPlus,
	unaryMinusOp:      tree.UnaryMinus,
	unaryComplementOp: tree.UnaryComplement,
}

// We allocate *scalarProps and *expr in chunks of these sizes.
const exprAllocChunk = 16
const scalarPropsAllocChunk = 16

type buildContext struct {
	preallocScalarProps []scalarProps
	preallocExprs       []Expr
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
func (bc *buildContext) newExpr() *Expr {
	if len(bc.preallocExprs) == 0 {
		bc.preallocExprs = make([]Expr, exprAllocChunk)
	}
	e := &bc.preallocExprs[0]
	bc.preallocExprs = bc.preallocExprs[1:]
	e.scalarProps = bc.newScalarProps()
	return e
}

// buildScalar converts a tree.TypedExpr to an expr tree.
func (bc *buildContext) buildScalar(pexpr tree.TypedExpr) *Expr {
	switch t := pexpr.(type) {
	case *tree.ParenExpr:
		return bc.buildScalar(t.TypedInnerExpr())
	}

	e := bc.newExpr()
	e.scalarProps.typ = pexpr.ResolvedType()

	switch t := pexpr.(type) {
	case *tree.AndExpr:
		initBinaryExpr(
			e, andOp,
			bc.buildScalar(t.TypedLeft()),
			bc.buildScalar(t.TypedRight()),
		)
	case *tree.OrExpr:
		initBinaryExpr(
			e, orOp,
			bc.buildScalar(t.TypedLeft()),
			bc.buildScalar(t.TypedRight()),
		)
	case *tree.NotExpr:
		initUnaryExpr(e, notOp, bc.buildScalar(t.TypedInnerExpr()))

	case *tree.BinaryExpr:
		initBinaryExpr(
			e, binaryOpMap[t.Operator],
			bc.buildScalar(t.TypedLeft()),
			bc.buildScalar(t.TypedRight()),
		)
	case *tree.ComparisonExpr:
		initBinaryExpr(
			e, comparisonOpMap[t.Operator],
			bc.buildScalar(t.TypedLeft()),
			bc.buildScalar(t.TypedRight()),
		)
	case *tree.UnaryExpr:
		initUnaryExpr(e, unaryOpMap[t.Operator], bc.buildScalar(t.TypedInnerExpr()))

	// TODO(radu): for now, we pass through FuncExprs as unsupported
	// expressions.
	//case *tree.FuncExpr:
	//	children := make([]*expr, len(t.Exprs))
	//	for i, pexpr := range t.Exprs {
	//		children[i] = bc.buildScalar(pexpr.(tree.TypedExpr))
	//	}
	//	initFunctionCallExpr(e, t.ResolvedFunc(), children)

	case *tree.IndexedVar:
		initVariableExpr(e, t.Idx)

	case *tree.Tuple:
		children := make([]*Expr, len(t.Exprs))
		for i, e := range t.Exprs {
			children[i] = bc.buildScalar(e.(tree.TypedExpr))
		}
		initTupleExpr(e, children)

	case *tree.DTuple:
		children := make([]*Expr, len(t.D))
		for i, d := range t.D {
			children[i] = bc.buildScalar(d)
		}
		initTupleExpr(e, children)

	case tree.Datum:
		initConstExpr(e, t)

	default:
		initUnsupportedExpr(e, t)
	}
	return e
}

// buildScalar converts a tree.TypedExpr to an expr tree.
func buildScalar(pexpr tree.TypedExpr) (_ *Expr, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%v", r)
		}
	}()
	buildCtx := buildContext{}
	return buildCtx.buildScalar(pexpr), nil
}

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
	return ivh.IndexedVar(e.private.(int))
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
	return tree.NewTypedComparisonExpr(
		comparisonOpReverseMap[e.op],
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

// BuildScalarExpr converts a TypedExpr to a *Expr tree and normalizes it.
func BuildScalarExpr(typedExpr tree.TypedExpr) (*Expr, error) {
	if typedExpr == nil {
		return nil, nil
	}
	e, err := buildScalar(typedExpr)
	if err != nil {
		return nil, err
	}
	normalizeExpr(e)
	return e, nil
}
