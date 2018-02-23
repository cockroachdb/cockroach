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
	"context"
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
	tree.Contains:          containsOp,
	tree.ContainedBy:       containedByOp,
	tree.JSONExists:        jsonExistsOp,
	tree.JSONAllExists:     jsonAllExistsOp,
	tree.JSONSomeExists:    jsonSomeExistsOp,
	tree.Any:               someOp,
	tree.Some:              someOp,
	tree.All:               allOp,
}

var comparisonOpReverseMap = [...]tree.ComparisonOperator{
	eqOp:             tree.EQ,
	ltOp:             tree.LT,
	gtOp:             tree.GT,
	leOp:             tree.LE,
	geOp:             tree.GE,
	neOp:             tree.NE,
	inOp:             tree.In,
	notInOp:          tree.NotIn,
	likeOp:           tree.Like,
	notLikeOp:        tree.NotLike,
	iLikeOp:          tree.ILike,
	notILikeOp:       tree.NotILike,
	similarToOp:      tree.SimilarTo,
	notSimilarToOp:   tree.NotSimilarTo,
	regMatchOp:       tree.RegMatch,
	notRegMatchOp:    tree.NotRegMatch,
	regIMatchOp:      tree.RegIMatch,
	notRegIMatchOp:   tree.NotRegIMatch,
	isOp:             tree.IsNotDistinctFrom,
	isNotOp:          tree.IsDistinctFrom,
	containsOp:       tree.Contains,
	containedByOp:    tree.ContainedBy,
	jsonExistsOp:     tree.JSONExists,
	jsonAllExistsOp:  tree.JSONAllExists,
	jsonSomeExistsOp: tree.JSONSomeExists,
	anyOp:            tree.Any,
	someOp:           tree.Some,
	allOp:            tree.All,
}

// Map from tree.BinaryOperator to operator.
var binaryOpMap = [...]operator{
	tree.Bitand:            bitandOp,
	tree.Bitor:             bitorOp,
	tree.Bitxor:            bitxorOp,
	tree.Plus:              plusOp,
	tree.Minus:             minusOp,
	tree.Mult:              multOp,
	tree.Div:               divOp,
	tree.FloorDiv:          floorDivOp,
	tree.Mod:               modOp,
	tree.Pow:               powOp,
	tree.Concat:            concatOp,
	tree.LShift:            lShiftOp,
	tree.RShift:            rShiftOp,
	tree.JSONFetchVal:      jsonFetchValOp,
	tree.JSONFetchText:     jsonFetchTextOp,
	tree.JSONFetchValPath:  jsonFetchValPathOp,
	tree.JSONFetchTextPath: jsonFetchTextPathOp,
}

var binaryOpReverseMap = [...]tree.BinaryOperator{
	bitandOp:            tree.Bitand,
	bitorOp:             tree.Bitor,
	bitxorOp:            tree.Bitxor,
	plusOp:              tree.Plus,
	minusOp:             tree.Minus,
	multOp:              tree.Mult,
	divOp:               tree.Div,
	floorDivOp:          tree.FloorDiv,
	modOp:               tree.Mod,
	powOp:               tree.Pow,
	concatOp:            tree.Concat,
	lShiftOp:            tree.LShift,
	rShiftOp:            tree.RShift,
	jsonFetchValOp:      tree.JSONFetchVal,
	jsonFetchTextOp:     tree.JSONFetchText,
	jsonFetchValPathOp:  tree.JSONFetchValPath,
	jsonFetchTextPathOp: tree.JSONFetchTextPath,
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

// We allocate *Expr, *scalarProps and *relationalProps in chunks of these sizes.
const exprAllocChunk = 16
const scalarPropsAllocChunk = 16

type buildContext struct {
	ctx                 context.Context
	evalCtx             *tree.EvalContext
	preallocScalarProps []scalarProps
	preallocExprs       []Expr
}

func makeBuildContext(ctx context.Context, evalCtx *tree.EvalContext) buildContext {
	return buildContext{
		ctx:     ctx,
		evalCtx: evalCtx,
	}
}

func (bc *buildContext) newScalarProps() *scalarProps {
	if len(bc.preallocScalarProps) == 0 {
		bc.preallocScalarProps = make([]scalarProps, scalarPropsAllocChunk)
	}
	p := &bc.preallocScalarProps[0]
	bc.preallocScalarProps = bc.preallocScalarProps[1:]
	return p
}

// newExpr returns a new *Expr.
func (bc *buildContext) newExpr() *Expr {
	if len(bc.preallocExprs) == 0 {
		bc.preallocExprs = make([]Expr, exprAllocChunk)
	}
	e := &bc.preallocExprs[0]
	bc.preallocExprs = bc.preallocExprs[1:]
	return e
}

// newScalarExpr returns a new *Expr with a new, blank scalarProps.
func (bc *buildContext) newScalarExpr() *Expr {
	e := bc.newExpr()
	e.scalarProps = bc.newScalarProps()
	return e
}

// buildScalar converts a tree.TypedExpr to an Expr tree.
func (bc *buildContext) buildScalar(pexpr tree.TypedExpr) *Expr {
	switch t := pexpr.(type) {
	case *tree.ParenExpr:
		return bc.buildScalar(t.TypedInnerExpr())
	}

	e := bc.newScalarExpr()
	e.scalarProps.typ = pexpr.ResolvedType()

	switch t := pexpr.(type) {
	case *columnProps:
		initVariableExpr(e, t)

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
		e.subOperator = comparisonOpMap[t.SubOperator]
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
		colProps := &columnProps{
			typ:   t.ResolvedType(),
			index: t.Idx,
		}
		initVariableExpr(e, colProps)

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

	// Because Placeholder is also a Datum, it must come before the Datum case.
	case *tree.Placeholder:
		d, err := t.Eval(bc.evalCtx)
		if err != nil {
			panic(builderError{err})
		}
		if _, ok := d.(*tree.Placeholder); ok {
			panic(errorf("no placeholder value"))
		}
		initConstExpr(e, d)

	case tree.Datum:
		initConstExpr(e, t)

	default:
		initUnsupportedExpr(e, t)
	}
	return e
}

// builderError is used for semantic errors that occur during the build process
// and is passed as an argument to panic. These panics are caught and converted
// back to errors inside Builder.Build.
type builderError struct {
	error
}

// errorf formats according to a format specifier and returns the
// string as a builderError.
func errorf(format string, a ...interface{}) builderError {
	err := fmt.Errorf(format, a...)
	return builderError{err}
}

// buildScalar converts a tree.TypedExpr to an Expr tree.
func buildScalar(pexpr tree.TypedExpr, evalCtx *tree.EvalContext) (_ *Expr, err error) {
	// We use panics in buildScalar code because it makes the code less tedious;
	// buildScalar doesn't alter global state so catching panics is safe.
	defer func() {
		if r := recover(); r != nil {
			if bldErr, ok := r.(builderError); ok {
				err = bldErr
			} else {
				panic(r)
			}
		}
	}()
	buildCtx := makeBuildContext(context.TODO(), evalCtx)
	return buildCtx.buildScalar(pexpr), nil
}

// BuildScalarExpr converts a TypedExpr to a *Expr tree and normalizes it.
func BuildScalarExpr(typedExpr tree.TypedExpr, evalCtx *tree.EvalContext) (*Expr, error) {
	if typedExpr == nil {
		return nil, nil
	}
	e, err := buildScalar(typedExpr, evalCtx)
	if err != nil {
		return nil, err
	}
	normalizeExpr(e)
	return e, nil
}
