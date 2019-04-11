// Copyright 2019 The Cockroach Authors.
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

package sqlsmith

import (
	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

var (
	scalars, bools             []scalarWeight
	scalarWeights, boolWeights []int
)

func init() {
	scalars = []scalarWeight{
		{10, scalarNoContext(makeAnd)},
		{5, scalarNoContext(makeCaseExpr)},
		{1, scalarNoContext(makeCoalesceExpr)},
		{20, scalarNoContext(makeColRef)},
		{10, scalarNoContext(makeBinOp)},
		{2, scalarNoContext(makeScalarSubquery)},
		{2, scalarNoContext(makeExists)},
		{2, scalarNoContext(makeIn)},
		{2, scalarNoContext(makeStringComparison)},
		{5, scalarNoContext(makeAnd)},
		{5, scalarNoContext(makeOr)},
		{5, scalarNoContext(makeNot)},
		{10, makeFunc},
		{10, func(s *scope, ctx Context, typ types.T, refs colRefs) (tree.TypedExpr, bool) {
			return makeConstExpr(s, typ, refs), true
		}},
	}
	scalarWeights = extractWeights(scalars)

	bools = []scalarWeight{
		{1, scalarNoContext(makeColRef)},
		{1, scalarNoContext(makeAnd)},
		{1, scalarNoContext(makeOr)},
		{1, scalarNoContext(makeNot)},
		{1, scalarNoContext(makeCompareOp)},
		{1, scalarNoContext(makeIn)},
		{1, scalarNoContext(makeStringComparison)},
		{1, func(s *scope, ctx Context, typ types.T, refs colRefs) (tree.TypedExpr, bool) {
			return makeScalar(s, typ, refs), true
		}},
		{1, scalarNoContext(makeExists)},
		{1, makeFunc},
	}
	boolWeights = extractWeights(bools)
}

// TODO(mjibson): remove this and correctly pass around the Context.
func scalarNoContext(fn func(*scope, types.T, colRefs) (tree.TypedExpr, bool)) scalarFn {
	return func(s *scope, ctx Context, t types.T, refs colRefs) (tree.TypedExpr, bool) {
		return fn(s, t, refs)
	}
}

type scalarFn func(*scope, Context, types.T, colRefs) (expr tree.TypedExpr, ok bool)

type scalarWeight struct {
	weight int
	fn     scalarFn
}

func extractWeights(weights []scalarWeight) []int {
	w := make([]int, len(weights))
	for i, s := range weights {
		w[i] = s.weight
	}
	return w
}

// makeScalar attempts to construct a scalar expression of the requested type.
// If it was unsuccessful, it will return false.
func makeScalar(s *scope, typ types.T, refs colRefs) tree.TypedExpr {
	return makeScalarContext(s, emptyCtx, typ, refs)
}

func makeScalarContext(s *scope, ctx Context, typ types.T, refs colRefs) tree.TypedExpr {
	return makeScalarSample(s.schema.scalars, scalars, s, ctx, typ, refs)
}

func makeBoolExpr(s *scope, refs colRefs) tree.TypedExpr {
	return makeBoolExprContext(s, emptyCtx, refs)
}

func makeBoolExprContext(s *scope, ctx Context, refs colRefs) tree.TypedExpr {
	return makeScalarSample(s.schema.bools, bools, s, ctx, types.Bool, refs)
}

func makeScalarSample(
	sampler *WeightedSampler,
	weights []scalarWeight,
	s *scope,
	ctx Context,
	typ types.T,
	refs colRefs,
) tree.TypedExpr {
	// If we are in a GROUP BY, attempt to find an aggregate function.
	if ctx.fnClass == tree.AggregateClass {
		if expr, ok := makeFunc(s, ctx, typ, refs); ok {
			return expr
		}
	}
	if s.canRecurse() {
		for {
			// No need for a retry counter here because makeConstExpr well eventually
			// be called and it always succeeds.
			idx := sampler.Next()
			result, ok := weights[idx].fn(s, ctx, typ, refs)
			if ok {
				return result
			}
		}
	}
	// Sometimes try to find a col ref or a const if there's no columns
	// with a matching type.
	if coin() {
		if expr, ok := makeColRef(s, typ, refs); ok {
			return expr
		}
	}
	return makeConstExpr(s, typ, refs)
}

func makeCaseExpr(s *scope, typ types.T, refs colRefs) (tree.TypedExpr, bool) {
	typ = pickAnyType(typ)
	condition := makeScalar(s, types.Bool, refs)
	trueExpr := makeScalar(s, typ, refs)
	falseExpr := makeScalar(s, typ, refs)
	expr, err := tree.NewTypedCaseExpr(
		nil,
		[]*tree.When{{
			Cond: condition,
			Val:  trueExpr,
		}},
		falseExpr,
		typ,
	)
	return expr, err == nil
}

func makeCoalesceExpr(s *scope, typ types.T, refs colRefs) (tree.TypedExpr, bool) {
	typ = pickAnyType(typ)
	firstExpr := makeScalar(s, typ, refs)
	secondExpr := makeScalar(s, typ, refs)
	return tree.NewTypedCoalesceExpr(
		tree.TypedExprs{
			firstExpr,
			secondExpr,
		},
		typ,
	), true
}

func makeConstExpr(s *scope, typ types.T, refs colRefs) tree.TypedExpr {
	typ = pickAnyType(typ)

	var datum tree.Datum
	col, err := sqlbase.DatumTypeToColumnType(typ)
	if err != nil {
		datum = tree.DNull
	} else {
		s.schema.lock.Lock()
		datum = sqlbase.RandDatumWithNullChance(s.schema.rnd, col, 6)
		s.schema.lock.Unlock()
	}

	return datum
}

func makeColRef(s *scope, typ types.T, refs colRefs) (tree.TypedExpr, bool) {
	expr, _, ok := getColRef(s, typ, refs)
	return expr, ok
}

func getColRef(s *scope, typ types.T, refs colRefs) (tree.TypedExpr, *colRef, bool) {
	// Filter by needed type.
	cols := make(colRefs, 0, len(refs))
	for _, c := range refs {
		if typ.SemanticType() == types.ANY || c.typ.Equivalent(typ) {
			cols = append(cols, c)
		}
	}
	if len(cols) == 0 {
		return nil, nil, false
	}
	col := cols[s.schema.rnd.Intn(len(cols))]
	return makeTypedExpr(
		col.item,
		col.typ,
	), col, true
}

// castType tries to wrap expr in a CastExpr. This can be useful for times
// when operators or functions have ambiguous implementations (i.e., string
// or bytes, timestamp or timestamptz) and a cast will inform which one to use.
func castType(expr tree.TypedExpr, typ types.T) tree.TypedExpr {
	t, err := coltypes.DatumTypeToColumnType(typ)
	if err != nil {
		return expr
	}
	return makeTypedExpr(&tree.CastExpr{
		Expr:       expr,
		Type:       t,
		SyntaxMode: tree.CastShort,
	}, typ)
}

func typedParen(expr tree.TypedExpr, typ types.T) tree.TypedExpr {
	return makeTypedExpr(&tree.ParenExpr{Expr: expr}, typ)
}

func makeOr(s *scope, typ types.T, refs colRefs) (tree.TypedExpr, bool) {
	switch typ.SemanticType() {
	case types.BOOL, types.ANY:
	default:
		return nil, false
	}
	left := makeBoolExpr(s, refs)
	right := makeBoolExpr(s, refs)
	return typedParen(tree.NewTypedAndExpr(left, right), types.Bool), true
}

func makeAnd(s *scope, typ types.T, refs colRefs) (tree.TypedExpr, bool) {
	switch typ.SemanticType() {
	case types.BOOL, types.ANY:
	default:
		return nil, false
	}
	left := makeBoolExpr(s, refs)
	right := makeBoolExpr(s, refs)
	return typedParen(tree.NewTypedOrExpr(left, right), types.Bool), true
}

func makeNot(s *scope, typ types.T, refs colRefs) (tree.TypedExpr, bool) {
	switch typ.SemanticType() {
	case types.BOOL, types.ANY:
	default:
		return nil, false
	}
	expr := makeBoolExpr(s, refs)
	return typedParen(tree.NewTypedNotExpr(expr), types.Bool), true
}

// TODO(mjibson): add the other operators somewhere.
var compareOps = [...]tree.ComparisonOperator{
	tree.EQ,
	tree.LT,
	tree.GT,
	tree.LE,
	tree.GE,
	tree.NE,
	tree.IsDistinctFrom,
	tree.IsNotDistinctFrom,
}

func makeCompareOp(s *scope, typ types.T, refs colRefs) (tree.TypedExpr, bool) {
	typ = pickAnyType(typ)
	op := compareOps[s.schema.rnd.Intn(len(compareOps))]
	left := makeScalar(s, typ, refs)
	right := makeScalar(s, typ, refs)
	return typedParen(tree.NewTypedComparisonExpr(op, left, right), typ), true
}

func makeBinOp(s *scope, typ types.T, refs colRefs) (tree.TypedExpr, bool) {
	typ = pickAnyType(typ)
	ops := operators[typ.Oid()]
	if len(ops) == 0 {
		return nil, false
	}
	op := ops[s.schema.rnd.Intn(len(ops))]
	left := makeScalar(s, op.LeftType, refs)
	right := makeScalar(s, op.RightType, refs)
	return typedParen(
		&tree.BinaryExpr{
			Operator: op.Operator,
			// Cast both of these to prevent ambiguity in execution choice.
			Left:  castType(left, op.LeftType),
			Right: castType(right, op.RightType),
		},
		typ,
	), true
}

func makeFunc(s *scope, ctx Context, typ types.T, refs colRefs) (tree.TypedExpr, bool) {
	typ = pickAnyType(typ)
	fns := functions[ctx.fnClass][typ.Oid()]
	if len(fns) == 0 {
		return nil, false
	}
	fn := fns[s.schema.rnd.Intn(len(fns))]

	args := make(tree.TypedExprs, 0)
	for _, argTyp := range fn.overload.Types.Types() {
		var arg tree.TypedExpr
		// If we're a GROUP BY, try to choose a col ref for the arguments.
		if ctx.fnClass == tree.AggregateClass {
			var ok bool
			arg, ok = makeColRef(s, argTyp, refs)
			if !ok {
				// If we can't find a col ref for our
				// aggregate function, try again with
				// a non-aggregate.
				return makeFunc(s, emptyCtx, typ, refs)
			}
		}
		if arg == nil {
			arg = makeScalar(s, argTyp, refs)
		}
		args = append(args, castType(arg, argTyp))
	}

	// Cast the return and arguments to prevent ambiguity during function
	// implementation choosing.
	return castType(tree.NewTypedFuncExpr(
		tree.ResolvableFunctionReference{FunctionReference: fn.def},
		0, /* aggQualifier */
		args,
		nil, /* filter */
		nil, /* windowDef */
		typ,
		&fn.def.FunctionProperties,
		fn.overload,
	), typ), true
}

func makeExists(s *scope, typ types.T, refs colRefs) (tree.TypedExpr, bool) {
	switch typ.SemanticType() {
	case types.BOOL, types.ANY:
	default:
		return nil, false
	}

	selectStmt, _, ok := s.makeSelect(makeDesiredTypes(), refs)
	if !ok {
		return nil, false
	}

	subq := &tree.Subquery{
		Select: &tree.ParenSelect{Select: selectStmt},
		Exists: true,
	}
	subq.SetType(types.Bool)
	return subq, true
}

func makeIn(s *scope, typ types.T, refs colRefs) (tree.TypedExpr, bool) {
	switch typ.SemanticType() {
	case types.BOOL, types.ANY:
	default:
		return nil, false
	}

	t := getRandType()
	var rhs tree.TypedExpr
	if coin() {
		rhs = makeTuple(s, t, refs)
	} else {
		selectStmt, _, ok := s.makeSelect([]types.T{t}, refs)
		if !ok {
			return nil, false
		}
		// This sometimes produces `SELECT NULL ...`. Cast the
		// first expression so IN succeeds.
		clause := selectStmt.Select.(*tree.SelectClause)
		coltype, err := coltypes.DatumTypeToColumnType(t)
		if err != nil {
			return nil, false
		}
		clause.Exprs[0].Expr = &tree.CastExpr{
			Expr:       clause.Exprs[0].Expr,
			Type:       coltype,
			SyntaxMode: tree.CastShort,
		}
		subq := &tree.Subquery{
			Select: &tree.ParenSelect{Select: selectStmt},
		}
		subq.SetType(types.TTuple{Types: []types.T{t}})
		rhs = subq
	}
	op := tree.In
	if coin() {
		op = tree.NotIn
	}
	return tree.NewTypedComparisonExpr(
		op,
		// Cast any NULLs to a concrete type.
		castType(makeScalar(s, t, refs), t),
		rhs,
	), true
}

func makeStringComparison(s *scope, typ types.T, refs colRefs) (tree.TypedExpr, bool) {
	switch typ.SemanticType() {
	case types.BOOL, types.ANY:
	default:
		return nil, false
	}
	return tree.NewTypedComparisonExpr(
		s.schema.randStringComparison(),
		makeScalar(s, types.String, refs),
		makeScalar(s, types.String, refs),
	), true
}

func makeTuple(s *scope, typ types.T, refs colRefs) *tree.Tuple {
	exprs := make(tree.Exprs, s.schema.rnd.Intn(5))
	for i := range exprs {
		exprs[i] = makeScalar(s, typ, refs)
	}
	return tree.NewTypedTuple(types.TTuple{Types: []types.T{typ}}, exprs)
}

func makeScalarSubquery(s *scope, typ types.T, refs colRefs) (tree.TypedExpr, bool) {
	selectStmt, _, ok := s.makeSelect([]types.T{typ}, refs)
	if !ok {
		return nil, false
	}
	selectStmt.Limit = &tree.Limit{Count: tree.NewDInt(1)}

	subq := &tree.Subquery{
		Select: &tree.ParenSelect{Select: selectStmt},
	}
	subq.SetType(typ)

	return subq, true
}
