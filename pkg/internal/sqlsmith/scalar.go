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
		{5, makeCaseExpr},
		{1, makeCoalesceExpr},
		{20, makeColRef},
		{10, makeBinOp},
		{10, makeFunc},
		{2, makeScalarSubquery},
		{2, makeExists},
		{10, func(s *scope, typ types.T, refs colRefs) (tree.TypedExpr, bool) {
			return makeConstExpr(s, typ, refs), true
		}},
	}
	scalarWeights = extractWeights(scalars)

	bools = []scalarWeight{
		{3, makeBinOp},
		{2, func(s *scope, typ types.T, refs colRefs) (tree.TypedExpr, bool) {
			return makeScalar(s, typ, refs), true
		}},
		{1, makeExists},
	}
	boolWeights = extractWeights(bools)
}

type scalarWeight struct {
	weight int
	fn     func(*scope, types.T, colRefs) (expr tree.TypedExpr, ok bool)
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
	return makeScalarSample(s.schema.scalars, scalars, s, typ, refs)
}

func makeBoolExpr(s *scope, refs colRefs) tree.TypedExpr {
	return makeScalarSample(s.schema.bools, bools, s, types.Bool, refs)
}

func makeScalarSample(
	sampler *WeightedSampler, weights []scalarWeight, s *scope, typ types.T, refs colRefs,
) tree.TypedExpr {
	if s.canRecurse() {
		for {
			// No need for a retry counter here because makeConstExpr well eventually
			// be called and it always succeeds.
			idx := sampler.Next()
			result, ok := weights[idx].fn(s, typ, refs)
			if ok {
				return result
			}
		}
	}
	// Try to find a col ref or a const if there's no columns with a matching type.
	if expr, ok := makeColRef(s, typ, refs); ok {
		return expr
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
		if typ == types.Any || c.typ == typ {
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

func makeBinOp(s *scope, typ types.T, refs colRefs) (tree.TypedExpr, bool) {
	if typ == types.Any {
		typ = getRandType()
	}
	ops := operators[typ.Oid()]
	if len(ops) == 0 {
		return nil, false
	}
	op := ops[s.schema.rnd.Intn(len(ops))]
	left := makeScalar(s, op.LeftType, refs)
	right := makeScalar(s, op.RightType, refs)
	return &tree.ParenExpr{
		Expr: &tree.BinaryExpr{
			Operator: op.Operator,
			// Cast both of these to prevent ambiguity in execution choice.
			Left:  castType(left, op.LeftType),
			Right: castType(right, op.RightType),
		},
	}, true
}

func makeFunc(s *scope, typ types.T, refs colRefs) (tree.TypedExpr, bool) {
	if typ == types.Any {
		typ = getRandType()
	}
	fns := functions[typ.Oid()]
	if len(fns) == 0 {
		return nil, false
	}
	fn := fns[s.schema.rnd.Intn(len(fns))]

	args := make(tree.TypedExprs, 0)
	for _, typ := range fn.overload.Types.Types() {
		args = append(args, castType(makeScalar(s, typ, refs), typ))
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
	if typ != types.Bool || typ != types.Any {
		return nil, false
	}

	selectStmt, _, ok := s.makeSelect(nil, refs)
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
