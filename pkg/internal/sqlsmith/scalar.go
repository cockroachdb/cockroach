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
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// makeScalar attempts to construct a scalar expression of the requested type.
// If it was unsuccessful, it will return false.
func (s *scope) makeScalar(typ types.T, refs colRefs) (tree.TypedExpr, bool) {
	s = s.push()

	pickedType := typ
	if typ == types.Any {
		pickedType = getRandType()
	}

	for i := 0; i < retryCount; i++ {
		var result tree.TypedExpr
		var ok bool
		// TODO(justin): this is how sqlsmith chooses what to do, but it feels
		// to me like there should be a more clean/principled approach here.
		if s.level < d6() && d9() == 1 {
			result, ok = s.makeCaseExpr(pickedType, refs)
		} else if s.level < d6() && d42() == 1 {
			result, ok = s.makeCoalesceExpr(pickedType, refs)
		} else if len(refs) > 0 && d20() > 1 {
			result, _, ok = s.makeColRef(typ, refs)
		} else if s.level < d6() && d9() == 1 {
			result, ok = s.makeBinOp(typ, refs)
		} else if s.level < d6() && d9() == 1 {
			result, ok = s.makeFunc(typ, refs)
		} else if s.level < d6() && d6() == 1 {
			result, ok = s.makeScalarSubquery(typ, refs)
		} else {
			result, ok = s.makeConstExpr(pickedType), true
		}
		if ok {
			return result, ok
		}
	}

	// Retried enough times, give up.
	return nil, false
}

// TODO(justin): sqlsmith separated this out from the general case for
// some reason - I think there must be a clean way to unify the two.
func (s *scope) makeBoolExpr(refs colRefs) (tree.TypedExpr, bool) {
	s = s.push()

	for i := 0; i < retryCount; i++ {
		var result tree.TypedExpr
		var ok bool

		if d6() < 4 {
			result, ok = s.makeBinOp(types.Bool, refs)
		} else if d6() < 4 {
			result, ok = s.makeScalar(types.Bool, refs)
		} else {
			result, ok = s.makeExists(refs)
		}

		if ok {
			return result, ok
		}
	}

	// Retried enough times, give up.
	return nil, false
}

func (s *scope) makeCaseExpr(typ types.T, refs colRefs) (*tree.CaseExpr, bool) {
	condition, ok := s.makeScalar(types.Bool, refs)
	if !ok {
		return nil, false
	}

	trueExpr, ok := s.makeScalar(typ, refs)
	if !ok {
		return nil, false
	}

	falseExpr, ok := s.makeScalar(typ, refs)
	if !ok {
		return nil, false
	}

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

func (s *scope) makeCoalesceExpr(typ types.T, refs colRefs) (tree.TypedExpr, bool) {
	firstExpr, ok := s.makeScalar(typ, refs)
	if !ok {
		return nil, false
	}

	secondExpr, ok := s.makeScalar(typ, refs)
	if !ok {
		return nil, false
	}

	return tree.NewTypedCoalesceExpr(
		tree.TypedExprs{
			firstExpr,
			secondExpr,
		},
		typ,
	), true
}

func (s *scope) makeConstExpr(typ types.T) tree.TypedExpr {
	var datum tree.Datum
	col, err := sqlbase.DatumTypeToColumnType(typ)
	if err != nil {
		datum = tree.DNull
	} else {
		s.schema.lock.Lock()
		datum = sqlbase.RandDatumWithNullChance(s.schema.rnd, col, 6)
		s.schema.lock.Unlock()
	}

	// TODO(justin): maintain context and see if we're in an INSERT, and maybe use
	// DEFAULT (which is a legal "value" in such a context).

	return datum
}

func (s *scope) makeColRef(typ types.T, refs colRefs) (tree.TypedExpr, *colRef, bool) {
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
	col := cols[rand.Intn(len(cols))]
	return makeTypedExpr(
		col.item,
		col.typ,
	), col, true
}

func (s *scope) makeBinOp(typ types.T, refs colRefs) (*tree.BinaryExpr, bool) {
	if typ == types.Any {
		typ = getRandType()
	}
	ops := operators[typ.Oid()]
	if len(ops) == 0 {
		return nil, false
	}
	op := ops[rand.Intn(len(ops))]

	left, ok := s.makeScalar(op.LeftType, refs)
	if !ok {
		return nil, false
	}
	right, ok := s.makeScalar(op.RightType, refs)
	if !ok {
		return nil, false
	}

	return tree.NewTypedBinaryExpr(
		op.Operator,
		left,
		right,
		typ,
	), true
}

func (s *scope) makeFunc(typ types.T, refs colRefs) (tree.TypedExpr, bool) {
	if typ == types.Any {
		typ = getRandType()
	}
	fns := functions[typ.Oid()]
	if len(fns) == 0 {
		return nil, false
	}
	fn := fns[rand.Intn(len(fns))]

	args := make(tree.TypedExprs, 0)
	for _, typ := range fn.overload.Types.Types() {
		in, ok := s.makeScalar(typ, refs)
		if !ok {
			return nil, false
		}
		args = append(args, in)
	}

	return tree.NewTypedFuncExpr(
		tree.ResolvableFunctionReference{FunctionReference: fn.def},
		0, /* aggQualifier */
		args,
		nil, /* filter */
		nil, /* windowDef */
		typ,
		&fn.def.FunctionProperties,
		fn.overload,
	), true
}

func (s *scope) makeExists(refs colRefs) (tree.TypedExpr, bool) {
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

func (s *scope) makeScalarSubquery(typ types.T, refs colRefs) (tree.TypedExpr, bool) {
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
