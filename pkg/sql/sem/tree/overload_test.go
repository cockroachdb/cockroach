// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import (
	"context"
	"fmt"
	"go/constant"
	"go/token"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

type variadicTestCase struct {
	args    []*types.T
	matches bool
}

type variadicTestData struct {
	name  string
	cases []variadicTestCase
}

func TestVariadicFunctions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testData := map[*VariadicType]variadicTestData{
		{VarType: types.String}: {
			"string...", []variadicTestCase{
				{[]*types.T{types.String}, true},
				{[]*types.T{types.String, types.String}, true},
				{[]*types.T{types.String, types.Unknown}, true},
				{[]*types.T{types.String, types.Unknown, types.String}, true},
				{[]*types.T{types.Int}, false},
			}},
		{FixedTypes: []*types.T{types.Int}, VarType: types.String}: {
			"int, string...", []variadicTestCase{
				{[]*types.T{types.Int}, true},
				{[]*types.T{types.Int, types.String}, true},
				{[]*types.T{types.Int, types.String, types.String}, true},
				{[]*types.T{types.Int, types.Unknown, types.String}, true},
				{[]*types.T{types.String}, false},
			}},
		{FixedTypes: []*types.T{types.Int, types.Bool}, VarType: types.String}: {
			"int, bool, string...", []variadicTestCase{
				{[]*types.T{types.Int}, false},
				{[]*types.T{types.Int, types.Bool}, true},
				{[]*types.T{types.Int, types.Bool, types.String}, true},
				{[]*types.T{types.Int, types.Unknown, types.String}, true},
				{[]*types.T{types.Int, types.Bool, types.String, types.Bool}, false},
				{[]*types.T{types.Int, types.String}, false},
				{[]*types.T{types.Int, types.String, types.String}, false},
				{[]*types.T{types.String}, false},
			}},
	}

	for fn, data := range testData {
		t.Run(fmt.Sprintf("%v", fn), func(t *testing.T) {
			if data.name != fn.String() {
				t.Fatalf("expected name %v, got %v", data.name, fn.String())
			}
		})

		for _, v := range data.cases {
			t.Run(fmt.Sprintf("%v/%v", fn, v), func(t *testing.T) {
				if v.matches {
					if !fn.MatchLen(len(v.args)) {
						t.Fatalf("expected fn %v to matchLen %v", fn, v.args)
					}

					if !fn.Match(v.args) {
						t.Fatalf("expected fn %v to match %v", fn, v.args)
					}
				} else if fn.MatchLen(len(v.args)) && fn.Match(v.args) {
					t.Fatalf("expected fn %v to not match %v", fn, v.args)
				}
			})
		}
	}
}

type testOverload struct {
	paramTypes ArgTypes
	retType    *types.T
	pref       bool
}

func (to *testOverload) params() TypeList {
	return to.paramTypes
}

func (to *testOverload) returnType() ReturnTyper {
	return FixedReturnType(to.retType)
}

func (to testOverload) preferred() bool {
	return to.pref
}

func (to *testOverload) String() string {
	typeNames := make([]string, len(to.paramTypes))
	for i, param := range to.paramTypes {
		typeNames[i] = param.Typ.String()
	}
	return fmt.Sprintf("func(%s) %s", strings.Join(typeNames, ","), to.retType)
}

func makeTestOverload(retType *types.T, params ...*types.T) overloadImpl {
	t := make(ArgTypes, len(params))
	for i := range params {
		t[i].Typ = params[i]
	}
	return &testOverload{
		paramTypes: t,
		retType:    retType,
	}
}

func TestTypeCheckOverloadedExprs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	intConst := func(s string) Expr {
		return NewNumVal(constant.MakeFromLiteral(s, token.INT, 0), s, false /* negative */)
	}
	decConst := func(s string) Expr {
		return NewNumVal(constant.MakeFromLiteral(s, token.FLOAT, 0), s, false /* negative */)
	}
	strConst := func(s string) Expr {
		return &StrVal{s: s}
	}
	plus := func(left, right Expr) Expr {
		return &BinaryExpr{Operator: MakeBinaryOperator(Plus), Left: left, Right: right}
	}
	placeholder := func(id int) *Placeholder {
		return &Placeholder{Idx: PlaceholderIdx(id)}
	}

	unaryIntFn := makeTestOverload(types.Int, types.Int)
	unaryIntFnPref := &testOverload{retType: types.Int, paramTypes: ArgTypes{}, pref: true}
	unaryFloatFn := makeTestOverload(types.Float, types.Float)
	unaryDecimalFn := makeTestOverload(types.Decimal, types.Decimal)
	unaryStringFn := makeTestOverload(types.String, types.String)
	unaryIntervalFn := makeTestOverload(types.Interval, types.Interval)
	unaryTimestampFn := makeTestOverload(types.Timestamp, types.Timestamp)
	binaryIntFn := makeTestOverload(types.Int, types.Int, types.Int)
	binaryFloatFn := makeTestOverload(types.Float, types.Float, types.Float)
	binaryDecimalFn := makeTestOverload(types.Decimal, types.Decimal, types.Decimal)
	binaryStringFn := makeTestOverload(types.String, types.String, types.String)
	binaryTimestampFn := makeTestOverload(types.Timestamp, types.Timestamp, types.Timestamp)
	binaryStringFloatFn1 := makeTestOverload(types.Int, types.String, types.Float)
	binaryStringFloatFn2 := makeTestOverload(types.Float, types.String, types.Float)
	binaryIntDateFn := makeTestOverload(types.Date, types.Int, types.Date)
	binaryArrayIntFn := makeTestOverload(types.Int, types.AnyArray, types.Int)

	// Out-of-band values used below to distinguish error cases.
	unsupported := &testOverload{}
	ambiguous := &testOverload{}
	shouldError := &testOverload{}

	testData := []struct {
		desired          *types.T
		exprs            []Expr
		overloads        []overloadImpl
		expectedOverload overloadImpl
		inBinOp          bool
	}{
		// Unary constants.
		{nil, []Expr{intConst("1")}, []overloadImpl{unaryIntFn, unaryFloatFn}, unaryIntFn, false},
		{nil, []Expr{decConst("1.0")}, []overloadImpl{unaryIntFn, unaryDecimalFn}, unaryDecimalFn, false},
		{nil, []Expr{decConst("1.0")}, []overloadImpl{unaryIntFn, unaryFloatFn}, unsupported, false},
		{nil, []Expr{intConst("1")}, []overloadImpl{unaryIntFn, binaryIntFn}, unaryIntFn, false},
		{nil, []Expr{intConst("1")}, []overloadImpl{unaryFloatFn, unaryStringFn}, unaryFloatFn, false},
		{nil, []Expr{intConst("1")}, []overloadImpl{unaryStringFn, binaryIntFn}, unsupported, false},
		{nil, []Expr{strConst("PT12H2M")}, []overloadImpl{unaryIntervalFn}, unaryIntervalFn, false},
		{nil, []Expr{strConst("PT12H2M")}, []overloadImpl{unaryIntervalFn, unaryStringFn}, unaryStringFn, false},
		{nil, []Expr{strConst("PT12H2M")}, []overloadImpl{unaryIntervalFn, unaryTimestampFn}, unaryIntervalFn, false},
		{nil, []Expr{}, []overloadImpl{unaryIntFn, unaryIntFnPref}, unaryIntFnPref, false},
		{nil, []Expr{}, []overloadImpl{unaryIntFnPref, unaryIntFnPref}, ambiguous, false},
		{nil, []Expr{strConst("PT12H2M")}, []overloadImpl{unaryIntervalFn, unaryIntFn}, unaryIntervalFn, false},
		// Unary unresolved Placeholders.
		{nil, []Expr{placeholder(0)}, []overloadImpl{unaryStringFn, unaryIntFn}, shouldError, false},
		{nil, []Expr{placeholder(0)}, []overloadImpl{unaryStringFn, binaryIntFn}, unaryStringFn, false},
		// Unary values (not constants).
		{nil, []Expr{NewDInt(1)}, []overloadImpl{unaryIntFn, unaryFloatFn}, unaryIntFn, false},
		{nil, []Expr{NewDFloat(1)}, []overloadImpl{unaryIntFn, unaryFloatFn}, unaryFloatFn, false},
		{nil, []Expr{NewDInt(1)}, []overloadImpl{unaryIntFn, binaryIntFn}, unaryIntFn, false},
		{nil, []Expr{NewDInt(1)}, []overloadImpl{unaryFloatFn, unaryStringFn}, unsupported, false},
		{nil, []Expr{NewDString("a")}, []overloadImpl{unaryIntFn, unaryFloatFn}, unsupported, false},
		{nil, []Expr{NewDString("a")}, []overloadImpl{unaryIntFn, unaryStringFn}, unaryStringFn, false},
		// Binary constants.
		{nil, []Expr{intConst("1"), intConst("1")}, []overloadImpl{binaryIntFn, binaryFloatFn, unaryIntFn}, binaryIntFn, false},
		{nil, []Expr{intConst("1"), decConst("1.0")}, []overloadImpl{binaryIntFn, binaryDecimalFn, unaryDecimalFn}, binaryDecimalFn, false},
		{nil, []Expr{strConst("2010-09-28"), strConst("2010-09-29")}, []overloadImpl{binaryTimestampFn}, binaryTimestampFn, false},
		{nil, []Expr{strConst("2010-09-28"), strConst("2010-09-29")}, []overloadImpl{binaryTimestampFn, binaryStringFn}, binaryStringFn, false},
		{nil, []Expr{strConst("2010-09-28"), strConst("2010-09-29")}, []overloadImpl{binaryTimestampFn, binaryIntFn}, binaryTimestampFn, false},
		// Binary unresolved Placeholders.
		{nil, []Expr{placeholder(0), placeholder(1)}, []overloadImpl{binaryIntFn, binaryFloatFn}, shouldError, false},
		{nil, []Expr{placeholder(0), placeholder(1)}, []overloadImpl{binaryIntFn, unaryStringFn}, binaryIntFn, false},
		{nil, []Expr{placeholder(0), NewDString("a")}, []overloadImpl{binaryIntFn, binaryStringFn}, binaryStringFn, false},
		{nil, []Expr{placeholder(0), intConst("1")}, []overloadImpl{binaryIntFn, binaryFloatFn}, binaryIntFn, false},
		{nil, []Expr{placeholder(0), intConst("1")}, []overloadImpl{binaryStringFn, binaryFloatFn}, binaryFloatFn, false},
		// Binary values.
		{nil, []Expr{NewDString("a"), NewDString("b")}, []overloadImpl{binaryStringFn, binaryFloatFn, unaryFloatFn}, binaryStringFn, false},
		{nil, []Expr{NewDString("a"), intConst("1")}, []overloadImpl{binaryStringFn, binaryFloatFn, binaryStringFloatFn1}, binaryStringFloatFn1, false},
		{nil, []Expr{NewDString("a"), NewDInt(1)}, []overloadImpl{binaryStringFn, binaryFloatFn, binaryStringFloatFn1}, unsupported, false},
		{nil, []Expr{NewDString("a"), NewDFloat(1)}, []overloadImpl{binaryStringFn, binaryFloatFn, binaryStringFloatFn1}, binaryStringFloatFn1, false},
		{nil, []Expr{NewDString("a"), NewDFloat(1)}, []overloadImpl{binaryStringFn, binaryFloatFn, binaryStringFloatFn2}, binaryStringFloatFn2, false},
		{nil, []Expr{NewDFloat(1), NewDString("a")}, []overloadImpl{binaryStringFn, binaryFloatFn, binaryStringFloatFn1}, unsupported, false},
		{nil, []Expr{NewDString("a"), NewDFloat(1)}, []overloadImpl{binaryStringFn, binaryFloatFn, binaryStringFloatFn1, binaryStringFloatFn2}, ambiguous, false},
		// Desired type with ambiguity.
		{types.Int, []Expr{intConst("1"), decConst("1.0")}, []overloadImpl{binaryIntFn, binaryDecimalFn, unaryDecimalFn}, binaryIntFn, false},
		{types.Int, []Expr{intConst("1"), NewDFloat(1)}, []overloadImpl{binaryIntFn, binaryFloatFn, unaryFloatFn}, binaryFloatFn, false},
		{types.Int, []Expr{NewDString("a"), NewDFloat(1)}, []overloadImpl{binaryStringFn, binaryFloatFn, binaryStringFloatFn1, binaryStringFloatFn2}, binaryStringFloatFn1, false},
		{types.Float, []Expr{NewDString("a"), NewDFloat(1)}, []overloadImpl{binaryStringFn, binaryFloatFn, binaryStringFloatFn1, binaryStringFloatFn2}, binaryStringFloatFn2, false},
		{types.Float, []Expr{placeholder(0), placeholder(1)}, []overloadImpl{binaryIntFn, binaryFloatFn}, binaryFloatFn, false},
		// Sub-expressions.
		{nil, []Expr{decConst("1.0"), plus(intConst("1"), intConst("2"))}, []overloadImpl{binaryIntFn, binaryDecimalFn}, binaryIntFn, false},
		{nil, []Expr{decConst("1.1"), plus(intConst("1"), intConst("2"))}, []overloadImpl{binaryIntFn, binaryDecimalFn}, shouldError, false},
		{nil, []Expr{NewDFloat(1.1), plus(intConst("1"), intConst("2"))}, []overloadImpl{binaryIntFn, binaryDecimalFn, binaryFloatFn}, binaryFloatFn, false},
		{types.Decimal, []Expr{decConst("1.0"), plus(intConst("1"), intConst("2"))}, []overloadImpl{binaryIntFn, binaryDecimalFn}, binaryIntFn, false},              // Limitation.
		{nil, []Expr{plus(intConst("1"), intConst("2")), plus(decConst("1.1"), decConst("2.2"))}, []overloadImpl{binaryIntFn, binaryDecimalFn}, shouldError, false}, // Limitation.
		{nil, []Expr{plus(decConst("1.1"), decConst("2.2")), plus(intConst("1"), intConst("2"))}, []overloadImpl{binaryIntFn, binaryDecimalFn}, shouldError, false},
		{nil, []Expr{plus(NewDFloat(1.1), NewDFloat(2.2)), plus(intConst("1"), intConst("2"))}, []overloadImpl{binaryIntFn, binaryFloatFn}, binaryFloatFn, false},
		// Homogenous preference.
		{nil, []Expr{NewDInt(1), placeholder(1)}, []overloadImpl{binaryIntFn, binaryIntDateFn}, binaryIntFn, false},
		{nil, []Expr{NewDFloat(1), placeholder(1)}, []overloadImpl{binaryIntFn, binaryIntDateFn}, unsupported, false},
		{nil, []Expr{intConst("1"), placeholder(1)}, []overloadImpl{binaryIntFn, binaryIntDateFn}, binaryIntFn, false},
		{nil, []Expr{decConst("1.0"), placeholder(1)}, []overloadImpl{binaryIntFn, binaryIntDateFn}, unsupported, false}, // Limitation.
		{types.Date, []Expr{NewDInt(1), placeholder(1)}, []overloadImpl{binaryIntFn, binaryIntDateFn}, binaryIntDateFn, false},
		{types.Date, []Expr{NewDFloat(1), placeholder(1)}, []overloadImpl{binaryIntFn, binaryIntDateFn}, unsupported, false},
		{types.Date, []Expr{intConst("1"), placeholder(1)}, []overloadImpl{binaryIntFn, binaryIntDateFn}, binaryIntDateFn, false},
		{types.Date, []Expr{decConst("1.0"), placeholder(1)}, []overloadImpl{binaryIntFn, binaryIntDateFn}, binaryIntDateFn, false},
		// BinOps
		{nil, []Expr{NewDInt(1), DNull}, []overloadImpl{binaryIntFn, binaryIntDateFn}, ambiguous, false},
		{nil, []Expr{NewDInt(1), DNull}, []overloadImpl{binaryIntFn, binaryIntDateFn}, binaryIntFn, true},
		// Verify that we don't return uninitialized typedExprs for a function like
		// array_length where the array argument is a placeholder (#36153).
		{nil, []Expr{placeholder(0), intConst("1")}, []overloadImpl{binaryArrayIntFn}, unsupported, false},
		{nil, []Expr{placeholder(0), intConst("1")}, []overloadImpl{binaryArrayIntFn}, unsupported, true},
	}
	ctx := context.Background()
	for i, d := range testData {
		t.Run(fmt.Sprintf("%v/%v", d.exprs, d.overloads), func(t *testing.T) {
			semaCtx := MakeSemaContext()
			if err := semaCtx.Placeholders.Init(2 /* numPlaceholders */, nil /* typeHints */); err != nil {
				t.Fatal(err)
			}
			desired := types.Any
			if d.desired != nil {
				desired = d.desired
			}
			typedExprs, fns, err := typeCheckOverloadedExprs(
				ctx, &semaCtx, desired, d.overloads, d.inBinOp, d.exprs...,
			)
			assertNoErr := func() {
				if err != nil {
					t.Fatalf("%d: unexpected error returned from overload resolution for exprs %s: %v",
						i, d.exprs, err)
				}
			}
			for _, e := range typedExprs {
				if e == nil {
					t.Errorf("%d: returned uninitialized TypedExpr", i)
				}
			}
			switch d.expectedOverload {
			case shouldError:
				if err == nil {
					t.Errorf("%d: expecting error to be returned from overload resolution for exprs %s",
						i, d.exprs)
				}
			case unsupported:
				assertNoErr()
				if len(fns) > 0 {
					t.Errorf("%d: expected unsupported overload resolution for exprs %s, found %v",
						i, d.exprs, fns)
				}
			case ambiguous:
				assertNoErr()
				if len(fns) < 2 {
					t.Errorf("%d: expected ambiguous overload resolution for exprs %s, found %v",
						i, d.exprs, fns)
				}
			default:
				assertNoErr()
				if len(fns) != 1 || fns[0] != d.expectedOverload {
					t.Errorf("%d: expected overload %s to be chosen when type checking %s, found %v",
						i, d.expectedOverload, d.exprs, fns)
				}
			}
		})
	}
}
