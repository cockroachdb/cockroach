// Copyright 2016 The Cockroach Authors.
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
//
// Author: Nathan VanBenschoten (nvanbenschoten@gmail.com)

package parser

import (
	"fmt"
	"go/constant"
	"go/token"
	"strings"
	"testing"
)

type testOverload struct {
	paramTypes ArgTypes
	retType    Type
	pref       bool
}

func (to *testOverload) params() typeList {
	return to.paramTypes
}

func (to *testOverload) returnType() returnTyper {
	return fixedReturnType(to.retType)
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

func makeTestOverload(retType Type, params ...Type) overloadImpl {
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
	intConst := func(s string) Expr {
		return &NumVal{Value: constant.MakeFromLiteral(s, token.INT, 0), OrigString: s}
	}
	decConst := func(s string) Expr {
		return &NumVal{Value: constant.MakeFromLiteral(s, token.FLOAT, 0), OrigString: s}
	}
	strConst := func(s string) Expr {
		return &StrVal{s: s}
	}
	plus := func(left, right Expr) Expr {
		return &BinaryExpr{Operator: Plus, Left: left, Right: right}
	}

	unaryIntFn := makeTestOverload(TypeInt, TypeInt)
	unaryIntFnPref := &testOverload{retType: TypeInt, paramTypes: ArgTypes{}, pref: true}
	unaryFloatFn := makeTestOverload(TypeFloat, TypeFloat)
	unaryDecimalFn := makeTestOverload(TypeDecimal, TypeDecimal)
	unaryStringFn := makeTestOverload(TypeString, TypeString)
	unaryIntervalFn := makeTestOverload(TypeInterval, TypeInterval)
	unaryTimestampFn := makeTestOverload(TypeTimestamp, TypeTimestamp)
	binaryIntFn := makeTestOverload(TypeInt, TypeInt, TypeInt)
	binaryFloatFn := makeTestOverload(TypeFloat, TypeFloat, TypeFloat)
	binaryDecimalFn := makeTestOverload(TypeDecimal, TypeDecimal, TypeDecimal)
	binaryStringFn := makeTestOverload(TypeString, TypeString, TypeString)
	binaryTimestampFn := makeTestOverload(TypeTimestamp, TypeTimestamp, TypeTimestamp)
	binaryStringFloatFn1 := makeTestOverload(TypeInt, TypeString, TypeFloat)
	binaryStringFloatFn2 := makeTestOverload(TypeFloat, TypeString, TypeFloat)
	binaryIntDateFn := makeTestOverload(TypeDate, TypeInt, TypeDate)

	// Out-of-band values used below to distinguish error cases.
	unsupported := &testOverload{}
	ambiguous := &testOverload{}
	shouldError := &testOverload{}

	testData := []struct {
		desired          Type
		exprs            []Expr
		overloads        []overloadImpl
		expectedOverload overloadImpl
	}{
		// Unary constants.
		{nil, []Expr{intConst("1")}, []overloadImpl{unaryIntFn, unaryFloatFn}, unaryIntFn},
		{nil, []Expr{decConst("1.0")}, []overloadImpl{unaryIntFn, unaryDecimalFn}, unaryDecimalFn},
		{nil, []Expr{decConst("1.0")}, []overloadImpl{unaryIntFn, unaryFloatFn}, unsupported},
		{nil, []Expr{intConst("1")}, []overloadImpl{unaryIntFn, binaryIntFn}, unaryIntFn},
		{nil, []Expr{intConst("1")}, []overloadImpl{unaryFloatFn, unaryStringFn}, unaryFloatFn},
		{nil, []Expr{intConst("1")}, []overloadImpl{unaryStringFn, binaryIntFn}, unsupported},
		{nil, []Expr{strConst("PT12H2M")}, []overloadImpl{unaryIntervalFn}, unaryIntervalFn},
		{nil, []Expr{strConst("PT12H2M")}, []overloadImpl{unaryIntervalFn, unaryStringFn}, unaryStringFn},
		{nil, []Expr{strConst("PT12H2M")}, []overloadImpl{unaryIntervalFn, unaryTimestampFn}, unsupported}, // Limitation.
		{nil, []Expr{}, []overloadImpl{unaryIntFn, unaryIntFnPref}, unaryIntFnPref},
		{nil, []Expr{}, []overloadImpl{unaryIntFnPref, unaryIntFnPref}, ambiguous},
		{nil, []Expr{strConst("PT12H2M")}, []overloadImpl{unaryIntervalFn, unaryIntFn}, unsupported}, // Limitation.
		// Unary unresolved Placeholders.
		{nil, []Expr{NewPlaceholder("a")}, []overloadImpl{unaryStringFn, unaryIntFn}, shouldError},
		{nil, []Expr{NewPlaceholder("a")}, []overloadImpl{unaryStringFn, binaryIntFn}, unaryStringFn},
		// Unary values (not constants).
		{nil, []Expr{NewDInt(1)}, []overloadImpl{unaryIntFn, unaryFloatFn}, unaryIntFn},
		{nil, []Expr{NewDFloat(1)}, []overloadImpl{unaryIntFn, unaryFloatFn}, unaryFloatFn},
		{nil, []Expr{NewDInt(1)}, []overloadImpl{unaryIntFn, binaryIntFn}, unaryIntFn},
		{nil, []Expr{NewDInt(1)}, []overloadImpl{unaryFloatFn, unaryStringFn}, unsupported},
		{nil, []Expr{NewDString("a")}, []overloadImpl{unaryIntFn, unaryFloatFn}, unsupported},
		{nil, []Expr{NewDString("a")}, []overloadImpl{unaryIntFn, unaryStringFn}, unaryStringFn},
		// Binary constants.
		{nil, []Expr{intConst("1"), intConst("1")}, []overloadImpl{binaryIntFn, binaryFloatFn, unaryIntFn}, binaryIntFn},
		{nil, []Expr{intConst("1"), decConst("1.0")}, []overloadImpl{binaryIntFn, binaryDecimalFn, unaryDecimalFn}, binaryDecimalFn},
		{nil, []Expr{strConst("2010-09-28"), strConst("2010-09-29")}, []overloadImpl{binaryTimestampFn}, binaryTimestampFn},
		{nil, []Expr{strConst("2010-09-28"), strConst("2010-09-29")}, []overloadImpl{binaryTimestampFn, binaryStringFn}, binaryStringFn},
		{nil, []Expr{strConst("2010-09-28"), strConst("2010-09-29")}, []overloadImpl{binaryTimestampFn, binaryIntFn}, unsupported}, // Limitation.
		// Binary unresolved Placeholders.
		{nil, []Expr{NewPlaceholder("a"), NewPlaceholder("b")}, []overloadImpl{binaryIntFn, binaryFloatFn}, shouldError},
		{nil, []Expr{NewPlaceholder("a"), NewPlaceholder("b")}, []overloadImpl{binaryIntFn, unaryStringFn}, binaryIntFn},
		{nil, []Expr{NewPlaceholder("a"), NewDString("a")}, []overloadImpl{binaryIntFn, binaryStringFn}, binaryStringFn},
		{nil, []Expr{NewPlaceholder("a"), intConst("1")}, []overloadImpl{binaryIntFn, binaryFloatFn}, binaryIntFn},
		{nil, []Expr{NewPlaceholder("a"), intConst("1")}, []overloadImpl{binaryStringFn, binaryFloatFn}, binaryFloatFn},
		// Binary values.
		{nil, []Expr{NewDString("a"), NewDString("b")}, []overloadImpl{binaryStringFn, binaryFloatFn, unaryFloatFn}, binaryStringFn},
		{nil, []Expr{NewDString("a"), intConst("1")}, []overloadImpl{binaryStringFn, binaryFloatFn, binaryStringFloatFn1}, binaryStringFloatFn1},
		{nil, []Expr{NewDString("a"), NewDInt(1)}, []overloadImpl{binaryStringFn, binaryFloatFn, binaryStringFloatFn1}, unsupported},
		{nil, []Expr{NewDString("a"), NewDFloat(1)}, []overloadImpl{binaryStringFn, binaryFloatFn, binaryStringFloatFn1}, binaryStringFloatFn1},
		{nil, []Expr{NewDString("a"), NewDFloat(1)}, []overloadImpl{binaryStringFn, binaryFloatFn, binaryStringFloatFn2}, binaryStringFloatFn2},
		{nil, []Expr{NewDFloat(1), NewDString("a")}, []overloadImpl{binaryStringFn, binaryFloatFn, binaryStringFloatFn1}, unsupported},
		{nil, []Expr{NewDString("a"), NewDFloat(1)}, []overloadImpl{binaryStringFn, binaryFloatFn, binaryStringFloatFn1, binaryStringFloatFn2}, ambiguous},
		// Desired type with ambiguity.
		{TypeInt, []Expr{intConst("1"), decConst("1.0")}, []overloadImpl{binaryIntFn, binaryDecimalFn, unaryDecimalFn}, binaryIntFn},
		{TypeInt, []Expr{intConst("1"), NewDFloat(1)}, []overloadImpl{binaryIntFn, binaryFloatFn, unaryFloatFn}, binaryFloatFn},
		{TypeInt, []Expr{NewDString("a"), NewDFloat(1)}, []overloadImpl{binaryStringFn, binaryFloatFn, binaryStringFloatFn1, binaryStringFloatFn2}, binaryStringFloatFn1},
		{TypeFloat, []Expr{NewDString("a"), NewDFloat(1)}, []overloadImpl{binaryStringFn, binaryFloatFn, binaryStringFloatFn1, binaryStringFloatFn2}, binaryStringFloatFn2},
		{TypeFloat, []Expr{NewPlaceholder("a"), NewPlaceholder("b")}, []overloadImpl{binaryIntFn, binaryFloatFn}, binaryFloatFn},
		// Sub-expressions.
		{nil, []Expr{decConst("1.0"), plus(intConst("1"), intConst("2"))}, []overloadImpl{binaryIntFn, binaryDecimalFn}, binaryIntFn},
		{nil, []Expr{decConst("1.1"), plus(intConst("1"), intConst("2"))}, []overloadImpl{binaryIntFn, binaryDecimalFn}, shouldError},
		{nil, []Expr{NewDFloat(1.1), plus(intConst("1"), intConst("2"))}, []overloadImpl{binaryIntFn, binaryDecimalFn, binaryFloatFn}, binaryFloatFn},
		{TypeDecimal, []Expr{decConst("1.0"), plus(intConst("1"), intConst("2"))}, []overloadImpl{binaryIntFn, binaryDecimalFn}, binaryIntFn},                // Limitation.
		{nil, []Expr{plus(intConst("1"), intConst("2")), plus(decConst("1.1"), decConst("2.2"))}, []overloadImpl{binaryIntFn, binaryDecimalFn}, shouldError}, // Limitation.
		{nil, []Expr{plus(decConst("1.1"), decConst("2.2")), plus(intConst("1"), intConst("2"))}, []overloadImpl{binaryIntFn, binaryDecimalFn}, shouldError},
		{nil, []Expr{plus(NewDFloat(1.1), NewDFloat(2.2)), plus(intConst("1"), intConst("2"))}, []overloadImpl{binaryIntFn, binaryFloatFn}, binaryFloatFn},
		// Homogenous preference.
		{nil, []Expr{NewDInt(1), NewPlaceholder("b")}, []overloadImpl{binaryIntFn, binaryIntDateFn}, binaryIntFn},
		{nil, []Expr{NewDFloat(1), NewPlaceholder("b")}, []overloadImpl{binaryIntFn, binaryIntDateFn}, unsupported},
		{nil, []Expr{intConst("1"), NewPlaceholder("b")}, []overloadImpl{binaryIntFn, binaryIntDateFn}, binaryIntFn},
		{nil, []Expr{decConst("1.0"), NewPlaceholder("b")}, []overloadImpl{binaryIntFn, binaryIntDateFn}, unsupported}, // Limitation.
		{TypeDate, []Expr{NewDInt(1), NewPlaceholder("b")}, []overloadImpl{binaryIntFn, binaryIntDateFn}, binaryIntDateFn},
		{TypeDate, []Expr{NewDFloat(1), NewPlaceholder("b")}, []overloadImpl{binaryIntFn, binaryIntDateFn}, unsupported},
		{TypeDate, []Expr{intConst("1"), NewPlaceholder("b")}, []overloadImpl{binaryIntFn, binaryIntDateFn}, binaryIntDateFn},
		{TypeDate, []Expr{decConst("1.0"), NewPlaceholder("b")}, []overloadImpl{binaryIntFn, binaryIntDateFn}, binaryIntDateFn},
	}
	for i, d := range testData {
		t.Run(fmt.Sprintf("%v/%v", d.exprs, d.overloads), func(t *testing.T) {
			ctx := MakeSemaContext()
			desired := TypeAny
			if d.desired != nil {
				desired = d.desired
			}
			_, fns, err := typeCheckOverloadedExprs(&ctx, desired, d.overloads, d.exprs...)
			assertNoErr := func() {
				if err != nil {
					t.Fatalf("%d: unexpected error returned from overload resolution for exprs %s: %v",
						i, d.exprs, err)
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
