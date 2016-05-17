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
	retType    Datum
}

func (to *testOverload) params() typeList {
	return to.paramTypes
}

func (to *testOverload) returnType() Datum {
	return to.retType
}

func (to *testOverload) String() string {
	typeNames := make([]string, len(to.paramTypes))
	for i, param := range to.paramTypes {
		typeNames[i] = param.Type()
	}
	return fmt.Sprintf("func(%s) %s", strings.Join(typeNames, ","), to.retType.Type())
}

func makeTestOverload(retType Datum, params ...Datum) overloadImpl {
	return &testOverload{
		paramTypes: ArgTypes(params),
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
	plus := func(left, right Expr) Expr {
		return &BinaryExpr{Operator: Plus, Left: left, Right: right}
	}

	unaryIntFn := makeTestOverload(TypeInt, TypeInt)
	unaryFloatFn := makeTestOverload(TypeFloat, TypeFloat)
	unaryDecimalFn := makeTestOverload(TypeDecimal, TypeDecimal)
	unaryStringFn := makeTestOverload(TypeString, TypeString)
	binaryIntFn := makeTestOverload(TypeInt, TypeInt, TypeInt)
	binaryFloatFn := makeTestOverload(TypeFloat, TypeFloat, TypeFloat)
	binaryDecimalFn := makeTestOverload(TypeDecimal, TypeDecimal, TypeDecimal)
	binaryStringFn := makeTestOverload(TypeString, TypeString, TypeString)
	binaryStringFloatFn1 := makeTestOverload(TypeInt, TypeString, TypeFloat)
	binaryStringFloatFn2 := makeTestOverload(TypeFloat, TypeString, TypeFloat)
	binaryIntDateFn := makeTestOverload(TypeDate, TypeInt, TypeDate)

	testData := []struct {
		args             MapArgs
		desired          Datum
		exprs            []Expr
		overloads        []overloadImpl
		expectedOverload overloadImpl
	}{
		// Unary constants.
		{nil, nil, []Expr{intConst("1")}, []overloadImpl{unaryIntFn, unaryFloatFn}, unaryIntFn},
		{nil, nil, []Expr{decConst("1.0")}, []overloadImpl{unaryIntFn, unaryDecimalFn}, unaryDecimalFn},
		{nil, nil, []Expr{decConst("1.0")}, []overloadImpl{unaryIntFn, unaryFloatFn}, nil},
		{nil, nil, []Expr{intConst("1")}, []overloadImpl{unaryIntFn, binaryIntFn}, unaryIntFn},
		{nil, nil, []Expr{intConst("1")}, []overloadImpl{unaryFloatFn, unaryStringFn}, unaryFloatFn},
		{nil, nil, []Expr{intConst("1")}, []overloadImpl{unaryStringFn, binaryIntFn}, nil},
		// Unary unresolved ValArgs.
		{nil, nil, []Expr{ValArg{"a"}}, []overloadImpl{unaryStringFn, unaryIntFn}, nil},
		{nil, nil, []Expr{ValArg{"a"}}, []overloadImpl{unaryStringFn, binaryIntFn}, unaryStringFn},
		// Unary values (not constants).
		{nil, nil, []Expr{NewDInt(1)}, []overloadImpl{unaryIntFn, unaryFloatFn}, unaryIntFn},
		{nil, nil, []Expr{NewDFloat(1)}, []overloadImpl{unaryIntFn, unaryFloatFn}, unaryFloatFn},
		{nil, nil, []Expr{NewDInt(1)}, []overloadImpl{unaryIntFn, binaryIntFn}, unaryIntFn},
		{nil, nil, []Expr{NewDInt(1)}, []overloadImpl{unaryFloatFn, unaryStringFn}, nil},
		{nil, nil, []Expr{NewDString("a")}, []overloadImpl{unaryIntFn, unaryFloatFn}, nil},
		{nil, nil, []Expr{NewDString("a")}, []overloadImpl{unaryIntFn, unaryStringFn}, unaryStringFn},
		// Binary constants.
		{nil, nil, []Expr{intConst("1"), intConst("1")}, []overloadImpl{binaryIntFn, binaryFloatFn, unaryIntFn}, binaryIntFn},
		{nil, nil, []Expr{intConst("1"), decConst("1.0")}, []overloadImpl{binaryIntFn, binaryDecimalFn, unaryDecimalFn}, binaryDecimalFn},
		// Binary unresolved ValArgs.
		{nil, nil, []Expr{ValArg{"a"}, ValArg{"b"}}, []overloadImpl{binaryIntFn, binaryFloatFn}, nil},
		{nil, nil, []Expr{ValArg{"a"}, ValArg{"b"}}, []overloadImpl{binaryIntFn, unaryStringFn}, binaryIntFn},
		{nil, nil, []Expr{ValArg{"a"}, NewDString("a")}, []overloadImpl{binaryIntFn, binaryStringFn}, binaryStringFn},
		{nil, nil, []Expr{ValArg{"a"}, intConst("1")}, []overloadImpl{binaryIntFn, binaryFloatFn}, binaryIntFn},
		{nil, nil, []Expr{ValArg{"a"}, intConst("1")}, []overloadImpl{binaryStringFn, binaryFloatFn}, binaryFloatFn},
		// Binary values.
		{nil, nil, []Expr{NewDString("a"), NewDString("b")}, []overloadImpl{binaryStringFn, binaryFloatFn, unaryFloatFn}, binaryStringFn},
		{nil, nil, []Expr{NewDString("a"), intConst("1")}, []overloadImpl{binaryStringFn, binaryFloatFn, binaryStringFloatFn1}, binaryStringFloatFn1},
		{nil, nil, []Expr{NewDString("a"), NewDInt(1)}, []overloadImpl{binaryStringFn, binaryFloatFn, binaryStringFloatFn1}, nil},
		{nil, nil, []Expr{NewDString("a"), NewDFloat(1)}, []overloadImpl{binaryStringFn, binaryFloatFn, binaryStringFloatFn1}, binaryStringFloatFn1},
		{nil, nil, []Expr{NewDString("a"), NewDFloat(1)}, []overloadImpl{binaryStringFn, binaryFloatFn, binaryStringFloatFn2}, binaryStringFloatFn2},
		{nil, nil, []Expr{NewDFloat(1), NewDString("a")}, []overloadImpl{binaryStringFn, binaryFloatFn, binaryStringFloatFn1}, nil},
		{nil, nil, []Expr{NewDString("a"), NewDFloat(1)}, []overloadImpl{binaryStringFn, binaryFloatFn, binaryStringFloatFn1, binaryStringFloatFn2}, nil},
		// Desired type with ambiguity.
		{nil, TypeInt, []Expr{intConst("1"), decConst("1.0")}, []overloadImpl{binaryIntFn, binaryDecimalFn, unaryDecimalFn}, binaryIntFn},
		{nil, TypeInt, []Expr{intConst("1"), NewDFloat(1)}, []overloadImpl{binaryIntFn, binaryFloatFn, unaryFloatFn}, binaryFloatFn},
		{nil, TypeInt, []Expr{NewDString("a"), NewDFloat(1)}, []overloadImpl{binaryStringFn, binaryFloatFn, binaryStringFloatFn1, binaryStringFloatFn2}, binaryStringFloatFn1},
		{nil, TypeFloat, []Expr{NewDString("a"), NewDFloat(1)}, []overloadImpl{binaryStringFn, binaryFloatFn, binaryStringFloatFn1, binaryStringFloatFn2}, binaryStringFloatFn2},
		{nil, TypeFloat, []Expr{ValArg{"a"}, ValArg{"b"}}, []overloadImpl{binaryIntFn, binaryFloatFn}, binaryFloatFn},
		// Sub-expressions.
		{nil, nil, []Expr{decConst("1.0"), plus(intConst("1"), intConst("2"))}, []overloadImpl{binaryIntFn, binaryDecimalFn}, binaryIntFn},
		{nil, nil, []Expr{decConst("1.1"), plus(intConst("1"), intConst("2"))}, []overloadImpl{binaryIntFn, binaryDecimalFn}, binaryDecimalFn},
		{nil, TypeDecimal, []Expr{decConst("1.0"), plus(intConst("1"), intConst("2"))}, []overloadImpl{binaryIntFn, binaryDecimalFn}, binaryIntFn},        // Limitation.
		{nil, nil, []Expr{plus(intConst("1"), intConst("2")), plus(decConst("1.1"), decConst("2.2"))}, []overloadImpl{binaryIntFn, binaryDecimalFn}, nil}, // Limitation.
		{nil, nil, []Expr{plus(decConst("1.1"), decConst("2.2")), plus(intConst("1"), intConst("2"))}, []overloadImpl{binaryIntFn, binaryDecimalFn}, binaryDecimalFn},
		// Homogenous preference.
		{nil, nil, []Expr{NewDInt(1), ValArg{"b"}}, []overloadImpl{binaryIntFn, binaryIntDateFn}, binaryIntFn},
		{nil, nil, []Expr{NewDFloat(1), ValArg{"b"}}, []overloadImpl{binaryIntFn, binaryIntDateFn}, nil},
		{nil, nil, []Expr{intConst("1"), ValArg{"b"}}, []overloadImpl{binaryIntFn, binaryIntDateFn}, binaryIntFn},
		{nil, nil, []Expr{decConst("1.0"), ValArg{"b"}}, []overloadImpl{binaryIntFn, binaryIntDateFn}, nil}, // Limitation.
		{nil, TypeDate, []Expr{NewDInt(1), ValArg{"b"}}, []overloadImpl{binaryIntFn, binaryIntDateFn}, binaryIntDateFn},
		{nil, TypeDate, []Expr{NewDFloat(1), ValArg{"b"}}, []overloadImpl{binaryIntFn, binaryIntDateFn}, nil},
		{nil, TypeDate, []Expr{intConst("1"), ValArg{"b"}}, []overloadImpl{binaryIntFn, binaryIntDateFn}, binaryIntDateFn},
		{nil, TypeDate, []Expr{decConst("1.0"), ValArg{"b"}}, []overloadImpl{binaryIntFn, binaryIntDateFn}, binaryIntDateFn},
	}
	for i, d := range testData {
		_, fn, err := typeCheckOverloadedExprs(d.args, d.desired, d.overloads, d.exprs...)
		if d.expectedOverload != nil {
			if err != nil {
				t.Errorf("%d: unexpected error returned from typeCheckOverloadedExprs: %v", i, err)
			} else if fn != d.expectedOverload {
				t.Errorf("%d: expected overload %s to be chosen when type checking %s, found %v", i, d.expectedOverload, d.exprs, fn)
			}
		} else if fn != nil {
			t.Errorf("%d: expected no matching overloads to be found when type checking %s, found %s", i, d.exprs, fn)
		}
	}
}
