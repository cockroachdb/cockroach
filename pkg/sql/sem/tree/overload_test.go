// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

import (
	"context"
	"fmt"
	"go/constant"
	"go/token"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treebin"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/lib/pq/oid"
	"github.com/stretchr/testify/require"
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
	paramTypes ParamTypes
	retType    *types.T
	OverloadPreference
}

func (to *testOverload) params() TypeList {
	return to.paramTypes
}

func (to *testOverload) returnType() ReturnTyper {
	return FixedReturnType(to.retType)
}

func (to testOverload) preference() OverloadPreference {
	return to.OverloadPreference
}

func (to *testOverload) outParamInfo() (RoutineType, []int32, TypeList) {
	return BuiltinRoutine, nil, nil
}

func (to *testOverload) defaultExprs() Exprs {
	return nil
}

func (to testOverload) preferred() *testOverload {
	to.OverloadPreference = OverloadPreferencePreferred
	return &to
}

func (to *testOverload) String() string {
	typeNames := make([]string, len(to.paramTypes))
	for i, param := range to.paramTypes {
		typeNames[i] = param.Typ.String()
	}
	return fmt.Sprintf("func(%s) %s", strings.Join(typeNames, ","), to.retType)
}

func makeTestOverload(retType *types.T, params ...*types.T) *testOverload {
	t := make(ParamTypes, len(params))
	for i := range params {
		t[i].Typ = params[i]
	}
	return &testOverload{
		paramTypes: t,
		retType:    retType,
	}
}

// overloadImpls implements overloadSet
type overloadImpls []overloadImpl

func (o overloadImpls) len() int               { return len(o) }
func (o overloadImpls) get(i int) overloadImpl { return o[i] }

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
		return &BinaryExpr{Operator: treebin.MakeBinaryOperator(treebin.Plus), Left: left, Right: right}
	}
	placeholder := func(id int) *Placeholder {
		return &Placeholder{Idx: PlaceholderIdx(id)}
	}

	unaryIntFn := makeTestOverload(types.Int, types.Int)
	unaryIntFnPref := makeTestOverload(types.Int, types.Int).preferred()
	unaryFloatFn := makeTestOverload(types.Float, types.Float)
	unaryDecimalFn := makeTestOverload(types.Decimal, types.Decimal)
	unaryStringFn := makeTestOverload(types.String, types.String)
	unaryIntervalFn := makeTestOverload(types.Interval, types.Interval)
	unaryTimestampFn := makeTestOverload(types.Timestamp, types.Timestamp)
	binaryIntFn := makeTestOverload(types.Int, types.Int, types.Int)
	binaryIntFnPref := makeTestOverload(types.Int, types.Int, types.Int).preferred()
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
		{nil, []Expr{intConst("1")}, []overloadImpl{unaryIntFn, unaryIntFn}, ambiguous, false},
		{nil, []Expr{intConst("1")}, []overloadImpl{unaryIntFn, unaryFloatFn}, unaryIntFn, false},
		{nil, []Expr{decConst("1.0")}, []overloadImpl{unaryIntFn, unaryDecimalFn}, unaryDecimalFn, false},
		{nil, []Expr{decConst("1.0")}, []overloadImpl{unaryIntFn, unaryFloatFn}, ambiguous, false},
		{nil, []Expr{intConst("1")}, []overloadImpl{unaryIntFn, binaryIntFn}, unaryIntFn, false},
		{nil, []Expr{intConst("1")}, []overloadImpl{unaryFloatFn, unaryStringFn}, unaryFloatFn, false},
		{nil, []Expr{intConst("1")}, []overloadImpl{unaryStringFn, binaryIntFn}, unsupported, false},
		{nil, []Expr{strConst("PT12H2M")}, []overloadImpl{unaryIntervalFn}, unaryIntervalFn, false},
		{nil, []Expr{strConst("PT12H2M")}, []overloadImpl{unaryIntervalFn, unaryStringFn}, unaryStringFn, false},
		{nil, []Expr{strConst("PT12H2M")}, []overloadImpl{unaryIntervalFn, unaryTimestampFn}, unaryIntervalFn, false},
		{nil, []Expr{intConst("1")}, []overloadImpl{unaryIntFn, unaryIntFnPref}, unaryIntFnPref, false},
		{nil, []Expr{intConst("1")}, []overloadImpl{unaryIntFnPref, unaryIntFnPref}, ambiguous, false},
		{nil, []Expr{strConst("PT12H2M")}, []overloadImpl{unaryIntervalFn, unaryIntFn}, unaryIntervalFn, false},
		// Unary unresolved Placeholders.
		{nil, []Expr{placeholder(0)}, []overloadImpl{unaryStringFn, unaryIntFn}, shouldError, false},
		{nil, []Expr{placeholder(0)}, []overloadImpl{unaryStringFn, binaryIntFn}, unaryStringFn, false},
		{nil, []Expr{placeholder(0)}, []overloadImpl{unaryStringFn, unaryIntFnPref}, unaryIntFnPref, false},
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
		{nil, []Expr{intConst("1"), intConst("1")}, []overloadImpl{binaryIntFn, binaryFloatFn, binaryIntFnPref}, binaryIntFnPref, false},
		{nil, []Expr{intConst("1"), intConst("1")}, []overloadImpl{binaryIntFn, binaryIntFnPref, binaryIntFnPref}, ambiguous, false},
		// Binary unresolved Placeholders.
		{nil, []Expr{placeholder(0), placeholder(1)}, []overloadImpl{binaryIntFn, binaryFloatFn}, shouldError, false},
		{nil, []Expr{placeholder(0), placeholder(1)}, []overloadImpl{binaryIntFn, unaryStringFn}, binaryIntFn, false},
		{nil, []Expr{placeholder(0), placeholder(1)}, []overloadImpl{binaryIntFnPref, binaryFloatFn}, binaryIntFnPref, false},
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
		{nil, []Expr{decConst("1.0"), placeholder(1)}, []overloadImpl{binaryIntFn, binaryIntDateFn}, ambiguous, false}, // Limitation.
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
			semaCtx := MakeSemaContext(nil /* resolver */)
			semaCtx.Placeholders.Init(2 /* numPlaceholders */, nil /* typeHints */)
			desired := types.AnyElement
			if d.desired != nil {
				desired = d.desired
			}
			s := getOverloadTypeChecker(overloadImpls(d.overloads), d.exprs...)
			defer s.release()

			err := s.typeCheckOverloadedExprs(
				ctx, &semaCtx, desired, d.inBinOp,
			)
			typedExprs := s.typedExprs
			var fns []overloadImpl
			for _, idx := range s.overloadIdxs {
				fns = append(fns, s.overloads[idx])
			}
			assertNoErr := func() {
				if err != nil {
					t.Fatalf("%d: unexpected error returned from overload resolution for exprs %s: %v",
						i, d.exprs, err)
				}
			}
			if err == nil {
				for _, e := range typedExprs {
					if e == nil {
						t.Errorf("%d: returned uninitialized TypedExpr", i)
					}
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

func TestGetMostSignificantOverload(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	makeSearchPath := func(schemas []string) SearchPath {
		path := sessiondata.MakeSearchPath(schemas)
		return &path
	}
	returnTyper := FixedReturnType(types.Int)
	newOverload := func(schema string, oid oid.Oid) QualifiedOverload {
		return QualifiedOverload{Schema: schema, Overload: &Overload{Oid: oid, Types: ParamTypes{}, ReturnType: returnTyper}}
	}

	testCases := []struct {
		testName    string
		overloads   []QualifiedOverload
		searchPath  SearchPath
		expectedOID int
		expectedErr string
	}{
		{
			testName: "empty search path",
			overloads: []QualifiedOverload{
				newOverload("sc1", 1),
			},
			searchPath:  EmptySearchPath,
			expectedOID: 1,
		},
		{
			testName: "empty search path but ambiguous",
			overloads: []QualifiedOverload{
				{Schema: "sc1", Overload: &Overload{Oid: 1, Types: ParamTypes{}, ReturnType: returnTyper}},
				{Schema: "sc1", Overload: &Overload{Oid: 2, Types: ParamTypes{}, ReturnType: returnTyper}},
			},
			searchPath:  EmptySearchPath,
			expectedErr: "ambiguous call",
		},
		{
			testName: "no udf overload",
			overloads: []QualifiedOverload{
				{Schema: "pg_catalog", Overload: &Overload{Oid: 1, Types: ParamTypes{}, ReturnType: returnTyper}},
			},
			searchPath:  makeSearchPath([]string{"sc3", "sc2", "sc1", "pg_catalog"}),
			expectedOID: 1,
		},
		{
			testName: "no udf overload but ambiguous",
			overloads: []QualifiedOverload{
				{Schema: "pg_catalog", Overload: &Overload{Oid: 1, Types: ParamTypes{}, ReturnType: returnTyper}},
				{Schema: "pg_catalog", Overload: &Overload{Oid: 2, Types: ParamTypes{}, ReturnType: returnTyper}},
			},
			searchPath:  makeSearchPath([]string{"sc3", "sc2", "sc1", "pg_catalog"}),
			expectedErr: "ambiguous call",
		},
		{
			testName: "overloads from all schemas in path",
			overloads: []QualifiedOverload{
				{Schema: "sc1", Overload: &Overload{Oid: 1, Type: UDFRoutine, Types: ParamTypes{}, ReturnType: returnTyper}},
				{Schema: "sc2", Overload: &Overload{Oid: 2, Type: UDFRoutine, Types: ParamTypes{}, ReturnType: returnTyper}},
				{Schema: "sc3", Overload: &Overload{Oid: 3, Type: UDFRoutine, Types: ParamTypes{}, ReturnType: returnTyper}},
			},
			searchPath:  makeSearchPath([]string{"sc3", "sc2", "sc1"}),
			expectedOID: 3,
		},
		{
			testName: "overloads from all schemas in path but ambiguous",
			overloads: []QualifiedOverload{
				{Schema: "sc1", Overload: &Overload{Oid: 1, Type: UDFRoutine, Types: ParamTypes{}, ReturnType: returnTyper}},
				{Schema: "sc2", Overload: &Overload{Oid: 2, Type: UDFRoutine, Types: ParamTypes{}, ReturnType: returnTyper}},
				{Schema: "sc3", Overload: &Overload{Oid: 3, Type: UDFRoutine, Types: ParamTypes{}, ReturnType: returnTyper}},
				{Schema: "sc3", Overload: &Overload{Oid: 4, Type: UDFRoutine, Types: ParamTypes{}, ReturnType: returnTyper}},
			},
			searchPath:  makeSearchPath([]string{"sc3", "sc2", "sc1"}),
			expectedErr: "ambiguous call",
		},
		{
			testName: "overloads from some schemas in path",
			overloads: []QualifiedOverload{
				{Schema: "sc1", Overload: &Overload{Oid: 1, Type: UDFRoutine, Types: ParamTypes{}, ReturnType: returnTyper}},
				{Schema: "sc2", Overload: &Overload{Oid: 2, Type: UDFRoutine, Types: ParamTypes{}, ReturnType: returnTyper}},
				{Schema: "sc2", Overload: &Overload{Oid: 3, Type: UDFRoutine, Types: ParamTypes{}, ReturnType: returnTyper}},
			},
			searchPath:  makeSearchPath([]string{"sc3", "sc1", "sc2"}),
			expectedOID: 1,
		},
		{
			testName: "overloads from some schemas in path but ambiguous",
			overloads: []QualifiedOverload{
				{Schema: "sc1", Overload: &Overload{Oid: 1, Type: UDFRoutine, Types: ParamTypes{}, ReturnType: returnTyper}},
				{Schema: "sc2", Overload: &Overload{Oid: 2, Type: UDFRoutine, Types: ParamTypes{}, ReturnType: returnTyper}},
				{Schema: "sc2", Overload: &Overload{Oid: 3, Type: UDFRoutine, Types: ParamTypes{}, ReturnType: returnTyper}},
			},
			searchPath:  makeSearchPath([]string{"sc3", "sc2", "sc1"}),
			expectedErr: "ambiguous call",
		},
		{
			testName: "implicit pg_catalog in path",
			overloads: []QualifiedOverload{
				{Schema: "sc1", Overload: &Overload{Oid: 1, Type: UDFRoutine, Types: ParamTypes{}, ReturnType: returnTyper}},
				{Schema: "sc3", Overload: &Overload{Oid: 2, Type: UDFRoutine, Types: ParamTypes{}, ReturnType: returnTyper}},
				{Schema: "pg_catalog", Overload: &Overload{Oid: 3}},
			},
			searchPath:  makeSearchPath([]string{"sc3", "sc2", "sc1"}),
			expectedOID: 3,
		},
		{
			testName: "explicit pg_catalog in path",
			overloads: []QualifiedOverload{
				{Schema: "sc1", Overload: &Overload{Oid: 1, Type: UDFRoutine, Types: ParamTypes{}, ReturnType: returnTyper}},
				{Schema: "sc3", Overload: &Overload{Oid: 2, Type: UDFRoutine, Types: ParamTypes{}, ReturnType: returnTyper}},
				{Schema: "pg_catalog", Overload: &Overload{Oid: 3}},
			},
			searchPath:  makeSearchPath([]string{"sc3", "sc2", "sc1", "pg_catalog"}),
			expectedOID: 2,
		},
		{
			testName: "unique schema not in path",
			overloads: []QualifiedOverload{
				{Schema: "sc1", Overload: &Overload{Oid: 1, Type: UDFRoutine, Types: ParamTypes{}, ReturnType: returnTyper}},
			},
			searchPath:  makeSearchPath([]string{"sc3", "sc2"}),
			expectedOID: 1,
		},
		{
			testName: "unique schema not in path but ambiguous",
			overloads: []QualifiedOverload{
				{Schema: "sc1", Overload: &Overload{Oid: 1, Type: UDFRoutine, Types: ParamTypes{}, ReturnType: returnTyper}},
				{Schema: "sc1", Overload: &Overload{Oid: 2, Type: UDFRoutine, Types: ParamTypes{}, ReturnType: returnTyper}},
			},
			searchPath:  makeSearchPath([]string{"sc3", "sc2"}),
			expectedErr: "ambiguous call",
		},
		{
			testName: "not unique schema and schema not in path",
			overloads: []QualifiedOverload{
				{Schema: "sc1", Overload: &Overload{Oid: 1, Type: UDFRoutine, Types: ParamTypes{}, ReturnType: returnTyper}},
				{Schema: "sc2", Overload: &Overload{Oid: 2, Type: UDFRoutine, Types: ParamTypes{}, ReturnType: returnTyper}},
			},
			searchPath:  makeSearchPath([]string{"sc3"}),
			expectedErr: "unknown signature",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			expr := FuncExpr{Func: ResolvableFunctionReference{FunctionReference: &ResolvedFunctionDefinition{Name: "some_f"}}}
			impls := make([]overloadImpl, len(tc.overloads))
			filters := make([]uint8, len(tc.overloads))
			for i := range tc.overloads {
				impls[i] = &tc.overloads[i]
				filters[i] = uint8(i)
			}
			overload, err := getMostSignificantOverload(
				tc.overloads, impls, filters, tc.searchPath, &expr, nil, /* typedInputExprs */
				func() string { return "some signature" },
			)
			if tc.expectedErr != "" {
				require.Error(t, err)
				require.True(t, strings.HasPrefix(err.Error(), tc.expectedErr))
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expectedOID, int(overload.Oid))
		})
	}
}
