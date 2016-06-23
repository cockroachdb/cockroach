// Copyright 2015 The Cockroach Authors.
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
// Author: Tobias Schottdorf

package sql

import (
	"go/constant"
	"reflect"
	"testing"
	"time"

	"gopkg.in/inf.v0"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/timeutil"
	"github.com/pkg/errors"
)

func TestValues(t *testing.T) {
	defer leaktest.AfterTest(t)()

	p := makePlanner()

	vInt := int64(5)
	vNum := 3.14159
	vStr := "two furs one cub"
	vBool := true

	unsupp := &parser.RangeCond{}

	intVal := func(v int64) *parser.NumVal {
		return &parser.NumVal{Value: constant.MakeInt64(v)}
	}
	floatVal := func(f float64) *parser.CastExpr {
		return &parser.CastExpr{
			Expr: &parser.NumVal{Value: constant.MakeFloat64(f)},
			Type: &parser.FloatColType{},
		}
	}
	asRow := func(datums ...parser.Datum) []parser.DTuple {
		return []parser.DTuple{datums}
	}

	makeValues := func(tuples ...*parser.Tuple) *parser.ValuesClause {
		return &parser.ValuesClause{Tuples: tuples}
	}
	makeTuple := func(exprs ...parser.Expr) *parser.Tuple {
		return &parser.Tuple{Exprs: exprs}
	}

	testCases := []struct {
		stmt *parser.ValuesClause
		rows []parser.DTuple
		ok   bool
	}{
		{
			makeValues(makeTuple(intVal(vInt))),
			asRow(parser.NewDInt(parser.DInt(vInt))),
			true,
		},
		{
			makeValues(makeTuple(intVal(vInt), intVal(vInt))),
			asRow(parser.NewDInt(parser.DInt(vInt)), parser.NewDInt(parser.DInt(vInt))),
			true,
		},
		{
			makeValues(makeTuple(floatVal(vNum))),
			asRow(parser.NewDFloat(parser.DFloat(vNum))),
			true,
		},
		{
			makeValues(makeTuple(parser.NewDString(vStr))),
			asRow(parser.NewDString(vStr)),
			true,
		},
		{
			makeValues(makeTuple(parser.NewDBytes(parser.DBytes(vStr)))),
			asRow(parser.NewDBytes(parser.DBytes(vStr))),
			true,
		},
		{
			makeValues(makeTuple(parser.MakeDBool(parser.DBool(vBool)))),
			asRow(parser.MakeDBool(parser.DBool(vBool))),
			true,
		},
		{
			makeValues(makeTuple(unsupp)),
			nil,
			false,
		},
	}

	for i, tc := range testCases {
		plan, err := func() (_ planNode, err error) {
			defer func() {
				if r := recover(); r != nil {
					err = errors.Errorf("%v", r)
				}
			}()
			return p.ValuesClause(tc.stmt, nil)
		}()
		if err == nil != tc.ok {
			t.Errorf("%d: error_expected=%t, but got error %v", i, tc.ok, err)
		}
		if plan != nil {
			if err := plan.expandPlan(); err != nil {
				t.Errorf("%d: unexpected error in expandPlan: %v", i, err)
				continue
			}
			if err := plan.Start(); err != nil {
				t.Errorf("%d: unexpected error in Start: %v", i, err)
				continue
			}
			var rows []parser.DTuple
			next, err := plan.Next()
			for ; next; next, err = plan.Next() {
				rows = append(rows, plan.Values())
			}
			if err != nil {
				t.Error(err)
				continue
			}
			if !reflect.DeepEqual(rows, tc.rows) {
				t.Errorf("%d: expected rows:\n%+v\nactual rows:\n%+v", i, tc.rows, rows)
			}
		}
	}
}

type floatAlias float32
type boolAlias bool
type stringAlias string

func TestGolangQueryArgs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Each test case pairs an arbitrary value and parser.Datum which has the same
	// type
	testCases := []struct {
		value        interface{}
		expectedType reflect.Type
	}{
		// Null type.
		{nil, reflect.TypeOf(parser.DNull)},

		// Bool type.
		{true, reflect.TypeOf(parser.TypeBool)},

		// Primitive Integer types.
		{int(1), reflect.TypeOf(parser.TypeInt)},
		{int8(1), reflect.TypeOf(parser.TypeInt)},
		{int16(1), reflect.TypeOf(parser.TypeInt)},
		{int32(1), reflect.TypeOf(parser.TypeInt)},
		{int64(1), reflect.TypeOf(parser.TypeInt)},
		{uint(1), reflect.TypeOf(parser.TypeInt)},
		{uint8(1), reflect.TypeOf(parser.TypeInt)},
		{uint16(1), reflect.TypeOf(parser.TypeInt)},
		{uint32(1), reflect.TypeOf(parser.TypeInt)},
		{uint64(1), reflect.TypeOf(parser.TypeInt)},

		// Primitive Float types.
		{float32(1.0), reflect.TypeOf(parser.TypeFloat)},
		{float64(1.0), reflect.TypeOf(parser.TypeFloat)},

		// Decimal type.
		{inf.NewDec(55, 1), reflect.TypeOf(parser.TypeDecimal)},

		// String type.
		{"test", reflect.TypeOf(parser.TypeString)},

		// Bytes type.
		{[]byte("abc"), reflect.TypeOf(parser.TypeBytes)},

		// Interval and timestamp.
		{time.Duration(1), reflect.TypeOf(parser.TypeInterval)},
		{timeutil.Now(), reflect.TypeOf(parser.TypeTimestamp)},

		// Primitive type aliases.
		{roachpb.NodeID(1), reflect.TypeOf(parser.TypeInt)},
		{sqlbase.ID(1), reflect.TypeOf(parser.TypeInt)},
		{floatAlias(1), reflect.TypeOf(parser.TypeFloat)},
		{boolAlias(true), reflect.TypeOf(parser.TypeBool)},
		{stringAlias("string"), reflect.TypeOf(parser.TypeString)},

		// Byte slice aliases.
		{roachpb.Key("key"), reflect.TypeOf(parser.TypeBytes)},
		{roachpb.RKey("key"), reflect.TypeOf(parser.TypeBytes)},
	}

	pinfo := &parser.PlaceholderInfo{}
	for i, tcase := range testCases {
		golangFillQueryArguments(pinfo, []interface{}{tcase.value})
		output, valid := pinfo.Type("1")
		if !valid {
			t.Errorf("case %d failed: argument was invalid", i)
			continue
		}
		if a, e := reflect.TypeOf(output), tcase.expectedType; a != e {
			t.Errorf("case %d failed: expected type %s, got %s", i, e.String(), a.String())
		}
	}
}
