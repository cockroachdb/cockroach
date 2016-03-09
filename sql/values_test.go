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
	"fmt"
	"reflect"
	"testing"
	"time"

	"gopkg.in/inf.v0"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/timeutil"
)

func TestValues(t *testing.T) {
	defer leaktest.AfterTest(t)()

	p := makePlanner()

	vInt := int64(5)
	vNum := 3.14159
	vStr := "two furs one cub"
	vBool := true

	unsupp := &parser.RangeCond{}

	intVal := func(v int64) *parser.IntVal {
		return &parser.IntVal{Val: v}
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
			asRow(parser.DInt(vInt)),
			true,
		},
		{
			makeValues(makeTuple(intVal(vInt), intVal(vInt))),
			asRow(parser.DInt(vInt), parser.DInt(vInt)),
			true,
		},
		{
			makeValues(makeTuple(parser.NumVal(fmt.Sprintf("%0.5f", vNum)))),
			asRow(parser.DFloat(vNum)),
			true,
		},
		{
			makeValues(makeTuple(parser.DString(vStr))),
			asRow(parser.DString(vStr)),
			true,
		},
		{
			makeValues(makeTuple(parser.DBytes(vStr))),
			asRow(parser.DBytes(vStr)),
			true,
		},
		{
			makeValues(makeTuple(parser.DBool(vBool))),
			asRow(parser.DBool(vBool)),
			true,
		},
		{
			makeValues(makeTuple(unsupp)),
			nil,
			false,
		},
	}

	for i, tc := range testCases {
		plan, pErr := func() (_ planNode, pErr *roachpb.Error) {
			defer func() {
				if r := recover(); r != nil {
					pErr = roachpb.NewErrorf("%v", r)
				}
			}()
			return p.ValuesClause(tc.stmt)
		}()
		if pErr == nil != tc.ok {
			t.Errorf("%d: error_expected=%t, but got error %v", i, tc.ok, pErr)
		}
		if plan != nil {
			var rows []parser.DTuple
			for plan.Next() {
				rows = append(rows, plan.Values())
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

func TestGolangParams(t *testing.T) {
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
		{true, reflect.TypeOf(parser.DummyBool)},

		// Primitive Integer types.
		{int(1), reflect.TypeOf(parser.DummyInt)},
		{int8(1), reflect.TypeOf(parser.DummyInt)},
		{int16(1), reflect.TypeOf(parser.DummyInt)},
		{int32(1), reflect.TypeOf(parser.DummyInt)},
		{int64(1), reflect.TypeOf(parser.DummyInt)},
		{uint(1), reflect.TypeOf(parser.DummyInt)},
		{uint8(1), reflect.TypeOf(parser.DummyInt)},
		{uint16(1), reflect.TypeOf(parser.DummyInt)},
		{uint32(1), reflect.TypeOf(parser.DummyInt)},
		{uint64(1), reflect.TypeOf(parser.DummyInt)},

		// Primitive Float types.
		{float32(1.0), reflect.TypeOf(parser.DummyFloat)},
		{float64(1.0), reflect.TypeOf(parser.DummyFloat)},

		// Decimal type.
		{inf.NewDec(55, 1), reflect.TypeOf(parser.DummyDecimal)},

		// String type.
		{"test", reflect.TypeOf(parser.DummyString)},

		// Bytes type.
		{[]byte("abc"), reflect.TypeOf(parser.DummyBytes)},

		// Interval and timestamp.
		{time.Duration(1), reflect.TypeOf(parser.DummyInterval)},
		{timeutil.Now(), reflect.TypeOf(parser.DummyTimestamp)},

		// Primitive type aliases.
		{roachpb.NodeID(1), reflect.TypeOf(parser.DummyInt)},
		{ID(1), reflect.TypeOf(parser.DummyInt)},
		{floatAlias(1), reflect.TypeOf(parser.DummyFloat)},
		{boolAlias(true), reflect.TypeOf(parser.DummyBool)},
		{stringAlias("string"), reflect.TypeOf(parser.DummyString)},

		// Byte slice aliases.
		{roachpb.Key("key"), reflect.TypeOf(parser.DummyBytes)},
		{roachpb.RKey("key"), reflect.TypeOf(parser.DummyBytes)},
	}

	for i, tcase := range testCases {
		params := golangParameters([]interface{}{tcase.value})
		output, valid := params.Arg("1")
		if !valid {
			t.Errorf("case %d failed: argument was invalid", i)
			continue
		}
		if a, e := reflect.TypeOf(output), tcase.expectedType; a != e {
			t.Errorf("case %d failed: expected type %s, got %s", i, e.String(), a.String())
		}
	}
}
