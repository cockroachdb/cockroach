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

package sql

import (
	"go/constant"
	"reflect"
	"testing"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func makeTestPlanner() *planner {
	return makeInternalPlanner("test", nil /* txn */, security.RootUser, &MemoryMetrics{})
}

func TestValues(t *testing.T) {
	defer leaktest.AfterTest(t)()

	p := makeTestPlanner()

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
	asRow := func(datums ...parser.Datum) []parser.Datums {
		return []parser.Datums{datums}
	}

	makeValues := func(tuples ...*parser.Tuple) *parser.ValuesClause {
		return &parser.ValuesClause{Tuples: tuples}
	}
	makeTuple := func(exprs ...parser.Expr) *parser.Tuple {
		return &parser.Tuple{Exprs: exprs}
	}

	testCases := []struct {
		stmt *parser.ValuesClause
		rows []parser.Datums
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
		ctx := context.TODO()
		plan, err := func() (_ planNode, err error) {
			defer func() {
				if r := recover(); r != nil {
					err = errors.Errorf("%v", r)
				}
			}()
			return p.ValuesClause(context.TODO(), tc.stmt, nil)
		}()
		if err == nil != tc.ok {
			t.Errorf("%d: error_expected=%t, but got error %v", i, tc.ok, err)
		}
		if plan != nil {
			defer plan.Close(ctx)
			plan, err = p.optimizePlan(ctx, plan, allColumns(plan))
			if err != nil {
				t.Errorf("%d: unexpected error in optimizePlan: %v", i, err)
				continue
			}
			if err := p.startPlan(ctx, plan); err != nil {
				t.Errorf("%d: unexpected error in Start: %v", i, err)
				continue
			}
			var rows []parser.Datums
			next, err := plan.Next(runParams{ctx: ctx})
			for ; next; next, err = plan.Next(runParams{ctx: ctx}) {
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
		{nil, reflect.TypeOf(types.Unknown)},

		// Bool type.
		{true, reflect.TypeOf(types.Bool)},

		// Primitive Integer types.
		{int(1), reflect.TypeOf(types.Int)},
		{int8(1), reflect.TypeOf(types.Int)},
		{int16(1), reflect.TypeOf(types.Int)},
		{int32(1), reflect.TypeOf(types.Int)},
		{int64(1), reflect.TypeOf(types.Int)},
		{uint(1), reflect.TypeOf(types.Int)},
		{uint8(1), reflect.TypeOf(types.Int)},
		{uint16(1), reflect.TypeOf(types.Int)},
		{uint32(1), reflect.TypeOf(types.Int)},
		{uint64(1), reflect.TypeOf(types.Int)},

		// Primitive Float types.
		{float32(1.0), reflect.TypeOf(types.Float)},
		{float64(1.0), reflect.TypeOf(types.Float)},

		// Decimal type.
		{apd.New(55, 1), reflect.TypeOf(types.Decimal)},

		// String type.
		{"test", reflect.TypeOf(types.String)},

		// Bytes type.
		{[]byte("abc"), reflect.TypeOf(types.Bytes)},

		// Interval and timestamp.
		{time.Duration(1), reflect.TypeOf(types.Interval)},
		{timeutil.Now(), reflect.TypeOf(types.Timestamp)},

		// Primitive type aliases.
		{roachpb.NodeID(1), reflect.TypeOf(types.Int)},
		{sqlbase.ID(1), reflect.TypeOf(types.Int)},
		{floatAlias(1), reflect.TypeOf(types.Float)},
		{boolAlias(true), reflect.TypeOf(types.Bool)},
		{stringAlias("string"), reflect.TypeOf(types.String)},

		// Byte slice aliases.
		{roachpb.Key("key"), reflect.TypeOf(types.Bytes)},
		{roachpb.RKey("key"), reflect.TypeOf(types.Bytes)},
	}

	pinfo := &parser.PlaceholderInfo{}
	for i, tcase := range testCases {
		golangFillQueryArguments(pinfo, []interface{}{tcase.value})
		output, valid := pinfo.Type("1", false)
		if !valid {
			t.Errorf("case %d failed: argument was invalid", i)
			continue
		}
		if a, e := reflect.TypeOf(output), tcase.expectedType; a != e {
			t.Errorf("case %d failed: expected type %s, got %s", i, e.String(), a.String())
		}
	}
}
