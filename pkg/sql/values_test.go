// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"fmt"
	"go/constant"
	"reflect"
	"testing"
	"time"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/bitarray"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/pkg/errors"
)

func makeTestPlanner() *planner {
	// Initialize an Executorconfig sufficiently for the purposes of creating a
	// planner.
	var nodeID base.NodeIDContainer
	nodeID.Set(context.TODO(), 1)
	execCfg := ExecutorConfig{
		NodeInfo: NodeInfo{
			NodeID: &nodeID,
			ClusterID: func() uuid.UUID {
				return uuid.MakeV4()
			},
		},
	}

	// TODO(andrei): pass the cleanup along to the caller.
	p, _ /* cleanup */ := newInternalPlanner(
		"test", nil /* txn */, security.RootUser, &MemoryMetrics{}, &execCfg,
	)
	return p
}

func TestValues(t *testing.T) {
	defer leaktest.AfterTest(t)()

	p := makeTestPlanner()

	vInt := int64(5)
	vNum := 3.14159
	vStr := "two furs one cub"
	vBool := true

	unsupp := &tree.RangeCond{}

	intVal := func(v int64) *tree.NumVal {
		return tree.NewNumVal(
			constant.MakeInt64(v),
			"", /* origString */
			false /* negative */)
	}
	floatVal := func(f float64) *tree.CastExpr {
		return &tree.CastExpr{
			Expr: tree.NewNumVal(
				constant.MakeFloat64(f),
				"", /* origString */
				false /* negative */),
			Type: types.Float,
		}
	}
	asRow := func(datums ...tree.Datum) []tree.Datums {
		return []tree.Datums{datums}
	}

	makeValues := func(tuples ...tree.Exprs) *tree.ValuesClause {
		return &tree.ValuesClause{Rows: tuples}
	}
	makeTuple := func(exprs ...tree.Expr) tree.Exprs {
		return tree.Exprs(exprs)
	}

	testCases := []struct {
		stmt *tree.ValuesClause
		rows []tree.Datums
		ok   bool
	}{
		{
			makeValues(makeTuple(intVal(vInt))),
			asRow(tree.NewDInt(tree.DInt(vInt))),
			true,
		},
		{
			makeValues(makeTuple(intVal(vInt), intVal(vInt))),
			asRow(tree.NewDInt(tree.DInt(vInt)), tree.NewDInt(tree.DInt(vInt))),
			true,
		},
		{
			makeValues(makeTuple(floatVal(vNum))),
			asRow(tree.NewDFloat(tree.DFloat(vNum))),
			true,
		},
		{
			makeValues(makeTuple(tree.NewDString(vStr))),
			asRow(tree.NewDString(vStr)),
			true,
		},
		{
			makeValues(makeTuple(tree.NewDBytes(tree.DBytes(vStr)))),
			asRow(tree.NewDBytes(tree.DBytes(vStr))),
			true,
		},
		{
			makeValues(makeTuple(tree.MakeDBool(tree.DBool(vBool)))),
			asRow(tree.MakeDBool(tree.DBool(vBool))),
			true,
		},
		{
			makeValues(makeTuple(unsupp)),
			nil,
			false,
		},
	}

	ctx := context.TODO()
	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			plan, err := func() (_ planNode, err error) {
				defer func() {
					if r := recover(); r != nil {
						err = errors.Errorf("%v", r)
					}
				}()
				return p.Values(context.TODO(), tc.stmt, nil)
			}()
			if plan != nil {
				defer plan.Close(ctx)
			}
			if err == nil != tc.ok {
				t.Errorf("%d: error_expected=%t, but got error %v", i, tc.ok, err)
			}
			if plan == nil {
				return
			}
			params := runParams{ctx: ctx, p: p, extendedEvalCtx: &p.extendedEvalCtx}
			if err := startExec(params, plan); err != nil {
				t.Fatalf("%d: unexpected error in Start: %v", i, err)
			}
			var rows []tree.Datums
			next, err := plan.Next(params)
			for ; next; next, err = plan.Next(params) {
				rows = append(rows, plan.Values())
			}
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(rows, tc.rows) {
				t.Errorf("%d: expected rows:\n%+v\nactual rows:\n%+v", i, tc.rows, rows)
			}
		})
	}
}

type floatAlias float32
type boolAlias bool
type stringAlias string

func TestGolangQueryArgs(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Each test case pairs an arbitrary value and tree.Datum which has the same
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

		// Bit array.
		{bitarray.MakeBitArrayFromInt64(8, 58, 7), reflect.TypeOf(types.VarBit)},
	}

	for i, tcase := range testCases {
		datums := golangFillQueryArguments(tcase.value)
		if len(datums) != 1 {
			t.Fatalf("epected 1 datum, got: %d", len(datums))
		}
		d := datums[0]
		if a, e := reflect.TypeOf(d.ResolvedType()), tcase.expectedType; a != e {
			t.Errorf("case %d failed: expected type %s, got %s", i, e.String(), a.String())
		}
	}
}
