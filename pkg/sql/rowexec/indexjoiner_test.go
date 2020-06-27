// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// This file doesn't live next to execinfra/indexjoiner.go in order to use
// unexported method runProcessorsTest.
// TODO(yuzefovich): move this test file next to indexjoiner.go.

package rowexec

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestIndexJoiner(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	// Create a table where each row is:
	//
	//  |     a    |     b    |         sum         |         s           |
	//  |-----------------------------------------------------------------|
	//  | rowId/10 | rowId%10 | rowId/10 + rowId%10 | IntToEnglish(rowId) |

	aFn := func(row int) tree.Datum {
		return tree.NewDInt(tree.DInt(row / 10))
	}
	bFn := func(row int) tree.Datum {
		return tree.NewDInt(tree.DInt(row % 10))
	}
	sumFn := func(row int) tree.Datum {
		return tree.NewDInt(tree.DInt(row/10 + row%10))
	}

	sqlutils.CreateTable(t, sqlDB, "t",
		"a INT, b INT, sum INT, s STRING, PRIMARY KEY (a,b), INDEX bs (b,s)",
		99,
		sqlutils.ToRowFn(aFn, bFn, sumFn, sqlutils.RowEnglishFn))

	sqlutils.CreateTable(t, sqlDB, "t2",
		"a INT, b INT, sum INT, s STRING, PRIMARY KEY (a,b), FAMILY f1 (a, b), FAMILY f2 (s), FAMILY f3 (sum), INDEX bs (b,s)",
		99,
		sqlutils.ToRowFn(aFn, bFn, sumFn, sqlutils.RowEnglishFn))

	td := sqlbase.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "t")
	tdf := sqlbase.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "t2")

	v := [10]sqlbase.EncDatum{}
	for i := range v {
		v[i] = sqlbase.IntEncDatum(i)
	}

	testCases := []struct {
		description string
		desc        *sqlbase.TableDescriptor
		post        execinfrapb.PostProcessSpec
		input       sqlbase.EncDatumRows
		outputTypes []*types.T
		expected    sqlbase.EncDatumRows
	}{
		{
			description: "Test selecting rows using the primary index",
			desc:        td,
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1, 2},
			},
			input: sqlbase.EncDatumRows{
				{v[0], v[2]},
				{v[0], v[5]},
				{v[1], v[0]},
				{v[1], v[5]},
			},
			outputTypes: sqlbase.ThreeIntCols,
			expected: sqlbase.EncDatumRows{
				{v[0], v[2], v[2]},
				{v[0], v[5], v[5]},
				{v[1], v[0], v[1]},
				{v[1], v[5], v[6]},
			},
		},
		{
			description: "Test a filter in the post process spec and using a secondary index",
			desc:        td,
			post: execinfrapb.PostProcessSpec{
				Filter:        execinfrapb.Expression{Expr: "@3 <= 5"}, // sum <= 5
				Projection:    true,
				OutputColumns: []uint32{3},
			},
			input: sqlbase.EncDatumRows{
				{v[0], v[1]},
				{v[2], v[5]},
				{v[0], v[5]},
				{v[2], v[1]},
				{v[3], v[4]},
				{v[1], v[3]},
				{v[5], v[1]},
				{v[5], v[0]},
			},
			outputTypes: []*types.T{types.String},
			expected: sqlbase.EncDatumRows{
				{sqlbase.StrEncDatum("one")},
				{sqlbase.StrEncDatum("five")},
				{sqlbase.StrEncDatum("two-one")},
				{sqlbase.StrEncDatum("one-three")},
				{sqlbase.StrEncDatum("five-zero")},
			},
		},
		{
			description: "Test selecting rows using the primary index with multiple family spans",
			desc:        tdf,
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1, 2},
			},
			input: sqlbase.EncDatumRows{
				{v[0], v[2]},
				{v[0], v[5]},
				{v[1], v[0]},
				{v[1], v[5]},
			},
			outputTypes: sqlbase.ThreeIntCols,
			expected: sqlbase.EncDatumRows{
				{v[0], v[2], v[2]},
				{v[0], v[5], v[5]},
				{v[1], v[0], v[1]},
				{v[1], v[5], v[6]},
			},
		},
	}

	for _, c := range testCases {
		t.Run(c.description, func(t *testing.T) {
			spec := execinfrapb.JoinReaderSpec{
				Table:    *c.desc,
				IndexIdx: 0,
			}
			txn := kv.NewTxn(context.Background(), s.DB(), s.NodeID())
			runProcessorTest(
				t,
				execinfrapb.ProcessorCoreUnion{JoinReader: &spec},
				c.post,
				sqlbase.TwoIntCols,
				c.input,
				c.outputTypes,
				c.expected,
				txn,
			)
		})
	}
}
