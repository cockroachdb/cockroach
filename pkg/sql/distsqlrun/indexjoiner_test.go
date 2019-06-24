// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package distsqlrun

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
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
	defer s.Stopper().Stop(context.TODO())

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

	td := sqlbase.GetTableDescriptor(kvDB, "test", "t")

	v := [10]sqlbase.EncDatum{}
	for i := range v {
		v[i] = sqlbase.IntEncDatum(i)
	}

	testCases := []struct {
		description string
		post        distsqlpb.PostProcessSpec
		input       sqlbase.EncDatumRows
		outputTypes []types.T
		expected    sqlbase.EncDatumRows
	}{
		{
			description: "Test selecting rows using the primary index",
			post: distsqlpb.PostProcessSpec{
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
			post: distsqlpb.PostProcessSpec{
				Filter:        distsqlpb.Expression{Expr: "@3 <= 5"}, // sum <= 5
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
			outputTypes: []types.T{*types.String},
			expected: sqlbase.EncDatumRows{
				{sqlbase.StrEncDatum("one")},
				{sqlbase.StrEncDatum("five")},
				{sqlbase.StrEncDatum("two-one")},
				{sqlbase.StrEncDatum("one-three")},
				{sqlbase.StrEncDatum("five-zero")},
			},
		},
	}

	for _, c := range testCases {
		t.Run(c.description, func(t *testing.T) {
			spec := distsqlpb.JoinReaderSpec{
				Table:    *td,
				IndexIdx: 0,
			}
			txn := client.NewTxn(context.Background(), s.DB(), s.NodeID(), client.RootTxn)
			runProcessorTest(
				t,
				distsqlpb.ProcessorCoreUnion{JoinReader: &spec},
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
