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

package distsqlrun

import (
	"context"
	"errors"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

func TestJoinReader(t *testing.T) {
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
	v := [10]tree.Datum{}
	for i := range v {
		v[i] = tree.NewDInt(tree.DInt(i))
	}

	sqlutils.CreateTable(t, sqlDB, "t",
		"a INT, b INT, sum INT, s STRING, PRIMARY KEY (a,b), INDEX bs (b,s)",
		99,
		sqlutils.ToRowFn(aFn, bFn, sumFn, sqlutils.RowEnglishFn))

	td := sqlbase.GetTableDescriptor(kvDB, "test", "t")

	testCases := []struct {
		description string
		post        PostProcessSpec
		onExpr      string
		input       [][]tree.Datum
		lookupCols  columns
		outputTypes []sqlbase.ColumnType
		expected    string
	}{
		{
			description: "Test selecting rows using the primary index",
			post: PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1, 2},
			},
			input: [][]tree.Datum{
				{aFn(2), bFn(2)},
				{aFn(5), bFn(5)},
				{aFn(10), bFn(10)},
				{aFn(15), bFn(15)},
			},
			outputTypes: threeIntCols,
			expected:    "[[0 2 2] [0 5 5] [1 0 1] [1 5 6]]",
		},
		{
			description: "Test selecting columns from second table",
			post: PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1, 4},
			},
			input: [][]tree.Datum{
				{aFn(2), bFn(2)},
				{aFn(5), bFn(5)},
				{aFn(10), bFn(10)},
				{aFn(15), bFn(15)},
			},
			lookupCols:  []uint32{0, 1},
			outputTypes: threeIntCols,
			expected:    "[[0 2 2] [0 5 5] [1 0 1] [1 5 6]]",
		},
		{
			description: "Test duplicates in the input of lookup joins",
			post: PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1, 3},
			},
			input: [][]tree.Datum{
				{aFn(2), bFn(2)},
				{aFn(2), bFn(2)},
				{aFn(5), bFn(5)},
				{aFn(10), bFn(10)},
				{aFn(15), bFn(15)},
			},
			lookupCols:  []uint32{0, 1},
			outputTypes: threeIntCols,
			expected:    "[[0 2 2] [0 2 2] [0 5 5] [1 0 0] [1 5 5]]",
		},
		{
			description: "Test selecting rows using the primary index in lookup join",
			post: PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1, 4},
			},
			input: [][]tree.Datum{
				{aFn(2), bFn(2)},
				{aFn(5), bFn(5)},
				{aFn(10), bFn(10)},
				{aFn(15), bFn(15)},
			},
			lookupCols:  []uint32{0, 1},
			outputTypes: threeIntCols,
			expected:    "[[0 2 2] [0 5 5] [1 0 1] [1 5 6]]",
		},
		{
			description: "Test a filter in the post process spec and using a secondary index",
			post: PostProcessSpec{
				Filter:        Expression{Expr: "@3 <= 5"}, // sum <= 5
				Projection:    true,
				OutputColumns: []uint32{3},
			},
			input: [][]tree.Datum{
				{aFn(1), bFn(1)},
				{aFn(25), bFn(25)},
				{aFn(5), bFn(5)},
				{aFn(21), bFn(21)},
				{aFn(34), bFn(34)},
				{aFn(13), bFn(13)},
				{aFn(51), bFn(51)},
				{aFn(50), bFn(50)},
			},
			outputTypes: []sqlbase.ColumnType{strType},
			expected:    "[['one'] ['five'] ['two-one'] ['one-three'] ['five-zero']]",
		},
		{
			description: "Test lookup join with onExpr",
			post: PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1, 4},
			},
			input: [][]tree.Datum{
				{aFn(2), bFn(2)},
				{aFn(5), bFn(5)},
				{aFn(10), bFn(10)},
				{aFn(15), bFn(15)},
			},
			lookupCols:  []uint32{0, 1},
			outputTypes: threeIntCols,
			onExpr:      "@2 < @5",
			expected:    "[[1 0 1] [1 5 6]]",
		},
	}
	for _, c := range testCases {
		t.Run(c.description, func(t *testing.T) {
			st := cluster.MakeTestingClusterSettings()
			evalCtx := tree.MakeTestingEvalContext(st)
			defer evalCtx.Stop(context.Background())
			flowCtx := FlowCtx{
				Ctx:      context.Background(),
				EvalCtx:  evalCtx,
				Settings: st,
				txn:      client.NewTxn(s.DB(), s.NodeID(), client.RootTxn),
			}

			encRows := make(sqlbase.EncDatumRows, len(c.input))
			for rowIdx, row := range c.input {
				encRow := make(sqlbase.EncDatumRow, len(row))
				for i, d := range row {
					encRow[i] = sqlbase.DatumToEncDatum(intType, d)
				}
				encRows[rowIdx] = encRow
			}
			in := NewRowBuffer(twoIntCols, encRows, RowBufferArgs{})

			out := &RowBuffer{}
			jr, err := newJoinReader(
				&flowCtx,
				&JoinReaderSpec{Table: *td, LookupColumns: c.lookupCols, OnExpr: Expression{Expr: c.onExpr}},
				in,
				&c.post,
				out,
			)
			if err != nil {
				t.Fatal(err)
			}

			jr.Run(nil)

			if !in.Done {
				t.Fatal("joinReader didn't consume all the rows")
			}
			if !out.ProducerClosed {
				t.Fatalf("output RowReceiver not closed")
			}

			var res sqlbase.EncDatumRows
			for {
				row := out.NextNoMeta(t)
				if row == nil {
					break
				}
				res = append(res, row)
			}

			if result := res.String(c.outputTypes); result != c.expected {
				t.Errorf("invalid results: %s, expected %s'", result, c.expected)
			}
		})
	}
}

// TestJoinReaderDrain tests various scenarios in which a joinReader's consumer
// is closed.
func TestJoinReaderDrain(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	sqlutils.CreateTable(
		t,
		sqlDB,
		"t",
		"a INT, PRIMARY KEY (a)",
		1, /* numRows */
		sqlutils.ToRowFn(sqlutils.RowIdxFn),
	)
	td := sqlbase.GetTableDescriptor(kvDB, "test", "t")

	evalCtx := tree.MakeTestingEvalContext(s.ClusterSettings())
	defer evalCtx.Stop(context.Background())

	// Run the flow in a snowball trace so that we can test for tracing info.
	tracer := tracing.NewTracer()
	ctx, sp, err := tracing.StartSnowballTrace(context.Background(), tracer, "test flow ctx")
	if err != nil {
		t.Fatal(err)
	}
	defer sp.Finish()

	flowCtx := FlowCtx{
		Ctx:      ctx,
		EvalCtx:  evalCtx,
		Settings: s.ClusterSettings(),
		txn:      client.NewTxn(s.DB(), s.NodeID(), client.LeafTxn),
	}

	encRow := make(sqlbase.EncDatumRow, 1)
	encRow[0] = sqlbase.DatumToEncDatum(intType, tree.NewDInt(1))

	// ConsumerClosed verifies that when a joinReader's consumer is closed, the
	// joinReader finishes gracefully.
	t.Run("ConsumerClosed", func(t *testing.T) {
		in := NewRowBuffer(oneIntCol, sqlbase.EncDatumRows{encRow}, RowBufferArgs{})

		out := &RowBuffer{}
		out.ConsumerClosed()
		jr, err := newJoinReader(&flowCtx, &JoinReaderSpec{Table: *td}, in, &PostProcessSpec{}, out)
		if err != nil {
			t.Fatal(err)
		}
		jr.Run(nil)
	})

	// ConsumerDone verifies that the producer drains properly by checking that
	// metadata coming from the producer is still read when ConsumerDone is
	// called on the consumer.
	t.Run("ConsumerDone", func(t *testing.T) {
		expectedMetaErr := errors.New("dummy")
		in := NewRowBuffer(oneIntCol, nil /* rows */, RowBufferArgs{})
		if status := in.Push(encRow, &ProducerMetadata{Err: expectedMetaErr}); status != NeedMoreRows {
			t.Fatalf("unexpected response: %d", status)
		}

		out := &RowBuffer{}
		out.ConsumerDone()
		jr, err := newJoinReader(&flowCtx, &JoinReaderSpec{Table: *td}, in, &PostProcessSpec{}, out)
		if err != nil {
			t.Fatal(err)
		}
		jr.Run(nil)
		row, meta := out.Next()
		if row != nil {
			t.Fatalf("row was pushed unexpectedly: %s", row.String(oneIntCol))
		}
		if meta.Err != expectedMetaErr {
			t.Fatalf("unexpected error in metadata: %v", meta.Err)
		}

		// Check for trailing metadata.
		var traceSeen, txnMetaSeen bool
		for {
			row, meta = out.Next()
			if row != nil {
				t.Fatalf("row was pushed unexpectedly: %s", row.String(oneIntCol))
			}
			if meta == nil {
				break
			}
			if meta.TraceData != nil {
				traceSeen = true
			}
			if meta.TxnMeta != nil {
				txnMetaSeen = true
			}
		}
		if !traceSeen {
			t.Fatal("missing tracing trailing metadata")
		}
		if !txnMetaSeen {
			t.Fatal("missing txn trailing metadata")
		}
	})
}
