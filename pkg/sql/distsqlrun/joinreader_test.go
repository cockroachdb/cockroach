// Copyright 2016 The Cockroach Authors.
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
	"errors"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

func TestJoinReader(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

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

	// Insert a row for NULL testing.
	if _, err := sqlDB.Exec("INSERT INTO test.t VALUES (10, 0, NULL, NULL)"); err != nil {
		t.Fatal(err)
	}

	tdSecondary := sqlbase.GetTableDescriptor(kvDB, "test", "t")

	sqlutils.CreateTable(t, sqlDB, "t2",
		"a INT, b INT, sum INT, s STRING, PRIMARY KEY (a,b), FAMILY f1 (a, b), FAMILY f2 (s), FAMILY f3 (sum), INDEX bs (b,s)",
		99,
		sqlutils.ToRowFn(aFn, bFn, sumFn, sqlutils.RowEnglishFn))

	tdFamily := sqlbase.GetTableDescriptor(kvDB, "test", "t2")

	sqlutils.CreateTable(t, sqlDB, "t3parent",
		"a INT PRIMARY KEY",
		0,
		sqlutils.ToRowFn(aFn))

	sqlutils.CreateTableInterleaved(t, sqlDB, "t3",
		"a INT, b INT, sum INT, s STRING, PRIMARY KEY (a,b), INDEX bs (b,s)",
		"t3parent(a)",
		99,
		sqlutils.ToRowFn(aFn, bFn, sumFn, sqlutils.RowEnglishFn))
	tdInterleaved := sqlbase.GetTableDescriptor(kvDB, "test", "t3")

	testCases := []struct {
		description string
		indexIdx    uint32
		post        distsqlpb.PostProcessSpec
		onExpr      string
		input       [][]tree.Datum
		lookupCols  columns
		joinType    sqlbase.JoinType
		inputTypes  []types.T
		outputTypes []types.T
		expected    string
	}{
		{
			description: "Test selecting columns from second table",
			post: distsqlpb.PostProcessSpec{
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
			inputTypes:  sqlbase.TwoIntCols,
			outputTypes: sqlbase.ThreeIntCols,
			expected:    "[[0 2 2] [0 5 5] [1 0 1] [1 5 6]]",
		},
		{
			description: "Test duplicates in the input of lookup joins",
			post: distsqlpb.PostProcessSpec{
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
			inputTypes:  sqlbase.TwoIntCols,
			outputTypes: sqlbase.ThreeIntCols,
			expected:    "[[0 2 2] [0 2 2] [0 5 5] [1 0 0] [1 5 5]]",
		},
		{
			description: "Test lookup join queries with separate families",
			post: distsqlpb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1, 3, 4},
			},
			input: [][]tree.Datum{
				{aFn(2), bFn(2)},
				{aFn(5), bFn(5)},
				{aFn(10), bFn(10)},
				{aFn(15), bFn(15)},
			},
			lookupCols:  []uint32{0, 1},
			inputTypes:  sqlbase.TwoIntCols,
			outputTypes: sqlbase.FourIntCols,
			expected:    "[[0 2 2 2] [0 5 5 5] [1 0 0 1] [1 5 5 6]]",
		},
		{
			description: "Test lookup joins preserve order of left input",
			post: distsqlpb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1, 3},
			},
			input: [][]tree.Datum{
				{aFn(2), bFn(2)},
				{aFn(5), bFn(5)},
				{aFn(2), bFn(2)},
				{aFn(10), bFn(10)},
				{aFn(15), bFn(15)},
			},
			lookupCols:  []uint32{0, 1},
			inputTypes:  sqlbase.TwoIntCols,
			outputTypes: sqlbase.ThreeIntCols,
			expected:    "[[0 2 2] [0 5 5] [0 2 2] [1 0 0] [1 5 5]]",
		},
		{
			description: "Test lookup join with onExpr",
			post: distsqlpb.PostProcessSpec{
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
			inputTypes:  sqlbase.TwoIntCols,
			outputTypes: sqlbase.ThreeIntCols,
			onExpr:      "@2 < @5",
			expected:    "[[1 0 1] [1 5 6]]",
		},
		{
			description: "Test left outer lookup join on primary index",
			post: distsqlpb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1, 4},
			},
			input: [][]tree.Datum{
				{aFn(100), bFn(100)},
				{aFn(2), bFn(2)},
			},
			lookupCols:  []uint32{0, 1},
			joinType:    sqlbase.LeftOuterJoin,
			inputTypes:  sqlbase.TwoIntCols,
			outputTypes: sqlbase.ThreeIntCols,
			expected:    "[[10 0 NULL] [0 2 2]]",
		},
		{
			description: "Test lookup join on secondary index with NULL lookup value",
			indexIdx:    1,
			post: distsqlpb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0},
			},
			input: [][]tree.Datum{
				{tree.NewDInt(0), tree.DNull},
			},
			lookupCols:  []uint32{0, 1},
			inputTypes:  sqlbase.TwoIntCols,
			outputTypes: sqlbase.OneIntCol,
			expected:    "[]",
		},
		{
			description: "Test left outer lookup join on secondary index with NULL lookup value",
			indexIdx:    1,
			post: distsqlpb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 2},
			},
			input: [][]tree.Datum{
				{tree.NewDInt(0), tree.DNull},
			},
			lookupCols:  []uint32{0, 1},
			joinType:    sqlbase.LeftOuterJoin,
			inputTypes:  sqlbase.TwoIntCols,
			outputTypes: sqlbase.TwoIntCols,
			expected:    "[[0 NULL]]",
		},
		{
			description: "Test lookup join on secondary index with an implicit key column",
			indexIdx:    1,
			post: distsqlpb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{2},
			},
			input: [][]tree.Datum{
				{aFn(2), bFn(2), sqlutils.RowEnglishFn(2)},
			},
			lookupCols:  []uint32{1, 2, 0},
			inputTypes:  []types.T{*types.Int, *types.Int, *types.String},
			outputTypes: sqlbase.OneIntCol,
			expected:    "[['two']]",
		},
		{
			description: "Test left semi lookup join",
			indexIdx:    1,
			post: distsqlpb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1},
			},
			input: [][]tree.Datum{
				{tree.NewDInt(tree.DInt(1)), sqlutils.RowEnglishFn(2)},
				{tree.NewDInt(tree.DInt(1)), sqlutils.RowEnglishFn(2)},
				{tree.NewDInt(tree.DInt(1234)), sqlutils.RowEnglishFn(2)},
				{tree.NewDInt(tree.DInt(6)), sqlutils.RowEnglishFn(2)},
				{tree.NewDInt(tree.DInt(7)), sqlutils.RowEnglishFn(2)},
				{tree.NewDInt(tree.DInt(1)), sqlutils.RowEnglishFn(2)},
			},
			lookupCols:  []uint32{0},
			joinType:    sqlbase.LeftSemiJoin,
			inputTypes:  []types.T{*types.Int, *types.String},
			outputTypes: sqlbase.TwoIntCols,
			expected:    "[[1 'two'] [1 'two'] [6 'two'] [7 'two'] [1 'two']]",
		},
		{
			description: "Test left semi lookup join on secondary index with NULL lookup value",
			indexIdx:    1,
			post: distsqlpb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0},
			},
			input: [][]tree.Datum{
				{tree.NewDInt(0), tree.DNull},
			},
			lookupCols:  []uint32{0, 1},
			joinType:    sqlbase.LeftSemiJoin,
			inputTypes:  sqlbase.TwoIntCols,
			outputTypes: sqlbase.OneIntCol,
			expected:    "[]",
		},
		{
			description: "Test left semi lookup join with onExpr",
			indexIdx:    1,
			post: distsqlpb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1},
			},
			input: [][]tree.Datum{
				{tree.NewDInt(tree.DInt(1)), bFn(3)},
				{tree.NewDInt(tree.DInt(1)), bFn(2)},
				{tree.NewDInt(tree.DInt(1234)), bFn(2)},
				{tree.NewDInt(tree.DInt(6)), bFn(2)},
				{tree.NewDInt(tree.DInt(7)), bFn(3)},
				{tree.NewDInt(tree.DInt(1)), bFn(2)},
			},
			lookupCols:  []uint32{0},
			joinType:    sqlbase.LeftSemiJoin,
			onExpr:      "@2 > 2",
			inputTypes:  sqlbase.TwoIntCols,
			outputTypes: sqlbase.TwoIntCols,
			expected:    "[[1 3] [7 3]]",
		},
		{
			description: "Test left anti lookup join",
			indexIdx:    1,
			post: distsqlpb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1},
			},
			input: [][]tree.Datum{
				{tree.NewDInt(tree.DInt(1234)), tree.NewDInt(tree.DInt(1234))},
			},
			lookupCols:  []uint32{0},
			joinType:    sqlbase.LeftAntiJoin,
			inputTypes:  sqlbase.TwoIntCols,
			outputTypes: sqlbase.TwoIntCols,
			expected:    "[[1234 1234]]",
		},
		{
			description: "Test left anti lookup join with onExpr",
			indexIdx:    1,
			post: distsqlpb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1},
			},
			input: [][]tree.Datum{
				{tree.NewDInt(tree.DInt(1)), bFn(3)},
				{tree.NewDInt(tree.DInt(1)), bFn(2)},
				{tree.NewDInt(tree.DInt(6)), bFn(2)},
				{tree.NewDInt(tree.DInt(7)), bFn(3)},
				{tree.NewDInt(tree.DInt(1)), bFn(2)},
			},
			lookupCols:  []uint32{0},
			joinType:    sqlbase.LeftAntiJoin,
			onExpr:      "@2 > 2",
			inputTypes:  sqlbase.TwoIntCols,
			outputTypes: sqlbase.TwoIntCols,
			expected:    "[[1 2] [6 2] [1 2]]",
		},
		{
			description: "Test left anti lookup join with match",
			indexIdx:    1,
			post: distsqlpb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1},
			},
			input: [][]tree.Datum{
				{aFn(10), tree.NewDInt(tree.DInt(1234))},
			},
			lookupCols:  []uint32{0},
			joinType:    sqlbase.LeftAntiJoin,
			inputTypes:  sqlbase.TwoIntCols,
			outputTypes: sqlbase.OneIntCol,
			expected:    "[]",
		},
		{
			description: "Test left anti lookup join on secondary index with NULL lookup value",
			indexIdx:    1,
			post: distsqlpb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1},
			},
			input: [][]tree.Datum{
				{tree.NewDInt(0), tree.DNull},
			},
			lookupCols:  []uint32{0, 1},
			joinType:    sqlbase.LeftAntiJoin,
			inputTypes:  sqlbase.TwoIntCols,
			outputTypes: sqlbase.TwoIntCols,
			expected:    "[[0 NULL]]",
		},
	}
	for i, td := range []*sqlbase.TableDescriptor{tdSecondary, tdFamily, tdInterleaved} {
		for _, c := range testCases {
			t.Run(fmt.Sprintf("%d/%s", i, c.description), func(t *testing.T) {
				st := cluster.MakeTestingClusterSettings()
				evalCtx := tree.MakeTestingEvalContext(st)
				defer evalCtx.Stop(ctx)
				flowCtx := FlowCtx{
					EvalCtx:  &evalCtx,
					Settings: st,
					txn:      client.NewTxn(ctx, s.DB(), s.NodeID(), client.RootTxn),
				}
				encRows := make(sqlbase.EncDatumRows, len(c.input))
				for rowIdx, row := range c.input {
					encRow := make(sqlbase.EncDatumRow, len(row))
					for i, d := range row {
						encRow[i] = sqlbase.DatumToEncDatum(&c.inputTypes[i], d)
					}
					encRows[rowIdx] = encRow
				}
				in := NewRowBuffer(c.inputTypes, encRows, RowBufferArgs{})

				out := &RowBuffer{}
				jr, err := newJoinReader(
					&flowCtx,
					0, /* processorID */
					&distsqlpb.JoinReaderSpec{
						Table:         *td,
						IndexIdx:      c.indexIdx,
						LookupColumns: c.lookupCols,
						OnExpr:        distsqlpb.Expression{Expr: c.onExpr},
						Type:          c.joinType,
					},
					in,
					&c.post,
					out,
				)
				if err != nil {
					t.Fatal(err)
				}

				// Set a lower batch size to force multiple batches.
				jr.batchSize = 3

				jr.Run(ctx)

				if !in.Done {
					t.Fatal("joinReader didn't consume all the rows")
				}
				if !out.ProducerClosed() {
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
		EvalCtx:  &evalCtx,
		Settings: s.ClusterSettings(),
		txn:      client.NewTxn(ctx, s.DB(), s.NodeID(), client.LeafTxn),
	}

	encRow := make(sqlbase.EncDatumRow, 1)
	encRow[0] = sqlbase.DatumToEncDatum(types.Int, tree.NewDInt(1))

	// ConsumerClosed verifies that when a joinReader's consumer is closed, the
	// joinReader finishes gracefully.
	t.Run("ConsumerClosed", func(t *testing.T) {
		in := NewRowBuffer(sqlbase.OneIntCol, sqlbase.EncDatumRows{encRow}, RowBufferArgs{})

		out := &RowBuffer{}
		out.ConsumerClosed()
		jr, err := newJoinReader(
			&flowCtx, 0 /* processorID */, &distsqlpb.JoinReaderSpec{Table: *td}, in, &distsqlpb.PostProcessSpec{}, out,
		)
		if err != nil {
			t.Fatal(err)
		}
		jr.Run(ctx)
	})

	// ConsumerDone verifies that the producer drains properly by checking that
	// metadata coming from the producer is still read when ConsumerDone is
	// called on the consumer.
	t.Run("ConsumerDone", func(t *testing.T) {
		expectedMetaErr := errors.New("dummy")
		in := NewRowBuffer(sqlbase.OneIntCol, nil /* rows */, RowBufferArgs{})
		if status := in.Push(encRow, &distsqlpb.ProducerMetadata{Err: expectedMetaErr}); status != NeedMoreRows {
			t.Fatalf("unexpected response: %d", status)
		}

		out := &RowBuffer{}
		out.ConsumerDone()
		jr, err := newJoinReader(
			&flowCtx, 0 /* processorID */, &distsqlpb.JoinReaderSpec{Table: *td}, in, &distsqlpb.PostProcessSpec{}, out,
		)
		if err != nil {
			t.Fatal(err)
		}
		jr.Run(ctx)
		row, meta := out.Next()
		if row != nil {
			t.Fatalf("row was pushed unexpectedly: %s", row.String(sqlbase.OneIntCol))
		}
		if meta.Err != expectedMetaErr {
			t.Fatalf("unexpected error in metadata: %v", meta.Err)
		}

		// Check for trailing metadata.
		var traceSeen, txnCoordMetaSeen bool
		for {
			row, meta = out.Next()
			if row != nil {
				t.Fatalf("row was pushed unexpectedly: %s", row.String(sqlbase.OneIntCol))
			}
			if meta == nil {
				break
			}
			if meta.TraceData != nil {
				traceSeen = true
			}
			if meta.TxnCoordMeta != nil {
				txnCoordMetaSeen = true
			}
		}
		if !traceSeen {
			t.Fatal("missing tracing trailing metadata")
		}
		if !txnCoordMetaSeen {
			t.Fatal("missing txn trailing metadata")
		}
	})
}

// BenchmarkJoinReader benchmarks an index join where there is a 1:1
// relationship between the two sides.
func BenchmarkJoinReader(b *testing.B) {
	logScope := log.Scope(b)
	defer logScope.Close(b)
	ctx := context.Background()

	s, sqlDB, kvDB := serverutils.StartServer(b, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	evalCtx := tree.MakeTestingEvalContext(s.ClusterSettings())
	defer evalCtx.Stop(ctx)

	flowCtx := FlowCtx{
		EvalCtx:  &evalCtx,
		Settings: s.ClusterSettings(),
		txn:      client.NewTxn(ctx, s.DB(), s.NodeID(), client.RootTxn),
	}

	const numCols = 2
	const numInputCols = 1
	for _, numRows := range []int{1 << 4, 1 << 8, 1 << 12, 1 << 16} {
		tableName := fmt.Sprintf("t%d", numRows)
		sqlutils.CreateTable(
			b, sqlDB, tableName, "k INT PRIMARY KEY, v INT", numRows,
			sqlutils.ToRowFn(sqlutils.RowIdxFn, sqlutils.RowIdxFn),
		)
		tableDesc := sqlbase.GetTableDescriptor(kvDB, "test", tableName)

		spec := distsqlpb.JoinReaderSpec{Table: *tableDesc}
		input := NewRepeatableRowSource(sqlbase.OneIntCol, sqlbase.MakeIntRows(numRows, numInputCols))
		post := distsqlpb.PostProcessSpec{}
		output := RowDisposer{}

		b.Run(fmt.Sprintf("rows=%d", numRows), func(b *testing.B) {
			b.SetBytes(int64(numRows * (numCols + numInputCols) * 8))
			for i := 0; i < b.N; i++ {
				jr, err := newJoinReader(&flowCtx, 0 /* processorID */, &spec, input, &post, &output)
				if err != nil {
					b.Fatal(err)
				}
				jr.Run(ctx)
				input.Reset()
			}
		})
	}
}
