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
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
		description     string
		indexIdx        uint32
		post            PostProcessSpec
		onExpr          string
		input           [][]tree.Datum
		lookupCols      columns
		indexFilterExpr Expression
		joinType        sqlbase.JoinType
		outputTypes     []sqlbase.ColumnType
		expected        string
	}{
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
		{
			description: "Test lookup join on covering secondary index",
			indexIdx:    1,
			post: PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{5},
			},
			input: [][]tree.Datum{
				{aFn(2), bFn(2)},
			},
			lookupCols:      []uint32{1},
			indexFilterExpr: Expression{Expr: "@4 LIKE 'one-%'"},
			outputTypes:     []sqlbase.ColumnType{strType},
			expected:        "[['one-two']]",
		},
		{
			description: "Test lookup join on non-covering secondary index",
			indexIdx:    1,
			post: PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{4},
			},
			input: [][]tree.Datum{
				{aFn(2), bFn(2)},
			},
			lookupCols:      []uint32{1},
			indexFilterExpr: Expression{Expr: "@4 LIKE 'one-%'"},
			outputTypes:     oneIntCol,
			expected:        "[[3]]",
		},
		{
			description: "Test left outer lookup join on primary index",
			post: PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1, 4},
			},
			input: [][]tree.Datum{
				{aFn(2), bFn(2)},
				{aFn(100), bFn(100)},
			},
			lookupCols:  []uint32{0, 1},
			joinType:    sqlbase.LeftOuterJoin,
			outputTypes: threeIntCols,
			expected:    "[[0 2 2] [10 0 NULL]]",
		},
		{
			description: "Test left outer lookup join on covering secondary index",
			indexIdx:    1,
			post: PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 5},
			},
			input: [][]tree.Datum{
				{tree.NewDInt(2), tree.DNull},
				{tree.NewDInt(10), tree.DNull},
			},
			lookupCols:      []uint32{0},
			indexFilterExpr: Expression{Expr: "@4 LIKE 'one-%'"},
			joinType:        sqlbase.LeftOuterJoin,
			outputTypes:     []sqlbase.ColumnType{intType, strType},
			expected:        "[[2 'one-two'] [10 NULL]]",
		},
		{
			description: "Test left outer lookup join on non-covering secondary index",
			indexIdx:    1,
			post: PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 4},
			},
			input: [][]tree.Datum{
				{tree.NewDInt(2), tree.DNull},
				{tree.NewDInt(10), tree.DNull},
			},
			lookupCols:      []uint32{0},
			indexFilterExpr: Expression{Expr: "@4 LIKE 'one-%'"},
			joinType:        sqlbase.LeftOuterJoin,
			outputTypes:     []sqlbase.ColumnType{intType, intType},
			expected:        "[[2 3] [10 NULL]]",
		},
		{
			description: "Test lookup join on secondary index with NULL lookup value",
			indexIdx:    1,
			post: PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0},
			},
			input: [][]tree.Datum{
				{tree.NewDInt(0), tree.DNull},
			},
			lookupCols:  []uint32{0, 1},
			outputTypes: oneIntCol,
			expected:    "[]",
		},
		{
			description: "Test left outer lookup join on secondary index with NULL lookup value",
			indexIdx:    1,
			post: PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 2},
			},
			input: [][]tree.Datum{
				{tree.NewDInt(0), tree.DNull},
			},
			lookupCols:  []uint32{0, 1},
			joinType:    sqlbase.LeftOuterJoin,
			outputTypes: twoIntCols,
			expected:    "[[0 NULL]]",
		},
	}
	for i, td := range []*sqlbase.TableDescriptor{tdSecondary, tdFamily, tdInterleaved} {
		for _, c := range testCases {
			t.Run(fmt.Sprintf("%d/%s", i, c.description), func(t *testing.T) {
				st := cluster.MakeTestingClusterSettings()
				evalCtx := tree.MakeTestingEvalContext(st)
				defer evalCtx.Stop(context.Background())
				flowCtx := FlowCtx{
					EvalCtx:  &evalCtx,
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
					0, /* processorID */
					&JoinReaderSpec{
						Table:           *td,
						IndexIdx:        c.indexIdx,
						LookupColumns:   c.lookupCols,
						OnExpr:          Expression{Expr: c.onExpr},
						IndexFilterExpr: c.indexFilterExpr,
						Type:            c.joinType,
					},
					in,
					&c.post,
					out,
				)
				if err != nil {
					t.Fatal(err)
				}

				// Set a lower batch size to force multiple batches.
				jr.batchSize = 2

				jr.Run(context.Background(), nil /* wg */)

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
}

// TestJoinReaderPrimaryLookup tests running the same secondary lookup join
// with different index configurations. It verifies that the results are always
// correct and the primary index is only fetched from when necessary.
func TestJoinReaderPrimaryLookup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	if _, err := sqlDB.Exec("CREATE DATABASE test"); err != nil {
		t.Fatalf("error creating database: %v", err)
	}

	// In each test case we will perform a lookup join on a table with four int
	// columns: a, b, c, d. We join on column b and output the value of column d.
	// We expect a primary index lookup only when column d is not covered by the
	// secondary index.
	testCases := []struct {
		description         string
		indexSchemas        string
		expectPrimaryLookup bool
	}{
		{
			description:         "Test output column not covered by index",
			indexSchemas:        "PRIMARY KEY (a), INDEX bc (b, c)",
			expectPrimaryLookup: true,
		},
		{
			description:         "Test output column in indexed columns",
			indexSchemas:        "PRIMARY KEY (a), INDEX bd (b, d)",
			expectPrimaryLookup: false,
		},
		{
			description:         "Test output column in extra columns",
			indexSchemas:        "PRIMARY KEY (a, d), INDEX b (b)",
			expectPrimaryLookup: false,
		},
		{
			description:         "Test output column in stored columns",
			indexSchemas:        "PRIMARY KEY (a), INDEX bd (b) STORING (d)",
			expectPrimaryLookup: false,
		},
	}
	for _, c := range testCases {
		t.Run(c.description, func(t *testing.T) {
			q := `
DROP TABLE IF EXISTS test.t;
CREATE TABLE test.t (a INT, b INT, c INT, d INT, %s);
INSERT INTO test.t VALUES
  (1, 2, 3, 4),
  (5, 6, 7, 8),
  (9, 10, 11, 12),
  (13, 14, 15, 16)`
			if _, err := sqlDB.Exec(fmt.Sprintf(q, c.indexSchemas)); err != nil {
				t.Fatalf("error creating table: %v", err)
			}
			td := sqlbase.GetTableDescriptor(kvDB, "test", "t")

			st := cluster.MakeTestingClusterSettings()
			evalCtx := tree.MakeTestingEvalContext(st)
			defer evalCtx.Stop(context.Background())

			// Initialize join reader args.
			indexIdx := uint32(1) // first (and only) secondary index
			flowCtx := FlowCtx{
				EvalCtx:  &evalCtx,
				Settings: st,
				txn:      client.NewTxn(s.DB(), s.NodeID(), client.RootTxn),
			}
			lookupCols := []uint32{0}
			in := NewRowBuffer(oneIntCol, genEncDatumRowsInt([][]int{{6}, {10}}), RowBufferArgs{})
			post := PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{4}, // the 'd' column
			}
			out := &RowBuffer{}

			// Create and run joinReader.
			jr, err := newJoinReader(
				&flowCtx,
				0, /* processorID */
				&JoinReaderSpec{Table: *td, IndexIdx: indexIdx, LookupColumns: lookupCols},
				in,
				&post,
				out,
			)
			if err != nil {
				t.Fatal(err)
			}

			// Set a lower batch size to force multiple batches.
			jr.batchSize = 2

			jr.Run(context.Background(), nil /* wg */)

			// Check results.
			var res sqlbase.EncDatumRows
			for {
				row := out.NextNoMeta(t)
				if row == nil {
					break
				}
				res = append(res, row)
			}
			expected := "[[8] [12]]"
			if result := res.String(oneIntCol); result != expected {
				t.Errorf("invalid results: %s, expected %s'", result, expected)
			}

			// Check that the primary index was used only if expected.
			if !c.expectPrimaryLookup && jr.primaryFetcher != nil {
				t.Errorf("jr.primaryFetcher is non-nil but did not expect a primary key lookup")
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
		EvalCtx:  &evalCtx,
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
		jr, err := newJoinReader(
			&flowCtx, 0 /* processorID */, &JoinReaderSpec{Table: *td}, in, &PostProcessSpec{}, out,
		)
		if err != nil {
			t.Fatal(err)
		}
		jr.Run(ctx, nil /* wg */)
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
		jr, err := newJoinReader(
			&flowCtx, 0 /* processorID */, &JoinReaderSpec{Table: *td}, in, &PostProcessSpec{}, out,
		)
		if err != nil {
			t.Fatal(err)
		}
		jr.Run(ctx, nil /* wg */)
		row, meta := out.Next()
		if row != nil {
			t.Fatalf("row was pushed unexpectedly: %s", row.String(oneIntCol))
		}
		if meta.Err != expectedMetaErr {
			t.Fatalf("unexpected error in metadata: %v", meta.Err)
		}

		// Check for trailing metadata.
		var traceSeen, txnCoordMetaSeen bool
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

	s, sqlDB, kvDB := serverutils.StartServer(b, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	evalCtx := tree.MakeTestingEvalContext(s.ClusterSettings())
	defer evalCtx.Stop(context.Background())

	flowCtx := FlowCtx{
		EvalCtx:  &evalCtx,
		Settings: s.ClusterSettings(),
		txn:      client.NewTxn(s.DB(), s.NodeID(), client.RootTxn),
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

		spec := JoinReaderSpec{Table: *tableDesc}
		input := NewRepeatableRowSource(oneIntCol, makeIntRows(numRows, numInputCols))
		post := PostProcessSpec{}
		output := RowDisposer{}

		b.Run(fmt.Sprintf("rows=%d", numRows), func(b *testing.B) {
			b.SetBytes(int64(numRows * (numCols + numInputCols) * 8))
			for i := 0; i < b.N; i++ {
				jr, err := newJoinReader(&flowCtx, 0 /* processorID */, &spec, input, &post, &output)
				if err != nil {
					b.Fatal(err)
				}
				jr.Run(context.Background(), nil)
				input.Reset()
			}
		})
	}
}
