// Copyright 2017 The Cockroach Authors.
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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"golang.org/x/net/context"
)

func TestInterleaveReaderJoiner(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	// Create a parent table and child table where each row is (not
	// including primary iD columns):
	//
	//  |     a    |
	//  |-----------
	//  | rowId/10 |

	aFn := func(row int) parser.Datum {
		return parser.NewDInt(parser.DInt(row % 10))
	}

	bFn := func(row int) parser.Datum {
		return parser.NewDInt(parser.DInt(row / 10))
	}

	sqlutils.CreateTable(t, sqlDB, "parent",
		"id INT PRIMARY KEY, a INT",
		30,
		sqlutils.ToRowFn(sqlutils.RowIdxFn, aFn),
	)

	// Child table has 3 rows for the first two parent rows and 2 rows for every
	// other parent row.
	sqlutils.CreateTableInterleave(t, sqlDB, "child1",
		"pid INT, id INT, b INT, PRIMARY KEY (pid, id)",
		"test.parent (pid)",
		62,
		sqlutils.ToRowFn(sqlutils.RowModuloShiftedFn(30), sqlutils.RowIdxFn, bFn),
	)

	// Create another table also interleaved into parent with 1 or 0 rows
	// interleaved into each parent row.
	sqlutils.CreateTableInterleave(t, sqlDB, "child2",
		"pid INT, id INT, s STRING, PRIMARY KEY (pid, id), INDEX (s)",
		"test.parent (pid)",
		15,
		sqlutils.ToRowFn(sqlutils.RowModuloShiftedFn(30), sqlutils.RowIdxFn, sqlutils.RowEnglishFn),
	)

	pd := sqlbase.GetTableDescriptor(kvDB, "test", "parent")
	cd1 := sqlbase.GetTableDescriptor(kvDB, "test", "child1")
	cd2 := sqlbase.GetTableDescriptor(kvDB, "test", "child2")

	testCases := []struct {
		spec     InterleaveReaderJoinerSpec
		post     PostProcessSpec
		expected string
	}{
		{
			spec: InterleaveReaderJoinerSpec{
				Tables: []InterleaveReaderJoinerTable{
					{
						Desc:     *pd,
						Ordering: Ordering{Columns: []Ordering_Column{{ColIdx: 1, Direction: Ordering_Column_ASC}}},
					},
					{
						Desc:     *cd1,
						Ordering: Ordering{Columns: []Ordering_Column{{ColIdx: 1, Direction: Ordering_Column_ASC}}},
					},
				},
				// Parent primary index span contains child table too.
				Spans: []TableReaderSpan{{Span: pd.PrimaryIndexSpan()}},
				// Join on the interleave prefix (pid).
				OnExpr: Expression{Expr: "@1 = @3"},
				Type:   JoinType_INNER,
			},
			post: PostProcessSpec{
				Filter:     Expression{Expr: "@1 <= 4 AND @3 <= 4"},
				Projection: true,
				// id column of parent and child table.
				OutputColumns: []uint32{0, 3, 4},
			},
			expected: "[[1 1 0] [1 31 3] [1 61 6] [2 2 0] [2 32 3] [2 62 6] [3 3 0] [3 33 3] [4 4 0] [4 34 3]]",
		},

		{
			spec: InterleaveReaderJoinerSpec{
				Tables: []InterleaveReaderJoinerTable{
					{
						Desc: *pd,
						Post: PostProcessSpec{
							Filter: Expression{Expr: "@1 < 4"},
						},
						Ordering: Ordering{Columns: []Ordering_Column{{ColIdx: 1, Direction: Ordering_Column_ASC}}},
					},
					{
						Desc: *cd1,
						Post: PostProcessSpec{
							Filter: Expression{Expr: "@1 > 4"},
						},
						Ordering: Ordering{Columns: []Ordering_Column{{ColIdx: 1, Direction: Ordering_Column_ASC}}},
					},
				},
				// Parent primary index span contains child table too.
				Spans: []TableReaderSpan{{Span: pd.PrimaryIndexSpan()}},
				// Join on the interleave prefix (pid).
				OnExpr: Expression{Expr: "@1 = @3"},
				Type:   JoinType_INNER,
			},
			expected: "[]",
		},

		{
			spec: InterleaveReaderJoinerSpec{
				Tables: []InterleaveReaderJoinerTable{
					{
						Desc: *pd,
						Post: PostProcessSpec{
							Filter: Expression{Expr: "@1 <= 18"},
						},
						Ordering: Ordering{Columns: []Ordering_Column{{ColIdx: 1, Direction: Ordering_Column_ASC}}},
					},
					{
						Desc: *cd2,
						Post: PostProcessSpec{
							Filter: Expression{Expr: "@1 >= 12"},
						},
						Ordering: Ordering{Columns: []Ordering_Column{{ColIdx: 1, Direction: Ordering_Column_ASC}}},
					},
				},
				// Parent primary index span contains child table too.
				Spans: []TableReaderSpan{{Span: pd.PrimaryIndexSpan()}},
				// Join on the interleave prefix (pid).
				OnExpr: Expression{Expr: "@1 = @3"},
				Type:   JoinType_INNER,
			},
			post: PostProcessSpec{
				Projection: true,
				// id column of parent and child table.
				OutputColumns: []uint32{0, 3, 4},
			},
			expected: "[[12 12 'one-two'] [13 13 'one-three'] [14 14 'one-four'] [15 15 'one-five']]",
		},

		{
			spec: InterleaveReaderJoinerSpec{
				Tables: []InterleaveReaderJoinerTable{
					{
						Desc: *pd,
						Post: PostProcessSpec{
							Filter:        Expression{Expr: "@1 <= 18"},
							Projection:    true,
							OutputColumns: []uint32{0},
						},
						Ordering: Ordering{Columns: []Ordering_Column{{ColIdx: 1, Direction: Ordering_Column_ASC}}},
					},
					{
						Desc: *cd2,
						Post: PostProcessSpec{
							Filter:     Expression{Expr: "@1 >= 12"},
							Projection: true,
							// Skip the primary ID of child2.
							OutputColumns: []uint32{0, 2},
						},
						Ordering: Ordering{Columns: []Ordering_Column{{ColIdx: 1, Direction: Ordering_Column_ASC}}},
					},
				},
				// Parent primary index span contains child table too.
				Spans: []TableReaderSpan{{Span: pd.PrimaryIndexSpan()}},
				// Join on the interleave prefix (pid).
				OnExpr: Expression{Expr: "@1 = @2"},
				Type:   JoinType_INNER,
			},
			expected: "[[12 12 'one-two'] [13 13 'one-three'] [14 14 'one-four'] [15 15 'one-five']]",
		},

		{
			spec: InterleaveReaderJoinerSpec{
				Tables: []InterleaveReaderJoinerTable{
					{
						Desc:     *pd,
						Ordering: Ordering{Columns: []Ordering_Column{{ColIdx: 1, Direction: Ordering_Column_ASC}}},
					},
					{
						Desc:     *cd1,
						Ordering: Ordering{Columns: []Ordering_Column{{ColIdx: 1, Direction: Ordering_Column_ASC}}},
					},
				},
				// Parent primary index span contains child table too.
				Spans: []TableReaderSpan{{Span: pd.PrimaryIndexSpan()}},
				// Join on the interleave prefix (pid).
				OnExpr: Expression{Expr: "@1 = @3"},
				Type:   JoinType_INNER,
			},
			post: PostProcessSpec{
				Limit: 5,
			},
			expected: "[[1 1 1 1 0] [1 1 1 31 3] [1 1 1 61 6] [2 2 2 2 0] [2 2 2 32 3]]",
		},
	}

	for _, tc := range testCases {
		evalCtx := parser.MakeTestingEvalContext()
		defer evalCtx.Stop(context.Background())
		flowCtx := FlowCtx{
			EvalCtx:  evalCtx,
			Settings: s.ClusterSettings(),
			// Pass a DB without a TxnCoordSender.
			txn:    client.NewTxn(client.NewDB(s.DistSender(), s.Clock()), s.NodeID()),
			nodeID: s.NodeID(),
		}

		out := &RowBuffer{}
		irj, err := newInterleaveReaderJoiner(&flowCtx, &tc.spec, &tc.post, out)
		if err != nil {
			t.Fatal(err)
		}
		irj.Run(context.Background(), nil)
		if !out.ProducerClosed {
			t.Fatalf("output RowReceiver not closed")
		}

		var res sqlbase.EncDatumRows
		for {
			row, meta := out.Next()
			if !meta.Empty() {
				t.Fatalf("unexpected metadata: %+v", meta)
			}
			if row == nil {
				break
			}
			res = append(res, row)
		}

		if result := res.String(irj.OutputTypes()); result != tc.expected {
			t.Errorf("invalid results: %s, expected %s'", result, tc.expected)
		}
	}
}

func TestInterleaveReaderJoinerErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	sqlutils.CreateTable(t, sqlDB, "parent",
		"id INT PRIMARY KEY",
		5,
		sqlutils.ToRowFn(sqlutils.RowIdxFn),
	)

	sqlutils.CreateTableInterleave(t, sqlDB, "child",
		"pid INT, id INT, PRIMARY KEY (pid, id)",
		"test.parent (pid)",
		10,
		sqlutils.ToRowFn(sqlutils.RowModuloShiftedFn(5), sqlutils.RowIdxFn),
	)

	pd := sqlbase.GetTableDescriptor(kvDB, "test", "parent")
	cd := sqlbase.GetTableDescriptor(kvDB, "test", "child")

	testCases := []struct {
		spec     InterleaveReaderJoinerSpec
		post     PostProcessSpec
		expected string
	}{
		{
			spec: InterleaveReaderJoinerSpec{
				Tables: []InterleaveReaderJoinerTable{
					{
						Desc:     *pd,
						Ordering: Ordering{Columns: []Ordering_Column{{ColIdx: 1, Direction: Ordering_Column_ASC}}},
					},
					{
						Desc:     *cd,
						Ordering: Ordering{Columns: []Ordering_Column{{ColIdx: 1, Direction: Ordering_Column_DESC}}},
					},
				},
				Spans:  []TableReaderSpan{{Span: pd.PrimaryIndexSpan()}},
				OnExpr: Expression{Expr: "@1 = @3"},
				Type:   JoinType_INNER,
			},
			expected: "unmatched column orderings",
		},

		{
			spec: InterleaveReaderJoinerSpec{
				Tables: []InterleaveReaderJoinerTable{
					{
						Desc:     *pd,
						Ordering: Ordering{Columns: []Ordering_Column{{ColIdx: 1, Direction: Ordering_Column_ASC}}},
					},
					{
						Desc:     *cd,
						Ordering: Ordering{Columns: []Ordering_Column{{ColIdx: 1, Direction: Ordering_Column_ASC}}},
					},
				},
				Spans:  []TableReaderSpan{{Span: pd.PrimaryIndexSpan()}},
				OnExpr: Expression{Expr: "@1 = @3"},
				Type:   JoinType_FULL_OUTER,
			},
			expected: "interleaveReaderJoiner only supports inner joins",
		},

		{
			spec: InterleaveReaderJoinerSpec{
				Tables: []InterleaveReaderJoinerTable{
					{
						Desc:     *pd,
						Ordering: Ordering{Columns: []Ordering_Column{{ColIdx: 1, Direction: Ordering_Column_ASC}}},
					},
				},
				Spans:  []TableReaderSpan{{Span: pd.PrimaryIndexSpan()}},
				OnExpr: Expression{Expr: "@1 = @3"},
				Type:   JoinType_INNER,
			},
			expected: "interleaveReaderJoiner only reads from two tables in an interleaved hierarchy",
		},
	}

	for _, tc := range testCases {
		evalCtx := parser.MakeTestingEvalContext()
		defer evalCtx.Stop(context.Background())
		flowCtx := FlowCtx{
			EvalCtx:  evalCtx,
			Settings: s.ClusterSettings(),
			// Pass a DB without a TxnCoordSender.
			txn:    client.NewTxn(client.NewDB(s.DistSender(), s.Clock()), s.NodeID()),
			nodeID: s.NodeID(),
		}

		out := &RowBuffer{}
		_, err := newInterleaveReaderJoiner(&flowCtx, &tc.spec, &tc.post, out)
		if err == nil {
			t.Fatalf("expected an error")
		}

		if actual := err.Error(); actual != tc.expected {
			t.Errorf("expected error: %s, actual: %s", tc.expected, actual)
		}
	}
}
