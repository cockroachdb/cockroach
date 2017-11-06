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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"golang.org/x/net/context"
)

// min and max are inclusive bounds on the root table's ID.
// If min and/or max is -1, then no bound is used for that endpoint.
func makeSpanWithRootBound(desc *sqlbase.TableDescriptor, min int, max int) roachpb.Span {
	keyPrefix := sqlbase.MakeIndexKeyPrefix(desc, desc.PrimaryIndex.ID)

	startKey := roachpb.Key(append([]byte(nil), keyPrefix...))
	if min != -1 {
		startKey = encoding.EncodeUvarintAscending(startKey, uint64(min))
	}
	endKey := roachpb.Key(append([]byte(nil), keyPrefix...))
	if max != -1 {
		endKey = encoding.EncodeUvarintAscending(endKey, uint64(max))
	}
	// endKey needs to be exclusive.
	endKey = endKey.PrefixEnd()

	return roachpb.Span{Key: startKey, EndKey: endKey}
}

func TestInterleavedReaderJoiner(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	// Create a parent table and child table where each row is (not
	// including primary iD columns):
	//
	//  |     a    |
	//  |-----------
	//  | rowId/10 |

	aFn := func(row int) tree.Datum {
		return tree.NewDInt(tree.DInt(row % 10))
	}

	bFn := func(row int) tree.Datum {
		return tree.NewDInt(tree.DInt(row / 10))
	}

	sqlutils.CreateTable(t, sqlDB, "parent",
		"id INT PRIMARY KEY, a INT",
		30,
		sqlutils.ToRowFn(sqlutils.RowIdxFn, aFn),
	)

	// Child table has 3 rows for the first two parent rows and 2 rows for every
	// other parent row.
	sqlutils.CreateTableInterleaved(t, sqlDB, "child1",
		"pid INT, id INT, b INT, PRIMARY KEY (pid, id)",
		"parent (pid)",
		62,
		sqlutils.ToRowFn(sqlutils.RowModuloShiftedFn(30), sqlutils.RowIdxFn, bFn),
	)

	// Create another table also interleaved into parent with 1 or 0 rows
	// interleaved into each parent row.
	sqlutils.CreateTableInterleaved(t, sqlDB, "child2",
		"pid INT, id INT, s STRING, PRIMARY KEY (pid, id), INDEX (s)",
		"parent (pid)",
		15,
		sqlutils.ToRowFn(sqlutils.RowModuloShiftedFn(30), sqlutils.RowIdxFn, sqlutils.RowEnglishFn),
	)

	pd := sqlbase.GetTableDescriptor(kvDB, sqlutils.TestDB, "parent")
	cd1 := sqlbase.GetTableDescriptor(kvDB, sqlutils.TestDB, "child1")
	cd2 := sqlbase.GetTableDescriptor(kvDB, sqlutils.TestDB, "child2")

	pdSpans := []TableReaderSpan{{Span: makeSpanWithRootBound(pd, -1, 18)}}
	cd2Spans := []TableReaderSpan{{Span: makeSpanWithRootBound(pd, 12, -1)}}

	testCases := []struct {
		spec     InterleavedReaderJoinerSpec
		post     PostProcessSpec
		expected string
	}{
		// Simple join with a post process filter and projection.
		{
			spec: InterleavedReaderJoinerSpec{
				Tables: []InterleavedReaderJoinerSpec_Table{
					{
						Desc:     *pd,
						Ordering: Ordering{Columns: []Ordering_Column{{ColIdx: 1, Direction: Ordering_Column_ASC}}},
						Spans:    []TableReaderSpan{{Span: pd.PrimaryIndexSpan()}},
					},
					{
						Desc:     *cd1,
						Ordering: Ordering{Columns: []Ordering_Column{{ColIdx: 1, Direction: Ordering_Column_ASC}}},
						Spans:    []TableReaderSpan{{Span: cd1.PrimaryIndexSpan()}},
					},
				},
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

		// Not specifying spans on either table should return no joined rows.
		{
			spec: InterleavedReaderJoinerSpec{
				Tables: []InterleavedReaderJoinerSpec_Table{
					{
						Desc:     *pd,
						Ordering: Ordering{Columns: []Ordering_Column{{ColIdx: 1, Direction: Ordering_Column_ASC}}},
						Spans:    []TableReaderSpan{{Span: pd.PrimaryIndexSpan()}},
					},
					{
						Desc:     *cd1,
						Ordering: Ordering{Columns: []Ordering_Column{{ColIdx: 1, Direction: Ordering_Column_ASC}}},
						// No spans specified for cd1 should return no rows.
					},
				},
				// Join on the interleave prefix (pid).
				OnExpr: Expression{Expr: "@1 = @3"},
				Type:   JoinType_INNER,
			},
			expected: "[]",
		},
		{
			spec: InterleavedReaderJoinerSpec{
				Tables: []InterleavedReaderJoinerSpec_Table{
					{
						Desc:     *pd,
						Ordering: Ordering{Columns: []Ordering_Column{{ColIdx: 1, Direction: Ordering_Column_ASC}}},
						// No spans specified for pd should return no rows.
					},
					{
						Desc:     *cd1,
						Ordering: Ordering{Columns: []Ordering_Column{{ColIdx: 1, Direction: Ordering_Column_ASC}}},
						Spans:    []TableReaderSpan{{Span: cd1.PrimaryIndexSpan()}},
					},
				},
				// Join on the interleave prefix (pid).
				OnExpr: Expression{Expr: "@1 = @3"},
				Type:   JoinType_INNER,
			},
			expected: "[]",
		},

		// Intermediate filters that are logically disjoint returns no
		// joined rows.
		{
			spec: InterleavedReaderJoinerSpec{
				Tables: []InterleavedReaderJoinerSpec_Table{
					{
						Desc: *pd,
						Post: PostProcessSpec{
							Filter: Expression{Expr: "@1 < 4"},
						},
						Ordering: Ordering{Columns: []Ordering_Column{{ColIdx: 1, Direction: Ordering_Column_ASC}}},
						Spans:    []TableReaderSpan{{Span: pd.PrimaryIndexSpan()}},
					},
					{
						Desc: *cd1,
						Post: PostProcessSpec{
							Filter: Expression{Expr: "@1 > 4"},
						},
						Ordering: Ordering{Columns: []Ordering_Column{{ColIdx: 1, Direction: Ordering_Column_ASC}}},
						Spans:    []TableReaderSpan{{Span: cd1.PrimaryIndexSpan()}},
					},
				},
				// Join on the interleave prefix (pid).
				OnExpr: Expression{Expr: "@1 = @3"},
				Type:   JoinType_INNER,
			},
			expected: "[]",
		},

		// Intermediate filters restrict range of joined rows.
		{
			spec: InterleavedReaderJoinerSpec{
				Tables: []InterleavedReaderJoinerSpec_Table{
					{
						Desc: *pd,
						Post: PostProcessSpec{
							Filter: Expression{Expr: "@1 <= 18"},
						},
						Ordering: Ordering{Columns: []Ordering_Column{{ColIdx: 1, Direction: Ordering_Column_ASC}}},
						Spans:    []TableReaderSpan{{Span: pd.PrimaryIndexSpan()}},
					},
					{
						Desc: *cd2,
						Post: PostProcessSpec{
							Filter: Expression{Expr: "@1 >= 12"},
						},
						Ordering: Ordering{Columns: []Ordering_Column{{ColIdx: 1, Direction: Ordering_Column_ASC}}},
						Spans:    []TableReaderSpan{{Span: cd2.PrimaryIndexSpan()}},
					},
				},
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

		// Filters that are converted to spans with index constraints.
		{
			spec: InterleavedReaderJoinerSpec{
				Tables: []InterleavedReaderJoinerSpec_Table{
					{
						Desc:     *pd,
						Ordering: Ordering{Columns: []Ordering_Column{{ColIdx: 1, Direction: Ordering_Column_ASC}}},
						// Filter on id <= 18.
						Spans: pdSpans,
					},
					{
						Desc:     *cd2,
						Ordering: Ordering{Columns: []Ordering_Column{{ColIdx: 1, Direction: Ordering_Column_ASC}}},
						// Filter on pid >= 12.
						Spans: cd2Spans,
					},
				},
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

		// Check that even though InterleavedReaderJoiner scans all
		// children rows, only those that fit the filter on cd2Spans
		// (pid >= 12) are not ignored and ultimately joined.
		{
			spec: InterleavedReaderJoinerSpec{
				Tables: []InterleavedReaderJoinerSpec_Table{
					{
						Desc:     *pd,
						Ordering: Ordering{Columns: []Ordering_Column{{ColIdx: 1, Direction: Ordering_Column_ASC}}},
						Spans:    []TableReaderSpan{{Span: pd.PrimaryIndexSpan()}},
					},
					{
						Desc:     *cd2,
						Ordering: Ordering{Columns: []Ordering_Column{{ColIdx: 1, Direction: Ordering_Column_ASC}}},
						// Filter on pid >= 12.
						Spans: cd2Spans,
					},
				},
				// Join on the interleave prefix (pid).
				OnExpr: Expression{Expr: "@1 = @3"},
				Type:   JoinType_INNER,
			},
			post: PostProcessSpec{
				// Filter on id <= 18.
				Filter:     Expression{Expr: "@1 <= 18"},
				Projection: true,
				// id column of parent and child table.
				OutputColumns: []uint32{0, 3, 4},
			},
			expected: "[[12 12 'one-two'] [13 13 'one-three'] [14 14 'one-four'] [15 15 'one-five']]",
		},

		// Intermediate projection may modify the ON condition
		// ordinal reference.
		{
			spec: InterleavedReaderJoinerSpec{
				Tables: []InterleavedReaderJoinerSpec_Table{
					{
						Desc: *pd,
						Post: PostProcessSpec{
							Filter:        Expression{Expr: "@1 <= 18"},
							Projection:    true,
							OutputColumns: []uint32{0},
						},
						Ordering: Ordering{Columns: []Ordering_Column{{ColIdx: 1, Direction: Ordering_Column_ASC}}},
						Spans:    []TableReaderSpan{{Span: pd.PrimaryIndexSpan()}},
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
						Spans:    []TableReaderSpan{{Span: cd2.PrimaryIndexSpan()}},
					},
				},
				// Join on the interleave prefix (pid).
				OnExpr: Expression{Expr: "@1 = @2"},
				Type:   JoinType_INNER,
			},
			expected: "[[12 12 'one-two'] [13 13 'one-three'] [14 14 'one-four'] [15 15 'one-five']]",
		},

		// Postprocess limit.
		{
			spec: InterleavedReaderJoinerSpec{
				Tables: []InterleavedReaderJoinerSpec_Table{
					{
						Desc:     *pd,
						Ordering: Ordering{Columns: []Ordering_Column{{ColIdx: 1, Direction: Ordering_Column_ASC}}},
						Spans:    []TableReaderSpan{{Span: pd.PrimaryIndexSpan()}},
					},
					{
						Desc:     *cd1,
						Ordering: Ordering{Columns: []Ordering_Column{{ColIdx: 1, Direction: Ordering_Column_ASC}}},
						Spans:    []TableReaderSpan{{Span: cd1.PrimaryIndexSpan()}},
					},
				},
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

	for i, tc := range testCases {
		t.Run(string(i), func(t *testing.T) {
			evalCtx := tree.MakeTestingEvalContext()
			defer evalCtx.Stop(context.Background())
			flowCtx := FlowCtx{
				EvalCtx:  evalCtx,
				Settings: s.ClusterSettings(),
				// Pass a DB without a TxnCoordSender.
				txn:    client.NewTxn(client.NewDB(s.DistSender(), s.Clock()), s.NodeID()),
				nodeID: s.NodeID(),
			}

			out := &RowBuffer{}
			irj, err := newInterleavedReaderJoiner(&flowCtx, &tc.spec, &tc.post, out)
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
		})
	}
}

func TestInterleavedReaderJoinerErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	sqlutils.CreateTable(t, sqlDB, "parent",
		"id INT PRIMARY KEY",
		5,
		sqlutils.ToRowFn(sqlutils.RowIdxFn),
	)

	sqlutils.CreateTableInterleaved(t, sqlDB, "child",
		"pid INT, id INT, PRIMARY KEY (pid, id)",
		"test.parent (pid)",
		10,
		sqlutils.ToRowFn(sqlutils.RowModuloShiftedFn(5), sqlutils.RowIdxFn),
	)

	pd := sqlbase.GetTableDescriptor(kvDB, sqlutils.TestDB, "parent")
	cd := sqlbase.GetTableDescriptor(kvDB, sqlutils.TestDB, "child")

	testCases := []struct {
		spec     InterleavedReaderJoinerSpec
		post     PostProcessSpec
		expected string
	}{
		{
			spec: InterleavedReaderJoinerSpec{
				Tables: []InterleavedReaderJoinerSpec_Table{
					{
						Desc:     *pd,
						Ordering: Ordering{Columns: []Ordering_Column{{ColIdx: 1, Direction: Ordering_Column_ASC}}},
						Spans:    []TableReaderSpan{{Span: pd.PrimaryIndexSpan()}},
					},
					{
						Desc:     *cd,
						Ordering: Ordering{Columns: []Ordering_Column{{ColIdx: 1, Direction: Ordering_Column_DESC}}},
						Spans:    []TableReaderSpan{{Span: cd.PrimaryIndexSpan()}},
					},
				},
				OnExpr: Expression{Expr: "@1 = @3"},
				Type:   JoinType_INNER,
			},
			expected: "unmatched column orderings",
		},

		{
			spec: InterleavedReaderJoinerSpec{
				Tables: []InterleavedReaderJoinerSpec_Table{
					{
						Desc:     *pd,
						Ordering: Ordering{Columns: []Ordering_Column{{ColIdx: 1, Direction: Ordering_Column_ASC}}},
						Spans:    []TableReaderSpan{{Span: pd.PrimaryIndexSpan()}},
					},
					{
						Desc:     *cd,
						Ordering: Ordering{Columns: []Ordering_Column{{ColIdx: 1, Direction: Ordering_Column_ASC}}},
						Spans:    []TableReaderSpan{{Span: cd.PrimaryIndexSpan()}},
					},
				},
				OnExpr: Expression{Expr: "@1 = @3"},
				Type:   JoinType_FULL_OUTER,
			},
			expected: "interleavedReaderJoiner only supports inner joins",
		},

		{
			spec: InterleavedReaderJoinerSpec{
				Tables: []InterleavedReaderJoinerSpec_Table{
					{
						Desc:     *pd,
						Ordering: Ordering{Columns: []Ordering_Column{{ColIdx: 1, Direction: Ordering_Column_ASC}}},
						Spans:    []TableReaderSpan{{Span: pd.PrimaryIndexSpan()}},
					},
				},
				OnExpr: Expression{Expr: "@1 = @3"},
				Type:   JoinType_INNER,
			},
			expected: "interleavedReaderJoiner only reads from two tables in an interleaved hierarchy",
		},
	}

	for _, tc := range testCases {
		evalCtx := tree.MakeTestingEvalContext()
		defer evalCtx.Stop(context.Background())
		flowCtx := FlowCtx{
			EvalCtx:  evalCtx,
			Settings: s.ClusterSettings(),
			// Pass a DB without a TxnCoordSender.
			txn:    client.NewTxn(client.NewDB(s.DistSender(), s.Clock()), s.NodeID()),
			nodeID: s.NodeID(),
		}

		out := &RowBuffer{}
		_, err := newInterleavedReaderJoiner(&flowCtx, &tc.spec, &tc.post, out)
		if err == nil {
			t.Fatalf("expected an error")
		}

		if actual := err.Error(); actual != tc.expected {
			t.Errorf("expected error: %s, actual: %s", tc.expected, actual)
		}
	}
}
