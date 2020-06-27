// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rowexec

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils/distsqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// min and max are inclusive bounds on the root table's ID.
// If min and/or max is -1, then no bound is used for that endpoint.
func makeSpanWithRootBound(desc *sqlbase.TableDescriptor, min int, max int) roachpb.Span {
	keyPrefix := sqlbase.MakeIndexKeyPrefix(keys.SystemSQLCodec, desc, desc.PrimaryIndex.ID)

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
	ctx := context.Background()

	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

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
	r := sqlutils.MakeSQLRunner(sqlDB)
	// Insert additional rows into child1 not interleaved in parent.
	r.Exec(t, fmt.Sprintf(`INSERT INTO %s.child1 VALUES
	(-1, -1, 0),
	(0, 0, 0),
	(63, 63, 6),
	(70, 70, 7)
	`, sqlutils.TestDB))

	// Create child table that do not correspond to a parent row to
	// exercise OUTER joins.
	sqlutils.CreateTableInterleaved(t, sqlDB, "child3",
		"pid INT, id INT, s STRING, PRIMARY KEY (pid, id), INDEX (s)",
		"parent (pid)",
		0,
		sqlutils.ToRowFn(),
	)
	r.Exec(t, fmt.Sprintf(`INSERT INTO %s.child3 VALUES
	(-1, -1, '-1'),
	(-1, -101, '-101'),
	(0, 0, '0'),
	(1, 1, '1'),
	(3, 3, '3'),
	(3, 103, '103'),
	(5, 5, '5'),
	(31, 31, '31'),
	(31, 131, '131'),
	(32, 32, '32')
	`, sqlutils.TestDB))

	pd := sqlbase.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, sqlutils.TestDB, "parent")
	cd1 := sqlbase.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, sqlutils.TestDB, "child1")
	cd2 := sqlbase.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, sqlutils.TestDB, "child2")
	cd3 := sqlbase.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, sqlutils.TestDB, "child3")

	// InterleavedReaderJoiner specs for each parent-child combination used
	// throughout the test cases.
	// They are copied and/or modified via closures for each test case.
	// For example, pdCd1Spec is the spec for full table INNER JOIN between
	// parent and child1.
	pdCd1Spec := execinfrapb.InterleavedReaderJoinerSpec{
		Tables: []execinfrapb.InterleavedReaderJoinerSpec_Table{
			{
				Desc:     *pd,
				Ordering: execinfrapb.Ordering{Columns: []execinfrapb.Ordering_Column{{ColIdx: 0, Direction: execinfrapb.Ordering_Column_ASC}}},
				Spans:    []execinfrapb.TableReaderSpan{{Span: pd.PrimaryIndexSpan(keys.SystemSQLCodec)}},
			},
			{
				Desc:     *cd1,
				Ordering: execinfrapb.Ordering{Columns: []execinfrapb.Ordering_Column{{ColIdx: 0, Direction: execinfrapb.Ordering_Column_ASC}}},
				Spans:    []execinfrapb.TableReaderSpan{{Span: cd1.PrimaryIndexSpan(keys.SystemSQLCodec)}},
			},
		},
		Type: sqlbase.InnerJoin,
	}

	copySpec := func(spec execinfrapb.InterleavedReaderJoinerSpec) execinfrapb.InterleavedReaderJoinerSpec {
		spec.Tables = append([]execinfrapb.InterleavedReaderJoinerSpec_Table(nil), spec.Tables...)
		return spec
	}

	pdCd2Spec := copySpec(pdCd1Spec)
	pdCd2Spec.Tables[1].Desc = *cd2
	pdCd2Spec.Tables[1].Spans = []execinfrapb.TableReaderSpan{{Span: cd2.PrimaryIndexSpan(keys.SystemSQLCodec)}}
	pdCd3Spec := copySpec(pdCd1Spec)
	pdCd3Spec.Tables[1].Desc = *cd3
	pdCd3Spec.Tables[1].Spans = []execinfrapb.TableReaderSpan{{Span: cd3.PrimaryIndexSpan(keys.SystemSQLCodec)}}

	testCases := []struct {
		spec     execinfrapb.InterleavedReaderJoinerSpec
		post     execinfrapb.PostProcessSpec
		expected string
	}{
		// Simple join with a post process filter and projection.
		{
			spec: pdCd1Spec,
			post: execinfrapb.PostProcessSpec{
				Filter:     execinfrapb.Expression{Expr: "@1 <= 4 OR @3 <= 4 OR @3 > 30"},
				Projection: true,
				// pd.pid1, cid1, cid2
				OutputColumns: []uint32{0, 3, 4},
			},
			expected: "[[1 1 0] [1 31 3] [1 61 6] [2 2 0] [2 32 3] [2 62 6] [3 3 0] [3 33 3] [4 4 0] [4 34 3]]",
		},

		// Swapped parent-child tables.
		{
			spec: func() execinfrapb.InterleavedReaderJoinerSpec {
				spec := copySpec(pdCd1Spec)
				spec.Tables[0], spec.Tables[1] = spec.Tables[1], spec.Tables[0]
				return spec
			}(),
			post: execinfrapb.PostProcessSpec{
				Filter:     execinfrapb.Expression{Expr: "@1 <= 4 OR @1 > 30"},
				Projection: true,
				// pd.pid1, cid1, cid2
				OutputColumns: []uint32{4, 1, 2},
			},
			expected: "[[1 1 0] [1 31 3] [1 61 6] [2 2 0] [2 32 3] [2 62 6] [3 3 0] [3 33 3] [4 4 0] [4 34 3]]",
		},

		// Not specifying spans on either table should return no joined rows.
		{
			spec: func() execinfrapb.InterleavedReaderJoinerSpec {
				spec := copySpec(pdCd1Spec)
				// No spans specified for cd1 should return no rows.
				spec.Tables[1].Spans = nil
				return spec
			}(),
			expected: "[]",
		},

		{
			spec: func() execinfrapb.InterleavedReaderJoinerSpec {
				spec := copySpec(pdCd1Spec)
				// No spans specified for pd should return no rows.
				spec.Tables[0].Spans = nil
				return spec
			}(),
			expected: "[]",
		},

		// Intermediate filters that are logically disjoint returns no
		// joined rows.
		{
			spec: func() execinfrapb.InterleavedReaderJoinerSpec {
				spec := copySpec(pdCd1Spec)
				spec.Tables[0].Post = execinfrapb.PostProcessSpec{
					Filter: execinfrapb.Expression{Expr: "@1 < 4"},
				}
				spec.Tables[1].Post = execinfrapb.PostProcessSpec{
					Filter: execinfrapb.Expression{Expr: "@1 > 4"},
				}
				return spec
			}(),
			expected: "[]",
		},

		// Intermediate filters restrict range of joined rows.
		{
			spec: func() execinfrapb.InterleavedReaderJoinerSpec {
				spec := copySpec(pdCd2Spec)
				spec.Tables[0].Post = execinfrapb.PostProcessSpec{
					Filter: execinfrapb.Expression{Expr: "@1 <= 18"},
				}
				spec.Tables[1].Post = execinfrapb.PostProcessSpec{
					Filter: execinfrapb.Expression{Expr: "@1 >= 12"},
				}
				return spec
			}(),
			post: execinfrapb.PostProcessSpec{
				Projection: true,
				// id column of parent and child table.
				OutputColumns: []uint32{0, 3, 4},
			},
			expected: "[[12 12 'one-two'] [13 13 'one-three'] [14 14 'one-four'] [15 15 'one-five']]",
		},

		// Filters that are converted to spans with index constraints.
		{
			spec: func() execinfrapb.InterleavedReaderJoinerSpec {
				spec := copySpec(pdCd2Spec)
				// Filter on id <= 18.
				spec.Tables[0].Spans = []execinfrapb.TableReaderSpan{{Span: makeSpanWithRootBound(pd, -1, 18)}}
				// Filter on pid >= 12.
				spec.Tables[1].Spans = []execinfrapb.TableReaderSpan{{Span: makeSpanWithRootBound(pd, 12, -1)}}
				return spec
			}(),
			post: execinfrapb.PostProcessSpec{
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
			spec: func() execinfrapb.InterleavedReaderJoinerSpec {
				spec := copySpec(pdCd2Spec)
				// Filter on pid >= 12.
				spec.Tables[1].Spans = []execinfrapb.TableReaderSpan{{Span: makeSpanWithRootBound(pd, 12, -1)}}
				return spec
			}(),
			post: execinfrapb.PostProcessSpec{
				// Filter on id <= 18.
				Filter:     execinfrapb.Expression{Expr: "@1 <= 18"},
				Projection: true,
				// id column of parent and child table.
				OutputColumns: []uint32{0, 3, 4},
			},
			expected: "[[12 12 'one-two'] [13 13 'one-three'] [14 14 'one-four'] [15 15 'one-five']]",
		},

		// Intermediate projections.
		{
			spec: func() execinfrapb.InterleavedReaderJoinerSpec {
				spec := copySpec(pdCd2Spec)
				spec.Tables[0].Post = execinfrapb.PostProcessSpec{
					Filter:        execinfrapb.Expression{Expr: "@1 <= 18"},
					Projection:    true,
					OutputColumns: []uint32{0},
				}
				spec.Tables[1].Post = execinfrapb.PostProcessSpec{
					Filter:     execinfrapb.Expression{Expr: "@1 >= 12"},
					Projection: true,
					// Skip the primary ID of child2.
					OutputColumns: []uint32{0, 2},
				}
				return spec
			}(),
			expected: "[[12 12 'one-two'] [13 13 'one-three'] [14 14 'one-four'] [15 15 'one-five']]",
		},

		// Postprocess limit.
		{
			spec: pdCd1Spec,
			post: execinfrapb.PostProcessSpec{
				Limit: 5,
			},
			expected: "[[1 1 1 1 0] [1 1 1 31 3] [1 1 1 61 6] [2 2 2 2 0] [2 2 2 32 3]]",
		},

		// With an OnExpr.
		{
			spec: func() execinfrapb.InterleavedReaderJoinerSpec {
				spec := pdCd1Spec
				spec.OnExpr = execinfrapb.Expression{Expr: "@4 >= 60"}
				return spec
			}(),
			expected: "[[1 1 1 61 6] [2 2 2 62 6] [30 0 30 60 6]]",
		},

		// FULL OUTER joins.
		{
			spec: func() execinfrapb.InterleavedReaderJoinerSpec {
				spec := pdCd3Spec
				spec.Type = sqlbase.FullOuterJoin
				return spec
			}(),
			post: execinfrapb.PostProcessSpec{
				Filter: execinfrapb.Expression{Expr: "@1 <= 7 OR @1 IS NOT DISTINCT FROM NULL"},
			},
			expected: `[[NULL NULL -1 -101 '-101'] [NULL NULL -1 -1 '-1'] [NULL NULL 0 0 '0'] [1 1 1 1 '1'] [2 2 NULL NULL NULL] [3 3 3 3 '3'] [3 3 3 103 '103'] [4 4 NULL NULL NULL] [5 5 5 5 '5'] [6 6 NULL NULL NULL] [7 7 NULL NULL NULL] [NULL NULL 31 31 '31'] [NULL NULL 31 131 '131'] [NULL NULL 32 32 '32']]`,
		},

		{
			spec: func() execinfrapb.InterleavedReaderJoinerSpec {
				spec := copySpec(pdCd3Spec)
				spec.Tables[0], spec.Tables[1] = spec.Tables[1], spec.Tables[0]
				spec.Type = sqlbase.FullOuterJoin
				return spec
			}(),
			post: execinfrapb.PostProcessSpec{
				Filter: execinfrapb.Expression{Expr: "@4 <= 7 OR @4 IS NOT DISTINCT FROM NULL"},
			},
			expected: `[[-1 -101 '-101' NULL NULL] [-1 -1 '-1' NULL NULL] [0 0 '0' NULL NULL] [1 1 '1' 1 1] [NULL NULL NULL 2 2] [3 3 '3' 3 3] [3 103 '103' 3 3] [NULL NULL NULL 4 4] [5 5 '5' 5 5] [NULL NULL NULL 6 6] [NULL NULL NULL 7 7] [31 31 '31' NULL NULL] [31 131 '131' NULL NULL] [32 32 '32' NULL NULL]]`,
		},

		// LEFT OUTER joins.
		{
			spec: func() execinfrapb.InterleavedReaderJoinerSpec {
				spec := pdCd3Spec
				spec.Type = sqlbase.LeftOuterJoin
				return spec
			}(),
			post: execinfrapb.PostProcessSpec{
				Filter: execinfrapb.Expression{Expr: "@1 <= 7 OR @1 IS NOT DISTINCT FROM NULL"},
			},
			expected: `[[1 1 1 1 '1'] [2 2 NULL NULL NULL] [3 3 3 3 '3'] [3 3 3 103 '103'] [4 4 NULL NULL NULL] [5 5 5 5 '5'] [6 6 NULL NULL NULL] [7 7 NULL NULL NULL]]`,
		},
		{
			spec: func() execinfrapb.InterleavedReaderJoinerSpec {
				spec := copySpec(pdCd3Spec)
				spec.Tables[0], spec.Tables[1] = spec.Tables[1], spec.Tables[0]
				spec.Type = sqlbase.LeftOuterJoin
				return spec
			}(),
			expected: `[[-1 -101 '-101' NULL NULL] [-1 -1 '-1' NULL NULL] [0 0 '0' NULL NULL] [1 1 '1' 1 1] [3 3 '3' 3 3] [3 103 '103' 3 3] [5 5 '5' 5 5] [31 31 '31' NULL NULL] [31 131 '131' NULL NULL] [32 32 '32' NULL NULL]]`,
		},

		// RIGHT OUTER joins.
		{
			spec: func() execinfrapb.InterleavedReaderJoinerSpec {
				spec := pdCd3Spec
				spec.Type = sqlbase.RightOuterJoin
				return spec
			}(),
			expected: `[[NULL NULL -1 -101 '-101'] [NULL NULL -1 -1 '-1'] [NULL NULL 0 0 '0'] [1 1 1 1 '1'] [3 3 3 3 '3'] [3 3 3 103 '103'] [5 5 5 5 '5'] [NULL NULL 31 31 '31'] [NULL NULL 31 131 '131'] [NULL NULL 32 32 '32']]`,
		},
		{
			spec: func() execinfrapb.InterleavedReaderJoinerSpec {
				spec := copySpec(pdCd3Spec)
				spec.Tables[0], spec.Tables[1] = spec.Tables[1], spec.Tables[0]
				spec.Type = sqlbase.RightOuterJoin
				return spec
			}(),
			post: execinfrapb.PostProcessSpec{
				Filter: execinfrapb.Expression{Expr: "@4 <= 7 OR @4 IS NOT DISTINCT FROM NULL"},
			},
			expected: `[[1 1 '1' 1 1] [NULL NULL NULL 2 2] [3 3 '3' 3 3] [3 103 '103' 3 3] [NULL NULL NULL 4 4] [5 5 '5' 5 5] [NULL NULL NULL 6 6] [NULL NULL NULL 7 7]]`,
		},
	}

	for i, tc := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			evalCtx := tree.MakeTestingEvalContext(s.ClusterSettings())
			defer evalCtx.Stop(ctx)
			flowCtx := execinfra.FlowCtx{
				EvalCtx: &evalCtx,
				Cfg:     &execinfra.ServerConfig{Settings: s.ClusterSettings()},
				// Run in a RootTxn so that there's no txn metadata produced.
				Txn:    kv.NewTxn(ctx, s.DB(), s.NodeID()),
				NodeID: evalCtx.NodeID,
			}

			out := &distsqlutils.RowBuffer{}
			irj, err := newInterleavedReaderJoiner(&flowCtx, 0 /* processorID */, &tc.spec, &tc.post, out)
			if err != nil {
				t.Fatal(err)
			}
			irj.Run(ctx)
			if !out.ProducerClosed() {
				t.Fatalf("output RowReceiver not closed")
			}

			var res sqlbase.EncDatumRows
			for {
				row, meta := out.Next()
				if meta != nil {
					t.Fatalf("unexpected metadata: %v", meta)
				}
				if row == nil {
					break
				}
				res = append(res, row.Copy())
			}

			if result := res.String(irj.OutputTypes()); result != tc.expected {
				t.Errorf("invalid results.\ngot: %s\nexpected: %s'", result, tc.expected)
			}
		})
	}
}

func TestInterleavedReaderJoinerErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	sqlutils.CreateTable(t, sqlDB, "parent",
		"id INT PRIMARY KEY",
		0,
		sqlutils.ToRowFn(sqlutils.RowIdxFn),
	)

	sqlutils.CreateTableInterleaved(t, sqlDB, "child",
		"pid INT, id INT, PRIMARY KEY (pid, id)",
		"parent (pid)",
		0,
		sqlutils.ToRowFn(sqlutils.RowModuloShiftedFn(0), sqlutils.RowIdxFn),
	)

	sqlutils.CreateTableInterleaved(t, sqlDB, "grandchild",
		"pid INT, cid INT, id INT, PRIMARY KEY (pid, cid, id)",
		"child (pid, cid)",
		0,
		sqlutils.ToRowFn(sqlutils.RowModuloShiftedFn(0, 0), sqlutils.RowModuloShiftedFn(0), sqlutils.RowIdxFn),
	)

	pd := sqlbase.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, sqlutils.TestDB, "parent")
	cd := sqlbase.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, sqlutils.TestDB, "child")
	gcd := sqlbase.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, sqlutils.TestDB, "grandchild")

	testCases := []struct {
		spec     execinfrapb.InterleavedReaderJoinerSpec
		post     execinfrapb.PostProcessSpec
		expected string
	}{
		{
			spec: execinfrapb.InterleavedReaderJoinerSpec{
				Tables: []execinfrapb.InterleavedReaderJoinerSpec_Table{
					{
						Desc:     *pd,
						Ordering: execinfrapb.Ordering{Columns: []execinfrapb.Ordering_Column{{ColIdx: 0, Direction: execinfrapb.Ordering_Column_ASC}}},
						Spans:    []execinfrapb.TableReaderSpan{{Span: pd.PrimaryIndexSpan(keys.SystemSQLCodec)}},
					},
					{
						Desc:     *cd,
						Ordering: execinfrapb.Ordering{Columns: []execinfrapb.Ordering_Column{{ColIdx: 0, Direction: execinfrapb.Ordering_Column_DESC}}},
						Spans:    []execinfrapb.TableReaderSpan{{Span: cd.PrimaryIndexSpan(keys.SystemSQLCodec)}},
					},
				},
				Type: sqlbase.InnerJoin,
			},
			expected: "internal error: unmatched column orderings",
		},

		{
			spec: execinfrapb.InterleavedReaderJoinerSpec{
				Tables: []execinfrapb.InterleavedReaderJoinerSpec_Table{
					{
						Desc:     *pd,
						Ordering: execinfrapb.Ordering{Columns: []execinfrapb.Ordering_Column{{ColIdx: 0, Direction: execinfrapb.Ordering_Column_ASC}}},
						Spans:    []execinfrapb.TableReaderSpan{{Span: pd.PrimaryIndexSpan(keys.SystemSQLCodec)}},
					},
				},
				Type: sqlbase.InnerJoin,
			},
			expected: "internal error: interleavedReaderJoiner only reads from two tables in an interleaved hierarchy",
		},

		{
			spec: execinfrapb.InterleavedReaderJoinerSpec{
				Tables: []execinfrapb.InterleavedReaderJoinerSpec_Table{
					{
						Desc:     *cd,
						Ordering: execinfrapb.Ordering{Columns: []execinfrapb.Ordering_Column{{ColIdx: 0, Direction: execinfrapb.Ordering_Column_ASC}}},
						Spans:    []execinfrapb.TableReaderSpan{{Span: pd.PrimaryIndexSpan(keys.SystemSQLCodec)}},
					},
					{
						Desc:     *gcd,
						Ordering: execinfrapb.Ordering{Columns: []execinfrapb.Ordering_Column{{ColIdx: 0, Direction: execinfrapb.Ordering_Column_ASC}}},
						Spans:    []execinfrapb.TableReaderSpan{{Span: gcd.PrimaryIndexSpan(keys.SystemSQLCodec)}},
					},
				},
				Type: sqlbase.InnerJoin,
			},
			expected: "internal error: interleavedReaderJoiner only supports joins on the entire interleaved prefix",
		},
	}

	for i, tc := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			evalCtx := tree.MakeTestingEvalContext(s.ClusterSettings())
			defer evalCtx.Stop(ctx)
			flowCtx := execinfra.FlowCtx{
				EvalCtx: &evalCtx,
				Cfg:     &execinfra.ServerConfig{Settings: s.ClusterSettings()},
				// Run in a RootTxn so that there's no txn metadata produced.
				Txn:    kv.NewTxn(ctx, s.DB(), s.NodeID()),
				NodeID: evalCtx.NodeID,
			}

			out := &distsqlutils.RowBuffer{}
			_, err := newInterleavedReaderJoiner(&flowCtx, 0 /* processorID */, &tc.spec, &tc.post, out)
			if err == nil {
				t.Fatalf("expected an error")
			}

			// We flatten the error here since the test asserts that the
			// "internal error: " prefix was added.
			pgErr := pgerror.Flatten(err)
			if actual := pgErr.Error(); actual != tc.expected {
				t.Errorf("expected error: %s, actual: %s", tc.expected, actual)
			}
		})
	}
}

func TestInterleavedReaderJoinerTrailingMetadata(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	sqlutils.CreateTable(t, sqlDB, "parent",
		"id INT PRIMARY KEY",
		0, // numRows
		func(row int) []tree.Datum { return nil },
	)

	sqlutils.CreateTableInterleaved(t, sqlDB, "child",
		"pid INT, id INT, PRIMARY KEY (pid, id)",
		"parent (pid)",
		0,
		func(row int) []tree.Datum { return nil },
	)

	pd := sqlbase.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, sqlutils.TestDB, "parent")
	cd := sqlbase.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, sqlutils.TestDB, "child")

	evalCtx := tree.MakeTestingEvalContext(s.ClusterSettings())
	defer evalCtx.Stop(ctx)

	// Run the flow in a snowball trace so that we can test for tracing info.
	tracer := tracing.NewTracer()
	ctx, sp := tracing.StartSnowballTrace(ctx, tracer, "test flow ctx")
	defer sp.Finish()

	rootTxn := kv.NewTxn(ctx, s.DB(), s.NodeID())
	leafInputState := rootTxn.GetLeafTxnInputState(ctx)
	leafTxn := kv.NewLeafTxn(ctx, s.DB(), s.NodeID(), &leafInputState)

	flowCtx := execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg:     &execinfra.ServerConfig{Settings: s.ClusterSettings()},
		// Run in a LeafTxn so that txn metadata is produced.
		Txn:    leafTxn,
		NodeID: evalCtx.NodeID,
	}

	innerJoinSpec := execinfrapb.InterleavedReaderJoinerSpec{
		Tables: []execinfrapb.InterleavedReaderJoinerSpec_Table{
			{
				Desc:     *pd,
				Ordering: execinfrapb.Ordering{Columns: []execinfrapb.Ordering_Column{{ColIdx: 0, Direction: execinfrapb.Ordering_Column_ASC}}},
				Spans:    []execinfrapb.TableReaderSpan{{Span: pd.PrimaryIndexSpan(keys.SystemSQLCodec)}},
			},
			{
				Desc:     *cd,
				Ordering: execinfrapb.Ordering{Columns: []execinfrapb.Ordering_Column{{ColIdx: 0, Direction: execinfrapb.Ordering_Column_ASC}}},
				Spans:    []execinfrapb.TableReaderSpan{{Span: cd.PrimaryIndexSpan(keys.SystemSQLCodec)}},
			},
		},
		Type: sqlbase.InnerJoin,
	}

	out := &distsqlutils.RowBuffer{}
	irj, err := newInterleavedReaderJoiner(&flowCtx, 0 /* processorID */, &innerJoinSpec, &execinfrapb.PostProcessSpec{}, out)
	if err != nil {
		t.Fatal(err)
	}
	irj.Run(ctx)
	if !out.ProducerClosed() {
		t.Fatalf("output RowReceiver not closed")
	}

	// Check for trailing metadata.
	var traceSeen, txnFinalStateSeen bool
	for {
		row, meta := out.Next()
		if row != nil {
			t.Fatalf("row was pushed unexpectedly: %s", row.String(sqlbase.OneIntCol))
		}
		if meta == nil {
			break
		}
		if meta.TraceData != nil {
			traceSeen = true
		}
		if meta.LeafTxnFinalState != nil {
			txnFinalStateSeen = true
		}
	}
	if !traceSeen {
		t.Fatal("missing tracing trailing metadata")
	}
	if !txnFinalStateSeen {
		t.Fatal("missing txn final state")
	}
}
