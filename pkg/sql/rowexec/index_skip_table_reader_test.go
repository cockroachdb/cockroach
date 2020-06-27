// Copyright 2019 The Cockroach Authors.
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
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestIndexSkipTableReader(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// create a table t1 where each row is:
	//
	// |     x     |     y     |
	// |-----------------------|
	// | rowId/10  | rowId%10  |

	xFnt1 := func(row int) tree.Datum {
		return tree.NewDInt(tree.DInt(row / 10))
	}
	yFnt1 := func(row int) tree.Datum {
		return tree.NewDInt(tree.DInt(row % 10))
	}

	sqlutils.CreateTable(t, sqlDB, "t1",
		"x INT, y INT, PRIMARY KEY (x, y)",
		99,
		sqlutils.ToRowFn(xFnt1, yFnt1),
	)

	// create a table t2 where each row is:
	//
	// |     x     |     y         |     z     |
	// |---------------------------------------|
	// | rowId / 3 | rowId / 3 + 1 |  rowId    |

	xFnt2 := func(row int) tree.Datum {
		return tree.NewDInt(tree.DInt(row / 3))
	}
	yFnt2 := func(row int) tree.Datum {
		return tree.NewDInt(tree.DInt(row/3 + 1))
	}
	zFnt2 := func(row int) tree.Datum {
		return tree.NewDInt(tree.DInt(row))
	}
	sqlutils.CreateTable(t, sqlDB, "t2",
		"x INT, y INT, z INT, PRIMARY KEY (x, y, z)",
		9,
		sqlutils.ToRowFn(xFnt2, yFnt2, zFnt2),
	)

	// create a table t3 where each row is:
	//
	// |     x     |     y      |                      z                   |
	// |-------------------------------------------------------------------|
	// | rowId / 3 | rowId % 3  |  if rowId % 2 == 0 then NULL else rowID  |

	xFnt3 := func(row int) tree.Datum {
		return tree.NewDInt(tree.DInt(row / 3))
	}
	yFnt3 := func(row int) tree.Datum {
		return tree.NewDInt(tree.DInt(row % 3))
	}
	zFnt3 := func(row int) tree.Datum {
		if row%2 == 0 {
			return tree.DNull
		}
		return tree.NewDInt(tree.DInt(row))
	}
	sqlutils.CreateTable(t, sqlDB, "t3",
		"x INT, y INT, z INT, PRIMARY KEY (x, y)",
		9,
		sqlutils.ToRowFn(xFnt3, yFnt3, zFnt3),
	)

	// create a table t4 where each row is:
	//
	// |     x     |        y      |
	// |---------------------------|
	// | rowId/10  | rowId%10 + 1  |

	xFnt4 := func(row int) tree.Datum {
		return tree.NewDInt(tree.DInt(row / 10))
	}
	yFnt4 := func(row int) tree.Datum {
		return tree.NewDInt(tree.DInt(row%10 + 1))
	}

	sqlutils.CreateTable(t, sqlDB, "t4",
		"x INT, y INT, PRIMARY KEY (x, y)",
		99,
		sqlutils.ToRowFn(xFnt4, yFnt4),
	)
	// create a secondary index on (y, x) on t4
	runner := sqlutils.MakeSQLRunner(sqlDB)
	runner.Exec(t, "CREATE INDEX t4_test_index ON test.t4 (y, x)")

	// create some interleaved tables
	sqlutils.CreateTable(t, sqlDB, "t5",
		"x INT PRIMARY KEY",
		10,
		sqlutils.ToRowFn(yFnt1))

	xFnt6 := func(row int) tree.Datum {
		return tree.NewDInt(tree.DInt(row/10) + 1)
	}
	yFnt6 := func(row int) tree.Datum {
		return tree.NewDInt(tree.DInt(row%10) + 1)
	}
	// interleave a table now
	sqlutils.CreateTableInterleaved(t, sqlDB, "t6",
		"x INT, y INT, PRIMARY KEY(x, y)",
		"t5 (x)",
		99,
		sqlutils.ToRowFn(xFnt6, yFnt6))

	// create a table t7 where each row is:
	//
	// |     x     |   y   |  z  |
	// |-------------------------|
	// | rowId%10  | NULL | NULL|
	xFnt7 := func(row int) tree.Datum {
		return tree.NewDInt(tree.DInt(row % 10))
	}
	nullt7 := func(_ int) tree.Datum {
		return tree.DNull
	}
	sqlutils.CreateTable(t, sqlDB, "t7",
		"x INT, y INT, z INT, PRIMARY KEY (x), INDEX i1 (x, y DESC, z DESC), INDEX i2 (y DESC, z DESC)",
		10,
		sqlutils.ToRowFn(xFnt7, nullt7, nullt7))

	td1 := sqlbase.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "t1")
	td2 := sqlbase.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "t2")
	td3 := sqlbase.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "t3")
	td4 := sqlbase.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "t4")
	td5 := sqlbase.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "t5")
	td6 := sqlbase.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "t6")
	td7 := sqlbase.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "t7")

	makeIndexSpan := func(td *sqlbase.TableDescriptor, start, end int) execinfrapb.TableReaderSpan {
		var span roachpb.Span
		prefix := roachpb.Key(sqlbase.MakeIndexKeyPrefix(keys.SystemSQLCodec, td, td.PrimaryIndex.ID))
		span.Key = append(prefix, encoding.EncodeVarintAscending(nil, int64(start))...)
		span.EndKey = append(span.EndKey, prefix...)
		span.EndKey = append(span.EndKey, encoding.EncodeVarintAscending(nil, int64(end))...)
		return execinfrapb.TableReaderSpan{Span: span}
	}

	testCases := []struct {
		desc      string
		tableDesc *sqlbase.TableDescriptor
		spec      execinfrapb.IndexSkipTableReaderSpec
		post      execinfrapb.PostProcessSpec
		expected  string
	}{
		{
			// Distinct scan simple.
			desc:      "SimpleForward",
			tableDesc: td1,
			spec: execinfrapb.IndexSkipTableReaderSpec{
				Spans: []execinfrapb.TableReaderSpan{{Span: td1.PrimaryIndexSpan(keys.SystemSQLCodec)}},
			},
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0},
			},
			expected: "[[0] [1] [2] [3] [4] [5] [6] [7] [8] [9]]",
		},
		{
			// Distinct scan on interleaved table parent.
			desc:      "InterleavedParent",
			tableDesc: td5,
			spec: execinfrapb.IndexSkipTableReaderSpec{
				Spans: []execinfrapb.TableReaderSpan{{Span: td5.PrimaryIndexSpan(keys.SystemSQLCodec)}},
			},
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0},
			},
			expected: "[[0] [1] [2] [3] [4] [5] [6] [7] [8] [9]]",
		},
		{
			// Distinct scan on interleaved table child.
			desc:      "InterleavedChild",
			tableDesc: td6,
			spec: execinfrapb.IndexSkipTableReaderSpec{
				Spans: []execinfrapb.TableReaderSpan{{Span: td6.PrimaryIndexSpan(keys.SystemSQLCodec)}},
			},
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0},
			},
			expected: "[[1] [2] [3] [4] [5] [6] [7] [8] [9] [10]]",
		},
		{
			// Distinct scan with multiple spans.
			desc:      "MultipleSpans",
			tableDesc: td1,
			spec: execinfrapb.IndexSkipTableReaderSpec{
				Spans: []execinfrapb.TableReaderSpan{makeIndexSpan(td1, 0, 3), makeIndexSpan(td1, 5, 8)},
			},
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0},
			},
			expected: "[[0] [1] [2] [5] [6] [7]]",
		},
		{
			// Distinct scan with multiple spans and filter,
			desc:      "MultipleSpansWithFilter",
			tableDesc: td1,
			spec: execinfrapb.IndexSkipTableReaderSpec{
				Spans: []execinfrapb.TableReaderSpan{makeIndexSpan(td1, 0, 3), makeIndexSpan(td1, 5, 8)},
			},
			post: execinfrapb.PostProcessSpec{
				Filter:        execinfrapb.Expression{Expr: "@1 > 3 AND @1 < 7"},
				Projection:    true,
				OutputColumns: []uint32{0},
			},
			expected: "[[5] [6]]",
		},
		{
			// Distinct scan with filter.
			desc:      "Filter",
			tableDesc: td1,
			spec: execinfrapb.IndexSkipTableReaderSpec{
				Spans: []execinfrapb.TableReaderSpan{{Span: td1.PrimaryIndexSpan(keys.SystemSQLCodec)}},
			},
			post: execinfrapb.PostProcessSpec{
				Filter:        execinfrapb.Expression{Expr: "@1 > 3 AND @1 < 7"},
				Projection:    true,
				OutputColumns: []uint32{0},
			},
			expected: "[[4] [5] [6]]",
		},
		{
			// Distinct scan with multiple requested columns.
			desc:      "MultipleOutputCols",
			tableDesc: td2,
			spec: execinfrapb.IndexSkipTableReaderSpec{
				Spans: []execinfrapb.TableReaderSpan{{Span: td2.PrimaryIndexSpan(keys.SystemSQLCodec)}},
			},
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1},
			},
			expected: "[[0 1] [1 2] [2 3] [3 4]]",
		},
		{
			// Distinct scan on table with NULLs.
			desc:      "Nulls",
			tableDesc: td3,
			spec: execinfrapb.IndexSkipTableReaderSpec{
				Spans: []execinfrapb.TableReaderSpan{{Span: td3.PrimaryIndexSpan(keys.SystemSQLCodec)}},
			},
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0},
			},
			expected: "[[0] [1] [2] [3]]",
		},
		{
			// Distinct scan on secondary index",
			desc:      "SecondaryIdx",
			tableDesc: td4,
			spec: execinfrapb.IndexSkipTableReaderSpec{
				Spans:    []execinfrapb.TableReaderSpan{{Span: td4.IndexSpan(keys.SystemSQLCodec, 2)}},
				IndexIdx: 1,
			},
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{1},
			},
			expected: "[[1] [2] [3] [4] [5] [6] [7] [8] [9] [10]]",
		},
		{
			// Distinct reverse scan simple.
			desc:      "SimpleReverse",
			tableDesc: td1,
			spec: execinfrapb.IndexSkipTableReaderSpec{
				Spans:   []execinfrapb.TableReaderSpan{{Span: td1.PrimaryIndexSpan(keys.SystemSQLCodec)}},
				Reverse: true,
			},
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0},
			},
			expected: "[[9] [8] [7] [6] [5] [4] [3] [2] [1] [0]]",
		},
		{
			// Distinct reverse scan with multiple spans.
			desc:      "MultipleSpansReverse",
			tableDesc: td1,
			spec: execinfrapb.IndexSkipTableReaderSpec{
				Spans:   []execinfrapb.TableReaderSpan{makeIndexSpan(td1, 0, 3), makeIndexSpan(td1, 5, 8)},
				Reverse: true,
			},
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0},
			},
			expected: "[[7] [6] [5] [2] [1] [0]]",
		},
		{
			// Distinct reverse scan with multiple spans and filter.
			desc:      "MultipleSpansWithFilterReverse",
			tableDesc: td1,
			spec: execinfrapb.IndexSkipTableReaderSpec{
				Spans:   []execinfrapb.TableReaderSpan{makeIndexSpan(td1, 0, 3), makeIndexSpan(td1, 5, 8)},
				Reverse: true,
			},
			post: execinfrapb.PostProcessSpec{
				Filter:        execinfrapb.Expression{Expr: "@1 > 3 AND @1 < 7"},
				Projection:    true,
				OutputColumns: []uint32{0},
			},
			expected: "[[6] [5]]",
		},
		{
			// Distinct reverse scan on interleaved parent.
			desc:      "InterleavedParentReverse",
			tableDesc: td5,
			spec: execinfrapb.IndexSkipTableReaderSpec{
				Spans:   []execinfrapb.TableReaderSpan{{Span: td5.PrimaryIndexSpan(keys.SystemSQLCodec)}},
				Reverse: true,
			},
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0},
			},
			expected: "[[9] [8] [7] [6] [5] [4] [3] [2] [1] [0]]",
		},
		{
			// Distinct reverse scan with multiple spans on interleaved parent.
			desc:      "InterleavedParentMultipleSpansReverse",
			tableDesc: td5,
			spec: execinfrapb.IndexSkipTableReaderSpec{
				Spans:   []execinfrapb.TableReaderSpan{makeIndexSpan(td5, 0, 3), makeIndexSpan(td5, 5, 8)},
				Reverse: true,
			},
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0},
			},
			expected: "[[7] [6] [5] [2] [1] [0]]",
		},
		{
			// Distinct reverse scan on interleaved child.
			desc:      "InterleavedChildReverse",
			tableDesc: td6,
			spec: execinfrapb.IndexSkipTableReaderSpec{
				Spans:   []execinfrapb.TableReaderSpan{{Span: td6.PrimaryIndexSpan(keys.SystemSQLCodec)}},
				Reverse: true,
			},
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0},
			},
			expected: "[[10] [9] [8] [7] [6] [5] [4] [3] [2] [1]]",
		},
		{
			// Distinct scan on index with multiple null values
			desc:      "IndexMultipleNulls",
			tableDesc: td7,
			spec: execinfrapb.IndexSkipTableReaderSpec{
				Spans:    []execinfrapb.TableReaderSpan{{Span: td7.IndexSpan(keys.SystemSQLCodec, 2)}},
				IndexIdx: 1,
			},
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0},
			},
			expected: "[[0] [1] [2] [3] [4] [5] [6] [7] [8] [9]]",
		},
		{
			// Distinct scan on index with only null values
			desc:      "IndexAllNulls",
			tableDesc: td7,
			spec: execinfrapb.IndexSkipTableReaderSpec{
				Spans:    []execinfrapb.TableReaderSpan{{Span: td7.IndexSpan(keys.SystemSQLCodec, 3)}},
				IndexIdx: 2,
			},
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{1},
			},
			expected: "[[NULL]]",
		},
	}

	for _, c := range testCases {
		t.Run(c.desc, func(t *testing.T) {
			ts := c.spec
			ts.Table = *c.tableDesc

			evalCtx := tree.MakeTestingEvalContext(s.ClusterSettings())
			defer evalCtx.Stop(ctx)
			flowCtx := execinfra.FlowCtx{
				EvalCtx: &evalCtx,
				Cfg:     &execinfra.ServerConfig{Settings: s.ClusterSettings()},
				Txn:     kv.NewTxn(ctx, s.DB(), s.NodeID()),
				NodeID:  evalCtx.NodeID,
			}

			tr, err := newIndexSkipTableReader(&flowCtx, 0 /* processorID */, &ts, &c.post, nil)

			if err != nil {
				t.Fatal(err)
			}

			var results execinfra.RowSource
			tr.Start(ctx)
			results = tr

			var res sqlbase.EncDatumRows
			for {
				row, meta := results.Next()
				if meta != nil && meta.LeafTxnFinalState == nil {
					t.Fatalf("unexpected metadata: %+v", meta)
				}
				if row == nil {
					break
				}
				res = append(res, row.Copy())
			}
			if result := res.String(tr.OutputTypes()); result != c.expected {
				t.Errorf("invalid results: %s, expected %s'", result, c.expected)
			}

		})
	}

}

func TestIndexSkipTableReaderMisplannedRangesMetadata(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	tc := serverutils.StartTestCluster(t, 3,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				UseDatabase: "test",
			},
		})
	defer tc.Stopper().Stop(ctx)

	db := tc.ServerConn(0)
	sqlutils.CreateTable(t, db, "t",
		"num INT PRIMARY KEY",
		3,
		sqlutils.ToRowFn(sqlutils.RowIdxFn),
	)

	_, err := db.Exec(`
ALTER TABLE t SPLIT AT VALUES (1), (2), (3);
ALTER TABLE t EXPERIMENTAL_RELOCATE VALUES (ARRAY[2], 1), (ARRAY[1], 2), (ARRAY[3], 3);
`)
	if err != nil {
		t.Fatal(err)
	}

	kvDB := tc.Server(0).DB()
	td := sqlbase.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "t")

	st := tc.Server(0).ClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	nodeID := tc.Server(0).NodeID()
	flowCtx := execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg:     &execinfra.ServerConfig{Settings: st},
		Txn:     kv.NewTxn(ctx, tc.Server(0).DB(), nodeID),
		NodeID:  evalCtx.NodeID,
	}
	spec := execinfrapb.IndexSkipTableReaderSpec{
		Spans: []execinfrapb.TableReaderSpan{{Span: td.PrimaryIndexSpan(keys.SystemSQLCodec)}},
		Table: *td,
	}
	post := execinfrapb.PostProcessSpec{
		Projection:    true,
		OutputColumns: []uint32{0},
	}

	t.Run("", func(t *testing.T) {
		tr, err := newIndexSkipTableReader(&flowCtx, 0, &spec, &post, nil)
		if err != nil {
			t.Fatal(err)
		}
		tr.Start(ctx)
		var res sqlbase.EncDatumRows
		var metas []*execinfrapb.ProducerMetadata
		for {
			row, meta := tr.Next()
			if meta != nil {
				metas = append(metas, meta)
				continue
			}
			if row == nil {
				break
			}
			res = append(res, row)
		}
		if len(res) != 3 {
			t.Fatalf("expected 3 rows, got: %d", len(res))
		}
		var misplannedRanges []roachpb.RangeInfo
		for _, m := range metas {
			if len(m.Ranges) > 0 {
				misplannedRanges = m.Ranges
			} else if m.LeafTxnFinalState == nil {
				t.Fatalf("expected only txn coord meta or misplanned ranges, got: %+v", metas)
			}
		}

		if len(misplannedRanges) != 2 {
			t.Fatalf("expected 2 misplanned ranges, got: %+v", misplannedRanges)
		}

		// The metadata about misplanned ranges can come in any order (it depends on
		// the order in which parallel sub-batches complete after having been split by
		// DistSender).
		sort.Slice(misplannedRanges, func(i, j int) bool {
			return misplannedRanges[i].Lease.Replica.NodeID < misplannedRanges[j].Lease.Replica.NodeID
		})
		if misplannedRanges[0].Lease.Replica.NodeID != 2 ||
			misplannedRanges[1].Lease.Replica.NodeID != 3 {
			t.Fatalf("expected misplanned ranges from nodes 2 and 3, got: %+v", metas[0])
		}
	})
}

func BenchmarkIndexScanTableReader(b *testing.B) {
	defer leaktest.AfterTest(b)()

	logScope := log.Scope(b)
	defer logScope.Close(b)

	ctx := context.Background()

	s, sqlDB, kvDB := serverutils.StartServer(b, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	evalCtx := tree.MakeTestingEvalContext(s.ClusterSettings())
	defer evalCtx.Stop(ctx)

	// test for number of rows in the table
	for _, numRows := range []int{1 << 4, 1 << 8, 1 << 12, 1 << 16, 1 << 18} {
		// test for a ratio of values from 1 unique value of the first column
		// of the primary key to x values of the second column
		for _, valueRatio := range []int{1, 100, 500, 1000, 5000, 10000} {
			if valueRatio > numRows {
				continue
			}
			tableName := fmt.Sprintf("t_%d_%d", numRows, valueRatio)
			xFn := func(row int) tree.Datum {
				return tree.NewDInt(tree.DInt(row / valueRatio))
			}
			yFn := func(row int) tree.Datum {
				return tree.NewDInt(tree.DInt(row % valueRatio))
			}

			sqlutils.CreateTable(
				b, sqlDB, tableName,
				"x INT, y INT, PRIMARY KEY (x, y)",
				numRows,
				sqlutils.ToRowFn(xFn, yFn))

			expectedCount := (numRows / valueRatio)
			if valueRatio != 1 {
				expectedCount++
			}

			tableDesc := sqlbase.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "test", tableName)

			runner := func(reader execinfra.RowSource, b *testing.B) {
				reader.Start(ctx)
				count := 0
				for {
					row, meta := reader.Next()
					if meta != nil && meta.LeafTxnFinalState == nil && meta.Metrics == nil {
						b.Fatalf("unexpected metadata: %+v", meta)
					}
					if row != nil {
						count++
					} else if meta == nil {
						break
					}
				}
				if count != expectedCount {
					b.Fatalf("found %d rows, expected %d", count, expectedCount)
				}
			}

			flowCtxTableReader := execinfra.FlowCtx{
				EvalCtx: &evalCtx,
				Cfg:     &execinfra.ServerConfig{Settings: s.ClusterSettings()},
				Txn:     kv.NewTxn(ctx, s.DB(), s.NodeID()),
				NodeID:  evalCtx.NodeID,
			}

			b.Run(fmt.Sprintf("TableReader+Distinct-rows=%d-ratio=%d", numRows, valueRatio), func(b *testing.B) {
				spec := execinfrapb.TableReaderSpec{
					Table: *tableDesc,
					Spans: []execinfrapb.TableReaderSpan{{Span: tableDesc.PrimaryIndexSpan(keys.SystemSQLCodec)}},
				}
				post := execinfrapb.PostProcessSpec{
					Projection:    true,
					OutputColumns: []uint32{0},
				}

				specDistinct := execinfrapb.DistinctSpec{
					OrderedColumns:  []uint32{0},
					DistinctColumns: []uint32{0},
				}
				postDistinct := execinfrapb.PostProcessSpec{}

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					tr, err := newTableReader(&flowCtxTableReader, 0, &spec, &post, nil)
					if err != nil {
						b.Fatal(err)
					}
					dist, err := newDistinct(&flowCtxTableReader, 0, &specDistinct, tr, &postDistinct, nil)
					if err != nil {
						b.Fatal(err)
					}
					runner(dist, b)
				}
			})

			flowCtxIndexSkipTableReader := execinfra.FlowCtx{
				EvalCtx: &evalCtx,
				Cfg:     &execinfra.ServerConfig{Settings: s.ClusterSettings()},
				Txn:     kv.NewTxn(ctx, s.DB(), s.NodeID()),
				NodeID:  evalCtx.NodeID,
			}

			// run the index skip table reader
			b.Run(fmt.Sprintf("IndexSkipTableReader-rows=%d-ratio=%d", numRows, valueRatio), func(b *testing.B) {
				spec := execinfrapb.IndexSkipTableReaderSpec{
					Table: *tableDesc,
					Spans: []execinfrapb.TableReaderSpan{{Span: tableDesc.PrimaryIndexSpan(keys.SystemSQLCodec)}},
				}
				post := execinfrapb.PostProcessSpec{
					OutputColumns: []uint32{0},
					Projection:    true,
				}
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					it, err := newIndexSkipTableReader(&flowCtxIndexSkipTableReader, 0, &spec, &post, nil)
					if err != nil {
						b.Fatal(err)
					}
					runner(it, b)
				}
			})
		}
	}
}
