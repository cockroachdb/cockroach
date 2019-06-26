// Copyright 2019 The Cockroach Authors.
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
	"fmt"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
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

	td1 := sqlbase.GetTableDescriptor(kvDB, "test", "t1")
	td2 := sqlbase.GetTableDescriptor(kvDB, "test", "t2")
	td3 := sqlbase.GetTableDescriptor(kvDB, "test", "t3")
	td4 := sqlbase.GetTableDescriptor(kvDB, "test", "t4")
	td5 := sqlbase.GetTableDescriptor(kvDB, "test", "t5")
	td6 := sqlbase.GetTableDescriptor(kvDB, "test", "t6")

	makeIndexSpan := func(td *sqlbase.TableDescriptor, start, end int) distsqlpb.TableReaderSpan {
		var span roachpb.Span
		prefix := roachpb.Key(sqlbase.MakeIndexKeyPrefix(td, td.PrimaryIndex.ID))
		span.Key = append(prefix, encoding.EncodeVarintAscending(nil, int64(start))...)
		span.EndKey = append(span.EndKey, prefix...)
		span.EndKey = append(span.EndKey, encoding.EncodeVarintAscending(nil, int64(end))...)
		return distsqlpb.TableReaderSpan{Span: span}
	}

	testCases := []struct {
		desc      string
		tableDesc *sqlbase.TableDescriptor
		spec      distsqlpb.IndexSkipTableReaderSpec
		post      distsqlpb.PostProcessSpec
		expected  string
	}{
		{
			desc:      "Distinct scan simple",
			tableDesc: td1,
			spec: distsqlpb.IndexSkipTableReaderSpec{
				Spans: []distsqlpb.TableReaderSpan{{Span: td1.PrimaryIndexSpan()}},
			},
			post: distsqlpb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0},
			},
			expected: "[[0] [1] [2] [3] [4] [5] [6] [7] [8] [9]]",
		},
		{
			desc:      "Distinct scan on interleaved table parent",
			tableDesc: td5,
			spec: distsqlpb.IndexSkipTableReaderSpec{
				Spans: []distsqlpb.TableReaderSpan{{Span: td5.PrimaryIndexSpan()}},
			},
			post: distsqlpb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0},
			},
			expected: "[[0] [1] [2] [3] [4] [5] [6] [7] [8] [9]]",
		},
		{
			desc:      "Distinct scan on interleaved table child",
			tableDesc: td6,
			spec: distsqlpb.IndexSkipTableReaderSpec{
				Spans: []distsqlpb.TableReaderSpan{{Span: td6.PrimaryIndexSpan()}},
			},
			post: distsqlpb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0},
			},
			expected: "[[1] [2] [3] [4] [5] [6] [7] [8] [9] [10]]",
		},
		{
			desc:      "Distinct scan with multiple spans",
			tableDesc: td1,
			spec: distsqlpb.IndexSkipTableReaderSpec{
				Spans: []distsqlpb.TableReaderSpan{makeIndexSpan(td1, 0, 3), makeIndexSpan(td1, 5, 8)},
			},
			post: distsqlpb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0},
			},
			expected: "[[0] [1] [2] [5] [6] [7]]",
		},
		{
			desc:      "Distinct scan with multiple spans and filter",
			tableDesc: td1,
			spec: distsqlpb.IndexSkipTableReaderSpec{
				Spans: []distsqlpb.TableReaderSpan{makeIndexSpan(td1, 0, 3), makeIndexSpan(td1, 5, 8)},
			},
			post: distsqlpb.PostProcessSpec{
				Filter:        distsqlpb.Expression{Expr: "@1 > 3 AND @1 < 7"},
				Projection:    true,
				OutputColumns: []uint32{0},
			},
			expected: "[[5] [6]]",
		},
		{
			desc:      "Distinct scan with filter",
			tableDesc: td1,
			spec: distsqlpb.IndexSkipTableReaderSpec{
				Spans: []distsqlpb.TableReaderSpan{{Span: td1.PrimaryIndexSpan()}},
			},
			post: distsqlpb.PostProcessSpec{
				Filter:        distsqlpb.Expression{Expr: "@1 > 3 AND @1 < 7"},
				Projection:    true,
				OutputColumns: []uint32{0},
			},
			expected: "[[4] [5] [6]]",
		},
		{
			desc:      "Distinct scan with multiple requested columns",
			tableDesc: td2,
			spec: distsqlpb.IndexSkipTableReaderSpec{
				Spans: []distsqlpb.TableReaderSpan{{Span: td2.PrimaryIndexSpan()}},
			},
			post: distsqlpb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1},
			},
			expected: "[[0 1] [1 2] [2 3] [3 4]]",
		},
		{
			desc:      "Distinct scan on table with NULLs",
			tableDesc: td3,
			spec: distsqlpb.IndexSkipTableReaderSpec{
				Spans: []distsqlpb.TableReaderSpan{{Span: td3.PrimaryIndexSpan()}},
			},
			post: distsqlpb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0},
			},
			expected: "[[0] [1] [2] [3]]",
		},
		{
			desc:      "Distinct scan on secondary index",
			tableDesc: td4,
			spec: distsqlpb.IndexSkipTableReaderSpec{
				Spans:    []distsqlpb.TableReaderSpan{{Span: td4.IndexSpan(2)}},
				IndexIdx: 1,
			},
			post: distsqlpb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{1},
			},
			expected: "[[1] [2] [3] [4] [5] [6] [7] [8] [9] [10]]",
		},
	}

	for _, c := range testCases {
		t.Run(c.desc, func(t *testing.T) {
			ts := c.spec
			ts.Table = *c.tableDesc

			evalCtx := tree.MakeTestingEvalContext(s.ClusterSettings())
			defer evalCtx.Stop(ctx)
			flowCtx := FlowCtx{
				EvalCtx:  &evalCtx,
				Settings: s.ClusterSettings(),
				txn:      client.NewTxn(ctx, s.DB(), s.NodeID(), client.RootTxn),
				nodeID:   s.NodeID(),
			}

			tr, err := newIndexSkipTableReader(&flowCtx, 0 /* processorID */, &ts, &c.post, nil)

			if err != nil {
				t.Fatal(err)
			}

			var results RowSource
			tr.Start(ctx)
			results = tr

			var res sqlbase.EncDatumRows
			for {
				row, meta := results.Next()
				if meta != nil && meta.TxnCoordMeta == nil {
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
	td := sqlbase.GetTableDescriptor(kvDB, "test", "t")

	st := tc.Server(0).ClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	nodeID := tc.Server(0).NodeID()
	flowCtx := FlowCtx{
		EvalCtx:  &evalCtx,
		Settings: st,
		txn:      client.NewTxn(ctx, tc.Server(0).DB(), nodeID, client.RootTxn),
		nodeID:   nodeID,
	}
	spec := distsqlpb.IndexSkipTableReaderSpec{
		Spans: []distsqlpb.TableReaderSpan{{Span: td.PrimaryIndexSpan()}},
		Table: *td,
	}
	post := distsqlpb.PostProcessSpec{
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
		var metas []*distsqlpb.ProducerMetadata
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
			} else if m.TxnCoordMeta == nil {
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

			tableDesc := sqlbase.GetTableDescriptor(kvDB, "test", tableName)

			runner := func(reader RowSource, b *testing.B) {
				reader.Start(ctx)
				count := 0
				for {
					row, meta := reader.Next()
					if meta != nil && meta.TxnCoordMeta == nil && meta.Metrics == nil {
						b.Fatalf("unexpected metadata: %+v", meta)
					}
					if row == nil {
						break
					}
					count++
				}
				if count != expectedCount {
					b.Fatalf("found %d rows, expected %d", count, expectedCount)
				}
			}

			flowCtxTableReader := FlowCtx{
				EvalCtx:  &evalCtx,
				Settings: s.ClusterSettings(),
				txn:      client.NewTxn(ctx, s.DB(), s.NodeID(), client.RootTxn),
				nodeID:   s.NodeID(),
			}

			b.Run(fmt.Sprintf("TableReader+Distinct-rows=%d-ratio=%d", numRows, valueRatio), func(b *testing.B) {
				spec := distsqlpb.TableReaderSpec{
					Table: *tableDesc,
					Spans: []distsqlpb.TableReaderSpan{{Span: tableDesc.PrimaryIndexSpan()}},
				}
				post := distsqlpb.PostProcessSpec{
					Projection:    true,
					OutputColumns: []uint32{0},
				}

				specDistinct := distsqlpb.DistinctSpec{
					OrderedColumns:  []uint32{0},
					DistinctColumns: []uint32{0},
				}
				postDistinct := distsqlpb.PostProcessSpec{}

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					tr, err := newTableReader(&flowCtxTableReader, 0, &spec, &post, nil)
					if err != nil {
						b.Fatal(err)
					}
					dist, err := NewDistinct(&flowCtxTableReader, 0, &specDistinct, tr, &postDistinct, nil)
					if err != nil {
						b.Fatal(err)
					}
					runner(dist, b)
				}
			})

			flowCtxIndexSkipTableReader := FlowCtx{
				EvalCtx:  &evalCtx,
				Settings: s.ClusterSettings(),
				txn:      client.NewTxn(ctx, s.DB(), s.NodeID(), client.RootTxn),
				nodeID:   s.NodeID(),
			}

			// run the index skip table reader
			b.Run(fmt.Sprintf("IndexSkipTableReader-rows=%d-ratio=%d", numRows, valueRatio), func(b *testing.B) {
				spec := distsqlpb.IndexSkipTableReaderSpec{
					Table: *tableDesc,
					Spans: []distsqlpb.TableReaderSpan{{Span: tableDesc.PrimaryIndexSpan()}},
				}
				post := distsqlpb.PostProcessSpec{
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
