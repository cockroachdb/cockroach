// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package distsqlrun

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestIndexSkipTableReader(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Log("Beginning test run\n")
	ctx := context.Background()

	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// create a table where each row is:
	//
	// |     x     |     y     |
	// |-----------------------|
	// | rowId/10  | rowId%10  |

	xfn := func(row int) tree.Datum {
		return tree.NewDInt(tree.DInt(row / 10))
	}
	yfn := func(row int) tree.Datum {
		return tree.NewDInt(tree.DInt(row % 10))
	}

	sqlutils.CreateTable(t, sqlDB, "t",
		"x INT, y INT, PRIMARY KEY (x, y)",
		99,
		sqlutils.ToRowFn(xfn, yfn),
	)

	td := sqlbase.GetTableDescriptor(kvDB, "test", "t")

	makeIndexSpan := func(start, end int) distsqlpb.TableReaderSpan {
		var span roachpb.Span
		prefix := roachpb.Key(sqlbase.MakeIndexKeyPrefix(td, td.PrimaryIndex.ID))
		span.Key = append(prefix, encoding.EncodeVarintAscending(nil, int64(start))...)
		span.EndKey = append(span.EndKey, prefix...)
		span.EndKey = append(span.EndKey, encoding.EncodeVarintAscending(nil, int64(end))...)
		return distsqlpb.TableReaderSpan{Span: span}
	}

	testCases := []struct {
		spec     distsqlpb.IndexSkipTableReaderSpec
		post     distsqlpb.PostProcessSpec
		expected string
	}{
		{
			spec: distsqlpb.IndexSkipTableReaderSpec{
				Spans: []distsqlpb.TableReaderSpan{{Span: td.PrimaryIndexSpan()}},
			},
			post: distsqlpb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0},
			},
			expected: "[[0] [1] [2] [3] [4] [5] [6] [7] [8] [9]]",
		},
		{
			spec: distsqlpb.IndexSkipTableReaderSpec{
				Spans: []distsqlpb.TableReaderSpan{makeIndexSpan(0, 3), makeIndexSpan(5, 8)},
			},
			post: distsqlpb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0},
			},
			expected: "[[0] [1] [2] [5] [6] [7]]",
		},
		{
			spec: distsqlpb.IndexSkipTableReaderSpec{
				Spans: []distsqlpb.TableReaderSpan{makeIndexSpan(0, 3), makeIndexSpan(5, 8)},
			},
			post: distsqlpb.PostProcessSpec{
				Filter:        distsqlpb.Expression{Expr: "@1 > 3 AND @1 < 7"},
				Projection:    true,
				OutputColumns: []uint32{0},
			},
			expected: "[[5] [6]]",
		},
		{
			spec: distsqlpb.IndexSkipTableReaderSpec{
				Spans: []distsqlpb.TableReaderSpan{{Span: td.PrimaryIndexSpan()}},
			},
			post: distsqlpb.PostProcessSpec{
				Filter:        distsqlpb.Expression{Expr: "@1 > 3 AND @1 < 7"},
				Projection:    true,
				OutputColumns: []uint32{0},
			},
			expected: "[[4] [5] [6]]",
		},
	}

	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			testutils.RunTrueAndFalse(t, "row-source", func(t *testing.T, rowSource bool) {
				ts := c.spec
				ts.Table = *td

				evalCtx := tree.MakeTestingEvalContext(s.ClusterSettings())
				defer evalCtx.Stop(ctx)
				flowCtx := FlowCtx{
					EvalCtx:  &evalCtx,
					Settings: s.ClusterSettings(),
					txn:      client.NewTxn(ctx, s.DB(), s.NodeID(), client.RootTxn),
					nodeID:   s.NodeID(),
				}

				var out RowReceiver
				var buf *RowBuffer
				if !rowSource {
					buf = &RowBuffer{}
					out = buf
				}

				tr, err := newIndexSkipTableReader(&flowCtx, 0 /* processorID */, &ts, &c.post, out)

				if err != nil {
					t.Fatal(err)
				}

				var results RowSource
				if rowSource {
					tr.Start(ctx)
					results = tr
				} else {
					tr.Run(ctx)
					if !buf.ProducerClosed() {
						t.Fatalf("output RowReceiver not closed")
					}
					buf.Start(ctx)
					results = buf
				}

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

		})
	}

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

	const numCols = 2

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
					if meta != nil && meta.TxnCoordMeta == nil {
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

				// b.SetBytes(int64(numRows * numCols * 8))
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
				// b.SetBytes(int64)
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
