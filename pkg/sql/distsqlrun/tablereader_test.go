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
	"fmt"
	"sort"
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
)

func TestTableReader(t *testing.T) {
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

	makeIndexSpan := func(start, end int) TableReaderSpan {
		var span roachpb.Span
		prefix := roachpb.Key(sqlbase.MakeIndexKeyPrefix(td, td.Indexes[0].ID))
		span.Key = append(prefix, encoding.EncodeVarintAscending(nil, int64(start))...)
		span.EndKey = append(span.EndKey, prefix...)
		span.EndKey = append(span.EndKey, encoding.EncodeVarintAscending(nil, int64(end))...)
		return TableReaderSpan{Span: span}
	}

	testCases := []struct {
		spec     TableReaderSpec
		post     PostProcessSpec
		expected string
	}{
		{
			spec: TableReaderSpec{
				Spans: []TableReaderSpan{{Span: td.PrimaryIndexSpan()}},
			},
			post: PostProcessSpec{
				Filter:        Expression{Expr: "@3 < 5 AND @2 != 3"}, // sum < 5 && b != 3
				Projection:    true,
				OutputColumns: []uint32{0, 1},
			},
			expected: "[[0 1] [0 2] [0 4] [1 0] [1 1] [1 2] [2 0] [2 1] [2 2] [3 0] [3 1] [4 0]]",
		},
		{
			spec: TableReaderSpec{
				Spans: []TableReaderSpan{{Span: td.PrimaryIndexSpan()}},
			},
			post: PostProcessSpec{
				Filter:        Expression{Expr: "@3 < 5 AND @2 != 3"},
				Projection:    true,
				OutputColumns: []uint32{3}, // s
				Limit:         4,
			},
			expected: "[['one'] ['two'] ['four'] ['one-zero']]",
		},
		{
			spec: TableReaderSpec{
				IndexIdx:  1,
				Reverse:   true,
				Spans:     []TableReaderSpan{makeIndexSpan(4, 6)},
				LimitHint: 1,
			},
			post: PostProcessSpec{
				Filter:        Expression{Expr: "@1 < 3"}, // sum < 8
				Projection:    true,
				OutputColumns: []uint32{0, 1},
			},
			expected: "[[2 5] [1 5] [0 5] [2 4] [1 4] [0 4]]",
		},
	}

	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			for _, rowSource := range []bool{false, true} {
				t.Run(fmt.Sprintf("row-source=%t", rowSource), func(t *testing.T) {
					ts := c.spec
					ts.Table = *td

					evalCtx := tree.MakeTestingEvalContext()
					defer evalCtx.Stop(context.Background())
					flowCtx := FlowCtx{
						ctx:      context.Background(),
						EvalCtx:  evalCtx,
						Settings: s.ClusterSettings(),
						// Pass a DB without a TxnCoordSender.
						txn:    client.NewTxn(client.NewDB(s.DistSender(), s.Clock()), s.NodeID()),
						nodeID: s.NodeID(),
					}

					buf := &RowBuffer{}
					tr, err := newTableReader(&flowCtx, &ts, &c.post, buf)
					if err != nil {
						t.Fatal(err)
					}

					var out RowSource
					if rowSource {
						out = tr
					} else {
						tr.Run(context.Background(), nil)
						if !buf.ProducerClosed {
							t.Fatalf("output RowReceiver not closed")
						}
						out = buf
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
					if result := res.String(tr.OutputTypes()); result != c.expected {
						t.Errorf("invalid results: %s, expected %s'", result, c.expected)
					}
				})
			}
		})
	}
}

// Test that a TableReader outputs metadata about non-local ranges that it read.
func TestMisplannedRangesMetadata(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tc := serverutils.StartTestCluster(t, 3, /* numNodes */
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				UseDatabase: "test",
			},
		})
	defer tc.Stopper().Stop(context.TODO())

	db := tc.ServerConn(0)
	sqlutils.CreateTable(t, db, "t",
		"num INT PRIMARY KEY",
		3, /* numRows */
		sqlutils.ToRowFn(sqlutils.RowIdxFn))

	_, err := db.Exec(`
ALTER TABLE t SPLIT AT VALUES (1), (2), (3);
ALTER TABLE t TESTING_RELOCATE VALUES (ARRAY[2], 1), (ARRAY[1], 2), (ARRAY[3], 3);
`)
	if err != nil {
		t.Fatal(err)
	}

	kvDB := tc.Server(0).KVClient().(*client.DB)
	td := sqlbase.GetTableDescriptor(kvDB, "test", "t")

	evalCtx := tree.MakeTestingEvalContext()
	defer evalCtx.Stop(context.Background())
	nodeID := tc.Server(0).NodeID()
	flowCtx := FlowCtx{
		ctx:      context.Background(),
		EvalCtx:  evalCtx,
		Settings: tc.Server(0).ClusterSettings(),
		// Pass a DB without a TxnCoordSender.
		txn:    client.NewTxn(client.NewDB(tc.Server(0).DistSender(), tc.Server(0).Clock()), nodeID),
		nodeID: nodeID,
	}
	spec := TableReaderSpec{
		Spans: []TableReaderSpan{{Span: td.PrimaryIndexSpan()}},
		Table: *td,
	}
	post := PostProcessSpec{
		Projection:    true,
		OutputColumns: []uint32{0},
	}

	for _, rowSource := range []bool{false, true} {
		t.Run(fmt.Sprintf("row-source=%t", rowSource), func(t *testing.T) {
			buf := &RowBuffer{}
			tr, err := newTableReader(&flowCtx, &spec, &post, buf)
			if err != nil {
				t.Fatal(err)
			}

			var out RowSource
			if rowSource {
				out = tr
			} else {
				tr.Run(context.Background(), nil)
				if !buf.ProducerClosed {
					t.Fatalf("output RowReceiver not closed")
				}
				out = buf
			}

			var res sqlbase.EncDatumRows
			var metas []ProducerMetadata
			for {
				row, meta := out.Next()
				if !meta.Empty() {
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
			if len(metas) != 1 {
				t.Fatalf("expected one meta with misplanned ranges, got: %+v", metas)
			}
			misplannedRanges := metas[0].Ranges
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
}

func BenchmarkTableReader(b *testing.B) {
	s, sqlDB, kvDB := serverutils.StartServer(b, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	sqlutils.CreateTable(
		b, sqlDB, "t",
		"k INT PRIMARY KEY, v INT",
		10000,
		sqlutils.ToRowFn(sqlutils.RowIdxFn, sqlutils.RowModuloFn(42)),
	)

	tableDesc := sqlbase.GetTableDescriptor(kvDB, "test", "t")

	evalCtx := tree.MakeTestingEvalContext()
	defer evalCtx.Stop(context.Background())
	flowCtx := FlowCtx{
		ctx:      context.Background(),
		EvalCtx:  evalCtx,
		Settings: s.ClusterSettings(),
		// Pass a DB without a TxnCoordSender.
		txn:    client.NewTxn(client.NewDB(s.DistSender(), s.Clock()), s.NodeID()),
		nodeID: s.NodeID(),
	}
	spec := TableReaderSpec{
		Table: *tableDesc,
		Spans: []TableReaderSpan{{Span: tableDesc.PrimaryIndexSpan()}},
	}
	post := PostProcessSpec{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tr, err := newTableReader(&flowCtx, &spec, &post, nil /* output */)
		if err != nil {
			b.Fatal(err)
		}
		for {
			row, meta := tr.Next()
			if !meta.Empty() {
				b.Fatalf("unexpected metadata: %+v", meta)
			}
			if row == nil {
				break
			}
		}
	}
}
