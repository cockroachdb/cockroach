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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func BenchmarkColBatchScan(b *testing.B) {
	defer leaktest.AfterTest(b)()
	logScope := log.Scope(b)
	defer logScope.Close(b)
	ctx := context.Background()

	s, sqlDB, kvDB := serverutils.StartServer(b, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	const numCols = 2
	for _, numRows := range []int{1 << 4, 1 << 8, 1 << 12, 1 << 16} {
		tableName := fmt.Sprintf("t%d", numRows)
		sqlutils.CreateTable(
			b, sqlDB, tableName,
			"k INT PRIMARY KEY, v INT",
			numRows,
			sqlutils.ToRowFn(sqlutils.RowIdxFn, sqlutils.RowModuloFn(42)),
		)
		tableDesc := sqlbase.GetTableDescriptor(kvDB, "test", tableName)
		b.Run(fmt.Sprintf("rows=%d", numRows), func(b *testing.B) {
			spec := TableReaderSpec{
				Table: *tableDesc,
				Spans: []TableReaderSpan{{Span: tableDesc.PrimaryIndexSpan()}},
			}
			post := PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1},
			}

			evalCtx := tree.MakeTestingEvalContext(s.ClusterSettings())
			defer evalCtx.Stop(ctx)

			flowCtx := FlowCtx{
				EvalCtx:  &evalCtx,
				Settings: s.ClusterSettings(),
				txn:      client.NewTxn(ctx, s.DB(), s.NodeID(), client.RootTxn),
				nodeID:   s.NodeID(),
			}

			b.SetBytes(int64(numRows * numCols * 8))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				tr, err := newColBatchScan(&flowCtx, &spec, &post)
				tr.Init()
				b.StartTimer()
				if err != nil {
					b.Fatal(err)
				}
				for {
					bat := tr.Next()
					if err != nil {
						b.Fatal(err)
					}
					if bat.Length() == 0 {
						break
					}
				}
			}
		})
	}
}
