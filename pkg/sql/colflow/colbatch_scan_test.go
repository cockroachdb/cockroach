// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Note that this file is not in pkg/sql/colexec because it instantiates a
// server, and if it were moved into sql/colexec, that would create a cycle
// with pkg/server.

package colflow_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
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
		tableDesc := sqlbase.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "test", tableName)
		b.Run(fmt.Sprintf("rows=%d", numRows), func(b *testing.B) {
			spec := execinfrapb.ProcessorSpec{
				Core: execinfrapb.ProcessorCoreUnion{
					TableReader: &execinfrapb.TableReaderSpec{
						Table: *tableDesc,
						Spans: []execinfrapb.TableReaderSpan{
							{Span: tableDesc.PrimaryIndexSpan(keys.SystemSQLCodec)},
						},
					}},
				Post: execinfrapb.PostProcessSpec{
					Projection:    true,
					OutputColumns: []uint32{0, 1},
				},
			}

			evalCtx := tree.MakeTestingEvalContext(s.ClusterSettings())
			defer evalCtx.Stop(ctx)

			flowCtx := execinfra.FlowCtx{
				EvalCtx: &evalCtx,
				Cfg:     &execinfra.ServerConfig{Settings: s.ClusterSettings()},
				Txn:     kv.NewTxn(ctx, s.DB(), s.NodeID()),
				NodeID:  evalCtx.NodeID,
			}

			b.SetBytes(int64(numRows * numCols * 8))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				args := colexec.NewColOperatorArgs{
					Spec:                &spec,
					StreamingMemAccount: testMemAcc,
				}
				args.TestingKnobs.UseStreamingMemAccountForBuffering = true
				res, err := colbuilder.NewColOperator(ctx, &flowCtx, args)
				if err != nil {
					b.Fatal(err)
				}
				tr := res.Op
				tr.Init()
				b.StartTimer()
				for {
					bat := tr.Next(ctx)
					if bat.Length() == 0 {
						break
					}
				}
			}
		})
	}
}
