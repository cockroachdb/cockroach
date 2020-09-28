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
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// TestColBatchScanMeta makes sure that the ColBatchScan propagates the leaf
// txn final state metadata which is necessary to notify the kvCoordSender
// about the spans that have been read.
func TestColBatchScanMeta(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	sqlutils.CreateTable(t, sqlDB, "t",
		"num INT PRIMARY KEY",
		3, /* numRows */
		sqlutils.ToRowFn(sqlutils.RowIdxFn))

	td := sqlbase.GetTableDescriptor(kvDB, "test", "t")

	st := s.ClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)

	rootTxn := kv.NewTxn(ctx, s.DB(), s.NodeID())
	leafInputState := rootTxn.GetLeafTxnInputState(ctx)
	leafTxn := kv.NewLeafTxn(ctx, s.DB(), s.NodeID(), &leafInputState)
	flowCtx := execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg: &execinfra.ServerConfig{
			Settings: st,
		},
		Txn:    leafTxn,
		Local:  true,
		NodeID: s.NodeID(),
	}
	spec := execinfrapb.ProcessorSpec{
		Core: execinfrapb.ProcessorCoreUnion{
			TableReader: &execinfrapb.TableReaderSpec{
				Spans: []execinfrapb.TableReaderSpan{{Span: td.PrimaryIndexSpan()}},
				Table: *td,
			}},
		Post: execinfrapb.PostProcessSpec{
			Projection:    true,
			OutputColumns: []uint32{0},
		},
	}

	args := colexec.NewColOperatorArgs{
		Spec:                &spec,
		StreamingMemAccount: testMemAcc,
	}
	args.TestingKnobs.UseStreamingMemAccountForBuffering = true
	res, err := colexec.NewColOperator(ctx, &flowCtx, args)
	if err != nil {
		t.Fatal(err)
	}
	tr := res.Op
	tr.Init()
	meta := tr.(*colexec.CancelChecker).Input().(execinfrapb.MetadataSource).DrainMeta(ctx)
	var txnFinalStateSeen bool
	for _, m := range meta {
		if m.LeafTxnFinalState != nil {
			txnFinalStateSeen = true
			break
		}
	}
	if !txnFinalStateSeen {
		t.Fatal("missing txn final state")
	}
}

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
			spec := execinfrapb.ProcessorSpec{
				Core: execinfrapb.ProcessorCoreUnion{
					TableReader: &execinfrapb.TableReaderSpec{
						Table: *tableDesc,
						Spans: []execinfrapb.TableReaderSpan{{Span: tableDesc.PrimaryIndexSpan()}},
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
				NodeID:  s.NodeID(),
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
				res, err := colexec.NewColOperator(ctx, &flowCtx, args)
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
