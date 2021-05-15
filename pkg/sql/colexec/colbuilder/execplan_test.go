// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colbuilder

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecargs"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestNewColOperatorExpectedTypeSchema ensures that NewColOperator call
// creates such an operator chain that its output type schema is exactly as the
// processor spec expects.
func TestNewColOperatorExpectedTypeSchema(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// We will set up the following chain:
	//
	//   ColBatchScan -> a binary projection operator -> a materializer
	//
	// such that the scan operator reads INT2 type but is expected to output
	// INT4 column, then the projection operator performs a binary operation
	// and returns an INT8 column.
	//
	// The crux of the test is an artificial setup of the table reader spec
	// that forces the planning of a cast operator on top of the scan - if such
	// doesn't occur, then the binary projection operator will panic because
	// it expects an Int32 vector whereas an Int16 vector is provided.

	const numRows = 10
	sqlutils.CreateTable(
		t, sqlDB, "t",
		"k INT2 PRIMARY KEY",
		numRows,
		sqlutils.ToRowFn(sqlutils.RowIdxFn),
	)

	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	txn := kv.NewTxn(ctx, s.DB(), s.NodeID())
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg: &execinfra.ServerConfig{
			Settings: st,
		},
		Txn:    txn,
		NodeID: evalCtx.NodeID,
	}

	streamingMemAcc := evalCtx.Mon.MakeBoundAccount()
	defer streamingMemAcc.Close(ctx)

	desc := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "t")
	tr := execinfrapb.TableReaderSpec{
		Table:         *desc.TableDesc(),
		Spans:         make([]execinfrapb.TableReaderSpan, 1),
		NeededColumns: []uint32{0},
	}
	var err error
	tr.Spans[0].Span.Key, err = randgen.TestingMakePrimaryIndexKey(desc, 0)
	if err != nil {
		t.Fatal(err)
	}
	tr.Spans[0].Span.EndKey, err = randgen.TestingMakePrimaryIndexKey(desc, numRows+1)
	if err != nil {
		t.Fatal(err)
	}
	args := &colexecargs.NewColOperatorArgs{
		Spec: &execinfrapb.ProcessorSpec{
			Core:        execinfrapb.ProcessorCoreUnion{TableReader: &tr},
			ResultTypes: []*types.T{types.Int4},
		},
		StreamingMemAccount: &streamingMemAcc,
	}
	r, err := NewColOperator(ctx, flowCtx, args)
	require.NoError(t, err)

	args = &colexecargs.NewColOperatorArgs{
		Spec: &execinfrapb.ProcessorSpec{
			Input:       []execinfrapb.InputSyncSpec{{ColumnTypes: []*types.T{types.Int4}}},
			Core:        execinfrapb.ProcessorCoreUnion{Noop: &execinfrapb.NoopCoreSpec{}},
			Post:        execinfrapb.PostProcessSpec{RenderExprs: []execinfrapb.Expression{{Expr: "@1 - 1"}}},
			ResultTypes: []*types.T{types.Int},
		},
		Inputs:              []colexecargs.OpWithMetaInfo{{Root: r.Root}},
		StreamingMemAccount: &streamingMemAcc,
	}
	r, err = NewColOperator(ctx, flowCtx, args)
	require.NoError(t, err)

	m := colexec.NewMaterializer(
		flowCtx,
		0, /* processorID */
		r.OpWithMetaInfo,
		[]*types.T{types.Int},
	)

	m.Start(ctx)
	var rowIdx int
	for {
		row, meta := m.Next()
		require.Nil(t, meta)
		if row == nil {
			break
		}
		require.Equal(t, 1, len(row))
		expected := tree.DInt(rowIdx)
		require.True(t, row[0].Datum.Compare(&evalCtx, &expected) == 0)
		rowIdx++
	}
	require.Equal(t, numRows, rowIdx)
}
