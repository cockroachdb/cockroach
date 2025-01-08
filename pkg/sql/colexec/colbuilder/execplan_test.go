// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colbuilder

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/fetchpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecargs"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
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
	srv, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

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
	evalCtx := eval.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	txn := kv.NewTxn(ctx, s.DB(), s.DistSQLPlanningNodeID())
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Mon:     evalCtx.TestingMon,
		Cfg: &execinfra.ServerConfig{
			Settings: st,
		},
		Txn:    txn,
		NodeID: evalCtx.NodeID,
	}

	desc := desctestutils.TestingGetPublicTableDescriptor(kvDB, s.Codec(), "test", "t")
	var spec fetchpb.IndexFetchSpec
	if err := rowenc.InitIndexFetchSpec(
		&spec, s.Codec(),
		desc, desc.GetPrimaryIndex(),
		[]descpb.ColumnID{desc.PublicColumns()[0].GetID()},
	); err != nil {
		t.Fatal(err)
	}
	tr := execinfrapb.TableReaderSpec{
		FetchSpec: spec,
		Spans:     make([]roachpb.Span, 1),
	}
	var err error
	tr.Spans[0].Key, err = randgen.TestingMakePrimaryIndexKeyForTenant(desc, s.Codec(), 0)
	if err != nil {
		t.Fatal(err)
	}
	tr.Spans[0].EndKey, err = randgen.TestingMakePrimaryIndexKeyForTenant(desc, s.Codec(), numRows+1)
	if err != nil {
		t.Fatal(err)
	}
	var monitorRegistry colexecargs.MonitorRegistry
	defer monitorRegistry.Close(ctx)
	var closerRegistry colexecargs.CloserRegistry
	defer closerRegistry.Close(ctx)
	args := &colexecargs.NewColOperatorArgs{
		Spec: &execinfrapb.ProcessorSpec{
			Core:        execinfrapb.ProcessorCoreUnion{TableReader: &tr},
			ResultTypes: []*types.T{types.Int4},
		},
		MonitorRegistry: &monitorRegistry,
		CloserRegistry:  &closerRegistry,
	}
	r1, err := NewColOperator(ctx, flowCtx, args)
	require.NoError(t, err)

	args = &colexecargs.NewColOperatorArgs{
		Spec: &execinfrapb.ProcessorSpec{
			Input:       []execinfrapb.InputSyncSpec{{ColumnTypes: []*types.T{types.Int4}}},
			Core:        execinfrapb.ProcessorCoreUnion{Noop: &execinfrapb.NoopCoreSpec{}},
			Post:        execinfrapb.PostProcessSpec{RenderExprs: []execinfrapb.Expression{{Expr: "@1 - 1"}}},
			ResultTypes: []*types.T{types.Int},
		},
		Inputs:          []colexecargs.OpWithMetaInfo{{Root: r1.Root}},
		MonitorRegistry: &monitorRegistry,
		CloserRegistry:  &closerRegistry,
	}
	r, err := NewColOperator(ctx, flowCtx, args)
	require.NoError(t, err)

	m := colexec.NewMaterializer(
		nil, /* streamingMemAcc */
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
		cmp, err := row[0].Datum.Compare(ctx, &evalCtx, &expected)
		require.NoError(t, err)
		require.True(t, cmp == 0)
		rowIdx++
	}
	require.Equal(t, numRows, rowIdx)
}

// BenchmarkRenderPlanning benchmarks how long it takes to run a query with many
// render expressions inside. With small number of rows to read, the overhead of
// allocating the initial vectors for the projection operators dominates.
func BenchmarkRenderPlanning(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(b, base.TestServerArgs{SQLMemoryPoolSize: 10 << 30})
	defer s.Stopper().Stop(ctx)

	jsonValue := `'{"string": "string", "int": 123, "null": null, "nested": {"string": "string", "int": 123, "null": null, "nested": {"string": "string", "int": 123, "null": null}}}'`

	sqlDB := sqlutils.MakeSQLRunner(db)
	for _, numRows := range []int{1, 1 << 3, 1 << 6, 1 << 9} {
		sqlDB.Exec(b, "DROP TABLE IF EXISTS bench")
		sqlDB.Exec(b, "CREATE TABLE bench (id INT PRIMARY KEY, state JSONB)")
		sqlDB.Exec(b, fmt.Sprintf(`INSERT INTO bench SELECT i, %s FROM generate_series(1, %d) AS g(i)`, jsonValue, numRows))
		sqlDB.Exec(b, "ANALYZE bench")
		for _, numRenders := range []int{1, 1 << 4, 1 << 8, 1 << 12} {
			var sb strings.Builder
			sb.WriteString("SELECT ")
			for i := 0; i < numRenders; i++ {
				if i > 0 {
					sb.WriteString(", ")
				}
				sb.WriteString(fmt.Sprintf("state->'nested'->>'nested' AS test%d", i+1))
			}
			sb.WriteString(" FROM bench")
			query := sb.String()
			b.Run(fmt.Sprintf("rows=%d/renders=%d", numRows, numRenders), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					sqlDB.Exec(b, query)
				}
			})
		}
	}
}

// BenchmarkNestedAndPlanning benchmarks how long it takes to run a query with
// many expressions logically AND'ed in a single output column.
func BenchmarkNestedAndPlanning(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(b, base.TestServerArgs{SQLMemoryPoolSize: 10 << 30})
	defer s.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(db)
	// Disable fallback to the row-by-row processing wrapping.
	sqlDB.Exec(b, "SET CLUSTER SETTING sql.distsql.vectorize_render_wrapping.min_render_count = 9999999")
	sqlDB.Exec(b, "CREATE TABLE bench (i INT)")
	for _, numRenders := range []int{1 << 4, 1 << 8, 1 << 12} {
		var sb strings.Builder
		sb.WriteString("SELECT ")
		for i := 0; i < numRenders; i++ {
			if i > 0 {
				sb.WriteString(" AND ")
			}
			sb.WriteString(fmt.Sprintf("i < %d", i+1))
		}
		sb.WriteString(" FROM bench")
		query := sb.String()
		b.Run(fmt.Sprintf("renders=%d", numRenders), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				sqlDB.Exec(b, query)
			}
		})
	}
}
