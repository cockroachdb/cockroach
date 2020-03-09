// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// This file doesn't live next to execinfra/joinreader.go in order to avoid
// the import cycle with distsqlutils.

package rowexec

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils/distsqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/stretchr/testify/require"
)

func TestJoinReader(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

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

	// Insert a row for NULL testing.
	if _, err := sqlDB.Exec("INSERT INTO test.t VALUES (10, 0, NULL, NULL)"); err != nil {
		t.Fatal(err)
	}

	tdSecondary := sqlbase.GetTableDescriptor(kvDB, "test", "t")

	sqlutils.CreateTable(t, sqlDB, "t2",
		"a INT, b INT, sum INT, s STRING, PRIMARY KEY (a,b), FAMILY f1 (a, b), FAMILY f2 (s), FAMILY f3 (sum), INDEX bs (b,s)",
		99,
		sqlutils.ToRowFn(aFn, bFn, sumFn, sqlutils.RowEnglishFn))

	tdFamily := sqlbase.GetTableDescriptor(kvDB, "test", "t2")

	sqlutils.CreateTable(t, sqlDB, "t3parent",
		"a INT PRIMARY KEY",
		0,
		sqlutils.ToRowFn(aFn))

	sqlutils.CreateTableInterleaved(t, sqlDB, "t3",
		"a INT, b INT, sum INT, s STRING, PRIMARY KEY (a,b), INDEX bs (b,s)",
		"t3parent(a)",
		99,
		sqlutils.ToRowFn(aFn, bFn, sumFn, sqlutils.RowEnglishFn))
	tdInterleaved := sqlbase.GetTableDescriptor(kvDB, "test", "t3")

	testCases := []struct {
		description string
		indexIdx    uint32
		post        execinfrapb.PostProcessSpec
		onExpr      string
		input       [][]tree.Datum
		lookupCols  []uint32
		joinType    sqlbase.JoinType
		inputTypes  []types.T
		outputTypes []types.T
		expected    string
	}{
		{
			description: "Test selecting columns from second table",
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1, 4},
			},
			input: [][]tree.Datum{
				{aFn(2), bFn(2)},
				{aFn(5), bFn(5)},
				{aFn(10), bFn(10)},
				{aFn(15), bFn(15)},
			},
			lookupCols:  []uint32{0, 1},
			inputTypes:  sqlbase.TwoIntCols,
			outputTypes: sqlbase.ThreeIntCols,
			expected:    "[[0 2 2] [0 5 5] [1 0 1] [1 5 6]]",
		},
		{
			description: "Test duplicates in the input of lookup joins",
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1, 3},
			},
			input: [][]tree.Datum{
				{aFn(2), bFn(2)},
				{aFn(2), bFn(2)},
				{aFn(5), bFn(5)},
				{aFn(10), bFn(10)},
				{aFn(15), bFn(15)},
			},
			lookupCols:  []uint32{0, 1},
			inputTypes:  sqlbase.TwoIntCols,
			outputTypes: sqlbase.ThreeIntCols,
			expected:    "[[0 2 2] [0 2 2] [0 5 5] [1 0 0] [1 5 5]]",
		},
		{
			description: "Test lookup join queries with separate families",
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1, 3, 4},
			},
			input: [][]tree.Datum{
				{aFn(2), bFn(2)},
				{aFn(5), bFn(5)},
				{aFn(10), bFn(10)},
				{aFn(15), bFn(15)},
			},
			lookupCols:  []uint32{0, 1},
			inputTypes:  sqlbase.TwoIntCols,
			outputTypes: sqlbase.FourIntCols,
			expected:    "[[0 2 2 2] [0 5 5 5] [1 0 0 1] [1 5 5 6]]",
		},
		{
			description: "Test lookup joins preserve order of left input",
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1, 3},
			},
			input: [][]tree.Datum{
				{aFn(2), bFn(2)},
				{aFn(5), bFn(5)},
				{aFn(2), bFn(2)},
				{aFn(10), bFn(10)},
				{aFn(15), bFn(15)},
			},
			lookupCols:  []uint32{0, 1},
			inputTypes:  sqlbase.TwoIntCols,
			outputTypes: sqlbase.ThreeIntCols,
			expected:    "[[0 2 2] [0 5 5] [0 2 2] [1 0 0] [1 5 5]]",
		},
		{
			description: "Test lookup join with onExpr",
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1, 4},
			},
			input: [][]tree.Datum{
				{aFn(2), bFn(2)},
				{aFn(5), bFn(5)},
				{aFn(10), bFn(10)},
				{aFn(15), bFn(15)},
			},
			lookupCols:  []uint32{0, 1},
			inputTypes:  sqlbase.TwoIntCols,
			outputTypes: sqlbase.ThreeIntCols,
			onExpr:      "@2 < @5",
			expected:    "[[1 0 1] [1 5 6]]",
		},
		{
			description: "Test left outer lookup join on primary index",
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1, 4},
			},
			input: [][]tree.Datum{
				{aFn(100), bFn(100)},
				{aFn(2), bFn(2)},
			},
			lookupCols:  []uint32{0, 1},
			joinType:    sqlbase.LeftOuterJoin,
			inputTypes:  sqlbase.TwoIntCols,
			outputTypes: sqlbase.ThreeIntCols,
			expected:    "[[10 0 NULL] [0 2 2]]",
		},
		{
			description: "Test lookup join on secondary index with NULL lookup value",
			indexIdx:    1,
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0},
			},
			input: [][]tree.Datum{
				{tree.NewDInt(0), tree.DNull},
			},
			lookupCols:  []uint32{0, 1},
			inputTypes:  sqlbase.TwoIntCols,
			outputTypes: sqlbase.OneIntCol,
			expected:    "[]",
		},
		{
			description: "Test left outer lookup join on secondary index with NULL lookup value",
			indexIdx:    1,
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 2},
			},
			input: [][]tree.Datum{
				{tree.NewDInt(0), tree.DNull},
			},
			lookupCols:  []uint32{0, 1},
			joinType:    sqlbase.LeftOuterJoin,
			inputTypes:  sqlbase.TwoIntCols,
			outputTypes: sqlbase.TwoIntCols,
			expected:    "[[0 NULL]]",
		},
		{
			description: "Test lookup join on secondary index with an implicit key column",
			indexIdx:    1,
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{2},
			},
			input: [][]tree.Datum{
				{aFn(2), bFn(2), sqlutils.RowEnglishFn(2)},
			},
			lookupCols:  []uint32{1, 2, 0},
			inputTypes:  []types.T{*types.Int, *types.Int, *types.String},
			outputTypes: sqlbase.OneIntCol,
			expected:    "[['two']]",
		},
		{
			description: "Test left semi lookup join",
			indexIdx:    1,
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1},
			},
			input: [][]tree.Datum{
				{tree.NewDInt(tree.DInt(1)), sqlutils.RowEnglishFn(2)},
				{tree.NewDInt(tree.DInt(1)), sqlutils.RowEnglishFn(2)},
				{tree.NewDInt(tree.DInt(1234)), sqlutils.RowEnglishFn(2)},
				{tree.NewDInt(tree.DInt(6)), sqlutils.RowEnglishFn(2)},
				{tree.NewDInt(tree.DInt(7)), sqlutils.RowEnglishFn(2)},
				{tree.NewDInt(tree.DInt(1)), sqlutils.RowEnglishFn(2)},
			},
			lookupCols:  []uint32{0},
			joinType:    sqlbase.LeftSemiJoin,
			inputTypes:  []types.T{*types.Int, *types.String},
			outputTypes: sqlbase.TwoIntCols,
			expected:    "[[1 'two'] [1 'two'] [6 'two'] [7 'two'] [1 'two']]",
		},
		{
			description: "Test left semi lookup join on secondary index with NULL lookup value",
			indexIdx:    1,
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0},
			},
			input: [][]tree.Datum{
				{tree.NewDInt(0), tree.DNull},
			},
			lookupCols:  []uint32{0, 1},
			joinType:    sqlbase.LeftSemiJoin,
			inputTypes:  sqlbase.TwoIntCols,
			outputTypes: sqlbase.OneIntCol,
			expected:    "[]",
		},
		{
			description: "Test left semi lookup join with onExpr",
			indexIdx:    1,
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1},
			},
			input: [][]tree.Datum{
				{tree.NewDInt(tree.DInt(1)), bFn(3)},
				{tree.NewDInt(tree.DInt(1)), bFn(2)},
				{tree.NewDInt(tree.DInt(1234)), bFn(2)},
				{tree.NewDInt(tree.DInt(6)), bFn(2)},
				{tree.NewDInt(tree.DInt(7)), bFn(3)},
				{tree.NewDInt(tree.DInt(1)), bFn(2)},
			},
			lookupCols:  []uint32{0},
			joinType:    sqlbase.LeftSemiJoin,
			onExpr:      "@2 > 2",
			inputTypes:  sqlbase.TwoIntCols,
			outputTypes: sqlbase.TwoIntCols,
			expected:    "[[1 3] [7 3]]",
		},
		{
			description: "Test left anti lookup join",
			indexIdx:    1,
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1},
			},
			input: [][]tree.Datum{
				{tree.NewDInt(tree.DInt(1234)), tree.NewDInt(tree.DInt(1234))},
			},
			lookupCols:  []uint32{0},
			joinType:    sqlbase.LeftAntiJoin,
			inputTypes:  sqlbase.TwoIntCols,
			outputTypes: sqlbase.TwoIntCols,
			expected:    "[[1234 1234]]",
		},
		{
			description: "Test left anti lookup join with onExpr",
			indexIdx:    1,
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1},
			},
			input: [][]tree.Datum{
				{tree.NewDInt(tree.DInt(1)), bFn(3)},
				{tree.NewDInt(tree.DInt(1)), bFn(2)},
				{tree.NewDInt(tree.DInt(6)), bFn(2)},
				{tree.NewDInt(tree.DInt(7)), bFn(3)},
				{tree.NewDInt(tree.DInt(1)), bFn(2)},
			},
			lookupCols:  []uint32{0},
			joinType:    sqlbase.LeftAntiJoin,
			onExpr:      "@2 > 2",
			inputTypes:  sqlbase.TwoIntCols,
			outputTypes: sqlbase.TwoIntCols,
			expected:    "[[1 2] [6 2] [1 2]]",
		},
		{
			description: "Test left anti lookup join with match",
			indexIdx:    1,
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1},
			},
			input: [][]tree.Datum{
				{aFn(10), tree.NewDInt(tree.DInt(1234))},
			},
			lookupCols:  []uint32{0},
			joinType:    sqlbase.LeftAntiJoin,
			inputTypes:  sqlbase.TwoIntCols,
			outputTypes: sqlbase.OneIntCol,
			expected:    "[]",
		},
		{
			description: "Test left anti lookup join on secondary index with NULL lookup value",
			indexIdx:    1,
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1},
			},
			input: [][]tree.Datum{
				{tree.NewDInt(0), tree.DNull},
			},
			lookupCols:  []uint32{0, 1},
			joinType:    sqlbase.LeftAntiJoin,
			inputTypes:  sqlbase.TwoIntCols,
			outputTypes: sqlbase.TwoIntCols,
			expected:    "[[0 NULL]]",
		},
	}
	st := cluster.MakeTestingClusterSettings()
	tempEngine, _, err := storage.NewTempEngine(ctx, storage.DefaultStorageEngine, base.DefaultTestTempStorageConfig(st), base.DefaultTestStoreSpec)
	if err != nil {
		t.Fatal(err)
	}
	defer tempEngine.Close()
	diskMonitor := mon.MakeMonitor(
		"test-disk",
		mon.DiskResource,
		nil, /* curCount */
		nil, /* maxHist */
		-1,  /* increment: use default block size */
		math.MaxInt64,
		st,
	)
	diskMonitor.Start(ctx, nil /* pool */, mon.MakeStandaloneBudget(math.MaxInt64))
	defer diskMonitor.Stop(ctx)
	for i, td := range []*sqlbase.TableDescriptor{tdSecondary, tdFamily, tdInterleaved} {
		for _, c := range testCases {
			t.Run(fmt.Sprintf("%d/%s", i, c.description), func(t *testing.T) {
				evalCtx := tree.MakeTestingEvalContext(st)
				defer evalCtx.Stop(ctx)
				flowCtx := execinfra.FlowCtx{
					EvalCtx: &evalCtx,
					Cfg: &execinfra.ServerConfig{
						Settings:    st,
						TempStorage: tempEngine,
						DiskMonitor: &diskMonitor,
					},
					Txn: kv.NewTxn(ctx, s.DB(), s.NodeID()),
				}
				encRows := make(sqlbase.EncDatumRows, len(c.input))
				for rowIdx, row := range c.input {
					encRow := make(sqlbase.EncDatumRow, len(row))
					for i, d := range row {
						encRow[i] = sqlbase.DatumToEncDatum(&c.inputTypes[i], d)
					}
					encRows[rowIdx] = encRow
				}
				in := distsqlutils.NewRowBuffer(c.inputTypes, encRows, distsqlutils.RowBufferArgs{})

				out := &distsqlutils.RowBuffer{}
				jr, err := newJoinReader(
					&flowCtx,
					0, /* processorID */
					&execinfrapb.JoinReaderSpec{
						Table:         *td,
						IndexIdx:      c.indexIdx,
						LookupColumns: c.lookupCols,
						OnExpr:        execinfrapb.Expression{Expr: c.onExpr},
						Type:          c.joinType,
					},
					in,
					&c.post,
					out,
				)
				if err != nil {
					t.Fatal(err)
				}

				// Set a lower batch size to force multiple batches.
				jr.(*joinReader).SetBatchSize(3 /* batchSize */)

				jr.Run(ctx)

				if !in.Done {
					t.Fatal("joinReader didn't consume all the rows")
				}
				if !out.ProducerClosed() {
					t.Fatalf("output RowReceiver not closed")
				}

				var res sqlbase.EncDatumRows
				for {
					row := out.NextNoMeta(t)
					if row == nil {
						break
					}
					res = append(res, row)
				}

				if result := res.String(c.outputTypes); result != c.expected {
					t.Errorf("invalid results: %s, expected %s'", result, c.expected)
				}
			})
		}
	}
}

func TestJoinReaderDiskSpill(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// Create our lookup table, with a single key which has enough rows to trigger
	// a disk spill.
	key := 0
	stringColVal := "0123456789"
	numRows := 100
	if _, err := sqlDB.Exec(`
CREATE DATABASE test;
CREATE TABLE test.t (a INT, s STRING, INDEX (a, s))`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(
		`INSERT INTO test.t SELECT $1, $2 FROM generate_series(1, $3)`,
		key, stringColVal, numRows); err != nil {
		t.Fatal(err)
	}
	td := sqlbase.GetTableDescriptor(kvDB, "test", "t")

	st := cluster.MakeTestingClusterSettings()
	tempEngine, _, err := storage.NewTempEngine(ctx, storage.DefaultStorageEngine, base.DefaultTestTempStorageConfig(st), base.DefaultTestStoreSpec)
	if err != nil {
		t.Fatal(err)
	}
	defer tempEngine.Close()

	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	diskMonitor := mon.MakeMonitor(
		"test-disk",
		mon.DiskResource,
		nil, /* curCount */
		nil, /* maxHist */
		-1,  /* increment: use default block size */
		math.MaxInt64,
		st,
	)
	diskMonitor.Start(ctx, nil /* pool */, mon.MakeStandaloneBudget(math.MaxInt64))
	defer diskMonitor.Stop(ctx)
	flowCtx := execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg: &execinfra.ServerConfig{
			Settings:    st,
			TempStorage: tempEngine,
			DiskMonitor: &diskMonitor,
		},
		Txn: kv.NewTxn(ctx, s.DB(), s.NodeID()),
	}
	// Set the memory limit to the minimum allocation size so that the row
	// container can buffer some rows in memory before spilling to disk. This
	// also means we don't need to disable caching in the
	// DiskBackedIndexedRowContainer.
	flowCtx.Cfg.TestingKnobs.MemoryLimitBytes = mon.DefaultPoolAllocationSize

	// Input row is just a single 0.
	inputRows := sqlbase.EncDatumRows{
		sqlbase.EncDatumRow{sqlbase.EncDatum{Datum: tree.NewDInt(tree.DInt(key))}},
	}

	out := &distsqlutils.RowBuffer{}
	jr, err := newJoinReader(
		&flowCtx,
		0, /* processorID */
		&execinfrapb.JoinReaderSpec{
			Table:         *td,
			IndexIdx:      1,
			LookupColumns: []uint32{0},
			Type:          sqlbase.InnerJoin,
		},
		distsqlutils.NewRowBuffer(sqlbase.OneIntCol, inputRows, distsqlutils.RowBufferArgs{}),
		&execinfrapb.PostProcessSpec{
			Projection:    true,
			OutputColumns: []uint32{2},
		},
		out,
	)
	if err != nil {
		t.Fatal(err)
	}
	jr.Run(ctx)

	count := 0
	for {
		row := out.NextNoMeta(t)
		if row == nil {
			break
		}
		expected := fmt.Sprintf("['%s']", stringColVal)
		actual := row.String([]types.T{*types.String})
		require.Equal(t, expected, actual)
		count++
	}
	require.Equal(t, numRows, count)
	require.True(t, jr.(*joinReader).Spilled())
}

// TestJoinReaderDrain tests various scenarios in which a joinReader's consumer
// is closed.
func TestJoinReaderDrain(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	sqlutils.CreateTable(
		t,
		sqlDB,
		"t",
		"a INT, PRIMARY KEY (a)",
		1, /* numRows */
		sqlutils.ToRowFn(sqlutils.RowIdxFn),
	)
	td := sqlbase.GetTableDescriptor(kvDB, "test", "t")

	st := s.ClusterSettings()
	tempEngine, _, err := storage.NewTempEngine(context.Background(), storage.DefaultStorageEngine, base.DefaultTestTempStorageConfig(st), base.DefaultTestStoreSpec)
	if err != nil {
		t.Fatal(err)
	}
	defer tempEngine.Close()

	// Run the flow in a snowball trace so that we can test for tracing info.
	tracer := tracing.NewTracer()
	ctx, sp := tracing.StartSnowballTrace(context.Background(), tracer, "test flow ctx")
	defer sp.Finish()

	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(context.Background())
	diskMonitor := mon.MakeMonitor(
		"test-disk",
		mon.DiskResource,
		nil, /* curCount */
		nil, /* maxHist */
		-1,  /* increment: use default block size */
		math.MaxInt64,
		st,
	)
	diskMonitor.Start(ctx, nil /* pool */, mon.MakeStandaloneBudget(math.MaxInt64))
	defer diskMonitor.Stop(ctx)

	rootTxn := kv.NewTxn(ctx, s.DB(), s.NodeID())
	leafInputState := rootTxn.GetLeafTxnInputState(ctx)
	leafTxn := kv.NewLeafTxn(ctx, s.DB(), s.NodeID(), &leafInputState)

	flowCtx := execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg: &execinfra.ServerConfig{
			Settings:    st,
			TempStorage: tempEngine,
			DiskMonitor: &diskMonitor,
		},
		Txn: leafTxn,
	}

	encRow := make(sqlbase.EncDatumRow, 1)
	encRow[0] = sqlbase.DatumToEncDatum(types.Int, tree.NewDInt(1))

	// ConsumerClosed verifies that when a joinReader's consumer is closed, the
	// joinReader finishes gracefully.
	t.Run("ConsumerClosed", func(t *testing.T) {
		in := distsqlutils.NewRowBuffer(sqlbase.OneIntCol, sqlbase.EncDatumRows{encRow}, distsqlutils.RowBufferArgs{})

		out := &distsqlutils.RowBuffer{}
		out.ConsumerClosed()
		jr, err := newJoinReader(
			&flowCtx, 0 /* processorID */, &execinfrapb.JoinReaderSpec{Table: *td}, in, &execinfrapb.PostProcessSpec{}, out,
		)
		if err != nil {
			t.Fatal(err)
		}
		jr.Run(ctx)
	})

	// ConsumerDone verifies that the producer drains properly by checking that
	// metadata coming from the producer is still read when ConsumerDone is
	// called on the consumer.
	t.Run("ConsumerDone", func(t *testing.T) {
		expectedMetaErr := errors.New("dummy")
		in := distsqlutils.NewRowBuffer(sqlbase.OneIntCol, nil /* rows */, distsqlutils.RowBufferArgs{})
		if status := in.Push(encRow, &execinfrapb.ProducerMetadata{Err: expectedMetaErr}); status != execinfra.NeedMoreRows {
			t.Fatalf("unexpected response: %d", status)
		}

		out := &distsqlutils.RowBuffer{}
		out.ConsumerDone()
		jr, err := newJoinReader(
			&flowCtx, 0 /* processorID */, &execinfrapb.JoinReaderSpec{Table: *td}, in, &execinfrapb.PostProcessSpec{}, out,
		)
		if err != nil {
			t.Fatal(err)
		}
		jr.Run(ctx)
		row, meta := out.Next()
		if row != nil {
			t.Fatalf("row was pushed unexpectedly: %s", row.String(sqlbase.OneIntCol))
		}
		if meta.Err != expectedMetaErr {
			t.Fatalf("unexpected error in metadata: %v", meta.Err)
		}

		// Check for trailing metadata.
		var traceSeen, txnFinalStateSeen bool
		for {
			row, meta = out.Next()
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
	})
}

// BenchmarkJoinReader benchmarks an index join where there is a 1:1
// relationship between the two sides.
func BenchmarkJoinReader(b *testing.B) {
	logScope := log.Scope(b)
	defer logScope.Close(b)
	ctx := context.Background()

	s, sqlDB, kvDB := serverutils.StartServer(b, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	st := s.ClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	diskMonitor := execinfra.NewTestDiskMonitor(ctx, st)
	defer diskMonitor.Stop(ctx)

	flowCtx := execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg: &execinfra.ServerConfig{
			DiskMonitor: diskMonitor,
			Settings:    st,
		},
		Txn: kv.NewTxn(ctx, s.DB(), s.NodeID()),
	}

	const numCols = 2
	const numInputCols = 1
	for _, numRows := range []int{1 << 4, 1 << 8, 1 << 12, 1 << 16} {
		tableName := fmt.Sprintf("t%d", numRows)
		sqlutils.CreateTable(
			b, sqlDB, tableName, "k INT PRIMARY KEY, v INT", numRows,
			sqlutils.ToRowFn(sqlutils.RowIdxFn, sqlutils.RowIdxFn),
		)
		tableDesc := sqlbase.GetTableDescriptor(kvDB, "test", tableName)

		spec := execinfrapb.JoinReaderSpec{Table: *tableDesc}
		input := execinfra.NewRepeatableRowSource(sqlbase.OneIntCol, sqlbase.MakeIntRows(numRows, numInputCols))
		post := execinfrapb.PostProcessSpec{}
		output := execinfra.RowDisposer{}

		b.Run(fmt.Sprintf("rows=%d", numRows), func(b *testing.B) {
			b.SetBytes(int64(numRows * (numCols + numInputCols) * 8))
			for i := 0; i < b.N; i++ {
				jr, err := newJoinReader(&flowCtx, 0 /* processorID */, &spec, input, &post, &output)
				if err != nil {
					b.Fatal(err)
				}
				jr.Run(ctx)
				input.Reset()
			}
		})
	}
}
