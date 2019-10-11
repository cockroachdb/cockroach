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
	"math/rand"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/testutils/distsqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

type mergeHashJoinerTestCase struct {
	spec          execinfrapb.MergeHashJoinerSpec
	outCols       []uint32
	leftTypes     []types.T
	leftInput     sqlbase.EncDatumRows
	rightTypes    []types.T
	rightInput    sqlbase.EncDatumRows
	expectedTypes []types.T
	expected      sqlbase.EncDatumRows
}

func TestMergeHashJoiner(t *testing.T) {
	defer leaktest.AfterTest(t)()

	v := [10]sqlbase.EncDatum{}
	for i := range v {
		v[i] = sqlbase.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(i)))
	}
	null := sqlbase.EncDatum{Datum: tree.DNull}

	testCases := []mergeHashJoinerTestCase{
		{
			spec: execinfrapb.MergeHashJoinerSpec{
				LeftEqColumns: []uint32{0, 1},
				LeftOrdering: execinfrapb.ConvertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				RightEqColumns: []uint32{0, 1},
				RightOrdering: execinfrapb.ConvertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				Type: sqlbase.InnerJoin,
				// @1 = @3 AND @2 = @4.
			},
			outCols:   []uint32{0, 3, 4},
			leftTypes: sqlbase.TwoIntCols,
			leftInput: sqlbase.EncDatumRows{
				{v[0], v[0]},
				{v[1], v[0]},
				{v[1], v[0]},
				{v[2], v[4]},
				{v[3], v[1]},
				{v[4], v[5]},
				{v[5], v[5]},
			},
			rightTypes: sqlbase.ThreeIntCols,
			rightInput: sqlbase.EncDatumRows{
				{v[1], v[0], v[4]},
				{v[3], v[4], v[1]},
				{v[4], v[5], v[5]},
			},
			expectedTypes: sqlbase.ThreeIntCols,
			expected: sqlbase.EncDatumRows{
				{v[1], v[0], v[4]},
				{v[1], v[0], v[4]},
				{v[4], v[5], v[5]},
			},
		},
		{
			spec: execinfrapb.MergeHashJoinerSpec{
				LeftEqColumns: []uint32{0, 1},
				LeftOrdering: execinfrapb.ConvertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				RightEqColumns: []uint32{0, 1},
				RightOrdering: execinfrapb.ConvertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				Type: sqlbase.InnerJoin,
				// @1 = @3 AND @2 = @4.
			},
			outCols:   []uint32{0, 1, 3},
			leftTypes: sqlbase.TwoIntCols,
			leftInput: sqlbase.EncDatumRows{
				{v[0], v[1]},
				{v[0], v[0]},
			},
			rightTypes: sqlbase.TwoIntCols,
			rightInput: sqlbase.EncDatumRows{
				{v[0], v[4]},
				{v[0], v[1]},
				{v[0], v[0]},
				{v[0], v[5]},
				{v[0], v[1]},
			},
			expectedTypes: sqlbase.ThreeIntCols,
			expected: sqlbase.EncDatumRows{
				{v[0], v[0], v[0]},
				{v[0], v[1], v[1]},
				{v[0], v[1], v[1]},
			},
		},
		{
			spec: execinfrapb.MergeHashJoinerSpec{
				LeftEqColumns: []uint32{0, 1},
				LeftOrdering: execinfrapb.ConvertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				RightEqColumns: []uint32{0, 1},
				RightOrdering: execinfrapb.ConvertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				Type:   sqlbase.InnerJoin,
				OnExpr: execinfrapb.Expression{Expr: "@4 >= 3"},
				// @1 = @3 AND @2 = @4 AND @4 >= 3.
			},
			outCols:   []uint32{0, 2, 3},
			leftTypes: sqlbase.TwoIntCols,
			leftInput: sqlbase.EncDatumRows{
				{v[0], v[4]},
				{v[0], v[1]},
				{v[1], v[3]},
				{v[1], v[4]},
			},
			rightTypes: sqlbase.TwoIntCols,
			rightInput: sqlbase.EncDatumRows{
				{v[0], v[4]},
				{v[0], v[1]},
				{v[0], v[0]},
				{v[0], v[5]},
				{v[0], v[4]},
				{v[1], v[4]},
				{v[1], v[1]},
				{v[1], v[0]},
				{v[1], v[5]},
				{v[1], v[4]},
			},
			expectedTypes: sqlbase.ThreeIntCols,
			expected: sqlbase.EncDatumRows{
				{v[0], v[0], v[4]},
				{v[0], v[0], v[4]},
				{v[1], v[1], v[4]},
				{v[1], v[1], v[4]},
			},
		},
		{
			// Ensure that NULL = NULL is not matched.
			spec: execinfrapb.MergeHashJoinerSpec{
				LeftEqColumns: []uint32{0, 1},
				LeftOrdering: execinfrapb.ConvertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				RightEqColumns: []uint32{0, 1},
				RightOrdering: execinfrapb.ConvertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				Type: sqlbase.InnerJoin,
				// @1 = @3 AND @2 = @4.
			},
			outCols:   []uint32{0, 2},
			leftTypes: sqlbase.TwoIntCols,
			leftInput: sqlbase.EncDatumRows{
				{null, v[0]},
				{v[0], v[0]},
			},
			rightTypes: sqlbase.TwoIntCols,
			rightInput: sqlbase.EncDatumRows{
				{null, v[0]},
				{v[1], v[0]},
			},
			expectedTypes: sqlbase.TwoIntCols,
			expected:      sqlbase.EncDatumRows{},
		},
	}

	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			ms := c.spec
			leftInput := distsqlutils.NewRowBuffer(c.leftTypes, c.leftInput, distsqlutils.RowBufferArgs{})
			rightInput := distsqlutils.NewRowBuffer(c.rightTypes, c.rightInput, distsqlutils.RowBufferArgs{})
			out := &distsqlutils.RowBuffer{}
			st := cluster.MakeTestingClusterSettings()
			evalCtx := tree.MakeTestingEvalContext(st)
			defer evalCtx.Stop(context.Background())
			flowCtx := execinfra.FlowCtx{
				Cfg:     &execinfra.ServerConfig{Settings: st},
				EvalCtx: &evalCtx,
			}

			post := execinfrapb.PostProcessSpec{Projection: true, OutputColumns: c.outCols}
			m, err := newMergeHashJoiner(&flowCtx, 0 /* processorID */, &ms, leftInput, rightInput, &post, out)
			if err != nil {
				t.Fatal(err)
			}

			m.Run(context.Background())

			if !out.ProducerClosed() {
				t.Fatalf("output RowReceiver not closed")
			}

			if err := checkExpectedRows(c.expectedTypes, c.expected, out); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestMergeHashJoinerAgainstHashJoiner tests that MergeHashJoiner processor
// returns the same output as HashJoiner. First, HashJoiner is run on the test
// input, and if any error is encountered, that test is skipped. If there was
// no error, then MergeHashJoiner is run (if any error occurs, it fails the
// test), and the outputs are then compared.
func TestMergeHashJoinerAgainstHashJoiner(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var da sqlbase.DatumAlloc

	const (
		nullProbability      = 0.2
		randTypesProbability = 0.5
	)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	tempEngine, err := engine.NewTempEngine(base.DefaultTestTempStorageConfig(st), base.DefaultTestStoreSpec)
	if err != nil {
		t.Fatal(err)
	}
	defer tempEngine.Close()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	diskMonitor := execinfra.MakeTestDiskMonitor(ctx, st)
	defer diskMonitor.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg: &execinfra.ServerConfig{
			Settings:    st,
			TempStorage: tempEngine,
			DiskMonitor: diskMonitor,
		},
	}

	supportedJoinTypes := []sqlbase.JoinType{
		sqlbase.JoinType_INNER,
	}

	seed := rand.Int()
	rng := rand.New(rand.NewSource(int64(seed)))
	nRuns := 10
	nRows := 10
	maxCols := 6
	maxNum := 5
	intTyps := make([]types.T, maxCols)
	for i := range intTyps {
		intTyps[i] = *types.Int
	}

	for run := 0; run < nRuns; run++ {
		for _, joinType := range supportedJoinTypes {
			for nCols := 2; nCols <= maxCols; nCols++ {
				for nEqCols := 2; nEqCols <= nCols; nEqCols++ {
					for nOrderingCols := 1; nOrderingCols < nEqCols; nOrderingCols++ {
						triedWithoutOnExpr, triedWithOnExpr := false, false
					SKIPTEST:
						for !triedWithoutOnExpr || !triedWithOnExpr {
							var (
								lRows, rRows                 sqlbase.EncDatumRows
								lEqCols, rEqCols             []uint32
								lOrderingCols, rOrderingCols []execinfrapb.Ordering_Column
								inputTypes                   []types.T
								usingRandomTypes             bool
							)
							if rng.Float64() < randTypesProbability {
								inputTypes = sqlbase.RandColumnTypes(rng, nCols)
								lRows = sqlbase.RandEncDatumRowsOfTypes(rng, nRows, inputTypes)
								rRows = sqlbase.RandEncDatumRowsOfTypes(rng, nRows, inputTypes)
								lEqCols = execinfra.GenerateEqualityColumns(rng, nCols, nEqCols)
								// Since random types might not be comparable, we use the same
								// equality columns for both inputs.
								rEqCols = lEqCols
								lOrderingCols = generateColOrderingFromEqCols(rng, lEqCols, nOrderingCols)
								// We use the same ordering columns in the same order because the
								// columns can be not comparable in different order.
								rOrderingCols = lOrderingCols
								usingRandomTypes = true
							} else {
								inputTypes = intTyps[:nCols]
								lRows = sqlbase.MakeRandIntRowsInRange(rng, nRows, nCols, maxNum, nullProbability)
								rRows = sqlbase.MakeRandIntRowsInRange(rng, nRows, nCols, maxNum, nullProbability)
								lEqCols = execinfra.GenerateEqualityColumns(rng, nCols, nEqCols)
								rEqCols = execinfra.GenerateEqualityColumns(rng, nCols, nEqCols)
								lOrderingCols = generateColOrderingFromEqCols(rng, lEqCols, nOrderingCols)
								// Right ordering columns are uniquely defined by left ordering
								// columns and the equality columns from both sides. Suppose we
								// have the following:
								//   lEqCols = [2 0 1] rEqCols = [1 0 2] lOrderingCols = [{1 ASC}]
								// In order to populate right ordering columns, for each left
								// ordering column, we first need to find a position within
								// lEqCols that lOrdCol appears. In the example above, it is
								// position 2:              [2 0 1]
								//   looking for 1               ^
								//   <position>              0 1 2
								// We then remap the position using rEqCols. In the example,
								// the desired right ordering column has ColIdx 2.
								rOrderingCols = make([]execinfrapb.Ordering_Column, 0, nOrderingCols)
								for _, lOrdCol := range lOrderingCols {
									for position, lEqCol := range lEqCols {
										if lEqCol == lOrdCol.ColIdx {
											rOrderingCols = append(rOrderingCols, execinfrapb.Ordering_Column{
												ColIdx:    rEqCols[position],
												Direction: lOrdCol.Direction,
											})
											break
										}
									}
								}
							}
							lMatchedCols := execinfrapb.ConvertToColumnOrdering(execinfrapb.Ordering{Columns: lOrderingCols})
							rMatchedCols := execinfrapb.ConvertToColumnOrdering(execinfrapb.Ordering{Columns: rOrderingCols})
							sort.Slice(lRows, func(i, j int) bool {
								cmp, err := lRows[i].Compare(inputTypes, &da, lMatchedCols, &evalCtx, lRows[j])
								if err != nil {
									t.Fatal(err)
								}
								return cmp < 0
							})
							sort.Slice(rRows, func(i, j int) bool {
								cmp, err := rRows[i].Compare(inputTypes, &da, rMatchedCols, &evalCtx, rRows[j])
								if err != nil {
									t.Fatal(err)
								}
								return cmp < 0
							})

							outputTypes := append(inputTypes, inputTypes...)
							outputColumns := make([]uint32, len(outputTypes))
							for i := range outputColumns {
								outputColumns[i] = uint32(i)
							}

							var onExpr execinfrapb.Expression
							if triedWithoutOnExpr {
								colTypes := append(inputTypes, inputTypes...)
								onExpr = execinfra.GenerateOnExpr(rng, nCols, nEqCols, colTypes, usingRandomTypes)
							}

							post := execinfrapb.PostProcessSpec{Projection: true, OutputColumns: outputColumns}

							hjSpec := &execinfrapb.HashJoinerSpec{
								LeftEqColumns:  lEqCols,
								RightEqColumns: rEqCols,
								OnExpr:         onExpr,
								Type:           joinType,
							}

							hjOut := &distsqlutils.RowBuffer{}
							hj, err := newHashJoiner(
								flowCtx,
								0,
								hjSpec,
								execinfra.NewRepeatableRowSource(inputTypes, lRows),
								execinfra.NewRepeatableRowSource(inputTypes, rRows),
								&post,
								hjOut,
							)
							if err != nil {
								continue SKIPTEST
							}
							hj.Start(ctx)
							hj.Run(ctx)
							var expected sqlbase.EncDatumRows
							for {
								row, meta := hjOut.Next()
								if meta != nil {
									if meta.Err == nil {
										t.Fatalf("unexpected meta: %v", meta)
									}
									continue SKIPTEST
								}
								if row == nil {
									break
								}
								expected = append(expected, row)
							}

							mhjSpec := &execinfrapb.MergeHashJoinerSpec{
								LeftOrdering:   execinfrapb.Ordering{Columns: lOrderingCols},
								RightOrdering:  execinfrapb.Ordering{Columns: rOrderingCols},
								LeftEqColumns:  lEqCols,
								RightEqColumns: rEqCols,
								OnExpr:         onExpr,
								Type:           joinType,
							}

							mhjOut := &distsqlutils.RowBuffer{}
							mhj, err := newMergeHashJoiner(
								flowCtx,
								0,
								mhjSpec,
								execinfra.NewRepeatableRowSource(inputTypes, lRows),
								execinfra.NewRepeatableRowSource(inputTypes, rRows),
								&post,
								mhjOut,
							)
							if err != nil {
								t.Fatal(err)
							}
							mhj.Start(ctx)
							mhj.Run(ctx)

							if err := checkExpectedRows(outputTypes, expected, mhjOut); err != nil {
								fmt.Printf("--- join type = %s onExpr = %q seed = %d run = %d ---\n",
									joinType.String(), onExpr.Expr, seed, run)
								fmt.Printf("--- lEqCols = %v rEqCols = %v ---\n", lEqCols, rEqCols)
								fmt.Printf("--- lOrderingCols = %v rOrderingCols = %v ---\n", lOrderingCols, rOrderingCols)
								fmt.Printf("--- inputTypes = %v ---\n", inputTypes)
								t.Fatal(err)
							}
							if onExpr.Expr == "" {
								triedWithoutOnExpr = true
							} else {
								triedWithOnExpr = true
							}
						}
					}
				}
			}
		}
	}
}

// generateColOrderingFromEqCols produces a random ordering of nOrderingCols
// columns chosen from eqCols, so nOrderingCols must be not greater than
// len(eqCols).
func generateColOrderingFromEqCols(
	rng *rand.Rand, eqCols []uint32, nOrderingCols int,
) []execinfrapb.Ordering_Column {
	if nOrderingCols > len(eqCols) {
		panic("nOrderingCols > len(eqCols) in generateColOrderingFromEqCols")
	}

	orderingCols := make([]execinfrapb.Ordering_Column, nOrderingCols)
	for i, col := range rng.Perm(len(eqCols))[:nOrderingCols] {
		orderingCols[i] = execinfrapb.Ordering_Column{
			ColIdx:    uint32(eqCols[col]),
			Direction: execinfrapb.Ordering_Column_Direction(rng.Intn(2)),
		}
	}
	return orderingCols
}

// Test that the joiner shuts down fine if the consumer is closed prematurely.
func TestMergeHashJoinerConsumerClosed(t *testing.T) {
	defer leaktest.AfterTest(t)()

	v := [10]sqlbase.EncDatum{}
	for i := range v {
		v[i] = sqlbase.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(i)))
	}

	spec := execinfrapb.MergeHashJoinerSpec{
		LeftEqColumns: []uint32{0, 1},
		LeftOrdering: execinfrapb.ConvertToSpecOrdering(
			sqlbase.ColumnOrdering{
				{ColIdx: 0, Direction: encoding.Ascending},
			}),
		RightEqColumns: []uint32{0, 1},
		RightOrdering: execinfrapb.ConvertToSpecOrdering(
			sqlbase.ColumnOrdering{
				{ColIdx: 0, Direction: encoding.Ascending},
			}),
		Type: sqlbase.InnerJoin,
	}
	outCols := []uint32{0}
	leftTypes := sqlbase.TwoIntCols
	rightTypes := sqlbase.TwoIntCols

	testCases := []struct {
		typ       sqlbase.JoinType
		leftRows  sqlbase.EncDatumRows
		rightRows sqlbase.EncDatumRows
	}{
		{
			typ: sqlbase.InnerJoin,
			leftRows: sqlbase.EncDatumRows{
				{v[0], v[0]},
			},
			rightRows: sqlbase.EncDatumRows{
				{v[0], v[0]},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.typ.String(), func(t *testing.T) {
			leftInput := distsqlutils.NewRowBuffer(leftTypes, tc.leftRows, distsqlutils.RowBufferArgs{})
			rightInput := distsqlutils.NewRowBuffer(rightTypes, tc.rightRows, distsqlutils.RowBufferArgs{})

			// Create a consumer and close it immediately. The mergeHashJoiner should
			// find out about this closer the first time it attempts to push a row.
			out := &distsqlutils.RowBuffer{}
			out.ConsumerDone()

			st := cluster.MakeTestingClusterSettings()
			evalCtx := tree.MakeTestingEvalContext(st)
			defer evalCtx.Stop(context.Background())
			flowCtx := execinfra.FlowCtx{
				Cfg:     &execinfra.ServerConfig{Settings: st},
				EvalCtx: &evalCtx,
			}
			post := execinfrapb.PostProcessSpec{Projection: true, OutputColumns: outCols}
			m, err := newMergeHashJoiner(&flowCtx, 0, &spec, leftInput, rightInput, &post, out)
			if err != nil {
				t.Fatal(err)
			}

			m.Run(context.Background())

			if !out.ProducerClosed() {
				t.Fatalf("output RowReceiver not closed")
			}
		})
	}
}

type joinStrategy int

const (
	hash joinStrategy = iota
	mergeHash
	sortMerge
)

func (s joinStrategy) String() string {
	return map[joinStrategy]string{
		hash:      "Hash",
		mergeHash: "MergeHash",
		sortMerge: "SortMerge",
	}[s]
}

// BenchmarkMergeHashJoiner benchmarks MergeHashJoiner against HashJoiner and
// SortChunks + MergeJoiner. It uses a dataset with two columns that is already
// ordered on the first column (to be precise, it is ordered on both columns,
// but we're not taking advantage of the ordering on the second column for the
// purpose of this benchmark).
func BenchmarkMergeHashJoiner(b *testing.B) {
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	diskMonitor := execinfra.MakeTestDiskMonitor(ctx, st)
	defer diskMonitor.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		Cfg: &execinfra.ServerConfig{
			Settings:    st,
			DiskMonitor: diskMonitor,
		},
		EvalCtx: &evalCtx,
	}

	spec := &execinfrapb.MergeHashJoinerSpec{
		LeftEqColumns: []uint32{0, 1},
		LeftOrdering: execinfrapb.ConvertToSpecOrdering(
			sqlbase.ColumnOrdering{
				{ColIdx: 0, Direction: encoding.Ascending},
			}),
		RightEqColumns: []uint32{0, 1},
		RightOrdering: execinfrapb.ConvertToSpecOrdering(
			sqlbase.ColumnOrdering{
				{ColIdx: 0, Direction: encoding.Ascending},
			}),
		Type: sqlbase.InnerJoin,
		// @1 = @3 AND @2 = @4
	}
	post := &execinfrapb.PostProcessSpec{}
	disposer := &execinfra.RowDisposer{}
	sorterSpec := &execinfrapb.SorterSpec{
		OutputOrdering: execinfrapb.ConvertToSpecOrdering(
			sqlbase.ColumnOrdering{
				{ColIdx: 0, Direction: encoding.Ascending},
				{ColIdx: 1, Direction: encoding.Ascending},
			}),
		OrderingMatchLen: 1,
	}

	rng, _ := randutil.NewPseudoRand()
	typs := sqlbase.TwoIntCols
	numCols := 2

	for _, inputSize := range []int{1 << 2, 1 << 4, 1 << 8, 1 << 12, 1 << 16} {
		for _, groupSize := range []int{inputSize >> 15, inputSize >> 11, inputSize >> 7, inputSize >> 3, inputSize >> 1} {
			if groupSize == 0 {
				continue
			}
			for _, strategy := range []joinStrategy{hash, mergeHash, sortMerge} {
				b.Run(fmt.Sprintf("InputSize=%d/GroupSize=%d/%s", inputSize, groupSize, strategy), func(b *testing.B) {
					generateInputRows := func() sqlbase.EncDatumRows {
						rows := make(sqlbase.EncDatumRows, inputSize)
						for idx := range rows {
							row := make(sqlbase.EncDatumRow, numCols)
							row[0] = sqlbase.IntEncDatum(idx / groupSize)
							row[1] = sqlbase.IntEncDatum(rng.Int())
							rows[idx] = row
						}
						return rows
					}
					leftInput := execinfra.NewRepeatableRowSource(typs, generateInputRows())
					rightInput := execinfra.NewRepeatableRowSource(typs, generateInputRows())
					leftBufferRows := make(sqlbase.EncDatumRows, inputSize)
					rightBufferRows := make(sqlbase.EncDatumRows, inputSize)
					b.SetBytes(int64(8 * inputSize * numCols * 2))
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						var m execinfra.Processor
						var err error
						switch strategy {
						case hash:
							hjSpec := &execinfrapb.HashJoinerSpec{
								LeftEqColumns:  spec.LeftEqColumns,
								RightEqColumns: spec.RightEqColumns,
								Type:           spec.Type,
							}
							m, err = newHashJoiner(flowCtx, 0 /* processorID */, hjSpec, leftInput, rightInput, post, disposer)
						case mergeHash:
							m, err = newMergeHashJoiner(flowCtx, 0 /* processorID */, spec, leftInput, rightInput, post, disposer)
						case sortMerge:
							leftBufferRows = leftBufferRows[:0]
							rightBufferRows = rightBufferRows[:0]
							leftOut := distsqlutils.NewRowBuffer(typs, leftBufferRows, distsqlutils.RowBufferArgs{})
							rightOut := distsqlutils.NewRowBuffer(typs, rightBufferRows, distsqlutils.RowBufferArgs{})
							sorter, err := newSortChunksProcessor(flowCtx, 0, sorterSpec, leftInput, post, leftOut)
							if err != nil {
								b.Fatal(err)
							}
							sorter.Run(ctx)
							sorter, err = newSortChunksProcessor(flowCtx, 0, sorterSpec, rightInput, post, rightOut)
							if err != nil {
								b.Fatal(err)
							}
							sorter.Run(ctx)
							mjSpec := &execinfrapb.MergeJoinerSpec{
								LeftOrdering:  sorterSpec.OutputOrdering,
								RightOrdering: sorterSpec.OutputOrdering,
								Type:          spec.Type,
							}
							m, err = newMergeJoiner(flowCtx, 0, mjSpec, leftOut, rightOut, post, disposer)
						}
						if err != nil {
							b.Fatal(err)
						}
						m.Run(ctx)
						leftInput.Reset()
						rightInput.Reset()
					}
				})
			}
		}
	}
}
