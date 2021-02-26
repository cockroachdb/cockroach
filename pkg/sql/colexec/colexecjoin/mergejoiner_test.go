// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexecjoin

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldatatestutils"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexectestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/colcontainerutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

// Merge joiner will be using two spillingQueues, and each of them will use
// 2 file descriptors.
const mjFDLimit = 4

// TestMergeJoinCrossProduct verifies that the merge joiner produces the same
// output as the hash joiner. The test aims at stressing randomly the building
// of cross product (from the buffered groups) in the merge joiner and does it
// by creating input sources such that they contain very big groups (each group
// is about coldata.BatchSize() in size) which will force the merge joiner to
// mostly build from the buffered groups. Join of such input sources results in
// an output quadratic in size, so the test is skipped unless coldata.BatchSize
// is set to relatively small number, but it is ok since we randomize this
// value.
func TestMergeJoinCrossProduct(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	if coldata.BatchSize() > 200 {
		skip.IgnoreLintf(t, "this test is too slow with relatively big batch size")
	}
	ctx := context.Background()
	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(ctx)
	nTuples := 2*coldata.BatchSize() + 1
	queueCfg, cleanup := colcontainerutils.NewTestingDiskQueueCfg(t, true /* inMem */)
	defer cleanup()
	rng, _ := randutil.NewPseudoRand()
	typs := []*types.T{types.Int, types.Bytes, types.Decimal}
	colsLeft := make([]coldata.Vec, len(typs))
	colsRight := make([]coldata.Vec, len(typs))
	for i, typ := range typs {
		colsLeft[i] = testAllocator.NewMemColumn(typ, nTuples)
		colsRight[i] = testAllocator.NewMemColumn(typ, nTuples)
	}
	groupsLeft := colsLeft[0].Int64()
	groupsRight := colsRight[0].Int64()
	leftGroupIdx, rightGroupIdx := 0, 0
	for i := range groupsLeft {
		if rng.Float64() < 1.0/float64(coldata.BatchSize()) {
			leftGroupIdx++
		}
		if rng.Float64() < 1.0/float64(coldata.BatchSize()) {
			rightGroupIdx++
		}
		groupsLeft[i] = int64(leftGroupIdx)
		groupsRight[i] = int64(rightGroupIdx)
	}
	for i := range typs[1:] {
		for _, vecs := range [][]coldata.Vec{colsLeft, colsRight} {
			coldatatestutils.RandomVec(coldatatestutils.RandomVecArgs{
				Rand:            rng,
				Vec:             vecs[i+1],
				N:               nTuples,
				NullProbability: 0.1,
			})
		}
	}
	leftMJSource := colexectestutils.NewChunkingBatchSource(testAllocator, typs, colsLeft, nTuples)
	rightMJSource := colexectestutils.NewChunkingBatchSource(testAllocator, typs, colsRight, nTuples)
	leftHJSource := colexectestutils.NewChunkingBatchSource(testAllocator, typs, colsLeft, nTuples)
	rightHJSource := colexectestutils.NewChunkingBatchSource(testAllocator, typs, colsRight, nTuples)
	mj, err := NewMergeJoinOp(
		testAllocator, execinfra.DefaultMemoryLimit, queueCfg,
		colexecop.NewTestingSemaphore(mjFDLimit), descpb.InnerJoin,
		leftMJSource, rightMJSource, typs, typs,
		[]execinfrapb.Ordering_Column{{ColIdx: 0, Direction: execinfrapb.Ordering_Column_ASC}},
		[]execinfrapb.Ordering_Column{{ColIdx: 0, Direction: execinfrapb.Ordering_Column_ASC}},
		testDiskAcc,
	)
	if err != nil {
		t.Fatal("error in merge join op constructor", err)
	}
	mj.Init(ctx)
	hj := NewHashJoiner(
		testAllocator, testAllocator, HashJoinerSpec{
			JoinType: descpb.InnerJoin,
			Left: hashJoinerSourceSpec{
				EqCols: []uint32{0}, SourceTypes: typs,
			},
			Right: hashJoinerSourceSpec{
				EqCols: []uint32{0}, SourceTypes: typs,
			},
		}, leftHJSource, rightHJSource, HashJoinerInitialNumBuckets, execinfra.DefaultMemoryLimit,
	)
	hj.Init(ctx)

	var mjOutputTuples, hjOutputTuples colexectestutils.Tuples
	for b := mj.Next(); b.Length() != 0; b = mj.Next() {
		for i := 0; i < b.Length(); i++ {
			mjOutputTuples = append(mjOutputTuples, colexectestutils.GetTupleFromBatch(b, i))
		}
	}
	for b := hj.Next(); b.Length() != 0; b = hj.Next() {
		for i := 0; i < b.Length(); i++ {
			hjOutputTuples = append(hjOutputTuples, colexectestutils.GetTupleFromBatch(b, i))
		}
	}
	err = colexectestutils.AssertTuplesSetsEqual(hjOutputTuples, mjOutputTuples, evalCtx)
	// Note that the error message can be extremely verbose (it
	// might contain all output tuples), so we manually check that
	// comparing err to nil returns true (if we were to use
	// require.NoError, then the error message would be output).
	require.True(t, err == nil)
}

func newBatchOfIntRows(nCols int, batch coldata.Batch, length int) coldata.Batch {
	for colIdx := 0; colIdx < nCols; colIdx++ {
		col := batch.ColVec(colIdx).Int64()
		for i := 0; i < length; i++ {
			col[i] = int64(i)
		}
	}

	batch.SetLength(length)

	for colIdx := 0; colIdx < nCols; colIdx++ {
		vec := batch.ColVec(colIdx)
		vec.Nulls().UnsetNulls()
	}
	return batch
}

func newBatchOfRepeatedIntRows(
	nCols int, batch coldata.Batch, length int, numRepeats int,
) coldata.Batch {
	for colIdx := 0; colIdx < nCols; colIdx++ {
		col := batch.ColVec(colIdx).Int64()
		for i := 0; i < length; i++ {
			col[i] = int64((i + 1) / numRepeats)
		}
	}

	batch.SetLength(length)

	for colIdx := 0; colIdx < nCols; colIdx++ {
		vec := batch.ColVec(colIdx)
		vec.Nulls().UnsetNulls()
	}
	return batch
}

func BenchmarkMergeJoiner(b *testing.B) {
	defer log.Scope(b).Close(b)
	ctx := context.Background()
	const nCols = 1
	sourceTypes := []*types.T{types.Int}

	queueCfg, cleanup := colcontainerutils.NewTestingDiskQueueCfg(b, false /* inMem */)
	defer cleanup()
	benchMemAccount := testMemMonitor.MakeBoundAccount()
	defer benchMemAccount.Close(ctx)

	getNewMergeJoiner := func(leftSource, rightSource colexecop.Operator) colexecop.Operator {
		benchMemAccount.Clear(ctx)
		base, err := newMergeJoinBase(
			colmem.NewAllocator(ctx, &benchMemAccount, testColumnFactory), execinfra.DefaultMemoryLimit, queueCfg, colexecop.NewTestingSemaphore(mjFDLimit),
			descpb.InnerJoin, leftSource, rightSource, sourceTypes, sourceTypes,
			[]execinfrapb.Ordering_Column{{ColIdx: 0, Direction: execinfrapb.Ordering_Column_ASC}},
			[]execinfrapb.Ordering_Column{{ColIdx: 0, Direction: execinfrapb.Ordering_Column_ASC}},
			testDiskAcc,
		)
		require.NoError(b, err)
		return &mergeJoinInnerOp{mergeJoinBase: base}
	}

	regularBatchCreator := func(batchLength, _ int) coldata.Batch {
		return newBatchOfIntRows(nCols, testAllocator.NewMemBatchWithMaxCapacity(sourceTypes), batchLength)
	}
	repeatedBatchCreator := func(batchLength, numRepeats int) coldata.Batch {
		return newBatchOfRepeatedIntRows(nCols, testAllocator.NewMemBatchWithMaxCapacity(sourceTypes), batchLength, numRepeats)
	}

	for _, c := range []struct {
		namePrefix        string
		numRepeatsGetter  func(nRows int) int
		leftBatchCreator  func(batchLength, numRepeats int) coldata.Batch
		rightBatchCreator func(batchLength, numRepeats int) coldata.Batch
	}{
		// 1:1 join.
		{
			namePrefix: "",
			numRepeatsGetter: func(nRows int) int {
				// This value will be ignored.
				return 0
			},
			leftBatchCreator:  regularBatchCreator,
			rightBatchCreator: regularBatchCreator,
		},
		// Groups on the right side.
		{
			namePrefix: "oneSideRepeat-",
			numRepeatsGetter: func(nRows int) int {
				return nRows
			},
			leftBatchCreator:  regularBatchCreator,
			rightBatchCreator: repeatedBatchCreator,
		},
		// Groups on both sides.
		{
			namePrefix: "bothSidesRepeat-",
			numRepeatsGetter: func(nRows int) int {
				return int(math.Sqrt(float64(nRows)))
			},
			leftBatchCreator:  repeatedBatchCreator,
			rightBatchCreator: repeatedBatchCreator,
		},
	} {
		rowsOptions := []int{32, 512, 4 * coldata.BatchSize(), 32 * coldata.BatchSize()}
		if testing.Short() {
			rowsOptions = []int{512, 4 * coldata.BatchSize()}
		}
		for _, nRows := range rowsOptions {
			b.Run(fmt.Sprintf("%srows=%d", c.namePrefix, nRows), func(b *testing.B) {
				batchLength := nRows
				nBatches := 1
				if nRows >= coldata.BatchSize() {
					batchLength = coldata.BatchSize()
					nBatches = nRows / coldata.BatchSize()
				}
				numRepeats := c.numRepeatsGetter(nRows)
				leftBatch := c.leftBatchCreator(batchLength, numRepeats)
				rightBatch := c.rightBatchCreator(batchLength, numRepeats)
				// 8 (bytes / int64) * nRows * nCols (number of columns / row) * 2 (number of sources).
				b.SetBytes(int64(8 * nRows * nCols * 2))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					leftSource := colexectestutils.NewFiniteChunksSource(testAllocator, leftBatch, sourceTypes, nBatches, 1)
					rightSource := colexectestutils.NewFiniteChunksSource(testAllocator, rightBatch, sourceTypes, nBatches, 1)
					s := getNewMergeJoiner(leftSource, rightSource)
					s.Init(ctx)

					b.StartTimer()
					for b := s.Next(); b.Length() != 0; b = s.Next() {
					}
					b.StopTimer()
				}
			})
		}
	}
}
