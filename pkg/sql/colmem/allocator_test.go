// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colmem_test

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldataext"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func init() {
	randutil.SeedForTests()
}

func TestMaybeAppendColumn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	testMemMonitor := execinfra.NewTestMemMonitor(ctx, st)
	defer testMemMonitor.Stop(ctx)
	memAcc := testMemMonitor.MakeBoundAccount()
	defer memAcc.Close(ctx)
	evalCtx := tree.MakeTestingEvalContext(st)
	testColumnFactory := coldataext.NewExtendedColumnFactory(&evalCtx)
	testAllocator := colmem.NewAllocator(ctx, &memAcc, testColumnFactory)

	t.Run("VectorAlreadyPresent", func(t *testing.T) {
		b := testAllocator.NewMemBatchWithMaxCapacity([]*types.T{types.Int})
		b.SetLength(coldata.BatchSize())
		colIdx := 0

		// We expect an error to occur because of a type mismatch.
		err := colexecerror.CatchVectorizedRuntimeError(func() {
			testAllocator.MaybeAppendColumn(b, types.Float, colIdx)
		})
		require.NotNil(t, err)

		// We expect that the old vector is reallocated because the present one
		// is made to be of insufficient capacity.
		b.ReplaceCol(testAllocator.NewMemColumn(types.Int, 1 /* capacity */), colIdx)
		testAllocator.MaybeAppendColumn(b, types.Int, colIdx)
		require.Equal(t, coldata.BatchSize(), b.ColVec(colIdx).Capacity())

		// We expect that Bytes vector is reset when it is being reused (if it
		// isn't, a panic will occur when we try to set at the same positions).
		bytesColIdx := 1
		testAllocator.MaybeAppendColumn(b, types.Bytes, bytesColIdx)
		b.ColVec(bytesColIdx).Bytes().Set(0, []byte{0})
		b.ColVec(bytesColIdx).Bytes().Set(1, []byte{1})
		testAllocator.MaybeAppendColumn(b, types.Bytes, bytesColIdx)
		b.ColVec(bytesColIdx).Bytes().Set(0, []byte{0})
		b.ColVec(bytesColIdx).Bytes().Set(1, []byte{1})
	})

	t.Run("WindowedBatchZeroCapacity", func(t *testing.T) {
		b := testAllocator.NewMemBatchWithFixedCapacity([]*types.T{}, 0 /* capacity */)
		b.SetLength(coldata.BatchSize())
		colIdx := 0

		// We expect that although the batch is of zero capacity, the newly
		// appended vectors are allocated of coldata.BatchSize() capacity.
		testAllocator.MaybeAppendColumn(b, types.Int, colIdx)
		require.Equal(t, 1, b.Width())
		require.Equal(t, coldata.BatchSize(), b.ColVec(colIdx).Length())
		_ = b.ColVec(colIdx).Int64()[0]
	})
}

func TestResetMaybeReallocate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	testMemMonitor := execinfra.NewTestMemMonitor(ctx, st)
	defer testMemMonitor.Stop(ctx)
	memAcc := testMemMonitor.MakeBoundAccount()
	defer memAcc.Close(ctx)
	evalCtx := tree.MakeTestingEvalContext(st)
	testColumnFactory := coldataext.NewExtendedColumnFactory(&evalCtx)
	testAllocator := colmem.NewAllocator(ctx, &memAcc, testColumnFactory)

	t.Run("ResettingBehavior", func(t *testing.T) {
		if coldata.BatchSize() == 1 {
			skip.IgnoreLint(t, "the test assumes coldata.BatchSize() is at least 2")
		}

		var b coldata.Batch
		typs := []*types.T{types.Bytes}

		// Allocate a new batch and modify it.
		b, _ = testAllocator.ResetMaybeReallocate(typs, b, coldata.BatchSize(), math.MaxInt64)
		b.SetSelection(true)
		b.Selection()[0] = 1
		b.ColVec(0).Bytes().Set(1, []byte("foo"))

		oldBatch := b
		b, _ = testAllocator.ResetMaybeReallocate(typs, b, coldata.BatchSize(), math.MaxInt64)
		// We should have used the same batch, and now it should be in a "reset"
		// state.
		require.Equal(t, oldBatch, b)
		require.Nil(t, b.Selection())
		// We should be able to set in the Bytes vector using an arbitrary
		// position since the vector should have been reset.
		require.NotPanics(t, func() { b.ColVec(0).Bytes().Set(0, []byte("bar")) })
	})

	t.Run("LimitingByMemSize", func(t *testing.T) {
		if coldata.BatchSize() == 1 {
			skip.IgnoreLint(t, "the test assumes coldata.BatchSize() is at least 2")
		}

		var b coldata.Batch
		typs := []*types.T{types.Int}
		const minDesiredCapacity = 2
		const smallMemSize = 0
		const largeMemSize = math.MaxInt64

		// Allocate a batch with smaller capacity.
		smallBatch := testAllocator.NewMemBatchWithFixedCapacity(typs, minDesiredCapacity/2)

		// Allocate a new batch attempting to use the batch with too small of a
		// capacity - new batch should **not** be allocated because the memory
		// limit is already exceeded.
		b, _ = testAllocator.ResetMaybeReallocate(typs, smallBatch, minDesiredCapacity, smallMemSize)
		require.Equal(t, smallBatch, b)
		require.Equal(t, minDesiredCapacity/2, b.Capacity())

		oldBatch := b

		// Reset the batch and confirm that a new batch is allocated because we
		// have given larger memory limit.
		b, _ = testAllocator.ResetMaybeReallocate(typs, b, minDesiredCapacity, largeMemSize)
		require.NotEqual(t, oldBatch, b)
		require.Equal(t, minDesiredCapacity, b.Capacity())

		if coldata.BatchSize() >= minDesiredCapacity*2 {
			// Now reset the batch with large memory limit - we should get a new
			// batch with the double capacity.
			//
			// ResetMaybeReallocate truncates the capacity at
			// coldata.BatchSize(), so we run this part of the test only when
			// doubled capacity will not be truncated.
			b, _ = testAllocator.ResetMaybeReallocate(typs, b, minDesiredCapacity, largeMemSize)
			require.NotEqual(t, oldBatch, b)
			require.Equal(t, 2*minDesiredCapacity, b.Capacity())
		}
	})
}

func TestPerformAppend(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Include decimal and geometry columns because PerformAppend differs from
	// PerformOperation for decimal and datum types.
	var typs = []*types.T{types.Int, types.Decimal, types.Geometry}
	const intIdx, decimalIdx, datumIdx = 0, 1, 2
	const maxBatchSize = 100
	const numRows = 1000
	const nullOk = false
	const resetChance = 0.5

	ctx := context.Background()
	rng, _ := randutil.NewTestRand()
	st := cluster.MakeTestingClusterSettings()
	testMemMonitor := execinfra.NewTestMemMonitor(ctx, st)
	defer testMemMonitor.Stop(ctx)
	memAcc := testMemMonitor.MakeBoundAccount()
	defer memAcc.Close(ctx)
	evalCtx := tree.MakeTestingEvalContext(st)
	testColumnFactory := coldataext.NewExtendedColumnFactory(&evalCtx)
	testAllocator := colmem.NewAllocator(ctx, &memAcc, testColumnFactory)

	batch1 := colexecutils.NewAppendOnlyBufferedBatch(testAllocator, typs, nil /* colsToStore */)
	batch2 := colexecutils.NewAppendOnlyBufferedBatch(testAllocator, typs, nil /* colsToStore */)

	getRandomInputBatch := func(count int) coldata.Batch {
		b := testAllocator.NewMemBatchWithFixedCapacity(typs, count)
		for i := 0; i < count; i++ {
			datum := randgen.RandDatum(rng, typs[intIdx], nullOk)
			b.ColVec(intIdx).Int64()[i] = int64(*(datum.(*tree.DInt)))
			datum = randgen.RandDatum(rng, typs[decimalIdx], nullOk)
			b.ColVec(decimalIdx).Decimal()[i] = datum.(*tree.DDecimal).Decimal
			datum = randgen.RandDatum(rng, typs[datumIdx], nullOk)
			b.ColVec(datumIdx).Datum().Set(i, datum)
		}
		b.SetLength(count)
		return b
	}

	rowsLeft := numRows
	for {
		if rowsLeft <= 0 {
			break
		}
		batchSize := rng.Intn(maxBatchSize-1) + 1 // Ensure a nonzero batch size.
		if batchSize > rowsLeft {
			batchSize = rowsLeft
		}
		rowsLeft -= batchSize
		inputBatch := getRandomInputBatch(batchSize)

		beforePerformOperation := testAllocator.Used()
		testAllocator.PerformOperation(batch1.ColVecs(), func() {
			for colIdx, destVec := range batch1.ColVecs() {
				destVec.Append(coldata.SliceArgs{
					Src:       inputBatch.ColVec(colIdx),
					DestIdx:   batch1.Length(),
					SrcEndIdx: inputBatch.Length(),
				})
			}
			batch1.SetLength(batch1.Length() + inputBatch.Length())
		})
		afterPerformOperation := testAllocator.Used()

		beforePerformAppend := afterPerformOperation
		batch2.AppendTuples(inputBatch, 0 /* startIdx */, inputBatch.Length())
		afterPerformAppend := testAllocator.Used()

		performOperationMem := afterPerformOperation - beforePerformOperation
		performAppendMem := afterPerformAppend - beforePerformAppend
		require.Equal(t, performOperationMem, performAppendMem)

		if rng.Float64() < resetChance {
			// Reset the test batches in order to simulate reuse.
			batch1.ResetInternalBatch()
			batch2.ResetInternalBatch()
		}
	}
}

func TestSetAccountingHelper(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	rng, _ := randutil.NewTestRand()
	st := cluster.MakeTestingClusterSettings()
	testMemMonitor := execinfra.NewTestMemMonitor(ctx, st)
	defer testMemMonitor.Stop(ctx)
	memAcc := testMemMonitor.MakeBoundAccount()
	defer memAcc.Close(ctx)
	evalCtx := tree.MakeTestingEvalContext(st)
	testColumnFactory := coldataext.NewExtendedColumnFactory(&evalCtx)
	testAllocator := colmem.NewAllocator(ctx, &memAcc, testColumnFactory)

	numCols := rng.Intn(10) + 1
	typs := make([]*types.T, numCols)
	for i := range typs {
		typs[i] = randgen.RandType(rng)
	}

	var helper colmem.SetAccountingHelper
	helper.Init(testAllocator, typs)

	numIterations := rng.Intn(10) + 1
	numRows := rng.Intn(coldata.BatchSize()) + 1

	const smallMemSize = 0
	const largeMemSize = math.MaxInt64

	var batch coldata.Batch
	for iteration := 0; iteration < numIterations; iteration++ {
		// We use zero memory limit so that the same batch is used between most
		// iterations.
		maxBatchMemSize := int64(smallMemSize)
		if rng.Float64() < 0.25 {
			// But occasionally we'll use the large mem limit - as a result, a
			// new batch with larger capacity might be allocated.
			maxBatchMemSize = largeMemSize
		}
		batch, _ = helper.ResetMaybeReallocate(typs, batch, numRows, maxBatchMemSize)

		for rowIdx := 0; rowIdx < batch.Capacity(); rowIdx++ {
			for vecIdx, typ := range typs {
				switch typ.Family() {
				case types.BytesFamily:
					// For Bytes, insert pretty large values.
					v := make([]byte, rng.Intn(8*coldata.BytesInitialAllocationFactor))
					_, _ = rng.Read(v)
					batch.ColVec(vecIdx).Bytes().Set(rowIdx, v)
				default:
					datum := randgen.RandDatum(rng, typ, false /* nullOk */)
					converter := colconv.GetDatumToPhysicalFn(typ)
					coldata.SetValueAt(batch.ColVec(vecIdx), converter(datum), rowIdx)
				}
			}
			helper.AccountForSet(rowIdx)
		}

		// At this point, we have set all rows in the batch and performed the
		// memory accounting for each set. We no longer have any uninitialized
		// elements, so the memory footprint of the batch must be exactly as
		// what we have accounted for.
		expected := colmem.GetBatchMemSize(batch)
		actual := testAllocator.Used()
		if expected != actual {
			fmt.Printf("iteration = %d numRows = %d\n", iteration, numRows)
			for i := range typs {
				fmt.Printf("%s ", typs[i].SQLString())
			}
			fmt.Println()
			t.Fatal(errors.Newf("expected %d, actual %d", expected, actual))
		}
	}
}

// TestEstimateBatchSizeBytes verifies that EstimateBatchSizeBytes returns such
// an estimate that it equals the actual footprint of the newly-created batch
// with no values set.
func TestEstimateBatchSizeBytes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	rng, _ := randutil.NewTestRand()
	st := cluster.MakeTestingClusterSettings()
	testMemMonitor := execinfra.NewTestMemMonitor(ctx, st)
	defer testMemMonitor.Stop(ctx)
	memAcc := testMemMonitor.MakeBoundAccount()
	defer memAcc.Close(ctx)
	evalCtx := tree.MakeTestingEvalContext(st)
	testColumnFactory := coldataext.NewExtendedColumnFactory(&evalCtx)
	testAllocator := colmem.NewAllocator(ctx, &memAcc, testColumnFactory)

	numCols := rng.Intn(10) + 1
	typs := make([]*types.T, numCols)
	for i := range typs {
		typs[i] = randgen.RandType(rng)
	}
	const numRuns = 10
	for run := 0; run < numRuns; run++ {
		memAcc.Clear(ctx)
		numRows := rng.Intn(coldata.BatchSize()) + 1
		batch := testAllocator.NewMemBatchWithFixedCapacity(typs, numRows)
		expected := memAcc.Used()
		actual := colmem.GetBatchMemSize(batch)
		if expected != actual {
			fmt.Printf("run = %d numRows = %d\n", run, numRows)
			for i := range typs {
				fmt.Printf("%s ", typs[i].SQLString())
			}
			fmt.Println()
			t.Fatal(errors.Newf("expected %d, actual %d", expected, actual))
		}
	}
}
