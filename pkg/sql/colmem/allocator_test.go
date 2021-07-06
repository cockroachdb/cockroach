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
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldataext"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
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
	"github.com/stretchr/testify/require"
)

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
		const minCapacity = 2
		const maxBatchMemSize = 0

		// Allocate a batch with smaller capacity.
		smallBatch := testAllocator.NewMemBatchWithFixedCapacity(typs, minCapacity/2)

		// Allocate a new batch attempting to use the batch with too small of a
		// capacity - new batch should be allocated.
		b, _ = testAllocator.ResetMaybeReallocate(typs, smallBatch, minCapacity, maxBatchMemSize)
		require.NotEqual(t, smallBatch, b)
		require.Equal(t, minCapacity, b.Capacity())

		oldBatch := b

		// Reset the batch and confirm that a new batch is not allocated because
		// the old batch has enough capacity and it has reached the memory
		// limit.
		b, _ = testAllocator.ResetMaybeReallocate(typs, b, minCapacity, maxBatchMemSize)
		require.Equal(t, oldBatch, b)
		require.Equal(t, minCapacity, b.Capacity())

		if coldata.BatchSize() >= minCapacity*2 {
			// Now reset the batch with large memory limit - we should get a new
			// batch with the double capacity.
			//
			// ResetMaybeReallocate truncates the capacity at
			// coldata.BatchSize(), so we run this part of the test only when
			// doubled capacity will not be truncated.
			b, _ = testAllocator.ResetMaybeReallocate(typs, b, minCapacity, math.MaxInt64)
			require.NotEqual(t, oldBatch, b)
			require.Equal(t, 2*minCapacity, b.Capacity())
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
	rng, _ := randutil.NewPseudoRand()
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
			batch1.AppendTuples(inputBatch, 0 /* startIdx */, inputBatch.Length())
		})
		afterPerformOperation := testAllocator.Used()

		beforePerformAppend := afterPerformOperation
		testAllocator.PerformAppend(batch2, func() {
			batch2.AppendTuples(inputBatch, 0 /* startIdx */, inputBatch.Length())
		})
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
